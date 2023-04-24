package gocb

import (
	"errors"
	"fmt"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
)

var _ kvProvider = &kvProviderProtoStellar{}

// // wraps kv and makes it compliant for gocb
type kvProviderProtoStellar struct {
	client kv_v1.KvServiceClient
}

func (p *kvProviderProtoStellar) LookupIn(opm *kvOpManager, ops []LookupInSpec, flag SubdocDocFlag) (*LookupInResult, error) {
	lookUpInPSSpecs := make([]*kv_v1.LookupInRequest_Spec, len(ops))

	for i, op := range ops {
		// this is uint8 to int32, need to check overflows etc
		translate := map[memd.SubDocOpType]kv_v1.LookupInRequest_Spec_Operation{
			memd.SubDocOpGet:     kv_v1.LookupInRequest_Spec_OPERATION_GET,
			memd.SubDocOpExists:  kv_v1.LookupInRequest_Spec_OPERATION_EXISTS,
			memd.SubDocOpCounter: kv_v1.LookupInRequest_Spec_OPERATION_COUNT,
		}

		if op.op == memd.SubDocOpGet && op.path == "" {
			if op.isXattr {
				return nil, errors.New("invalid xattr fetch with no path")
			}

			lookUpInPSSpecs[i] = &kv_v1.LookupInRequest_Spec{
				Operation: translate[op.op],
				Path:      op.path,
			}

			continue
		}

		newOp, ok := translate[op.op]
		if !ok {
			continue // TODO:// raise error unsupported op?
		}

		specFlag := &kv_v1.LookupInRequest_Spec_Flags{
			Xattr: &op.isXattr,
		}

		lookUpInPSSpecs[i] = &kv_v1.LookupInRequest_Spec{
			Operation: newOp,
			Path:      op.path,
			Flags:     specFlag,
		}
	}

	requestFlags := &kv_v1.LookupInRequest_Flags{}
	if flag == SubdocDocFlagAccessDeleted {
		truth := true
		requestFlags.AccessDeleted = &truth
	}

	req := &kv_v1.LookupInRequest{
		BucketName:     opm.BucketName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),
		Key:            string(opm.DocumentID()),
		Specs:          lookUpInPSSpecs,
		Flags:          requestFlags,
	}

	p.client.LookupIn(opm.ctx, req)
	return nil, nil
}

func (p *kvProviderProtoStellar) MutateIn(opm *kvOpManager, action StoreSemantics, ops []MutateInSpec, docFlags SubdocDocFlag) (*MutateInResult, error) {

	storeSemanticMap := map[StoreSemantics]kv_v1.MutateInRequest_StoreSemantic{
		StoreSemanticsReplace: kv_v1.MutateInRequest_STORE_SEMANTIC_REPLACE,
		StoreSemanticsUpsert:  kv_v1.MutateInRequest_STORE_SEMANTIC_UPSERT,
		StoreSemanticsInsert:  kv_v1.MutateInRequest_STORE_SEMANTIC_INSERT,
	}

	opMap := map[memd.SubDocOpType]kv_v1.MutateInRequest_Spec_Operation{
		memd.SubDocOpDictAdd:        kv_v1.MutateInRequest_Spec_OPERATION_INSERT,
		memd.SubDocOpDictSet:        kv_v1.MutateInRequest_Spec_OPERATION_UPSERT,
		memd.SubDocOpReplace:        kv_v1.MutateInRequest_Spec_OPERATION_REPLACE,
		memd.SubDocOpDelete:         kv_v1.MutateInRequest_Spec_OPERATION_REMOVE,
		memd.SubDocOpArrayPushFirst: kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_PREPEND,
		memd.SubDocOpArrayPushLast:  kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_APPEND,
		memd.SubDocOpArrayInsert:    kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_INSERT,
		memd.SubDocOpArrayAddUnique: kv_v1.MutateInRequest_Spec_OPERATION_ARRAY_ADD_UNIQUE,
		memd.SubDocOpCounter:        kv_v1.MutateInRequest_Spec_OPERATION_COUNTER,
	}

	psAction := storeSemanticMap[action]
	durability := memdDurToPs(opm.DurabilityLevel())

	cas := opm.Cas()
	psSpecs := make([]*kv_v1.MutateInRequest_Spec, len(ops))
	memdDocFlags := memd.SubdocDocFlag(docFlags)
	expiry := opm.Expiry()
	preserveTTL := opm.PreserveExpiry()

	switch action {
	case StoreSemanticsReplace:
		// this is default behavior
		if expiry > 0 && preserveTTL {
			return nil, makeInvalidArgumentsError("cannot use preserve ttl with expiry for replace store semantics")
		}
	case StoreSemanticsUpsert:
		memdDocFlags |= memd.SubdocDocFlagMkDoc
	case StoreSemanticsInsert:
		if preserveTTL {
			return nil, makeInvalidArgumentsError("cannot use preserve ttl with insert store semantics")
		}

		memdDocFlags |= memd.SubdocDocFlagAddDoc
	default:
		return nil, makeInvalidArgumentsError("invalid StoreSemantics value provided")
	}

	for i, op := range ops {
		// does PS take care of this?
		if op.path == "" {
			switch op.op {
			case memd.SubDocOpDictAdd:
				return nil, makeInvalidArgumentsError("cannot specify a blank path with InsertSpec")
			case memd.SubDocOpDictSet:
				return nil, makeInvalidArgumentsError("cannot specify a blank path with UpsertSpec")
			case memd.SubDocOpDelete:
				op.op = memd.SubDocOpDeleteDoc
			case memd.SubDocOpReplace:
				op.op = memd.SubDocOpSetDoc
			default:
			}
		}
		bytes, flags, err := jsonMarshalMutateSpec(op)
		if err != nil {
			return nil, err
		}

		if flags&memd.SubdocFlagExpandMacros == memd.SubdocFlagExpandMacros {
			return nil, fmt.Errorf("unsupported flag: macro expansion")
		}
		psmutateFlag := kv_v1.MutateInRequest_Spec_Flags{
			CreatePath: &op.createPath,
			Xattr:      &op.isXattr,
		}

		psSpecs[i] = &kv_v1.MutateInRequest_Spec{
			Operation: opMap[op.op],
			Path:      op.path,
			Content:   bytes,
			Flags:     &psmutateFlag,
		}
	}

	accessDeleted := memdDocFlags&memd.SubdocDocFlagAccessDeleted == memd.SubdocDocFlagAccessDeleted
	mutateInRequestFlags := kv_v1.MutateInRequest_Flags{
		AccessDeleted: &accessDeleted,
	}
	request := &kv_v1.MutateInRequest{
		BucketName:      opm.BucketName(),
		ScopeName:       opm.ScopeName(),
		CollectionName:  opm.CollectionName(),
		Key:             string(opm.DocumentID()),
		Specs:           nil,
		StoreSemantic:   &psAction,
		DurabilityLevel: durability,
		Cas:             (*uint64)(&cas),
		Flags:           &mutateInRequestFlags,
	}

	res, err := p.client.MutateIn(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	mt := psMutToGoCbMut(*res.MutationToken)
	return &MutateInResult{
		MutationResult: MutationResult{
			Result: Result{
				cas: Cas(res.Cas),
			},
			mt: &mt,
		},
	}, nil
}

func (p *kvProviderProtoStellar) Scan(ScanType, *kvOpManager) (*ScanResult, error) {

	return nil, nil
}

func (p *kvProviderProtoStellar) Add(opm *kvOpManager) (*MutationResult, error) {

	request := &kv_v1.InsertRequest{
		BucketName:     opm.BucketName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),

		Key:         string(opm.DocumentID()),
		Content:     opm.ValueBytes(),
		ContentType: kv_v1.DocumentContentType(opm.ValueFlags()),

		//TODO expiry support
		DurabilityLevel: memdDurToPs(opm.DurabilityLevel()),
	}
	res, err := p.client.Insert(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	mt := psMutToGoCbMut(*res.MutationToken)
	cas := res.Cas

	mutOut := MutationResult{
		mt: &mt,
		Result: Result{
			cas: Cas(cas),
		},
	}
	return &mutOut, nil
}

func (p *kvProviderProtoStellar) Set(opm *kvOpManager) (*MutationResult, error) {
	request := &kv_v1.UpsertRequest{
		Key:            string(opm.DocumentID()),
		BucketName:     opm.BucketName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),
		Content:        opm.ValueBytes(),
		ContentType:    kv_v1.DocumentContentType(opm.ValueFlags()),

		//TODO: expiry
		DurabilityLevel: memdDurToPs(opm.DurabilityLevel()),
	}

	res, err := p.client.Upsert(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	mt := psMutToGoCbMut(*res.MutationToken)
	cas := res.Cas

	mutOut := MutationResult{
		mt: &mt,
		Result: Result{
			cas: Cas(cas),
		},
	}
	return &mutOut, nil
}

func (p *kvProviderProtoStellar) Replace(opm *kvOpManager) (*MutationResult, error) {

	cas := opm.Cas()
	request := &kv_v1.ReplaceRequest{
		Key:         string(opm.DocumentID()),
		Content:     opm.ValueBytes(),
		ContentType: kv_v1.DocumentContentType(opm.ValueFlags()),

		Cas:            (*uint64)(&cas),
		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		BucketName:     opm.BucketName(),

		//TODO: expiry
		DurabilityLevel: memdDurToPs(opm.DurabilityLevel()),
	}

	res, err := p.client.Replace(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	mt := psMutToGoCbMut(*res.MutationToken)
	outCas := res.Cas

	mutOut := MutationResult{
		mt: &mt,
		Result: Result{
			cas: Cas(outCas),
		},
	}
	return &mutOut, nil
}

func (p *kvProviderProtoStellar) Get(opm *kvOpManager) (*GetResult, error) {
	request := &kv_v1.GetRequest{
		Key: string(opm.DocumentID()),

		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		BucketName:     opm.BucketName(),
	}

	res, err := p.client.Get(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	resOut := GetResult{
		Result:     Result{Cas(res.Cas)},
		transcoder: opm.Transcoder(),
		// TODO: check if this is valid
		contents: res.Content,
		flags:    uint32(res.ContentType),
	}

	return &resOut, nil

}

func (p *kvProviderProtoStellar) GetAndTouch(opm *kvOpManager) (*GetResult, error) {
	request := &kv_v1.GetAndTouchRequest{
		Key: string(opm.DocumentID()),

		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		BucketName:     opm.ScopeName(),

		//Expiry: ,
	}

	res, err := p.client.GetAndTouch(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	resOut := GetResult{
		Result:     Result{Cas(res.Cas)},
		transcoder: opm.Transcoder(),
		// TODO: check if this is valid
		contents: res.Content,
		flags:    uint32(res.ContentType),
	}
	return &resOut, nil
}

func (p *kvProviderProtoStellar) GetAndLock(opm *kvOpManager) (*GetResult, error) {
	request := &kv_v1.GetAndLockRequest{
		BucketName:     opm.ScopeName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),
		Key:            string(opm.DocumentID()),
		LockTime:       uint32(opm.LockTime()), // TODO: check the units on this.
	}

	res, err := p.client.GetAndLock(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	resOut := GetResult{
		Result:     Result{Cas(res.Cas)},
		transcoder: opm.Transcoder(),
		// TODO: check if this is valid
		contents: res.Content,
		flags:    uint32(res.ContentType),
	}
	return &resOut, nil
}

func (p *kvProviderProtoStellar) Exists(opm *kvOpManager) (*ExistsResult, error) {
	request := &kv_v1.ExistsRequest{
		Key: string(opm.DocumentID()),

		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		BucketName:     opm.BucketName(),
	}

	res, err := p.client.Exists(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	resOut := ExistsResult{
		Result: Result{
			Cas(res.Cas),
		},
		docExists: res.Result,
	}

	return &resOut, nil
}

func (p *kvProviderProtoStellar) Delete(opm *kvOpManager) (*MutationResult, error) {

	cas := opm.Cas()

	request := &kv_v1.RemoveRequest{
		BucketName:      opm.BucketName(),
		ScopeName:       opm.ScopeName(),
		CollectionName:  opm.CollectionName(),
		Key:             string(opm.DocumentID()),
		Cas:             (*uint64)(&cas),
		DurabilityLevel: memdDurToPs(opm.DurabilityLevel()),
	}

	res, err := p.client.Remove(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	mt := psMutToGoCbMut(*res.MutationToken)
	outCas := res.Cas

	mutOut := MutationResult{
		mt: &mt,
		Result: Result{
			cas: Cas(outCas),
		},
	}

	return &mutOut, nil
}

func (p *kvProviderProtoStellar) Unlock(opm *kvOpManager) error {

	cas := opm.Cas()

	request := &kv_v1.UnlockRequest{
		BucketName:     opm.BucketName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),
		Key:            string(opm.DocumentID()),
		Cas:            (uint64)(cas),
	}

	_, err := p.client.Unlock(opm.ctx, request)

	return err

}

func (p *kvProviderProtoStellar) Touch(opm *kvOpManager) (*MutationResult, error) {

	request := &kv_v1.TouchRequest{
		BucketName:     opm.BucketName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),
		Key:            string(opm.DocumentID()),
		//Expiry:         opm.Expiry(), // TODO: expiry
	}

	res, err := p.client.Touch(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	mt := psMutToGoCbMut(*res.MutationToken)
	outCas := res.Cas

	mutOut := MutationResult{
		mt: &mt,
		Result: Result{
			cas: Cas(outCas),
		},
	}

	return &mutOut, nil

}

func (p *kvProviderProtoStellar) GetReplica(opm *kvOpManager) (*GetReplicaResult, error) {
	request := &kv_v1.GetReplicaRequest{
		BucketName:     opm.BucketName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),
		Key:            string(opm.DocumentID()),
		ReplicaIndex:   uint32(opm.ReplicaIndex()),
	}

	res, err := p.client.GetReplica(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	outCas := res.Cas

	docOut := &GetReplicaResult{}
	docOut.cas = Cas(outCas)
	docOut.transcoder = opm.Transcoder()
	docOut.contents = res.Content
	docOut.flags = uint32(res.ContentType)
	docOut.isReplica = true

	return docOut, nil

}

func (p *kvProviderProtoStellar) Prepend(opm *kvOpManager) (*MutationResult, error) {
	cas := opm.Cas()
	request := &kv_v1.PrependRequest{
		BucketName:      opm.BucketName(),
		ScopeName:       opm.ScopeName(),
		CollectionName:  opm.CollectionName(),
		Key:             string(opm.DocumentID()),
		Content:         opm.AdjoinBytes(),
		Cas:             (*uint64)(&cas),
		DurabilityLevel: memdDurToPs(opm.DurabilityLevel()),
	}

	res, err := p.client.Prepend(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	mt := psMutToGoCbMut(*res.MutationToken)
	outCas := res.Cas
	mutOut := &MutationResult{
		mt: &mt,
		Result: Result{
			cas: Cas(outCas),
		},
	}

	return mutOut, nil
}

func (p *kvProviderProtoStellar) Append(opm *kvOpManager) (*MutationResult, error) {
	cas := opm.Cas()

	request := &kv_v1.AppendRequest{
		BucketName:      opm.BucketName(),
		ScopeName:       opm.ScopeName(),
		CollectionName:  opm.CollectionName(),
		Key:             string(opm.DocumentID()),
		Content:         opm.AdjoinBytes(),
		Cas:             (*uint64)(&cas),
		DurabilityLevel: memdDurToPs(opm.DurabilityLevel()),
	}

	res, err := p.client.Append(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	mt := psMutToGoCbMut(*res.MutationToken)
	outCas := res.Cas
	mutOut := &MutationResult{
		mt: &mt,
		Result: Result{
			cas: Cas(outCas),
		},
	}

	return mutOut, nil
}

func (p *kvProviderProtoStellar) Increment(opm *kvOpManager) (*CounterResult, error) {
	initial := int64(opm.Initial())

	request := &kv_v1.IncrementRequest{
		BucketName:     opm.BucketName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),
		Key:            string(opm.DocumentID()),
		Delta:          opm.Delta(),
		// Expiry:          opm.Expiry(), // TODO: expiry
		Initial:         &initial,
		DurabilityLevel: memdDurToPs(opm.DurabilityLevel()),
	}

	res, err := p.client.Increment(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	mt := psMutToGoCbMut(*res.MutationToken)
	countOut := &CounterResult{}
	countOut.cas = Cas(res.Cas)
	countOut.mt = &mt
	countOut.content = uint64(res.Content)

	return countOut, nil

}
func (p *kvProviderProtoStellar) Decrement(opm *kvOpManager) (*CounterResult, error) {
	initial := int64(opm.Initial())

	request := &kv_v1.DecrementRequest{
		BucketName:     opm.BucketName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),
		Key:            string(opm.DocumentID()),
		Delta:          opm.Delta(),
		// Expiry:          opm.Expiry(), // TODO: expiry
		Initial:         &initial,
		DurabilityLevel: memdDurToPs(opm.DurabilityLevel()),
	}

	res, err := p.client.Decrement(opm.ctx, request)
	if err != nil {
		return nil, err
	}

	mt := psMutToGoCbMut(*res.MutationToken)
	countOut := &CounterResult{}
	countOut.cas = Cas(res.Cas)
	countOut.mt = &mt
	countOut.content = uint64(res.Content)

	return countOut, nil

}

// converts memdDurability level to protostellar durability level
func memdDurToPs(dur memd.DurabilityLevel) *kv_v1.DurabilityLevel {
	// memd.Durability starts at 1.
	// assume 0x00 means not set.
	if dur == 0x00 {
		return nil

	}

	newDur := kv_v1.DurabilityLevel(dur - 1)
	return &newDur

}

func psMutToGoCbMut(in kv_v1.MutationToken) MutationToken {
	return MutationToken{
		bucketName: in.BucketName,
		token: gocbcore.MutationToken{
			VbID:   uint16(in.VbucketId),
			VbUUID: gocbcore.VbUUID(in.VbucketUuid),
			SeqNo:  gocbcore.SeqNo(in.SeqNo),
		},
	}
}
