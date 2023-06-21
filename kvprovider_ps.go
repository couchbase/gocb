// nolint: unused
package gocb

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"google.golang.org/grpc/status"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
)

var _ kvProvider = &kvProviderPs{}

// // wraps kv and makes it compliant for gocb
type kvProviderPs struct {
	client kv_v1.KvServiceClient
}

// this is uint8 to int32, need to check overflows etc
var memdToPsLookupinTranslation = map[memd.SubDocOpType]kv_v1.LookupInRequest_Spec_Operation{
	memd.SubDocOpGet:      kv_v1.LookupInRequest_Spec_OPERATION_GET,
	memd.SubDocOpExists:   kv_v1.LookupInRequest_Spec_OPERATION_EXISTS,
	memd.SubDocOpGetCount: kv_v1.LookupInRequest_Spec_OPERATION_COUNT,
}

var memdToPsMutateinTranslation = map[memd.SubDocOpType]kv_v1.MutateInRequest_Spec_Operation{
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

func (p *kvProviderPs) LookupIn(c *Collection, id string, ops []LookupInSpec, opts *LookupInOptions) (*LookupInResult, error) {
	opm := newKvOpManagerPs(c, "lookup_in", opts.ParentSpan)
	defer opm.Finish(opts.noMetrics)

	opm.SetDocumentID(id)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	lookUpInPSSpecs := make([]*kv_v1.LookupInRequest_Spec, len(ops))

	for i, op := range ops {

		if op.op == memd.SubDocOpGet && op.path == "" {
			if op.isXattr {
				return nil, makeInvalidArgumentsError("invalid xattr fetch with no path")
			}

			lookUpInPSSpecs[i] = &kv_v1.LookupInRequest_Spec{
				Operation: memdToPsLookupinTranslation[op.op],
				Path:      op.path,
			}

			continue
		}

		newOp, ok := memdToPsLookupinTranslation[op.op]
		if !ok {
			return nil, makeInvalidArgumentsError("unknown lookupin op")
		}

		isXattr := op.isXattr
		specFlag := &kv_v1.LookupInRequest_Spec_Flags{
			Xattr: &isXattr,
		}

		lookUpInPSSpecs[i] = &kv_v1.LookupInRequest_Spec{
			Operation: newOp,
			Path:      op.path,
			Flags:     specFlag,
		}
	}

	requestFlags := &kv_v1.LookupInRequest_Flags{}
	accessDeleted := opts.Internal.DocFlags|SubdocDocFlagAccessDeleted == 1
	if accessDeleted {
		requestFlags.AccessDeleted = &accessDeleted
	}

	req := &kv_v1.LookupInRequest{
		BucketName:     opm.BucketName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),
		Key:            opm.DocumentID(),
		Specs:          lookUpInPSSpecs,
		Flags:          requestFlags,
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.LookupIn(ctx, req)
	if err != nil {
		return nil, opm.EnhanceErr(err, true)
	}

	docOut := &LookupInResult{}
	docOut.cas = Cas(res.Cas)
	docOut.contents = make([]lookupInPartial, len(lookUpInPSSpecs))
	for i, opRes := range res.Specs {
		if opRes.Status != nil && opRes.Status.Code != 0 {
			docOut.contents[i].err = opm.EnhanceErrorStatus(status.FromProto(opRes.Status), true)
		} else if ops[i].op == memd.SubDocOpExists {
			// PS and classic do not return exists in the same way.
			// Classic indicates exists via status code, whereas PS uses an actual bool value.
			var exists bool
			err = json.Unmarshal(opRes.Content, &exists)
			if err != nil {
				logInfof("Failed to unmarshal exists spec response: %s", err)
				continue
			}

			if !exists {
				// If the path doesn't exist then populate the error value. Exists cannot be done at the doc level
				// so we know that this is path not found.
				docOut.contents[i].err = opm.EnhanceErr(ErrPathNotFound, false)
			}
		}
		docOut.contents[i].data = opRes.Content
	}

	return docOut, nil
}

func (p *kvProviderPs) LookupInAnyReplica(*Collection, string, []LookupInSpec, *LookupInAnyReplicaOptions) (*LookupInReplicaResult, error) {
	return nil, ErrFeatureNotAvailable
}

func (p *kvProviderPs) LookupInAllReplicas(*Collection, string, []LookupInSpec, *LookupInAllReplicaOptions) (*LookupInAllReplicasResult, error) {
	return nil, ErrFeatureNotAvailable
}

func (p *kvProviderPs) MutateIn(c *Collection, id string, ops []MutateInSpec, opts *MutateInOptions) (*MutateInResult, error) {
	opm := newKvOpManagerPs(c, "mutate_in", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetTimeout(opts.Timeout)

	opm.SetDuraOptions(opts.DurabilityLevel)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	psSpecs := make([]*kv_v1.MutateInRequest_Spec, len(ops))
	memdDocFlags := memd.SubdocDocFlag(opts.Internal.DocFlags)
	preserveTTL := opts.PreserveExpiry
	if preserveTTL {
		return nil, makeInvalidArgumentsError("cannot use preserve expiry with protostellar")
	}

	var psAction kv_v1.MutateInRequest_StoreSemantic
	switch opts.StoreSemantic {
	case StoreSemanticsReplace:
		// this is default behavior
		psAction = kv_v1.MutateInRequest_STORE_SEMANTIC_REPLACE
	case StoreSemanticsUpsert:
		memdDocFlags |= memd.SubdocDocFlagMkDoc
		psAction = kv_v1.MutateInRequest_STORE_SEMANTIC_UPSERT
	case StoreSemanticsInsert:
		memdDocFlags |= memd.SubdocDocFlagAddDoc
		psAction = kv_v1.MutateInRequest_STORE_SEMANTIC_INSERT
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
			default:
			}
		}
		etrace := opm.parent.startKvOpTrace("request_encoding", opm.TraceSpanContext(), true)
		bytes, flags, err := jsonMarshalMutateSpec(op)
		etrace.End()
		if err != nil {
			return nil, err
		}

		if flags&memd.SubdocFlagExpandMacros == memd.SubdocFlagExpandMacros {
			return nil, wrapError(ErrFeatureNotAvailable, "unsupported flag for protostellar: macro expansion")
		}
		createPath := op.createPath
		isXattr := op.isXattr
		psmutateFlag := &kv_v1.MutateInRequest_Spec_Flags{
			CreatePath: &createPath,
			Xattr:      &isXattr,
		}

		psSpecs[i] = &kv_v1.MutateInRequest_Spec{
			Operation: memdToPsMutateinTranslation[op.op],
			Path:      op.path,
			Content:   bytes,
			Flags:     psmutateFlag,
		}
	}

	accessDeleted := memdDocFlags&memd.SubdocDocFlagAccessDeleted == memd.SubdocDocFlagAccessDeleted
	var mutateInRequestFlags *kv_v1.MutateInRequest_Flags
	if accessDeleted {
		mutateInRequestFlags = &kv_v1.MutateInRequest_Flags{
			AccessDeleted: &accessDeleted,
		}
	}

	var cas *uint64
	if opts.Cas > 0 {
		cas = (*uint64)(&opts.Cas)
	}
	var expiry *kv_v1.MutateInRequest_ExpirySecs
	if opts.Expiry > 0 {
		expiry = &kv_v1.MutateInRequest_ExpirySecs{ExpirySecs: uint32(opts.Expiry.Seconds())}
	}

	request := &kv_v1.MutateInRequest{
		BucketName:      opm.BucketName(),
		ScopeName:       opm.ScopeName(),
		CollectionName:  opm.CollectionName(),
		Key:             opm.DocumentID(),
		Specs:           psSpecs,
		StoreSemantic:   &psAction,
		DurabilityLevel: opm.DurabilityLevel(),
		Cas:             cas,
		Flags:           mutateInRequestFlags,
		Expiry:          expiry,
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.MutateIn(ctx, request)
	if err != nil {
		err = opm.EnhanceErr(err, false)

		// GOCBC-1019: Due to a previous bug in gocbcore we need to convert cas mismatch back to exists to match classic
		// behaviour.
		if kvErr, ok := err.(*KeyValueError); ok {
			if errors.Is(kvErr.InnerError, ErrCasMismatch) {
				kvErr.InnerError = ErrDocumentExists
			}
		}

		return nil, err
	}

	mutOut := &MutateInResult{}
	mutOut.cas = Cas(res.Cas)
	mutOut.mt = psMutToGoCbMut(res.MutationToken)
	mutOut.contents = make([]mutateInPartial, len(res.Specs))
	for i, op := range res.Specs {
		mutOut.contents[i] = mutateInPartial{data: op.Content}
	}

	return mutOut, nil
}

func (p *kvProviderPs) Scan(c *Collection, scanType ScanType, opts *ScanOptions) (*ScanResult, error) {
	return nil, ErrFeatureNotAvailable
}

func (p *kvProviderPs) Insert(c *Collection, id string, val interface{}, opts *InsertOptions) (*MutationResult, error) {
	opm := newKvOpManagerPs(c, "insert", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetValue(val)
	opm.SetDuraOptions(opts.DurabilityLevel)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	var expiry *kv_v1.InsertRequest_ExpirySecs
	if opts.Expiry > 0 {
		expiry = &kv_v1.InsertRequest_ExpirySecs{ExpirySecs: uint32(opts.Expiry.Seconds())}
	}

	request := &kv_v1.InsertRequest{
		BucketName:     opm.BucketName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),

		Key:          opm.DocumentID(),
		Content:      opm.ValueBytes(),
		ContentFlags: opm.ValueFlags(),

		Expiry:          expiry,
		DurabilityLevel: opm.DurabilityLevel(),
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.Insert(ctx, request)
	if err != nil {
		return nil, opm.EnhanceErr(err, false)
	}

	mt := psMutToGoCbMut(res.MutationToken)
	cas := res.Cas

	mutOut := MutationResult{
		mt: mt,
		Result: Result{
			cas: Cas(cas),
		},
	}
	return &mutOut, nil
}

func (p *kvProviderPs) Upsert(c *Collection, id string, val interface{}, opts *UpsertOptions) (*MutationResult, error) {
	opm := newKvOpManagerPs(c, "upsert", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetValue(val)
	opm.SetDuraOptions(opts.DurabilityLevel)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	var expiry *kv_v1.UpsertRequest_ExpirySecs
	if opts.Expiry > 0 {
		expiry = &kv_v1.UpsertRequest_ExpirySecs{ExpirySecs: uint32(opts.Expiry.Seconds())}
	}

	request := &kv_v1.UpsertRequest{
		Key:            opm.DocumentID(),
		BucketName:     opm.BucketName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),
		Content:        opm.ValueBytes(),
		ContentFlags:   opm.ValueFlags(),

		Expiry:          expiry,
		DurabilityLevel: opm.DurabilityLevel(),
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.Upsert(ctx, request)
	if err != nil {
		return nil, opm.EnhanceErr(err, false)
	}

	mt := psMutToGoCbMut(res.MutationToken)
	cas := res.Cas

	mutOut := MutationResult{
		mt: mt,
		Result: Result{
			cas: Cas(cas),
		},
	}
	return &mutOut, nil
}

func (p *kvProviderPs) Replace(c *Collection, id string, val interface{}, opts *ReplaceOptions) (*MutationResult, error) {
	opm := newKvOpManagerPs(c, "replace", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetValue(val)
	opm.SetDuraOptions(opts.DurabilityLevel)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	var cas *uint64
	if opts.Cas > 0 {
		cas = (*uint64)(&opts.Cas)
	}

	var expiry *kv_v1.ReplaceRequest_ExpirySecs
	if opts.Expiry > 0 {
		expiry = &kv_v1.ReplaceRequest_ExpirySecs{ExpirySecs: uint32(opts.Expiry.Seconds())}
	}

	request := &kv_v1.ReplaceRequest{
		Key:          opm.DocumentID(),
		Content:      opm.ValueBytes(),
		ContentFlags: opm.ValueFlags(),

		Cas:            cas,
		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		BucketName:     opm.BucketName(),

		Expiry:          expiry,
		DurabilityLevel: opm.DurabilityLevel(),
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.Replace(ctx, request)
	if err != nil {
		return nil, opm.EnhanceErr(err, false)
	}

	mt := psMutToGoCbMut(res.MutationToken)
	outCas := res.Cas

	mutOut := MutationResult{
		mt: mt,
		Result: Result{
			cas: Cas(outCas),
		},
	}
	return &mutOut, nil
}

func (p *kvProviderPs) Get(c *Collection, id string, opts *GetOptions) (*GetResult, error) {
	if len(opts.Project) > 0 {
		return nil, wrapError(ErrFeatureNotAvailable, "cannot use project with protostellar")
	}

	opm := newKvOpManagerPs(c, "get", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	request := &kv_v1.GetRequest{
		Key: opm.DocumentID(),

		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		BucketName:     opm.BucketName(),
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.Get(ctx, request)
	if err != nil {
		return nil, opm.EnhanceErr(err, true)
	}

	resOut := GetResult{
		Result: Result{Cas(res.Cas)},

		contents: res.Content,
		flags:    res.ContentFlags,

		transcoder: opm.Transcoder(),
	}
	if res.Expiry != nil {
		t := res.Expiry.AsTime()
		resOut.expiryTime = &t
	}

	return &resOut, nil

}

func (p *kvProviderPs) GetAndTouch(c *Collection, id string, expiry time.Duration, opts *GetAndTouchOptions) (*GetResult, error) {
	opm := newKvOpManagerPs(c, "get_and_touch", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	reqExpiry := &kv_v1.GetAndTouchRequest_ExpirySecs{ExpirySecs: uint32(expiry.Seconds())}

	request := &kv_v1.GetAndTouchRequest{
		Key: opm.DocumentID(),

		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		BucketName:     opm.BucketName(),

		Expiry: reqExpiry,
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.GetAndTouch(ctx, request)
	if err != nil {
		return nil, opm.EnhanceErr(err, false)
	}

	resOut := GetResult{
		Result:     Result{Cas(res.Cas)},
		transcoder: opm.Transcoder(),

		contents: res.Content,
		flags:    res.ContentFlags,
	}
	if res.Expiry != nil {
		t := res.Expiry.AsTime()
		resOut.expiryTime = &t
	}

	return &resOut, nil
}

func (p *kvProviderPs) GetAndLock(c *Collection, id string, lockTime time.Duration, opts *GetAndLockOptions) (*GetResult, error) {
	opm := newKvOpManagerPs(c, "get_and_lock", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	request := &kv_v1.GetAndLockRequest{
		BucketName:     opm.BucketName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),
		Key:            opm.DocumentID(),
		LockTime:       uint32(lockTime.Seconds()),
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.GetAndLock(ctx, request)
	if err != nil {
		return nil, opm.EnhanceErr(err, false)
	}

	resOut := GetResult{
		Result:     Result{Cas(res.Cas)},
		transcoder: opm.Transcoder(),

		contents: res.Content,
		flags:    res.ContentFlags,
	}
	if res.Expiry != nil {
		t := res.Expiry.AsTime()
		resOut.expiryTime = &t
	}

	return &resOut, nil
}

func (p *kvProviderPs) Exists(c *Collection, id string, opts *ExistsOptions) (*ExistsResult, error) {
	opm := newKvOpManagerPs(c, "exists", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	request := &kv_v1.ExistsRequest{
		Key: opm.DocumentID(),

		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		BucketName:     opm.BucketName(),
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.Exists(ctx, request)
	if err != nil {
		return nil, opm.EnhanceErr(err, true)
	}

	resOut := ExistsResult{
		Result: Result{
			Cas(res.Cas),
		},
		docExists: res.Result,
	}

	return &resOut, nil
}

func (p *kvProviderPs) Remove(c *Collection, id string, opts *RemoveOptions) (*MutationResult, error) {
	opm := newKvOpManagerPs(c, "remove", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetDuraOptions(opts.DurabilityLevel)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	var cas *uint64
	if opts.Cas > 0 {
		cas = (*uint64)(&opts.Cas)
	}

	request := &kv_v1.RemoveRequest{
		BucketName:      opm.BucketName(),
		ScopeName:       opm.ScopeName(),
		CollectionName:  opm.CollectionName(),
		Key:             opm.DocumentID(),
		Cas:             cas,
		DurabilityLevel: opm.DurabilityLevel(),
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.Remove(ctx, request)
	if err != nil {
		return nil, opm.EnhanceErr(err, false)
	}

	mt := psMutToGoCbMut(res.MutationToken)
	outCas := res.Cas

	mutOut := MutationResult{
		mt: mt,
		Result: Result{
			cas: Cas(outCas),
		},
	}

	return &mutOut, nil
}

func (p *kvProviderPs) Unlock(c *Collection, id string, cas Cas, opts *UnlockOptions) error {
	opm := newKvOpManagerPs(c, "unlock", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return err
	}

	request := &kv_v1.UnlockRequest{
		BucketName:     opm.BucketName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),
		Key:            opm.DocumentID(),
		Cas:            (uint64)(cas),
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	_, err := p.client.Unlock(ctx, request)
	if err != nil {
		return opm.EnhanceErr(err, false)
	}

	return nil
}

func (p *kvProviderPs) Touch(c *Collection, id string, expiry time.Duration, opts *TouchOptions) (*MutationResult, error) {
	opm := newKvOpManagerPs(c, "touch", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	request := &kv_v1.TouchRequest{
		BucketName:     opm.BucketName(),
		ScopeName:      opm.ScopeName(),
		CollectionName: opm.CollectionName(),
		Key:            opm.DocumentID(),
		Expiry:         &kv_v1.TouchRequest_ExpirySecs{ExpirySecs: uint32(expiry.Seconds())},
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.Touch(ctx, request)
	if err != nil {
		return nil, opm.EnhanceErr(err, false)
	}

	mt := psMutToGoCbMut(res.MutationToken)
	outCas := res.Cas

	mutOut := MutationResult{
		mt: mt,
		Result: Result{
			cas: Cas(outCas),
		},
	}

	return &mutOut, nil
}

func (p *kvProviderPs) GetAllReplicas(c *Collection, id string, opts *GetAllReplicaOptions) (*GetAllReplicasResult, error) {
	return nil, ErrFeatureNotAvailable
}

func (p *kvProviderPs) GetAnyReplica(c *Collection, id string, opts *GetAnyReplicaOptions) (*GetReplicaResult, error) {
	return nil, ErrFeatureNotAvailable
}

// func (p *kvProviderPs) GetReplica(opm *kvOpManagerCore) (*GetReplicaResult, error) {
// 	request := &kv_v1.GetReplicaRequest{
// 		BucketName:     opm.BucketName(),
// 		ScopeName:      opm.ScopeName(),
// 		CollectionName: opm.CollectionName(),
// 		Key:            string(opm.DocumentID()),
// 		ReplicaIndex:   uint32(opm.ReplicaIndex()),
// 	}
//
// 	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
// 	defer cancel()
// 	res, err := p.client.GetReplica(opm.ctx, request)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	outCas := res.Cas
//
// 	docOut := &GetReplicaResult{}
// 	docOut.cas = Cas(outCas)
// 	docOut.transcoder = opm.Transcoder()
// 	docOut.contents = res.Content
// 	docOut.flags = res.ContentFlags
// 	docOut.isReplica = true
//
// 	return docOut, nil
//
// }

func (p *kvProviderPs) Prepend(c *Collection, id string, val []byte, opts *PrependOptions) (*MutationResult, error) {
	opm := newKvOpManagerPs(c, "prepend", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetDuraOptions(opts.DurabilityLevel)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	var cas *uint64
	if opts.Cas > 0 {
		cas = (*uint64)(&opts.Cas)
	}

	request := &kv_v1.PrependRequest{
		BucketName:      opm.BucketName(),
		ScopeName:       opm.ScopeName(),
		CollectionName:  opm.CollectionName(),
		Key:             opm.DocumentID(),
		Content:         val,
		Cas:             cas,
		DurabilityLevel: opm.DurabilityLevel(),
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.Prepend(ctx, request)
	if err != nil {
		return nil, opm.EnhanceErr(err, false)
	}

	mt := psMutToGoCbMut(res.MutationToken)
	outCas := res.Cas
	mutOut := &MutationResult{
		mt: mt,
		Result: Result{
			cas: Cas(outCas),
		},
	}

	return mutOut, nil
}

func (p *kvProviderPs) Append(c *Collection, id string, val []byte, opts *AppendOptions) (*MutationResult, error) {
	opm := newKvOpManagerPs(c, "append", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetDuraOptions(opts.DurabilityLevel)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	var cas *uint64
	if opts.Cas > 0 {
		cas = (*uint64)(&opts.Cas)
	}

	request := &kv_v1.AppendRequest{
		BucketName:      opm.BucketName(),
		ScopeName:       opm.ScopeName(),
		CollectionName:  opm.CollectionName(),
		Key:             opm.DocumentID(),
		Content:         val,
		Cas:             cas,
		DurabilityLevel: opm.DurabilityLevel(),
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.Append(ctx, request)
	if err != nil {
		return nil, opm.EnhanceErr(err, false)
	}

	mt := psMutToGoCbMut(res.MutationToken)
	outCas := res.Cas
	mutOut := &MutationResult{
		mt: mt,
		Result: Result{
			cas: Cas(outCas),
		},
	}

	return mutOut, nil
}

func (p *kvProviderPs) Increment(c *Collection, id string, opts *IncrementOptions) (*CounterResult, error) {
	if opts.Cas > 0 {
		return nil, makeInvalidArgumentsError("cas is not supported for the increment operation")
	}

	opm := newKvOpManagerPs(c, "increment", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetDuraOptions(opts.DurabilityLevel)
	opm.SetTimeout(opts.Timeout)

	var expiry *kv_v1.IncrementRequest_ExpirySecs
	if opts.Expiry > 0 {
		expiry = &kv_v1.IncrementRequest_ExpirySecs{ExpirySecs: uint32(opts.Expiry.Seconds())}
	}

	request := &kv_v1.IncrementRequest{
		BucketName:      opm.BucketName(),
		ScopeName:       opm.ScopeName(),
		CollectionName:  opm.CollectionName(),
		Key:             opm.DocumentID(),
		Delta:           opts.Delta,
		Expiry:          expiry,
		Initial:         &opts.Initial,
		DurabilityLevel: opm.DurabilityLevel(),
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.Increment(ctx, request)
	if err != nil {
		return nil, opm.EnhanceErr(err, false)
	}

	countOut := &CounterResult{}
	countOut.cas = Cas(res.Cas)
	countOut.mt = psMutToGoCbMut(res.MutationToken)
	countOut.content = uint64(res.Content)

	return countOut, nil

}
func (p *kvProviderPs) Decrement(c *Collection, id string, opts *DecrementOptions) (*CounterResult, error) {
	if opts.Cas > 0 {
		return nil, makeInvalidArgumentsError("cas is not supported for the decrement operation")
	}

	opm := newKvOpManagerPs(c, "decrement", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetDuraOptions(opts.DurabilityLevel)
	opm.SetTimeout(opts.Timeout)

	var expiry *kv_v1.DecrementRequest_ExpirySecs
	if opts.Expiry > 0 {
		expiry = &kv_v1.DecrementRequest_ExpirySecs{ExpirySecs: uint32(opts.Expiry.Seconds())}
	}

	request := &kv_v1.DecrementRequest{
		BucketName:      opm.BucketName(),
		ScopeName:       opm.ScopeName(),
		CollectionName:  opm.CollectionName(),
		Key:             opm.DocumentID(),
		Delta:           opts.Delta,
		Expiry:          expiry,
		Initial:         &opts.Initial,
		DurabilityLevel: opm.DurabilityLevel(),
	}

	ctx, cancel := p.newOpCtx(opts.Context, opm.getTimeout())
	defer cancel()
	res, err := p.client.Decrement(ctx, request)
	if err != nil {
		return nil, opm.EnhanceErr(err, false)
	}

	countOut := &CounterResult{}
	countOut.cas = Cas(res.Cas)
	countOut.mt = psMutToGoCbMut(res.MutationToken)
	countOut.content = uint64(res.Content)

	return countOut, nil

}

func psMutToGoCbMut(in *kv_v1.MutationToken) *MutationToken {
	if in != nil {
		return &MutationToken{
			bucketName: in.BucketName,
			token: gocbcore.MutationToken{
				VbID:   uint16(in.VbucketId),
				VbUUID: gocbcore.VbUUID(in.VbucketUuid),
				SeqNo:  gocbcore.SeqNo(in.SeqNo),
			},
		}
	}

	return nil
}

func (p *kvProviderPs) BulkGet(gocbcore.GetOptions, gocbcore.GetCallback) (gocbcore.PendingOp, error) {
	return nil, ErrFeatureNotAvailable
}
func (p *kvProviderPs) BulkGetAndTouch(gocbcore.GetAndTouchOptions, gocbcore.GetAndTouchCallback) (gocbcore.PendingOp, error) {
	return nil, ErrFeatureNotAvailable
}
func (p *kvProviderPs) BulkTouch(gocbcore.TouchOptions, gocbcore.TouchCallback) (gocbcore.PendingOp, error) {
	return nil, ErrFeatureNotAvailable
}
func (p *kvProviderPs) BulkDelete(gocbcore.DeleteOptions, gocbcore.DeleteCallback) (gocbcore.PendingOp, error) {
	return nil, ErrFeatureNotAvailable
}
func (p *kvProviderPs) BulkSet(gocbcore.SetOptions, gocbcore.StoreCallback) (gocbcore.PendingOp, error) {
	return nil, ErrFeatureNotAvailable
}
func (p *kvProviderPs) BulkAdd(gocbcore.AddOptions, gocbcore.StoreCallback) (gocbcore.PendingOp, error) {
	return nil, ErrFeatureNotAvailable
}
func (p *kvProviderPs) BulkReplace(gocbcore.ReplaceOptions, gocbcore.StoreCallback) (gocbcore.PendingOp, error) {
	return nil, ErrFeatureNotAvailable
}
func (p *kvProviderPs) BulkAppend(gocbcore.AdjoinOptions, gocbcore.AdjoinCallback) (gocbcore.PendingOp, error) {
	return nil, ErrFeatureNotAvailable
}
func (p *kvProviderPs) BulkPrepend(gocbcore.AdjoinOptions, gocbcore.AdjoinCallback) (gocbcore.PendingOp, error) {
	return nil, ErrFeatureNotAvailable
}
func (p *kvProviderPs) BulkIncrement(gocbcore.CounterOptions, gocbcore.CounterCallback) (gocbcore.PendingOp, error) {
	return nil, ErrFeatureNotAvailable
}
func (p *kvProviderPs) BulkDecrement(gocbcore.CounterOptions, gocbcore.CounterCallback) (gocbcore.PendingOp, error) {
	return nil, ErrFeatureNotAvailable
}

func (p *kvProviderPs) newOpCtx(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}

	return context.WithTimeout(ctx, timeout)
}
