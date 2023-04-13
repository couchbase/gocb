package gocb

import (
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbase/goprotostellar/genproto/kv_v1"
)

// // wraps kv and makes it compliant for gocb
type kvProviderProtoStellar struct {
	client kv_v1.KvServiceClient
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
