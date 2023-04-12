package gocb

import "github.com/couchbase/gocbcore/v10"

type kvProviderGocb struct {
	agent *gocbcore.Agent
}

var _ kvProvider = &kvProviderGocb{}

func (p *kvProviderGocb) Add(opm *kvOpManager) (*MutationResult, error) {
	synced := newSyncKvOpManager(opm)

	defer synced.Finish(false)

	var errOut error
	var mutOut *MutationResult
	err := synced.Wait(p.agent.Add(gocbcore.AddOptions{
		Key:   synced.DocumentID(),
		Value: synced.ValueBytes(),
		Flags: synced.ValueFlags(),
		// Expiry:                 durationToExpiry(opts.Expiry),
		CollectionName:         synced.CollectionName(),
		ScopeName:              synced.ScopeName(),
		DurabilityLevel:        synced.DurabilityLevel(),
		DurabilityLevelTimeout: synced.DurabilityTimeout(),
		RetryStrategy:          synced.RetryStrategy(),
		TraceContext:           synced.TraceSpanContext(),
		Deadline:               synced.Deadline(),
		User:                   synced.Impersonate(),
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = synced.EnhanceErr(err)
			synced.Reject()
			return
		}

		mutOut = &MutationResult{}
		mutOut.cas = Cas(res.Cas)
		mutOut.mt = synced.EnhanceMt(res.MutationToken)

		synced.Resolve(mutOut.mt)
	}))

	if err != nil {
		errOut = err
	}

	return mutOut, errOut
}

func (p *kvProviderGocb) Set(opm *kvOpManager) (*MutationResult, error) {
	synced := newSyncKvOpManager(opm)

	defer synced.Finish(false)

	var errOut error
	var mutOut *MutationResult

	err := synced.Wait(p.agent.Set(gocbcore.SetOptions{
		Key:   synced.DocumentID(),
		Value: synced.ValueBytes(),
		Flags: synced.ValueFlags(),
		//Expiry:                 durationToExpiry(opts.Expiry),
		CollectionName:         synced.CollectionName(),
		ScopeName:              synced.ScopeName(),
		DurabilityLevel:        synced.DurabilityLevel(),
		DurabilityLevelTimeout: synced.DurabilityTimeout(),
		RetryStrategy:          synced.RetryStrategy(),
		TraceContext:           synced.TraceSpanContext(),
		Deadline:               synced.Deadline(),
		User:                   synced.Impersonate(),
	}, func(sr *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = synced.EnhanceErr(err)
			synced.Reject()
			return
		}

		mutOut = &MutationResult{}
		mutOut.cas = Cas(sr.Cas)
		mutOut.mt = synced.EnhanceMt(sr.MutationToken)

		synced.Resolve(mutOut.mt)

	}))

	if err != nil {
		errOut = err
	}
	return mutOut, errOut
}

func (p *kvProviderGocb) Replace(opm *kvOpManager) (*MutationResult, error) {

	synced := newSyncKvOpManager(opm)

	defer synced.Finish(false)

	var errOut error
	var mutOut *MutationResult

	err := synced.Wait(p.agent.Replace(gocbcore.ReplaceOptions{
		Key:   synced.DocumentID(),
		Value: synced.ValueBytes(),
		Flags: synced.ValueFlags(),
		//Expiry:                 durationToExpiry(opts.Expiry),
		Cas:                    gocbcore.Cas(opm.Cas()),
		CollectionName:         synced.CollectionName(),
		ScopeName:              synced.ScopeName(),
		DurabilityLevel:        synced.DurabilityLevel(),
		DurabilityLevelTimeout: synced.DurabilityTimeout(),
		RetryStrategy:          synced.RetryStrategy(),
		TraceContext:           synced.TraceSpanContext(),
		Deadline:               synced.Deadline(),
		User:                   synced.Impersonate(),
		PreserveExpiry:         synced.PreserveExpiry(),
	}, func(sr *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = synced.EnhanceErr(err)
			synced.Reject()
			return
		}

		mutOut = &MutationResult{}
		mutOut.cas = Cas(sr.Cas)
		mutOut.mt = synced.EnhanceMt(sr.MutationToken)

		synced.Resolve(mutOut.mt)

	}))

	if err != nil {
		errOut = err
	}

	return mutOut, errOut
}

func (p *kvProviderGocb) Get(opm *kvOpManager) (*GetResult, error) {
	synced := newSyncKvOpManager(opm)

	defer synced.Finish(false)

	var errOut error
	var getOut *GetResult

	err := synced.Wait(p.agent.Get(gocbcore.GetOptions{
		Key:            opm.DocumentID(),
		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		RetryStrategy:  opm.RetryStrategy(),
		TraceContext:   opm.TraceSpanContext(),
		Deadline:       opm.Deadline(),
		User:           opm.Impersonate(),
	}, func(res *gocbcore.GetResult, err error) {
		if err != nil {
			errOut = opm.EnhanceErr(err)
			synced.Reject()
			return
		}

		doc := &GetResult{
			Result: Result{
				cas: Cas(res.Cas),
			},
			transcoder: opm.Transcoder(),
			contents:   res.Value,
			flags:      res.Flags,
		}

		getOut = doc

		synced.Resolve(nil)
	}))

	if err != nil {
		errOut = err
	}

	return getOut, errOut

}
