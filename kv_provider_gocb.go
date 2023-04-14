package gocb

import (
	"errors"
	"time"

	"github.com/couchbase/gocbcore/v10"
)

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

func (p *kvProviderGocb) GetAndTouch(opm *kvOpManager) (*GetResult, error) {
	synced := newSyncKvOpManager(opm)

	var getOut *GetResult
	var errOut error

	err := synced.Wait(p.agent.GetAndTouch(gocbcore.GetAndTouchOptions{
		Key: synced.DocumentID(),
		//Expiry:         durationToExpiry(expiry),
		CollectionName: synced.CollectionName(),
		ScopeName:      synced.ScopeName(),
		RetryStrategy:  synced.RetryStrategy(),
		TraceContext:   synced.TraceSpanContext(),
		Deadline:       synced.Deadline(),
		User:           synced.Impersonate(),
	}, func(res *gocbcore.GetAndTouchResult, err error) {
		if err != nil {
			errOut = synced.EnhanceErr(err)
			synced.Reject()
			return
		}

		if res != nil {
			getOut = &GetResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				transcoder: opm.Transcoder(),
				contents:   res.Value,
				flags:      res.Flags,
			}
		}

		synced.Resolve(nil)
	}))

	if err != nil {
		errOut = err
	}

	return getOut, errOut

}

func (p *kvProviderGocb) GetAndLock(opm *kvOpManager) (*GetResult, error) {
	synced := newSyncKvOpManager(opm)

	var errOut error
	var getResult *GetResult

	err := synced.Wait(p.agent.GetAndLock(gocbcore.GetAndLockOptions{
		Key:            synced.DocumentID(),
		LockTime:       uint32(synced.LockTime() / time.Second),
		CollectionName: synced.CollectionName(),
		ScopeName:      synced.ScopeName(),
		RetryStrategy:  synced.RetryStrategy(),
		TraceContext:   synced.TraceSpanContext(),
		Deadline:       synced.Deadline(),
		User:           synced.Impersonate(),
	}, func(res *gocbcore.GetAndLockResult, err error) {
		if err != nil {
			errOut = synced.EnhanceErr(err)
			synced.Reject()
			return
		}

		if res != nil {
			doc := &GetResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				transcoder: opm.Transcoder(),
				contents:   res.Value,
				flags:      res.Flags,
			}

			getResult = doc
		}

		synced.Resolve(nil)
	}))
	if err != nil {
		errOut = err
	}

	return getResult, errOut
}

func (p *kvProviderGocb) Exists(opm *kvOpManager) (*ExistsResult, error) {
	synced := newSyncKvOpManager(opm)
	defer synced.Finish(false)

	var docExists *ExistsResult
	var errOut error
	err := synced.Wait(p.agent.GetMeta(gocbcore.GetMetaOptions{
		Key:            synced.DocumentID(),
		CollectionName: synced.CollectionName(),
		ScopeName:      synced.ScopeName(),
		RetryStrategy:  synced.RetryStrategy(),
		TraceContext:   synced.TraceSpanContext(),
		Deadline:       synced.Deadline(),
		User:           synced.Impersonate(),
	}, func(res *gocbcore.GetMetaResult, err error) {
		if errors.Is(err, ErrDocumentNotFound) {
			docExists = &ExistsResult{
				Result: Result{
					cas: Cas(0),
				},
				docExists: false,
			}
			synced.Resolve(nil)
			return
		}

		if err != nil {
			errOut = synced.EnhanceErr(err)
			synced.Reject()
			return
		}

		if res != nil {
			docExists = &ExistsResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				docExists: res.Deleted == 0,
			}
		}

		synced.Resolve(nil)
	}))
	if err != nil {
		errOut = err
	}

	return docExists, errOut
}

func (p *kvProviderGocb) Delete(opm *kvOpManager) (*MutationResult, error) {
	synced := newSyncKvOpManager(opm)

	defer synced.Finish(false)

	var errOut error
	var mutOut *MutationResult
	err := synced.Wait(p.agent.Delete(gocbcore.DeleteOptions{
		Key:                    synced.DocumentID(),
		Cas:                    gocbcore.Cas(opm.Cas()),
		CollectionName:         synced.CollectionName(),
		ScopeName:              synced.ScopeName(),
		DurabilityLevel:        synced.DurabilityLevel(),
		DurabilityLevelTimeout: synced.DurabilityTimeout(),
		RetryStrategy:          synced.RetryStrategy(),
		TraceContext:           synced.TraceSpanContext(),
		Deadline:               synced.Deadline(),
		User:                   synced.Impersonate(),
	}, func(res *gocbcore.DeleteResult, err error) {
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

func (p *kvProviderGocb) Unlock(opm *kvOpManager) error {
	synced := newSyncKvOpManager(opm)

	defer synced.Finish(false)
	var errOut error
	err := synced.Wait(p.agent.Unlock(gocbcore.UnlockOptions{
		Key:            synced.DocumentID(),
		Cas:            gocbcore.Cas(opm.Cas()),
		CollectionName: synced.CollectionName(),
		ScopeName:      synced.ScopeName(),
		RetryStrategy:  synced.RetryStrategy(),
		TraceContext:   synced.TraceSpanContext(),
		Deadline:       synced.Deadline(),
		User:           synced.Impersonate(),
	}, func(res *gocbcore.UnlockResult, err error) {
		if err != nil {
			errOut = synced.EnhanceErr(err)
			synced.Reject()
			return
		}

		mt := synced.EnhanceMt(res.MutationToken)
		synced.Resolve(mt)
	}))

	if err != nil {
		errOut = err
	}
	return errOut
}
