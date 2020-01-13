package gocb

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcore/v8"
)

type mockClient struct {
	bucketName              string
	useMutationTokens       bool
	collectionID            uint32
	scopeID                 uint32
	mockKvProvider          kvProvider
	mockViewProvider        viewProvider
	mockQueryProvider       queryProvider
	mockAnalyticsProvider   analyticsProvider
	mockSearchProvider      searchProvider
	mockHTTPProvider        httpProvider
	mockDiagnosticsProvider diagnosticsProvider
}

type mockKvProvider struct {
	opWait   time.Duration
	value    interface{}
	cas      gocbcore.Cas
	mt       gocbcore.MutationToken
	flags    uint32
	datatype uint8
	err      error
}

type mockHTTPProvider struct {
	doFn      func(req *gocbcore.HTTPRequest) (*gocbcore.HTTPResponse, error)
	supportFn func(capability gocbcore.ClusterCapability) bool
}

type mockPendingOp struct {
	handler   func(error)
	completed uint32
}

func (mpo *mockPendingOp) maybeInvokeHandler(err error) {
	if atomic.CompareAndSwapUint32(&mpo.completed, 0, 1) {
		mpo.handler(err)
	}
}

func (mpo *mockPendingOp) Cancel(err error) {
	mpo.maybeInvokeHandler(err)
}

func (mko *mockKvProvider) waitForOp(fn func(error)) (gocbcore.PendingOp, error) {
	mpo := &mockPendingOp{
		handler:   fn,
		completed: 0,
	}

	time.AfterFunc(mko.opWait, func() {
		mpo.maybeInvokeHandler(mko.err)
	})

	return mpo, nil
}

func (mko *mockKvProvider) AddEx(opts gocbcore.AddOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.StoreResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		}
	})
}

func (mko *mockKvProvider) SetEx(opts gocbcore.SetOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.StoreResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		}
	})

}

func (mko *mockKvProvider) ReplaceEx(opts gocbcore.ReplaceOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.StoreResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		}
	})
}

func (mko *mockKvProvider) GetEx(opts gocbcore.GetOptions, cb gocbcore.GetExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.GetResult{
				Cas:      mko.cas,
				Flags:    mko.flags,
				Datatype: mko.datatype,
				Value:    mko.value.([]byte),
			}, nil)
		}
	})
}

func (mko *mockKvProvider) GetAndTouchEx(opts gocbcore.GetAndTouchOptions, cb gocbcore.GetAndTouchExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.GetAndTouchResult{
				Cas:      mko.cas,
				Flags:    mko.flags,
				Datatype: mko.datatype,
				Value:    mko.value.([]byte),
			}, nil)
		}
	})
}

func (mko *mockKvProvider) GetAndLockEx(opts gocbcore.GetAndLockOptions, cb gocbcore.GetAndLockExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.GetAndLockResult{
				Cas:      mko.cas,
				Flags:    mko.flags,
				Datatype: mko.datatype,
				Value:    mko.value.([]byte),
			}, nil)
		}
	})
}

func (mko *mockKvProvider) UnlockEx(opts gocbcore.UnlockOptions, cb gocbcore.UnlockExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.UnlockResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		}
	})
}

func (mko *mockKvProvider) TouchEx(opts gocbcore.TouchOptions, cb gocbcore.TouchExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.TouchResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		}
	})
}

func (mko *mockKvProvider) DeleteEx(opts gocbcore.DeleteOptions, cb gocbcore.DeleteExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.DeleteResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		}
	})
}

func (mko *mockKvProvider) IncrementEx(opts gocbcore.CounterOptions, cb gocbcore.CounterExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.CounterResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
				Value:         mko.value.(uint64),
			}, nil)
		}
	})
}

func (mko *mockKvProvider) DecrementEx(opts gocbcore.CounterOptions, cb gocbcore.CounterExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.CounterResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
				Value:         mko.value.(uint64),
			}, nil)
		}
	})
}

func (mko *mockKvProvider) AppendEx(opts gocbcore.AdjoinOptions, cb gocbcore.AdjoinExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.AdjoinResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		}
	})
}

func (mko *mockKvProvider) PrependEx(opts gocbcore.AdjoinOptions, cb gocbcore.AdjoinExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.AdjoinResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		}
	})
}

func (mko *mockKvProvider) LookupInEx(opts gocbcore.LookupInOptions, cb gocbcore.LookupInExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.LookupInResult{
				Cas: mko.cas,
				Ops: mko.value.([]gocbcore.SubDocResult),
			}, nil)
		}
	})
}

func (mko *mockKvProvider) MutateInEx(opts gocbcore.MutateInOptions, cb gocbcore.MutateInExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.MutateInResult{
				Cas:           mko.cas,
				Ops:           mko.value.([]gocbcore.SubDocResult),
				MutationToken: mko.mt,
			}, nil)
		}
	})
}

func (mko *mockKvProvider) ObserveEx(opts gocbcore.ObserveOptions, cb gocbcore.ObserveExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.ObserveResult{
				Cas:      mko.cas,
				KeyState: mko.value.(gocbcore.KeyState),
			}, nil)
		}
	})
}

func (mko *mockKvProvider) ObserveVbEx(opts gocbcore.ObserveVbOptions, cb gocbcore.ObserveVbExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.ObserveVbResult{}, nil)
		}
	})
}

func (mko *mockKvProvider) GetMetaEx(opts gocbcore.GetMetaOptions, cb gocbcore.GetMetaExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.GetMetaResult{
				Cas: mko.cas,
			}, nil)
		}
	})
}

func (mko *mockKvProvider) GetAnyReplicaEx(opts gocbcore.GetAnyReplicaOptions, cb gocbcore.GetReplicaExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.GetReplicaResult{
				Cas:      mko.cas,
				Flags:    mko.flags,
				Datatype: mko.datatype,
				Value:    mko.value.([]byte),
			}, nil)
		}
	})
}

func (mko *mockKvProvider) GetOneReplicaEx(opts gocbcore.GetOneReplicaOptions, cb gocbcore.GetReplicaExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(&gocbcore.GetReplicaResult{
				Cas:      mko.cas,
				Flags:    mko.flags,
				Datatype: mko.datatype,
				Value:    mko.value.([]byte),
			}, nil)
		}
	})
}

func (mko *mockKvProvider) PingKvEx(opts gocbcore.PingKvOptions, cb gocbcore.PingKvExCallback) (gocbcore.PendingOp, error) {
	return mko.waitForOp(func(err error) {
		if err != nil {
			cb(nil, err)
		} else {
			cb(mko.value.(*gocbcore.PingKvResult), nil)
		}
	})
}

func (mko *mockKvProvider) NumReplicas() int {
	return 0
}

func (p *mockHTTPProvider) DoHTTPRequest(req *gocbcore.HTTPRequest) (*gocbcore.HTTPResponse, error) {
	return p.doFn(req)
}

func (p *mockHTTPProvider) MaybeRetryRequest(req gocbcore.RetryRequest, reason gocbcore.RetryReason,
	strategy gocbcore.RetryStrategy, retryFunc func()) bool {
	time.AfterFunc(1*time.Millisecond, retryFunc)
	return true
}

func (p *mockHTTPProvider) SupportsClusterCapability(capability gocbcore.ClusterCapability) bool {
	return p.supportFn(capability)
}

func (mc *mockClient) Hash() string {
	return fmt.Sprintf("%s-%t",
		mc.bucketName,
		mc.useMutationTokens)
}

func (mc *mockClient) connect() error {
	return nil
}

func (mc *mockClient) buildConfig() error {
	return nil
}

func (mc *mockClient) close() error {
	return nil
}

func (mc *mockClient) selectBucket(bucketName string) error {
	return nil
}

func (mc *mockClient) setBootstrapError(err error) {
}

func (mc *mockClient) getBootstrapError() error {
	return nil
}

func (mc *mockClient) supportsGCCCP() bool {
	return true
}

func (mc *mockClient) connected() bool {
	return true
}

func (mc *mockClient) markBucketReady() {
}

func (mc *mockClient) getKvProvider() (kvProvider, error) {
	return mc.mockKvProvider, nil
}

func (mc *mockClient) getViewProvider() (viewProvider, error) {
	return mc.mockViewProvider, nil
}

func (mc *mockClient) getQueryProvider() (queryProvider, error) {
	return mc.mockQueryProvider, nil
}

func (mc *mockClient) getAnalyticsProvider() (analyticsProvider, error) {
	return mc.mockAnalyticsProvider, nil
}

func (mc *mockClient) getSearchProvider() (searchProvider, error) {
	return mc.mockSearchProvider, nil
}

func (mc *mockClient) getHTTPProvider() (httpProvider, error) {
	return mc.mockHTTPProvider, nil
}

func (mc *mockClient) getDiagnosticsProvider() (diagnosticsProvider, error) {
	return mc.mockDiagnosticsProvider, nil
}
