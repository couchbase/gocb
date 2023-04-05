package gocb

import (
	"context"
	"time"

	"github.com/couchbase/gocbcore/v10"
)

type waitUntilReadyProvider interface {
	WaitUntilReady(ctx context.Context, deadline time.Time, opts *WaitUntilReadyOptions) error
}

type gocbcoreWaitUntilReadyProvider interface {
	WaitUntilReady(deadline time.Time, opts gocbcore.WaitUntilReadyOptions,
		cb gocbcore.WaitUntilReadyCallback) (gocbcore.PendingOp, error)
}

type waitUntilReadyProviderCore struct {
	retryStrategyWrapper *retryStrategyWrapper
	provider             gocbcoreWaitUntilReadyProvider
}

func (wpw *waitUntilReadyProviderCore) WaitUntilReady(ctx context.Context, deadline time.Time,
	opts *WaitUntilReadyOptions) error {
	desiredState := opts.DesiredState
	if desiredState == 0 {
		desiredState = ClusterStateOnline
	}

	gocbcoreServices := make([]gocbcore.ServiceType, len(opts.ServiceTypes))
	for i, svc := range opts.ServiceTypes {
		gocbcoreServices[i] = gocbcore.ServiceType(svc)
	}

	wrapper := wpw.retryStrategyWrapper
	if opts.RetryStrategy != nil {
		wrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	coreOpts := gocbcore.WaitUntilReadyOptions{
		DesiredState:  gocbcore.ClusterState(desiredState),
		ServiceTypes:  gocbcoreServices,
		RetryStrategy: wrapper,
	}

	var errOut error
	opm := newAsyncOpManager(ctx)
	err := opm.Wait(wpw.provider.WaitUntilReady(deadline, coreOpts, func(res *gocbcore.WaitUntilReadyResult, err error) {
		if err != nil {
			errOut = maybeEnhanceCoreErr(err)
			opm.Reject()
			return
		}

		opm.Resolve()
	}))
	if err != nil {
		errOut = maybeEnhanceCoreErr(err)
	}

	return errOut
}

type waitUntilReadyProviderPs struct {
}

func (wpw *waitUntilReadyProviderPs) WaitUntilReady(ctx context.Context, deadline time.Time,
	opts *WaitUntilReadyOptions) error {
	return nil
}
