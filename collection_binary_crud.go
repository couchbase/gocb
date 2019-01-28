package gocb

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	"gopkg.in/couchbase/gocbcore.v8"
)

// CollectionBinary is a set of binary operations.
type CollectionBinary struct {
	*Collection
}

// AppendOptions are the options available to the Append operation.
type AppendOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// Append appends a byte value to a document.
func (c *CollectionBinary) Append(key string, val []byte, opts *AppendOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &AppendOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "BinaryAppend")
	defer span.Finish()

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	d := c.deadline(ctx, time.Now(), opts.Timeout)
	if currentD, ok := ctx.Deadline(); ok {
		if d.Before(currentD) {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, d)
			defer cancel()
		}
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, d)
		defer cancel()
	}

	res, err := c.append(ctx, span.Context(), key, val, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *CollectionBinary) append(ctx context.Context, traceCtx opentracing.SpanContext, key string, val []byte, opts AppendOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.AppendEx(gocbcore.AdjoinOptions{
		Key:            []byte(key),
		Value:          val,
		TraceContext:   traceCtx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.AdjoinResult, err error) {
		if err != nil {
			errOut = err
			ctrl.resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.sb.BucketName,
		}
		mutOut = &MutationResult{
			mt: mutTok,
		}
		mutOut.cas = Cas(res.Cas)

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// PrependOptions are the options available to the Prepend operation.
type PrependOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// Prepend prepends a byte value to a document.
func (c *CollectionBinary) Prepend(key string, val []byte, opts *PrependOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &PrependOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "BinaryPrepend")
	defer span.Finish()

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	d := c.deadline(ctx, time.Now(), opts.Timeout)
	if currentD, ok := ctx.Deadline(); ok {
		if d.Before(currentD) {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, d)
			defer cancel()
		}
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, d)
		defer cancel()
	}

	res, err := c.prepend(ctx, span.Context(), key, val, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *CollectionBinary) prepend(ctx context.Context, traceCtx opentracing.SpanContext, key string, val []byte, opts PrependOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.PrependEx(gocbcore.AdjoinOptions{
		Key:            []byte(key),
		Value:          val,
		TraceContext:   traceCtx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.AdjoinResult, err error) {
		if err != nil {
			errOut = err
			ctrl.resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.sb.BucketName,
		}
		mutOut = &MutationResult{
			mt: mutTok,
		}
		mutOut.cas = Cas(res.Cas)

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// CounterOptions are the options available to the Counter operation.
type CounterOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	// Expiration is the length of time in seconds that the document will be stored in Couchbase.
	// A value of 0 will set the document to never expire.
	Expiration uint32
	// Initial, if non-negative, is the `initial` value to use for the document if it does not exist.
	// If present, this is the value that will be returned by a successful operation.
	Initial int64
	// Delta is the value to use for incrementing/decrementing if Initial is not present.
	Delta uint64
}

// Increment performs an atomic addition for an integer document. Passing a
// non-negative `initial` value will cause the document to be created if it did not
// already exist.
func (c *CollectionBinary) Increment(key string, opts *CounterOptions) (countOut *CounterResult, errOut error) {
	if opts == nil {
		opts = &CounterOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Increment")
	defer span.Finish()

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	d := c.deadline(ctx, time.Now(), opts.Timeout)
	if currentD, ok := ctx.Deadline(); ok {
		if d.Before(currentD) {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, d)
			defer cancel()
		}
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, d)
		defer cancel()
	}

	res, err := c.increment(ctx, span.Context(), key, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *CollectionBinary) increment(ctx context.Context, traceCtx opentracing.SpanContext, key string, opts CounterOptions) (countOut *CounterResult, errOut error) {
	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if opts.Initial >= 0 {
		realInitial = uint64(opts.Initial)
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.IncrementEx(gocbcore.CounterOptions{
		Key:            []byte(key),
		Delta:          opts.Delta,
		Initial:        realInitial,
		Expiry:         opts.Expiration,
		TraceContext:   traceCtx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.CounterResult, err error) {
		if err != nil {
			errOut = err
			ctrl.resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.sb.BucketName,
		}
		countOut = &CounterResult{
			MutationResult: MutationResult{
				mt: mutTok,
				Result: Result{
					cas: Cas(res.Cas),
				},
			},
			content: res.Value,
		}

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// Decrement performs an atomic subtraction for an integer document. Passing a
// non-negative `initial` value will cause the document to be created if it did not
// already exist.
func (c *CollectionBinary) Decrement(key string, opts *CounterOptions) (countOut *CounterResult, errOut error) {
	if opts == nil {
		opts = &CounterOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Decrement")
	defer span.Finish()

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	d := c.deadline(ctx, time.Now(), opts.Timeout)
	if currentD, ok := ctx.Deadline(); ok {
		if d.Before(currentD) {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, d)
			defer cancel()
		}
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, d)
		defer cancel()
	}

	res, err := c.decrement(ctx, span.Context(), key, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *CollectionBinary) decrement(ctx context.Context, traceCtx opentracing.SpanContext, key string, opts CounterOptions) (countOut *CounterResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if opts.Initial >= 0 {
		realInitial = uint64(opts.Initial)
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.DecrementEx(gocbcore.CounterOptions{
		Key:            []byte(key),
		Delta:          opts.Delta,
		Initial:        realInitial,
		Expiry:         opts.Expiration,
		TraceContext:   traceCtx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.CounterResult, err error) {
		if err != nil {
			errOut = err
			ctrl.resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.sb.BucketName,
		}
		countOut = &CounterResult{
			MutationResult: MutationResult{
				mt: mutTok,
				Result: Result{
					cas: Cas(res.Cas),
				},
			},
			content: res.Value,
		}

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}
