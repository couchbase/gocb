package gocb

import (
	"time"

	"github.com/couchbase/gocbcore/v8"
)

type bulkOp struct {
	pendop gocbcore.PendingOp
	span   requestSpan
}

func (op *bulkOp) cancel(err error) {
	op.pendop.Cancel(err)
}

func (op *bulkOp) finish() {
	op.span.Finish()
}

// BulkOp represents a single operation that can be submitted (within a list of more operations) to .Do()
// You can create a bulk operation by instantiating one of the implementations of BulkOp,
// such as GetOp, UpsertOp, ReplaceOp, and more.
type BulkOp interface {
	execute(tracectx requestSpanContext, c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp,
		retryWrapper *retryStrategyWrapper, startSpanFunc func(string, requestSpanContext) requestSpan)
	markError(err error)
	cancel(err error)
	finish()
}

// BulkOpOptions are the set of options available when performing BulkOps using Do.
type BulkOpOptions struct {
	Timeout time.Duration

	// Transcoder is used to encode values for operations that perform mutations and to decode values for
	// operations that fetch values. It does not apply to all BulkOp operations.
	Transcoder    Transcoder
	RetryStrategy RetryStrategy
}

// Do execute one or more `BulkOp` items in parallel.
func (c *Collection) Do(ops []BulkOp, opts *BulkOpOptions) error {
	if opts == nil {
		opts = &BulkOpOptions{}
	}

	span := c.startKvOpTrace("Do", nil)

	timeout := c.sb.KvTimeout * time.Duration(len(ops))
	if opts.Timeout != 0 {
		timeout = opts.Timeout
	}

	retryWrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryWrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	if opts.Transcoder == nil {
		opts.Transcoder = c.sb.Transcoder
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return err
	}

	// Make the channel big enough to hold all our ops in case
	//   we get delayed inside execute (don't want to block the
	//   individual op handlers when they dispatch their signal).
	signal := make(chan BulkOp, len(ops))
	for _, item := range ops {
		item.execute(span.Context(), c, agent, opts.Transcoder, signal, retryWrapper, c.startKvOpTrace)
	}

	deadline := time.Now().Add(timeout)
	for range ops {
		select {
		case item := <-signal:
			// We're really just clearing the pendop from this thread,
			//   since it already completed, no cancel actually occurs
			item.finish()
		case <-time.After(deadline.Sub(time.Now())):
			// cancel everything
			for _, item := range ops {
				item.cancel(ErrAmbiguousTimeout)
			}

			// read an item to keep the loop intact
			item := <-signal
			item.finish()
		}
	}
	return nil
}

// GetOp represents a type of `BulkOp` used for Get operations. See BulkOp.
type GetOp struct {
	bulkOp

	ID     string
	Result *GetResult
	Err    error
}

func (item *GetOp) markError(err error) {
	item.Err = err
}

func (item *GetOp) execute(tracectx requestSpanContext, c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp,
	retryWrapper *retryStrategyWrapper, startSpanFunc func(string, requestSpanContext) requestSpan) {
	span := startSpanFunc("GetOp", tracectx)
	item.bulkOp.span = span

	op, err := provider.GetEx(gocbcore.GetOptions{
		Key:            []byte(item.ID),
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   span.Context(),
	}, func(res *gocbcore.GetResult, err error) {
		item.Err = maybeEnhanceCollKVErr(err, provider, c, item.ID)
		if item.Err == nil {
			item.Result = &GetResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				transcoder: transcoder,
				contents:   res.Value,
				flags:      res.Flags,
			}
		}
		signal <- item
	})
	if err != nil {
		item.Err = err
		signal <- item
	} else {
		item.bulkOp.pendop = op
	}
}

// GetAndTouchOp represents a type of `BulkOp` used for GetAndTouch operations. See BulkOp.
type GetAndTouchOp struct {
	bulkOp

	ID     string
	Expiry uint32
	Result *GetResult
	Err    error
}

func (item *GetAndTouchOp) markError(err error) {
	item.Err = err
}

func (item *GetAndTouchOp) execute(tracectx requestSpanContext, c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp,
	retryWrapper *retryStrategyWrapper, startSpanFunc func(string, requestSpanContext) requestSpan) {
	span := startSpanFunc("GetAndTouchOp", tracectx)
	item.bulkOp.span = span

	op, err := provider.GetAndTouchEx(gocbcore.GetAndTouchOptions{
		Key:            []byte(item.ID),
		Expiry:         item.Expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   span.Context(),
	}, func(res *gocbcore.GetAndTouchResult, err error) {
		item.Err = maybeEnhanceCollKVErr(err, provider, c, item.ID)
		if item.Err == nil {
			item.Result = &GetResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				transcoder: transcoder,
				contents:   res.Value,
				flags:      res.Flags,
			}
		}
		signal <- item
	})
	if err != nil {
		item.Err = err
		signal <- item
	} else {
		item.bulkOp.pendop = op
	}
}

// TouchOp represents a type of `BulkOp` used for Touch operations. See BulkOp.
type TouchOp struct {
	bulkOp

	ID     string
	Expiry uint32
	Result *MutationResult
	Err    error
}

func (item *TouchOp) markError(err error) {
	item.Err = err
}

func (item *TouchOp) execute(tracectx requestSpanContext, c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp,
	retryWrapper *retryStrategyWrapper, startSpanFunc func(string, requestSpanContext) requestSpan) {
	span := startSpanFunc("TouchOp", tracectx)
	item.bulkOp.span = span

	op, err := provider.TouchEx(gocbcore.TouchOptions{
		Key:            []byte(item.ID),
		Expiry:         item.Expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   span.Context(),
	}, func(res *gocbcore.TouchResult, err error) {
		item.Err = maybeEnhanceCollKVErr(err, provider, c, item.ID)
		if item.Err == nil {
			item.Result = &MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
			}

			if res.MutationToken.VbUUID != 0 {
				mutTok := &MutationToken{
					token:      res.MutationToken,
					bucketName: c.sb.BucketName,
				}
				item.Result.mt = mutTok
			}
		}
		signal <- item
	})
	if err != nil {
		item.Err = err
		signal <- item
	} else {
		item.bulkOp.pendop = op
	}
}

// RemoveOp represents a type of `BulkOp` used for Remove operations. See BulkOp.
type RemoveOp struct {
	bulkOp

	ID     string
	Cas    Cas
	Result *MutationResult
	Err    error
}

func (item *RemoveOp) markError(err error) {
	item.Err = err
}

func (item *RemoveOp) execute(tracectx requestSpanContext, c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp,
	retryWrapper *retryStrategyWrapper, startSpanFunc func(string, requestSpanContext) requestSpan) {
	span := startSpanFunc("RemoveOp", tracectx)
	item.bulkOp.span = span

	op, err := provider.DeleteEx(gocbcore.DeleteOptions{
		Key:            []byte(item.ID),
		Cas:            gocbcore.Cas(item.Cas),
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   span.Context(),
	}, func(res *gocbcore.DeleteResult, err error) {
		item.Err = maybeEnhanceCollKVErr(err, provider, c, item.ID)
		if item.Err == nil {
			item.Result = &MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
			}

			if res.MutationToken.VbUUID != 0 {
				mutTok := &MutationToken{
					token:      res.MutationToken,
					bucketName: c.sb.BucketName,
				}
				item.Result.mt = mutTok
			}
		}
		signal <- item
	})
	if err != nil {
		item.Err = err
		signal <- item
	} else {
		item.bulkOp.pendop = op
	}
}

// UpsertOp represents a type of `BulkOp` used for Upsert operations. See BulkOp.
type UpsertOp struct {
	bulkOp

	ID     string
	Value  interface{}
	Expiry uint32
	Cas    Cas
	Result *MutationResult
	Err    error
}

func (item *UpsertOp) markError(err error) {
	item.Err = err
}

func (item *UpsertOp) execute(tracectx requestSpanContext, c *Collection, provider kvProvider, transcoder Transcoder,
	signal chan BulkOp, retryWrapper *retryStrategyWrapper, startSpanFunc func(string, requestSpanContext) requestSpan) {
	span := startSpanFunc("UpsertOp", tracectx)
	item.bulkOp.span = span

	etrace := c.startKvOpTrace("encode", span.Context())
	bytes, flags, err := transcoder.Encode(item.Value)
	etrace.Finish()
	if err != nil {
		item.Err = err
		signal <- item
		return
	}

	op, err := provider.SetEx(gocbcore.SetOptions{
		Key:            []byte(item.ID),
		Value:          bytes,
		Flags:          flags,
		Expiry:         item.Expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   span.Context(),
	}, func(res *gocbcore.StoreResult, err error) {
		item.Err = maybeEnhanceCollKVErr(err, provider, c, item.ID)

		if item.Err == nil {
			item.Result = &MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
			}

			if res.MutationToken.VbUUID != 0 {
				mutTok := &MutationToken{
					token:      res.MutationToken,
					bucketName: c.sb.BucketName,
				}
				item.Result.mt = mutTok
			}
		}
		signal <- item
	})
	if err != nil {
		item.Err = err
		signal <- item
	} else {
		item.bulkOp.pendop = op
	}
}

// InsertOp represents a type of `BulkOp` used for Insert operations. See BulkOp.
type InsertOp struct {
	bulkOp

	ID     string
	Value  interface{}
	Expiry uint32
	Result *MutationResult
	Err    error
}

func (item *InsertOp) markError(err error) {
	item.Err = err
}

func (item *InsertOp) execute(tracectx requestSpanContext, c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp,
	retryWrapper *retryStrategyWrapper, startSpanFunc func(string, requestSpanContext) requestSpan) {
	span := startSpanFunc("InsertOp", tracectx)
	item.bulkOp.span = span

	etrace := c.startKvOpTrace("encode", span.Context())
	bytes, flags, err := transcoder.Encode(item.Value)
	if err != nil {
		etrace.Finish()
		item.Err = err
		signal <- item
		return
	}
	etrace.Finish()

	op, err := provider.AddEx(gocbcore.AddOptions{
		Key:            []byte(item.ID),
		Value:          bytes,
		Flags:          flags,
		Expiry:         item.Expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   span.Context(),
	}, func(res *gocbcore.StoreResult, err error) {
		item.Err = maybeEnhanceCollKVErr(err, provider, c, item.ID)
		if item.Err == nil {
			item.Result = &MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
			}

			if res.MutationToken.VbUUID != 0 {
				mutTok := &MutationToken{
					token:      res.MutationToken,
					bucketName: c.sb.BucketName,
				}
				item.Result.mt = mutTok
			}
		}
		signal <- item
	})
	if err != nil {
		item.Err = err
		signal <- item
	} else {
		item.bulkOp.pendop = op
	}
}

// ReplaceOp represents a type of `BulkOp` used for Replace operations. See BulkOp.
type ReplaceOp struct {
	bulkOp

	ID     string
	Value  interface{}
	Expiry uint32
	Cas    Cas
	Result *MutationResult
	Err    error
}

func (item *ReplaceOp) markError(err error) {
	item.Err = err
}

func (item *ReplaceOp) execute(tracectx requestSpanContext, c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp,
	retryWrapper *retryStrategyWrapper, startSpanFunc func(string, requestSpanContext) requestSpan) {
	span := startSpanFunc("ReplaceOp", tracectx)
	item.bulkOp.span = span

	etrace := c.startKvOpTrace("encode", span.Context())
	bytes, flags, err := transcoder.Encode(item.Value)
	if err != nil {
		etrace.Finish()
		item.Err = err
		signal <- item
		return
	}
	etrace.Finish()

	op, err := provider.ReplaceEx(gocbcore.ReplaceOptions{
		Key:            []byte(item.ID),
		Value:          bytes,
		Flags:          flags,
		Cas:            gocbcore.Cas(item.Cas),
		Expiry:         item.Expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   span.Context(),
	}, func(res *gocbcore.StoreResult, err error) {
		item.Err = maybeEnhanceCollKVErr(err, provider, c, item.ID)
		if item.Err == nil {
			item.Result = &MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
			}

			if res.MutationToken.VbUUID != 0 {
				mutTok := &MutationToken{
					token:      res.MutationToken,
					bucketName: c.sb.BucketName,
				}
				item.Result.mt = mutTok
			}
		}
		signal <- item
	})
	if err != nil {
		item.Err = err
		signal <- item
	} else {
		item.bulkOp.pendop = op
	}
}

// AppendOp represents a type of `BulkOp` used for Append operations. See BulkOp.
type AppendOp struct {
	bulkOp

	ID     string
	Value  string
	Result *MutationResult
	Err    error
}

func (item *AppendOp) markError(err error) {
	item.Err = err
}

func (item *AppendOp) execute(tracectx requestSpanContext, c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp,
	retryWrapper *retryStrategyWrapper, startSpanFunc func(string, requestSpanContext) requestSpan) {
	span := startSpanFunc("AppendOp", tracectx)
	item.bulkOp.span = span

	op, err := provider.AppendEx(gocbcore.AdjoinOptions{
		Key:            []byte(item.ID),
		Value:          []byte(item.Value),
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   span.Context(),
	}, func(res *gocbcore.AdjoinResult, err error) {
		item.Err = maybeEnhanceCollKVErr(err, provider, c, item.ID)
		if item.Err == nil {
			item.Result = &MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
			}

			if res.MutationToken.VbUUID != 0 {
				mutTok := &MutationToken{
					token:      res.MutationToken,
					bucketName: c.sb.BucketName,
				}
				item.Result.mt = mutTok
			}
		}
		signal <- item
	})
	if err != nil {
		item.Err = err
		signal <- item
	} else {
		item.bulkOp.pendop = op
	}
}

// PrependOp represents a type of `BulkOp` used for Prepend operations. See BulkOp.
type PrependOp struct {
	bulkOp

	ID     string
	Value  string
	Result *MutationResult
	Err    error
}

func (item *PrependOp) markError(err error) {
	item.Err = err
}

func (item *PrependOp) execute(tracectx requestSpanContext, c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp,
	retryWrapper *retryStrategyWrapper, startSpanFunc func(string, requestSpanContext) requestSpan) {
	span := startSpanFunc("PrependOp", tracectx)
	item.bulkOp.span = span

	op, err := provider.PrependEx(gocbcore.AdjoinOptions{
		Key:            []byte(item.ID),
		Value:          []byte(item.Value),
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   span.Context(),
	}, func(res *gocbcore.AdjoinResult, err error) {
		item.Err = maybeEnhanceCollKVErr(err, provider, c, item.ID)
		if item.Err == nil {
			item.Result = &MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
			}

			if res.MutationToken.VbUUID != 0 {
				mutTok := &MutationToken{
					token:      res.MutationToken,
					bucketName: c.sb.BucketName,
				}
				item.Result.mt = mutTok
			}
		}
		signal <- item
	})
	if err != nil {
		item.Err = err
		signal <- item
	} else {
		item.bulkOp.pendop = op
	}
}

// IncrementOp represents a type of `BulkOp` used for Increment operations. See BulkOp.
type IncrementOp struct {
	bulkOp

	ID      string
	Delta   int64
	Initial int64
	Expiry  uint32

	Result *CounterResult
	Err    error
}

func (item *IncrementOp) markError(err error) {
	item.Err = err
}

func (item *IncrementOp) execute(tracectx requestSpanContext, c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp,
	retryWrapper *retryStrategyWrapper, startSpanFunc func(string, requestSpanContext) requestSpan) {
	span := startSpanFunc("IncrementOp", tracectx)
	item.bulkOp.span = span

	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if item.Initial > 0 {
		realInitial = uint64(item.Initial)
	}

	op, err := provider.IncrementEx(gocbcore.CounterOptions{
		Key:            []byte(item.ID),
		Delta:          uint64(item.Delta),
		Initial:        realInitial,
		Expiry:         item.Expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   span.Context(),
	}, func(res *gocbcore.CounterResult, err error) {
		item.Err = maybeEnhanceCollKVErr(err, provider, c, item.ID)
		if item.Err == nil {
			item.Result = &CounterResult{
				MutationResult: MutationResult{
					Result: Result{
						cas: Cas(res.Cas),
					},
				},
				content: res.Value,
			}

			if res.MutationToken.VbUUID != 0 {
				mutTok := &MutationToken{
					token:      res.MutationToken,
					bucketName: c.sb.BucketName,
				}
				item.Result.mt = mutTok
			}
		}
		signal <- item
	})
	if err != nil {
		item.Err = err
		signal <- item
	} else {
		item.bulkOp.pendop = op
	}
}

// DecrementOp represents a type of `BulkOp` used for Decrement operations. See BulkOp.
type DecrementOp struct {
	bulkOp

	ID      string
	Delta   int64
	Initial int64
	Expiry  uint32

	Result *CounterResult
	Err    error
}

func (item *DecrementOp) markError(err error) {
	item.Err = err
}

func (item *DecrementOp) execute(tracectx requestSpanContext, c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp,
	retryWrapper *retryStrategyWrapper, startSpanFunc func(string, requestSpanContext) requestSpan) {
	span := startSpanFunc("DecrementOp", tracectx)
	item.bulkOp.span = span

	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if item.Initial > 0 {
		realInitial = uint64(item.Initial)
	}

	op, err := provider.DecrementEx(gocbcore.CounterOptions{
		Key:            []byte(item.ID),
		Delta:          uint64(item.Delta),
		Initial:        realInitial,
		Expiry:         item.Expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   span.Context(),
	}, func(res *gocbcore.CounterResult, err error) {
		item.Err = maybeEnhanceCollKVErr(err, provider, c, item.ID)
		if item.Err == nil {
			item.Result = &CounterResult{
				MutationResult: MutationResult{
					Result: Result{
						cas: Cas(res.Cas),
					},
				},
				content: res.Value,
			}

			if res.MutationToken.VbUUID != 0 {
				mutTok := &MutationToken{
					token:      res.MutationToken,
					bucketName: c.sb.BucketName,
				}
				item.Result.mt = mutTok
			}
		}
		signal <- item
	})
	if err != nil {
		item.Err = err
		signal <- item
	} else {
		item.bulkOp.pendop = op
	}
}
