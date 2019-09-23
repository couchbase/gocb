package gocb

import (
	"context"
	"time"

	"github.com/couchbase/gocbcore/v8"
)

type bulkOp struct {
	pendop gocbcore.PendingOp
}

func (op *bulkOp) cancel() bool {
	if op.pendop == nil {
		return true
	}
	res := op.pendop.Cancel()
	op.pendop = nil
	return res
}

// BulkOp represents a single operation that can be submitted (within a list of more operations) to .Do()
// You can create a bulk operation by instantiating one of the implementations of BulkOp,
// such as GetOp, UpsertOp, ReplaceOp, and more.
type BulkOp interface {
	execute(c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp)
	markError(err error)
	cancel() bool
}

// BulkOpOptions are the set of options available when performing BulkOps using Do.
type BulkOpOptions struct {
	Timeout time.Duration
	Context context.Context

	// Transcoder is used to encode values for operations that perform mutations and to decode values for
	// operations that fetch values. It does not apply to all BulkOp operations.
	Transcoder Transcoder
}

// Do execute one or more `BulkOp` items in parallel.
func (c *Collection) Do(ops []BulkOp, opts *BulkOpOptions) error {
	if opts == nil {
		opts = &BulkOpOptions{}
	}

	if opts.Timeout == 0 {
		// no operation level timeouts set, use cluster level
		opts.Timeout = c.sb.KvTimeout * time.Duration(len(ops))
	}

	var ctx context.Context
	var cancel context.CancelFunc
	if opts.Context == nil {
		// no context provided so just make a new one
		ctx, cancel = context.WithTimeout(context.Background(), opts.Timeout)
	} else {
		// a context has been provided so add whatever timeout to it. WithTimeout will pick the shortest anyway.
		ctx, cancel = context.WithTimeout(opts.Context, opts.Timeout)
	}
	defer cancel()

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
		item.execute(c, agent, opts.Transcoder, signal)
	}
	for range ops {
		select {
		case item := <-signal:
			// We're really just clearing the pendop from this thread,
			//   since it already completed, no cancel actually occurs
			item.cancel()
		case <-ctx.Done():
			for _, item := range ops {
				if !item.cancel() {
					<-signal
					continue
				}

				// We use this method to mark the individual items as
				// having timed out so we don't move `Err` in bulkOp
				// and break backwards compatibility.
				item.markError(timeoutError{})
			}
			return timeoutError{}
		}
	}
	return nil
}

// GetOp represents a type of `BulkOp` used for Get operations. See BulkOp.
type GetOp struct {
	bulkOp

	Key    string
	Result *GetResult
	Err    error
}

func (item *GetOp) markError(err error) {
	item.Err = err
}

func (item *GetOp) execute(c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp) {
	op, err := provider.GetEx(gocbcore.GetOptions{
		Key:            []byte(item.Key),
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.GetResult, err error) {
		item.Err = maybeEnhanceKVErr(err, item.Key, false)
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

	Key    string
	Expiry uint32
	Result *GetResult
	Err    error
}

func (item *GetAndTouchOp) markError(err error) {
	item.Err = err
}

func (item *GetAndTouchOp) execute(c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp) {
	op, err := provider.GetAndTouchEx(gocbcore.GetAndTouchOptions{
		Key:            []byte(item.Key),
		Expiry:         item.Expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.GetAndTouchResult, err error) {
		item.Err = maybeEnhanceKVErr(err, item.Key, false)
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

	Key    string
	Expiry uint32
	Result *MutationResult
	Err    error
}

func (item *TouchOp) markError(err error) {
	item.Err = err
}

func (item *TouchOp) execute(c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp) {
	op, err := provider.TouchEx(gocbcore.TouchOptions{
		Key:            []byte(item.Key),
		Expiry:         item.Expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.TouchResult, err error) {
		item.Err = maybeEnhanceKVErr(err, item.Key, false)
		if item.Err == nil {
			mutTok := MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			item.Result = &MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				mt: mutTok,
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

	Key    string
	Cas    Cas
	Result *MutationResult
	Err    error
}

func (item *RemoveOp) markError(err error) {
	item.Err = err
}

func (item *RemoveOp) execute(c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp) {
	op, err := provider.DeleteEx(gocbcore.DeleteOptions{
		Key:            []byte(item.Key),
		Cas:            gocbcore.Cas(item.Cas),
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.DeleteResult, err error) {
		item.Err = maybeEnhanceKVErr(err, item.Key, false)
		if item.Err == nil {
			mutTok := MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			item.Result = &MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				mt: mutTok,
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

	Key    string
	Value  interface{}
	Expiry uint32
	Cas    Cas
	Result *MutationResult
	Err    error
}

func (item *UpsertOp) markError(err error) {
	item.Err = err
}

func (item *UpsertOp) execute(c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp) {
	bytes, flags, err := transcoder.Encode(item.Value)
	if err != nil {
		item.Err = err
		signal <- item
		return
	}

	op, err := provider.SetEx(gocbcore.SetOptions{
		Key:            []byte(item.Key),
		Value:          bytes,
		Flags:          flags,
		Expiry:         item.Expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.StoreResult, err error) {
		item.Err = maybeEnhanceKVErr(err, item.Key, false)
		if item.Err == nil {
			mutTok := MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			item.Result = &MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				mt: mutTok,
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

	Key    string
	Value  interface{}
	Expiry uint32
	Result *MutationResult
	Err    error
}

func (item *InsertOp) markError(err error) {
	item.Err = err
}

func (item *InsertOp) execute(c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp) {
	bytes, flags, err := transcoder.Encode(item.Value)
	if err != nil {
		item.Err = err
		signal <- item
		return
	}

	op, err := provider.AddEx(gocbcore.AddOptions{
		Key:            []byte(item.Key),
		Value:          bytes,
		Flags:          flags,
		Expiry:         item.Expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.StoreResult, err error) {
		item.Err = maybeEnhanceKVErr(err, item.Key, true)
		if item.Err == nil {
			mutTok := MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			item.Result = &MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				mt: mutTok,
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

	Key    string
	Value  interface{}
	Expiry uint32
	Cas    Cas
	Result *MutationResult
	Err    error
}

func (item *ReplaceOp) markError(err error) {
	item.Err = err
}

func (item *ReplaceOp) execute(c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp) {
	bytes, flags, err := transcoder.Encode(item.Value)
	if err != nil {
		item.Err = err
		signal <- item
		return
	}

	op, err := provider.ReplaceEx(gocbcore.ReplaceOptions{
		Key:            []byte(item.Key),
		Value:          bytes,
		Flags:          flags,
		Cas:            gocbcore.Cas(item.Cas),
		Expiry:         item.Expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.StoreResult, err error) {
		item.Err = maybeEnhanceKVErr(err, item.Key, true)
		if item.Err == nil {
			mutTok := MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			item.Result = &MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				mt: mutTok,
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

	Key    string
	Value  string
	Result *MutationResult
	Err    error
}

func (item *AppendOp) markError(err error) {
	item.Err = err
}

func (item *AppendOp) execute(c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp) {
	op, err := provider.AppendEx(gocbcore.AdjoinOptions{
		Key:            []byte(item.Key),
		Value:          []byte(item.Value),
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.AdjoinResult, err error) {
		item.Err = maybeEnhanceKVErr(err, item.Key, true)
		if item.Err == nil {
			mutTok := MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			item.Result = &MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				mt: mutTok,
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

	Key    string
	Value  string
	Result *MutationResult
	Err    error
}

func (item *PrependOp) markError(err error) {
	item.Err = err
}

func (item *PrependOp) execute(c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp) {
	op, err := provider.PrependEx(gocbcore.AdjoinOptions{
		Key:            []byte(item.Key),
		Value:          []byte(item.Value),
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.AdjoinResult, err error) {
		item.Err = maybeEnhanceKVErr(err, item.Key, true)
		if item.Err == nil {
			mutTok := MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			item.Result = &MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				mt: mutTok,
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

	Key     string
	Delta   int64
	Initial int64
	Expiry  uint32

	Result *CounterResult
	Err    error
}

func (item *IncrementOp) markError(err error) {
	item.Err = err
}

func (item *IncrementOp) execute(c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp) {
	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if item.Initial > 0 {
		realInitial = uint64(item.Initial)
	}

	op, err := provider.IncrementEx(gocbcore.CounterOptions{
		Key:            []byte(item.Key),
		Delta:          uint64(item.Delta),
		Initial:        realInitial,
		Expiry:         item.Expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.CounterResult, err error) {
		item.Err = maybeEnhanceKVErr(err, item.Key, true)
		if item.Err == nil {
			mutTok := MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			item.Result = &CounterResult{
				MutationResult: MutationResult{
					mt: mutTok,
					Result: Result{
						cas: Cas(res.Cas),
					},
				},
				content: res.Value,
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

	Key     string
	Delta   int64
	Initial int64
	Expiry  uint32

	Result *CounterResult
	Err    error
}

func (item *DecrementOp) markError(err error) {
	item.Err = err
}

func (item *DecrementOp) execute(c *Collection, provider kvProvider, transcoder Transcoder, signal chan BulkOp) {
	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if item.Initial > 0 {
		realInitial = uint64(item.Initial)
	}

	op, err := provider.DecrementEx(gocbcore.CounterOptions{
		Key:            []byte(item.Key),
		Delta:          uint64(item.Delta),
		Initial:        realInitial,
		Expiry:         item.Expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.CounterResult, err error) {
		item.Err = maybeEnhanceKVErr(err, item.Key, true)
		if item.Err == nil {
			mutTok := MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			item.Result = &CounterResult{
				MutationResult: MutationResult{
					mt: mutTok,
					Result: Result{
						cas: Cas(res.Cas),
					},
				},
				content: res.Value,
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
