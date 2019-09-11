package gocb

import (
	"context"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

// BinaryCollection is a set of binary operations.
type BinaryCollection struct {
	*Collection
}

// AppendOptions are the options available to the Append operation.
type AppendOptions struct {
	Timeout         time.Duration
	Context         context.Context
	DurabilityLevel DurabilityLevel
	PersistTo       uint
	ReplicateTo     uint
}

// Append appends a byte value to a document.
func (c *BinaryCollection) Append(key string, val []byte, opts *AppendOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &AppendOptions{}
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.append(ctx, key, val, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(durabilitySettings{
		ctx:            opts.Context,
		key:            key,
		cas:            res.Cas(),
		mt:             res.MutationToken(),
		replicaTo:      opts.ReplicateTo,
		persistTo:      opts.PersistTo,
		forDelete:      true,
		scopeName:      c.scopeName(),
		collectionName: c.name(),
	})
}

func (c *BinaryCollection) append(ctx context.Context, key string, val []byte, opts AppendOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	coerced, durabilityTimeout := c.durabilityTimeout(ctx, opts.DurabilityLevel)
	if coerced {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(durabilityTimeout)*time.Millisecond)
		defer cancel()
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.AppendEx(gocbcore.AdjoinOptions{
		Key:                    []byte(key),
		Value:                  val,
		CollectionName:         c.name(),
		ScopeName:              c.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
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
	Timeout         time.Duration
	Context         context.Context
	DurabilityLevel DurabilityLevel
	PersistTo       uint
	ReplicateTo     uint
}

// Prepend prepends a byte value to a document.
func (c *BinaryCollection) Prepend(key string, val []byte, opts *PrependOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &PrependOptions{}
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.prepend(ctx, key, val, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(durabilitySettings{
		ctx:            opts.Context,
		key:            key,
		cas:            res.Cas(),
		mt:             res.MutationToken(),
		replicaTo:      opts.ReplicateTo,
		persistTo:      opts.PersistTo,
		forDelete:      true,
		scopeName:      c.scopeName(),
		collectionName: c.name(),
	})
}

func (c *BinaryCollection) prepend(ctx context.Context, key string, val []byte, opts PrependOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	coerced, durabilityTimeout := c.durabilityTimeout(ctx, opts.DurabilityLevel)
	if coerced {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(durabilityTimeout)*time.Millisecond)
		defer cancel()
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.PrependEx(gocbcore.AdjoinOptions{
		Key:                    []byte(key),
		Value:                  val,
		CollectionName:         c.name(),
		ScopeName:              c.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
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
	Timeout time.Duration
	Context context.Context
	// Expiration is the length of time in seconds that the document will be stored in Couchbase.
	// A value of 0 will set the document to never expire.
	Expiration uint32
	// Initial, if non-negative, is the `initial` value to use for the document if it does not exist.
	// If present, this is the value that will be returned by a successful operation.
	Initial int64
	// Delta is the value to use for incrementing/decrementing if Initial is not present.
	Delta           uint64
	DurabilityLevel DurabilityLevel
	PersistTo       uint
	ReplicateTo     uint
}

// Increment performs an atomic addition for an integer document. Passing a
// non-negative `initial` value will cause the document to be created if it did not
// already exist.
func (c *BinaryCollection) Increment(key string, opts *CounterOptions) (countOut *CounterResult, errOut error) {
	if opts == nil {
		opts = &CounterOptions{}
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.increment(ctx, key, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(durabilitySettings{
		ctx:            opts.Context,
		key:            key,
		cas:            res.Cas(),
		mt:             res.MutationToken(),
		replicaTo:      opts.ReplicateTo,
		persistTo:      opts.PersistTo,
		forDelete:      true,
		scopeName:      c.scopeName(),
		collectionName: c.name(),
	})
}

func (c *BinaryCollection) increment(ctx context.Context, key string, opts CounterOptions) (countOut *CounterResult, errOut error) {
	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if opts.Initial >= 0 {
		realInitial = uint64(opts.Initial)
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	coerced, durabilityTimeout := c.durabilityTimeout(ctx, opts.DurabilityLevel)
	if coerced {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(durabilityTimeout)*time.Millisecond)
		defer cancel()
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.IncrementEx(gocbcore.CounterOptions{
		Key:                    []byte(key),
		Delta:                  opts.Delta,
		Initial:                realInitial,
		Expiry:                 opts.Expiration,
		CollectionName:         c.name(),
		ScopeName:              c.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
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
func (c *BinaryCollection) Decrement(key string, opts *CounterOptions) (countOut *CounterResult, errOut error) {
	if opts == nil {
		opts = &CounterOptions{}
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.decrement(ctx, key, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(durabilitySettings{
		ctx:            opts.Context,
		key:            key,
		cas:            res.Cas(),
		mt:             res.MutationToken(),
		replicaTo:      opts.ReplicateTo,
		persistTo:      opts.PersistTo,
		forDelete:      true,
		scopeName:      c.scopeName(),
		collectionName: c.name(),
	})
}

func (c *BinaryCollection) decrement(ctx context.Context, key string, opts CounterOptions) (countOut *CounterResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if opts.Initial >= 0 {
		realInitial = uint64(opts.Initial)
	}

	coerced, durabilityTimeout := c.durabilityTimeout(ctx, opts.DurabilityLevel)
	if coerced {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(durabilityTimeout)*time.Millisecond)
		defer cancel()
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.DecrementEx(gocbcore.CounterOptions{
		Key:                    []byte(key),
		Delta:                  opts.Delta,
		Initial:                realInitial,
		Expiry:                 opts.Expiration,
		CollectionName:         c.name(),
		ScopeName:              c.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
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
