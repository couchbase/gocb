package gocb

import (
	"context"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

// BinaryCollection is a set of binary operations.
type BinaryCollection struct {
	collection *Collection
}

// AppendOptions are the options available to the Append operation.
type AppendOptions struct {
	Timeout         time.Duration
	Context         context.Context
	DurabilityLevel DurabilityLevel
	PersistTo       uint
	ReplicateTo     uint
	Cas             Cas
}

// Append appends a byte value to a document.
func (c *BinaryCollection) Append(id string, val []byte, opts *AppendOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &AppendOptions{}
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	ctx, cancel := c.collection.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.collection.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.append(ctx, id, val, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.collection.durability(durabilitySettings{
		ctx:            opts.Context,
		key:            id,
		cas:            res.Cas(),
		mt:             res.MutationToken(),
		replicaTo:      opts.ReplicateTo,
		persistTo:      opts.PersistTo,
		forDelete:      true,
		scopeName:      c.collection.scopeName(),
		collectionName: c.collection.name(),
	})
}

func (c *BinaryCollection) append(ctx context.Context, id string, val []byte, opts AppendOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.collection.getKvProvider()
	if err != nil {
		return nil, err
	}

	coerced, durabilityTimeout := c.collection.durabilityTimeout(ctx, opts.DurabilityLevel)
	if coerced {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(durabilityTimeout)*time.Millisecond)
		defer cancel()
	}

	ctrl := c.collection.newOpManager(ctx)
	err = ctrl.wait(agent.AppendEx(gocbcore.AdjoinOptions{
		Key:                    []byte(id),
		Value:                  val,
		CollectionName:         c.collection.name(),
		ScopeName:              c.collection.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
		Cas:                    gocbcore.Cas(opts.Cas),
	}, func(res *gocbcore.AdjoinResult, err error) {
		if err != nil {
			errOut = err
			ctrl.resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.collection.sb.BucketName,
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
	Cas             Cas
}

// Prepend prepends a byte value to a document.
func (c *BinaryCollection) Prepend(id string, val []byte, opts *PrependOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &PrependOptions{}
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	ctx, cancel := c.collection.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.collection.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.prepend(ctx, id, val, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.collection.durability(durabilitySettings{
		ctx:            opts.Context,
		key:            id,
		cas:            res.Cas(),
		mt:             res.MutationToken(),
		replicaTo:      opts.ReplicateTo,
		persistTo:      opts.PersistTo,
		forDelete:      true,
		scopeName:      c.collection.scopeName(),
		collectionName: c.collection.name(),
	})
}

func (c *BinaryCollection) prepend(ctx context.Context, id string, val []byte, opts PrependOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.collection.getKvProvider()
	if err != nil {
		return nil, err
	}

	coerced, durabilityTimeout := c.collection.durabilityTimeout(ctx, opts.DurabilityLevel)
	if coerced {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(durabilityTimeout)*time.Millisecond)
		defer cancel()
	}

	ctrl := c.collection.newOpManager(ctx)
	err = ctrl.wait(agent.PrependEx(gocbcore.AdjoinOptions{
		Key:                    []byte(id),
		Value:                  val,
		CollectionName:         c.collection.name(),
		ScopeName:              c.collection.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
		Cas:                    gocbcore.Cas(opts.Cas),
	}, func(res *gocbcore.AdjoinResult, err error) {
		if err != nil {
			errOut = err
			ctrl.resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.collection.sb.BucketName,
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
	// Expiry is the length of time in seconds that the document will be stored in Couchbase.
	// A value of 0 will set the document to never expire.
	Expiry uint32
	// Initial, if non-negative, is the `initial` value to use for the document if it does not exist.
	// If present, this is the value that will be returned by a successful operation.
	Initial int64
	// Delta is the value to use for incrementing/decrementing if Initial is not present.
	Delta           uint64
	DurabilityLevel DurabilityLevel
	PersistTo       uint
	ReplicateTo     uint
	Cas             Cas
}

// Increment performs an atomic addition for an integer document. Passing a
// non-negative `initial` value will cause the document to be created if it did not
// already exist.
func (c *BinaryCollection) Increment(id string, opts *CounterOptions) (countOut *CounterResult, errOut error) {
	if opts == nil {
		opts = &CounterOptions{}
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	ctx, cancel := c.collection.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.increment(ctx, id, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.collection.durability(durabilitySettings{
		ctx:            opts.Context,
		key:            id,
		cas:            res.Cas(),
		mt:             res.MutationToken(),
		replicaTo:      opts.ReplicateTo,
		persistTo:      opts.PersistTo,
		forDelete:      true,
		scopeName:      c.collection.scopeName(),
		collectionName: c.collection.name(),
	})
}

func (c *BinaryCollection) increment(ctx context.Context, id string, opts CounterOptions) (countOut *CounterResult, errOut error) {
	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if opts.Initial >= 0 {
		realInitial = uint64(opts.Initial)
	}

	agent, err := c.collection.getKvProvider()
	if err != nil {
		return nil, err
	}

	coerced, durabilityTimeout := c.collection.durabilityTimeout(ctx, opts.DurabilityLevel)
	if coerced {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(durabilityTimeout)*time.Millisecond)
		defer cancel()
	}

	ctrl := c.collection.newOpManager(ctx)
	err = ctrl.wait(agent.IncrementEx(gocbcore.CounterOptions{
		Key:                    []byte(id),
		Delta:                  opts.Delta,
		Initial:                realInitial,
		Expiry:                 opts.Expiry,
		CollectionName:         c.collection.name(),
		ScopeName:              c.collection.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
		Cas:                    gocbcore.Cas(opts.Cas),
	}, func(res *gocbcore.CounterResult, err error) {
		if err != nil {
			errOut = err
			ctrl.resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.collection.sb.BucketName,
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
func (c *BinaryCollection) Decrement(id string, opts *CounterOptions) (countOut *CounterResult, errOut error) {
	if opts == nil {
		opts = &CounterOptions{}
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	ctx, cancel := c.collection.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.collection.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.decrement(ctx, id, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.collection.durability(durabilitySettings{
		ctx:            opts.Context,
		key:            id,
		cas:            res.Cas(),
		mt:             res.MutationToken(),
		replicaTo:      opts.ReplicateTo,
		persistTo:      opts.PersistTo,
		forDelete:      true,
		scopeName:      c.collection.scopeName(),
		collectionName: c.collection.name(),
	})
}

func (c *BinaryCollection) decrement(ctx context.Context, id string, opts CounterOptions) (countOut *CounterResult, errOut error) {
	agent, err := c.collection.getKvProvider()
	if err != nil {
		return nil, err
	}

	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if opts.Initial >= 0 {
		realInitial = uint64(opts.Initial)
	}

	coerced, durabilityTimeout := c.collection.durabilityTimeout(ctx, opts.DurabilityLevel)
	if coerced {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(durabilityTimeout)*time.Millisecond)
		defer cancel()
	}

	ctrl := c.collection.newOpManager(ctx)
	err = ctrl.wait(agent.DecrementEx(gocbcore.CounterOptions{
		Key:                    []byte(id),
		Delta:                  opts.Delta,
		Initial:                realInitial,
		Expiry:                 opts.Expiry,
		CollectionName:         c.collection.name(),
		ScopeName:              c.collection.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
		Cas:                    gocbcore.Cas(opts.Cas),
	}, func(res *gocbcore.CounterResult, err error) {
		if err != nil {
			errOut = err
			ctrl.resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.collection.sb.BucketName,
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
