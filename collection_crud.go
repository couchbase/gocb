package gocb

import (
	"context"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

type kvProvider interface {
	AddEx(opts gocbcore.AddOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	SetEx(opts gocbcore.SetOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	ReplaceEx(opts gocbcore.ReplaceOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	GetEx(opts gocbcore.GetOptions, cb gocbcore.GetExCallback) (gocbcore.PendingOp, error)
	GetAnyReplicaEx(opts gocbcore.GetAnyReplicaOptions, cb gocbcore.GetReplicaExCallback) (gocbcore.PendingOp, error)
	GetOneReplicaEx(opts gocbcore.GetOneReplicaOptions, cb gocbcore.GetReplicaExCallback) (gocbcore.PendingOp, error)
	ObserveEx(opts gocbcore.ObserveOptions, cb gocbcore.ObserveExCallback) (gocbcore.PendingOp, error)
	ObserveVbEx(opts gocbcore.ObserveVbOptions, cb gocbcore.ObserveVbExCallback) (gocbcore.PendingOp, error)
	DeleteEx(opts gocbcore.DeleteOptions, cb gocbcore.DeleteExCallback) (gocbcore.PendingOp, error)
	LookupInEx(opts gocbcore.LookupInOptions, cb gocbcore.LookupInExCallback) (gocbcore.PendingOp, error)
	MutateInEx(opts gocbcore.MutateInOptions, cb gocbcore.MutateInExCallback) (gocbcore.PendingOp, error)
	GetAndTouchEx(opts gocbcore.GetAndTouchOptions, cb gocbcore.GetAndTouchExCallback) (gocbcore.PendingOp, error)
	GetAndLockEx(opts gocbcore.GetAndLockOptions, cb gocbcore.GetAndLockExCallback) (gocbcore.PendingOp, error)
	UnlockEx(opts gocbcore.UnlockOptions, cb gocbcore.UnlockExCallback) (gocbcore.PendingOp, error)
	TouchEx(opts gocbcore.TouchOptions, cb gocbcore.TouchExCallback) (gocbcore.PendingOp, error)
	IncrementEx(opts gocbcore.CounterOptions, cb gocbcore.CounterExCallback) (gocbcore.PendingOp, error)
	DecrementEx(opts gocbcore.CounterOptions, cb gocbcore.CounterExCallback) (gocbcore.PendingOp, error)
	AppendEx(opts gocbcore.AdjoinOptions, cb gocbcore.AdjoinExCallback) (gocbcore.PendingOp, error)
	PrependEx(opts gocbcore.AdjoinOptions, cb gocbcore.AdjoinExCallback) (gocbcore.PendingOp, error)
	PingKvEx(opts gocbcore.PingKvOptions, cb gocbcore.PingKvExCallback) (gocbcore.PendingOp, error)
	NumReplicas() int
}

func (c *Collection) context(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 {
		// no operation level timeouts set, use cluster level
		timeout = c.sb.KvTimeout
	}

	if ctx == nil {
		// no context provided so just make a new one
		return context.WithTimeout(context.Background(), timeout)
	}

	// a context has been provided so add whatever timeout to it. WithTimeout will pick the shortest anyway.
	return context.WithTimeout(ctx, timeout)
}

type opManager struct {
	signal chan struct{}
	ctx    context.Context
}

func (c *Collection) newOpManager(ctx context.Context) *opManager {
	return &opManager{
		signal: make(chan struct{}, 1),
		ctx:    ctx,
	}
}

func (ctrl *opManager) resolve() {
	ctrl.signal <- struct{}{}
}

func (ctrl *opManager) wait(op gocbcore.PendingOp, err error) (errOut error) {
	if err != nil {
		return err
	}

	select {
	case <-ctrl.ctx.Done():
		if op.Cancel() {
			ctxErr := ctrl.ctx.Err()
			if ctxErr == context.DeadlineExceeded {
				errOut = timeoutError{}
			} else {
				errOut = ctxErr
			}
		} else {
			<-ctrl.signal
		}
	case <-ctrl.signal:
	}

	return
}

func (c *Collection) durabilityTimeout(ctx context.Context, durabilityLevel DurabilityLevel) (bool, uint16) {
	var durabilityTimeout uint16
	var coerced bool
	if durabilityLevel > 0 {
		d, _ := ctx.Deadline()
		timeout := d.Sub(time.Now()) / time.Millisecond
		adjustedTimeout := float32(timeout) * 0.9
		if adjustedTimeout < persistenceTimeoutFloor {
			logWarnf("Coercing durability timeout to %d from %d", persistenceTimeoutFloor, adjustedTimeout)
			adjustedTimeout = persistenceTimeoutFloor
			coerced = true
		}

		durabilityTimeout = uint16(adjustedTimeout)
	}

	return coerced, durabilityTimeout
}

// Cas represents the specific state of a document on the cluster.
type Cas gocbcore.Cas

type pendingOp gocbcore.PendingOp

func (c *Collection) verifyObserveOptions(persistTo, replicateTo uint, durabilityLevel DurabilityLevel) error {
	if (persistTo != 0 || replicateTo != 0) && !c.sb.UseMutationTokens {
		return configurationError{"cannot use observe based durability without mutation tokens"}
	}

	if (persistTo != 0 || replicateTo != 0) && durabilityLevel > 0 {
		return configurationError{message: "cannot mix observe based durability and synchronous durability"}
	}

	return nil
}

// UpsertOptions are options that can be applied to an Upsert operation.
type UpsertOptions struct {
	Timeout time.Duration
	Context context.Context
	// The expiration length in seconds
	Expiration      uint32
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Transcoder      Transcoder
}

// InsertOptions are options that can be applied to an Insert operation.
type InsertOptions struct {
	Timeout time.Duration
	Context context.Context
	// The expiration length in seconds
	Expiration      uint32
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Transcoder      Transcoder
}

// Insert creates a new document in the Collection.
func (c *Collection) Insert(key string, val interface{}, opts *InsertOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &InsertOptions{}
	}

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.insert(ctx, key, val, *opts)
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
		forDelete:      false,
		scopeName:      c.scopeName(),
		collectionName: c.name(),
	})
}

func (c *Collection) insert(ctx context.Context, key string, val interface{}, opts InsertOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		errOut = err
		return
	}

	transcoder := opts.Transcoder
	if transcoder == nil {
		transcoder = c.sb.Transcoder
	}

	bytes, flags, err := transcoder.Encode(val)
	if err != nil {
		errOut = err
		return
	}

	coerced, durabilityTimeout := c.durabilityTimeout(ctx, opts.DurabilityLevel)
	if coerced {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(durabilityTimeout)*time.Millisecond)
		defer cancel()
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.AddEx(gocbcore.AddOptions{
		Key:                    []byte(key),
		Value:                  bytes,
		Flags:                  flags,
		Expiry:                 opts.Expiration,
		CollectionName:         c.name(),
		ScopeName:              c.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, key, true)
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

// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
func (c *Collection) Upsert(key string, val interface{}, opts *UpsertOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &UpsertOptions{}
	}

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.upsert(ctx, key, val, *opts)
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
		forDelete:      false,
		scopeName:      c.scopeName(),
		collectionName: c.name(),
	})
}

func (c *Collection) upsert(ctx context.Context, key string, val interface{}, opts UpsertOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		errOut = err
		return
	}

	transcoder := opts.Transcoder
	if transcoder == nil {
		transcoder = c.sb.Transcoder
	}

	bytes, flags, err := transcoder.Encode(val)
	if err != nil {
		errOut = err
		return
	}

	coerced, durabilityTimeout := c.durabilityTimeout(ctx, opts.DurabilityLevel)
	if coerced {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(durabilityTimeout)*time.Millisecond)
		defer cancel()
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.SetEx(gocbcore.SetOptions{
		Key:                    []byte(key),
		Value:                  bytes,
		Flags:                  flags,
		Expiry:                 opts.Expiration,
		CollectionName:         c.name(),
		ScopeName:              c.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, key, false)
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

// ReplaceOptions are the options available to a Replace operation.
type ReplaceOptions struct {
	Timeout         time.Duration
	Context         context.Context
	Expiration      uint32
	Cas             Cas
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Transcoder      Transcoder
}

// Replace updates a document in the collection.
func (c *Collection) Replace(key string, val interface{}, opts *ReplaceOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &ReplaceOptions{}
	}

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.replace(ctx, key, val, *opts)
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
		forDelete:      false,
		scopeName:      c.scopeName(),
		collectionName: c.name(),
	})
}

func (c *Collection) replace(ctx context.Context, key string, val interface{}, opts ReplaceOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	transcoder := opts.Transcoder
	if transcoder == nil {
		transcoder = c.sb.Transcoder
	}

	bytes, flags, err := transcoder.Encode(val)
	if err != nil {
		errOut = err
		return
	}

	coerced, durabilityTimeout := c.durabilityTimeout(ctx, opts.DurabilityLevel)
	if coerced {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(durabilityTimeout)*time.Millisecond)
		defer cancel()
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.ReplaceEx(gocbcore.ReplaceOptions{
		Key:                    []byte(key),
		Value:                  bytes,
		Flags:                  flags,
		Expiry:                 opts.Expiration,
		Cas:                    gocbcore.Cas(opts.Cas),
		CollectionName:         c.name(),
		ScopeName:              c.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, key, false)
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

// GetOptions are the options available to a Get operation.
type GetOptions struct {
	Timeout    time.Duration
	Context    context.Context
	WithExpiry bool
	// Project causes the Get operation to only fetch the fields indicated
	// by the paths. The result of the operation is then treated as a
	// standard GetResult.
	Project    *ProjectOptions
	Transcoder Transcoder
}

// ProjectOptions are the options for using projections as a part of a Get request.
type ProjectOptions struct {
	Fields                 []string
	IgnorePathMissingError bool
}

// Get performs a fetch operation against the collection. This can take 3 paths, a standard full document
// fetch, a subdocument full document fetch also fetching document expiry (when WithExpiry is set),
// or a subdocument fetch (when Project is used).
func (c *Collection) Get(key string, opts *GetOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetOptions{}
	}

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	if opts.Transcoder == nil {
		opts.Transcoder = c.sb.Transcoder
	}

	if (opts.Project == nil || (opts.Project != nil && len(opts.Project.Fields) > 16)) && !opts.WithExpiry {
		// Standard fulldoc
		doc, err := c.get(ctx, key, opts)
		if err != nil {
			return nil, err
		}
		return doc, nil
	}

	lookupOpts := LookupInOptions{Context: ctx, WithExpiry: opts.WithExpiry}
	spec := LookupInSpec{}
	var ops []LookupInOp
	if opts.Project == nil || (len(opts.Project.Fields) > 15 && opts.WithExpiry) {
		// This is a subdoc full doc as WithExpiry is set and projections are either missing or too many.
		ops = append(ops, spec.GetFull(nil))
		opts.Project = &ProjectOptions{}
	} else {
		for _, path := range opts.Project.Fields {
			ops = append(ops, spec.Get(path, nil))
		}
	}

	result, err := c.lookupIn(ctx, key, ops, lookupOpts)
	if err != nil {
		return nil, err
	}

	doc := &GetResult{}
	doc.transcoder = opts.Transcoder
	doc.withExpiration = result.withExpiration
	doc.expiration = result.expiration
	doc.cas = result.cas
	err = doc.fromSubDoc(ops, result, opts.Project.IgnorePathMissingError)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

// get performs a full document fetch against the collection
func (c *Collection) get(ctx context.Context, key string, opts *GetOptions) (docOut *GetResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.GetEx(gocbcore.GetOptions{
		Key:            []byte(key),
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.GetResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, key, false)
			ctrl.resolve()
			return
		}
		if res != nil {
			doc := &GetResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				transcoder: opts.Transcoder,
				contents:   res.Value,
				flags:      res.Flags,
			}

			docOut = doc
		}

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// ExistsOptions are the options available to the Exists command.
type ExistsOptions struct {
	Timeout time.Duration
	Context context.Context
}

// Exists checks if a document exists for the given key.
func (c *Collection) Exists(key string, opts *ExistsOptions) (docOut *ExistsResult, errOut error) {
	if opts == nil {
		opts = &ExistsOptions{}
	}

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.exists(ctx, key, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) exists(ctx context.Context, key string, opts ExistsOptions) (docOut *ExistsResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.ObserveEx(gocbcore.ObserveOptions{
		Key:            []byte(key),
		ReplicaIdx:     0,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.ObserveResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, key, false)
			ctrl.resolve()
			return
		}
		if res != nil {
			doc := &ExistsResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				keyState: res.KeyState,
			}

			docOut = doc
		}

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// GetAnyReplicaOptions are the options available to the GetAnyReplica command.
type GetAnyReplicaOptions struct {
	Timeout    time.Duration
	Context    context.Context
	Transcoder Transcoder
}

// GetAnyReplica returns the value of a particular document from a replica server.
func (c *Collection) GetAnyReplica(key string, opts *GetAnyReplicaOptions) (docOut *GetReplicaResult, errOut error) {
	if opts == nil {
		opts = &GetAnyReplicaOptions{}
	}

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.getAnyReplica(ctx, key, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) getAnyReplica(ctx context.Context, key string,
	opts GetAnyReplicaOptions) (docOut *GetReplicaResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	if opts.Transcoder == nil {
		opts.Transcoder = c.sb.Transcoder
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.GetAnyReplicaEx(gocbcore.GetAnyReplicaOptions{
		Key:            []byte(key),
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.GetReplicaResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, key, false)
			ctrl.resolve()
			return
		}

		doc := &GetReplicaResult{
			GetResult: GetResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				transcoder: opts.Transcoder,
				contents:   res.Value,
				flags:      res.Flags,
			},
		}
		docOut = doc

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// GetAllReplicaOptions are the options available to the GetAllReplicas command.
type GetAllReplicaOptions struct {
	Timeout    time.Duration
	Context    context.Context
	Transcoder Transcoder
}

// GetAllReplicas returns the value of a particular document from all replica servers. This will return an iterable
// which streams results one at a time.
func (c *Collection) GetAllReplicas(key string, opts *GetAllReplicaOptions) (docOut *GetAllReplicasResult, errOut error) {
	if opts == nil {
		opts = &GetAllReplicaOptions{}
	}

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	if opts.Transcoder == nil {
		opts.Transcoder = c.sb.Transcoder
	}

	return &GetAllReplicasResult{
		ctx: ctx,
		opts: gocbcore.GetOneReplicaOptions{
			Key:            []byte(key),
			CollectionName: c.name(),
			ScopeName:      c.scopeName(),
		},
		transcoder:  opts.Transcoder,
		provider:    agent,
		cancel:      cancel,
		maxReplicas: agent.NumReplicas(),
	}, nil
}

// RemoveOptions are the options available to the Remove command.
type RemoveOptions struct {
	Timeout         time.Duration
	Context         context.Context
	Cas             Cas
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
}

// Remove removes a document from the collection.
func (c *Collection) Remove(key string, opts *RemoveOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &RemoveOptions{}
	}

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.remove(ctx, key, *opts)
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

func (c *Collection) remove(ctx context.Context, key string, opts RemoveOptions) (mutOut *MutationResult, errOut error) {
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
	err = ctrl.wait(agent.DeleteEx(gocbcore.DeleteOptions{
		Key:                    []byte(key),
		Cas:                    gocbcore.Cas(opts.Cas),
		CollectionName:         c.name(),
		ScopeName:              c.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
	}, func(res *gocbcore.DeleteResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, key, false)
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

// GetAndTouchOptions are the options available to the GetAndTouch operation.
type GetAndTouchOptions struct {
	Timeout    time.Duration
	Context    context.Context
	Transcoder Transcoder
}

// GetAndTouch retrieves a document and simultaneously updates its expiry time.
func (c *Collection) GetAndTouch(key string, expiration uint32, opts *GetAndTouchOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetAndTouchOptions{}
	}

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.getAndTouch(ctx, key, expiration, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) getAndTouch(ctx context.Context, key string, expiration uint32, opts GetAndTouchOptions) (docOut *GetResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	if opts.Transcoder == nil {
		opts.Transcoder = c.sb.Transcoder
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.GetAndTouchEx(gocbcore.GetAndTouchOptions{
		Key:            []byte(key),
		Expiry:         expiration,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.GetAndTouchResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, key, false)
			ctrl.resolve()
			return
		}
		if res != nil {
			doc := &GetResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				transcoder: opts.Transcoder,
				contents:   res.Value,
				flags:      res.Flags,
			}

			docOut = doc
		}

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// GetAndLockOptions are the options available to the GetAndLock operation.
type GetAndLockOptions struct {
	Timeout    time.Duration
	Context    context.Context
	Transcoder Transcoder
}

// GetAndLock locks a document for a period of time, providing exclusive RW access to it.
func (c *Collection) GetAndLock(key string, lockTime uint32, opts *GetAndLockOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetAndLockOptions{}
	}

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.getAndLock(ctx, key, lockTime, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}
func (c *Collection) getAndLock(ctx context.Context, key string, lockTime uint32, opts GetAndLockOptions) (docOut *GetResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	if opts.Transcoder == nil {
		opts.Transcoder = c.sb.Transcoder
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.GetAndLockEx(gocbcore.GetAndLockOptions{
		Key:            []byte(key),
		LockTime:       lockTime,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.GetAndLockResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, key, false)
			ctrl.resolve()
			return
		}
		if res != nil {
			doc := &GetResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				transcoder: opts.Transcoder,
				contents:   res.Value,
				flags:      res.Flags,
			}

			docOut = doc
		}

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// UnlockOptions are the options available to the GetAndLock operation.
type UnlockOptions struct {
	Timeout time.Duration
	Context context.Context
}

// Unlock unlocks a document which was locked with GetAndLock.
func (c *Collection) Unlock(key string, cas Cas, opts *UnlockOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &UnlockOptions{}
	}

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.unlock(ctx, key, cas, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) unlock(ctx context.Context, key string, cas Cas, opts UnlockOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.UnlockEx(gocbcore.UnlockOptions{
		Key:            []byte(key),
		Cas:            gocbcore.Cas(cas),
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.UnlockResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, key, false)
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

// TouchOptions are the options available to the Touch operation.
type TouchOptions struct {
	Timeout         time.Duration
	Context         context.Context
	DurabilityLevel DurabilityLevel
}

// Touch touches a document, specifying a new expiry time for it.
func (c *Collection) Touch(key string, expiration uint32, opts *TouchOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &TouchOptions{}
	}

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.touch(ctx, key, expiration, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) touch(ctx context.Context, key string, expiration uint32, opts TouchOptions) (mutOut *MutationResult, errOut error) {
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
	err = ctrl.wait(agent.TouchEx(gocbcore.TouchOptions{
		Key:                    []byte(key),
		Expiry:                 expiration,
		CollectionName:         c.name(),
		ScopeName:              c.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
	}, func(res *gocbcore.TouchResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, key, false)
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

// Binary creates and returns a BinaryCollection object.
func (c *Collection) Binary() *BinaryCollection {
	return &BinaryCollection{c}
}
