package gocb

import (
	"context"
	"fmt"
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
	PingKvEx(opts gocbcore.PingKvOptions, cb gocbcore.PingKvExCallback) (gocbcore.CancellablePendingOp, error)
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
	signal    chan struct{}
	ctx       context.Context
	startTime time.Time
	operation string
}

func (c *Collection) newOpManager(ctx context.Context, start time.Time, operation string) *opManager {
	return &opManager{
		signal:    make(chan struct{}, 1),
		ctx:       ctx,
		startTime: start,
		operation: operation,
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
				err := timeoutError{}
				err.operation = fmt.Sprintf("kv:%s", ctrl.operation)
				err.operationID = op.Identifier()
				err.retryAttempts = op.RetryAttempts()
				err.retryReasons = op.RetryReasons()
				err.elapsed = time.Now().Sub(ctrl.startTime)
				err.local = op.LocalEndpoint()
				err.remote = op.RemoteEndpoint()
				err.connectionID = op.ConnectionId()
				errOut = err
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
		return invalidArgumentsError{"cannot use observe based durability without mutation tokens"}
	}

	if (persistTo != 0 || replicateTo != 0) && durabilityLevel > 0 {
		return invalidArgumentsError{message: "cannot mix observe based durability and synchronous durability"}
	}

	return nil
}

// UpsertOptions are options that can be applied to an Upsert operation.
type UpsertOptions struct {
	Timeout time.Duration
	Context context.Context
	// The expiry length in seconds
	Expiry          uint32
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Transcoder      Transcoder
	RetryStrategy   RetryStrategy
}

// InsertOptions are options that can be applied to an Insert operation.
type InsertOptions struct {
	Timeout time.Duration
	Context context.Context
	// The expiry length in seconds
	Expiry          uint32
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Transcoder      Transcoder
	RetryStrategy   RetryStrategy
}

// Insert creates a new document in the Collection.
func (c *Collection) Insert(id string, val interface{}, opts *InsertOptions) (mutOut *MutationResult, errOut error) {
	startTime := time.Now()
	if opts == nil {
		opts = &InsertOptions{}
	}

	span := c.startKvOpTrace("Insert", nil)
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.insert(ctx, span.Context(), id, val, startTime, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(durabilitySettings{
		ctx:            opts.Context,
		key:            id,
		cas:            res.Cas(),
		mt:             *res.MutationToken(),
		replicaTo:      opts.ReplicateTo,
		persistTo:      opts.PersistTo,
		forDelete:      false,
		scopeName:      c.scopeName(),
		collectionName: c.name(),
	})
}

func (c *Collection) insert(ctx context.Context, tracectx requestSpanContext, id string, val interface{},
	startTime time.Time, opts InsertOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		errOut = err
		return
	}

	transcoder := opts.Transcoder
	if transcoder == nil {
		transcoder = c.sb.Transcoder
	}

	espan := c.startKvOpTrace("encode", tracectx)
	bytes, flags, err := transcoder.Encode(val)
	espan.Finish()
	if err != nil {
		errOut = err
		return
	}

	retryWrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryWrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	coerced, durabilityTimeout := c.durabilityTimeout(ctx, opts.DurabilityLevel)
	if coerced {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(durabilityTimeout)*time.Millisecond)
		defer cancel()
	}

	ctrl := c.newOpManager(ctx, startTime, "Insert")
	err = ctrl.wait(agent.AddEx(gocbcore.AddOptions{
		Key:                    []byte(id),
		Value:                  bytes,
		Flags:                  flags,
		Expiry:                 opts.Expiry,
		CollectionName:         c.name(),
		ScopeName:              c.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
		RetryStrategy:          retryWrapper,
		TraceContext:           tracectx,
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, id, true)
			ctrl.resolve()
			return
		}

		mutOut = &MutationResult{}
		mutOut.cas = Cas(res.Cas)

		if res.MutationToken.VbUuid != 0 {
			mutTok := &MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			mutOut.mt = mutTok
		}

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
func (c *Collection) Upsert(id string, val interface{}, opts *UpsertOptions) (mutOut *MutationResult, errOut error) {
	startTime := time.Now()
	if opts == nil {
		opts = &UpsertOptions{}
	}

	span := c.startKvOpTrace("Upsert", nil)
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.upsert(ctx, span.Context(), id, val, startTime, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(durabilitySettings{
		ctx:            opts.Context,
		key:            id,
		cas:            res.Cas(),
		mt:             *res.MutationToken(),
		replicaTo:      opts.ReplicateTo,
		persistTo:      opts.PersistTo,
		forDelete:      false,
		scopeName:      c.scopeName(),
		collectionName: c.name(),
	})
}
func (c *Collection) upsert(ctx context.Context, tracectx requestSpanContext, id string, val interface{},
	startTime time.Time, opts UpsertOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		errOut = err
		return
	}

	transcoder := opts.Transcoder
	if transcoder == nil {
		transcoder = c.sb.Transcoder
	}

	retryWrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryWrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	espan := c.startKvOpTrace("encode", tracectx)
	bytes, flags, err := transcoder.Encode(val)
	espan.Finish()
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

	ctrl := c.newOpManager(ctx, startTime, "Upsert")
	err = ctrl.wait(agent.SetEx(gocbcore.SetOptions{
		Key:                    []byte(id),
		Value:                  bytes,
		Flags:                  flags,
		Expiry:                 opts.Expiry,
		CollectionName:         c.name(),
		ScopeName:              c.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
		RetryStrategy:          retryWrapper,
		TraceContext:           tracectx,
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, id, false)
			ctrl.resolve()
			return
		}

		mutOut = &MutationResult{}
		mutOut.cas = Cas(res.Cas)

		if res.MutationToken.VbUuid != 0 {
			mutTok := &MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			mutOut.mt = mutTok
		}

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
	Expiry          uint32
	Cas             Cas
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Transcoder      Transcoder
	RetryStrategy   RetryStrategy
}

// Replace updates a document in the collection.
func (c *Collection) Replace(id string, val interface{}, opts *ReplaceOptions) (mutOut *MutationResult, errOut error) {
	startTime := time.Now()
	if opts == nil {
		opts = &ReplaceOptions{}
	}

	span := c.startKvOpTrace("Replace", nil)
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.replace(ctx, span.Context(), id, val, startTime, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(durabilitySettings{
		ctx:            opts.Context,
		key:            id,
		cas:            res.Cas(),
		mt:             *res.MutationToken(),
		replicaTo:      opts.ReplicateTo,
		persistTo:      opts.PersistTo,
		forDelete:      false,
		scopeName:      c.scopeName(),
		collectionName: c.name(),
	})
}

func (c *Collection) replace(ctx context.Context, tracectx requestSpanContext, id string, val interface{},
	startTime time.Time, opts ReplaceOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	transcoder := opts.Transcoder
	if transcoder == nil {
		transcoder = c.sb.Transcoder
	}

	retryWrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryWrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	espan := c.startKvOpTrace("encode", tracectx)
	bytes, flags, err := transcoder.Encode(val)
	espan.Finish()
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

	ctrl := c.newOpManager(ctx, startTime, "Replace")
	err = ctrl.wait(agent.ReplaceEx(gocbcore.ReplaceOptions{
		Key:                    []byte(id),
		Value:                  bytes,
		Flags:                  flags,
		Expiry:                 opts.Expiry,
		Cas:                    gocbcore.Cas(opts.Cas),
		CollectionName:         c.name(),
		ScopeName:              c.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
		RetryStrategy:          retryWrapper,
		TraceContext:           tracectx,
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, id, false)
			ctrl.resolve()
			return
		}

		mutOut = &MutationResult{}
		mutOut.cas = Cas(res.Cas)

		if res.MutationToken.VbUuid != 0 {
			mutTok := &MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			mutOut.mt = mutTok
		}

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
	Project       []string
	Transcoder    Transcoder
	RetryStrategy RetryStrategy
}

// Get performs a fetch operation against the collection. This can take 3 paths, a standard full document
// fetch, a subdocument full document fetch also fetching document expiry (when WithExpiry is set),
// or a subdocument fetch (when Project is used).
func (c *Collection) Get(id string, opts *GetOptions) (docOut *GetResult, errOut error) {
	startTime := time.Now()
	if opts == nil {
		opts = &GetOptions{}
	}

	span := c.startKvOpTrace("Get", nil)
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	if opts.Transcoder == nil {
		opts.Transcoder = c.sb.Transcoder
	}

	projections := opts.Project
	if len(projections) == 0 && !opts.WithExpiry {
		// Standard fulldoc
		doc, err := c.get(ctx, span.Context(), id, startTime, opts)
		if err != nil {
			return nil, err
		}
		return doc, nil
	}

	if len(projections) > 16 {
		// Too many for subdoc so we need to do a full doc fetch
		projections = nil
	}
	if len(projections) > 15 && opts.WithExpiry {
		// Expiration will push us over subdoc limit so we need to do a full doc fetch
		projections = nil
	}

	var ops []LookupInSpec
	lookupOpts := &LookupInOptions{Context: ctx}

	if opts.WithExpiry {
		ops = append(ops, GetSpec("$document.exptime", &GetSpecOptions{IsXattr: true}))
	}

	if len(projections) == 0 {
		ops = append(ops, GetSpec("", nil))
	} else {
		for _, path := range projections {
			ops = append(ops, GetSpec(path, nil))
		}
	}

	result, err := c.lookupIn(ctx, span.Context(), id, ops, startTime, *lookupOpts)
	if err != nil {
		return nil, err
	}

	doc := &GetResult{}
	if opts.WithExpiry {
		// if expiration was requested then extract and remove it from the results
		err = result.ContentAt(0, &doc.expiry)
		if err != nil {
			return nil, err
		}
		ops = ops[1:]
		result.contents = result.contents[1:]
	}

	doc.transcoder = opts.Transcoder
	doc.cas = result.cas
	if projections == nil {
		err = doc.fromFullProjection(ops, result, opts.Project)
		if err != nil {
			return nil, err
		}
	} else {
		err = doc.fromSubDoc(ops, result)
		if err != nil {
			return nil, err
		}
	}

	return doc, nil
}

// get performs a full document fetch against the collection
func (c *Collection) get(ctx context.Context, tracectx requestSpanContext, id string,
	startTime time.Time, opts *GetOptions) (docOut *GetResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	retryWrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryWrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	span := c.startKvOpTrace("get", tracectx)
	defer span.Finish()

	ctrl := c.newOpManager(ctx, startTime, "Get")
	err = ctrl.wait(agent.GetEx(gocbcore.GetOptions{
		Key:            []byte(id),
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   span.Context(),
	}, func(res *gocbcore.GetResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, id, false)
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
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// Exists checks if a document exists for the given id.
func (c *Collection) Exists(id string, opts *ExistsOptions) (docOut *ExistsResult, errOut error) {
	startTime := time.Now()
	if opts == nil {
		opts = &ExistsOptions{}
	}

	span := c.startKvOpTrace("Exists", nil)
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.exists(ctx, span.Context(), id, startTime, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) exists(ctx context.Context, tracectx requestSpanContext, id string, startTime time.Time,
	opts ExistsOptions) (docOut *ExistsResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	retryWrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryWrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	ctrl := c.newOpManager(ctx, startTime, "Exists")
	err = ctrl.wait(agent.ObserveEx(gocbcore.ObserveOptions{
		Key:            []byte(id),
		ReplicaIdx:     0,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   tracectx,
	}, func(res *gocbcore.ObserveResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, id, false)
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
	Timeout       time.Duration
	Context       context.Context
	Transcoder    Transcoder
	RetryStrategy RetryStrategy
}

// GetAnyReplica returns the value of a particular document from a replica server.
func (c *Collection) GetAnyReplica(id string, opts *GetAnyReplicaOptions) (docOut *GetReplicaResult, errOut error) {
	startTime := time.Now()
	if opts == nil {
		opts = &GetAnyReplicaOptions{}
	}

	span := c.startKvOpTrace("GetAnyReplica", nil)
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.getAnyReplica(ctx, span.Context(), id, startTime, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) getAnyReplica(ctx context.Context, tracetx requestSpanContext, id string, startTime time.Time,
	opts GetAnyReplicaOptions) (docOut *GetReplicaResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	if opts.Transcoder == nil {
		opts.Transcoder = c.sb.Transcoder
	}

	retryWrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryWrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	ctrl := c.newOpManager(ctx, startTime, "GetAnyReplica")
	err = ctrl.wait(agent.GetAnyReplicaEx(gocbcore.GetAnyReplicaOptions{
		Key:            []byte(id),
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   tracetx,
	}, func(res *gocbcore.GetReplicaResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, id, false)
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
			isReplica: !res.IsActive,
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
	Timeout       time.Duration
	Context       context.Context
	Transcoder    Transcoder
	RetryStrategy RetryStrategy
}

// GetAllReplicas returns the value of a particular document from all replica servers. This will return an iterable
// which streams results one at a time.
func (c *Collection) GetAllReplicas(id string, opts *GetAllReplicaOptions) (docOut *GetAllReplicasResult, errOut error) {
	startTime := time.Now()
	if opts == nil {
		opts = &GetAllReplicaOptions{}
	}

	span := c.startKvOpTrace("GetAllReplicas", nil)
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	retryWrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryWrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	if opts.Transcoder == nil {
		opts.Transcoder = c.sb.Transcoder
	}

	return &GetAllReplicasResult{
		ctx: ctx,
		opts: gocbcore.GetOneReplicaOptions{
			Key:            []byte(id),
			CollectionName: c.name(),
			ScopeName:      c.scopeName(),
			RetryStrategy:  retryWrapper,
			TraceContext:   span.Context(),
		},
		transcoder:  opts.Transcoder,
		provider:    agent,
		cancel:      cancel,
		maxReplicas: agent.NumReplicas(),
		startTime:   startTime,
		span:        span,
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
	RetryStrategy   RetryStrategy
}

// Remove removes a document from the collection.
func (c *Collection) Remove(id string, opts *RemoveOptions) (mutOut *MutationResult, errOut error) {
	startTime := time.Now()
	if opts == nil {
		opts = &RemoveOptions{}
	}

	span := c.startKvOpTrace("Remove", nil)
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.remove(ctx, span.Context(), id, startTime, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(durabilitySettings{
		ctx:            opts.Context,
		key:            id,
		cas:            res.Cas(),
		mt:             *res.MutationToken(),
		replicaTo:      opts.ReplicateTo,
		persistTo:      opts.PersistTo,
		forDelete:      true,
		scopeName:      c.scopeName(),
		collectionName: c.name(),
	})
}

func (c *Collection) remove(ctx context.Context, tracectx requestSpanContext, id string, startTime time.Time,
	opts RemoveOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	retryWrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryWrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	coerced, durabilityTimeout := c.durabilityTimeout(ctx, opts.DurabilityLevel)
	if coerced {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(durabilityTimeout)*time.Millisecond)
		defer cancel()
	}

	ctrl := c.newOpManager(ctx, startTime, "Remove")
	err = ctrl.wait(agent.DeleteEx(gocbcore.DeleteOptions{
		Key:                    []byte(id),
		Cas:                    gocbcore.Cas(opts.Cas),
		CollectionName:         c.name(),
		ScopeName:              c.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
		RetryStrategy:          retryWrapper,
		TraceContext:           tracectx,
	}, func(res *gocbcore.DeleteResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, id, false)
			ctrl.resolve()
			return
		}

		mutOut = &MutationResult{}
		mutOut.cas = Cas(res.Cas)

		if res.MutationToken.VbUuid != 0 {
			mutTok := &MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			mutOut.mt = mutTok
		}

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// GetAndTouchOptions are the options available to the GetAndTouch operation.
type GetAndTouchOptions struct {
	Timeout       time.Duration
	Context       context.Context
	Transcoder    Transcoder
	RetryStrategy RetryStrategy
}

// GetAndTouch retrieves a document and simultaneously updates its expiry time.
func (c *Collection) GetAndTouch(id string, expiry uint32, opts *GetAndTouchOptions) (docOut *GetResult, errOut error) {
	startTime := time.Now()
	if opts == nil {
		opts = &GetAndTouchOptions{}
	}

	span := c.startKvOpTrace("GetAndTouch", nil)
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.getAndTouch(ctx, span.Context(), id, expiry, startTime, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) getAndTouch(ctx context.Context, tracectx requestSpanContext, id string, expiry uint32,
	startTime time.Time, opts GetAndTouchOptions) (docOut *GetResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	if opts.Transcoder == nil {
		opts.Transcoder = c.sb.Transcoder
	}

	retryWrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryWrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	ctrl := c.newOpManager(ctx, startTime, "GetAndTouch")
	err = ctrl.wait(agent.GetAndTouchEx(gocbcore.GetAndTouchOptions{
		Key:            []byte(id),
		Expiry:         expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   tracectx,
	}, func(res *gocbcore.GetAndTouchResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, id, false)
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
	Timeout       time.Duration
	Context       context.Context
	Transcoder    Transcoder
	RetryStrategy RetryStrategy
}

// GetAndLock locks a document for a period of time, providing exclusive RW access to it.
// A lockTime value of over 30 seconds will be treated as 30 seconds. The resolution used to send this value to
// the server is seconds and is calculated using uint32(lockTime/time.Second).
func (c *Collection) GetAndLock(id string, lockTime time.Duration, opts *GetAndLockOptions) (docOut *GetResult, errOut error) {
	startTime := time.Now()
	if opts == nil {
		opts = &GetAndLockOptions{}
	}

	span := c.startKvOpTrace("GetAndLock", nil)
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.getAndLock(ctx, span.Context(), id, uint32(lockTime/time.Second), startTime, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) getAndLock(ctx context.Context, tracectx requestSpanContext, id string, lockTime uint32,
	startTime time.Time, opts GetAndLockOptions) (docOut *GetResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	if opts.Transcoder == nil {
		opts.Transcoder = c.sb.Transcoder
	}

	retryWrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryWrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	ctrl := c.newOpManager(ctx, startTime, "GetAndLock")
	err = ctrl.wait(agent.GetAndLockEx(gocbcore.GetAndLockOptions{
		Key:            []byte(id),
		LockTime:       lockTime,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   tracectx,
	}, func(res *gocbcore.GetAndLockResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, id, false)
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
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// Unlock unlocks a document which was locked with GetAndLock.
func (c *Collection) Unlock(id string, cas Cas, opts *UnlockOptions) (mutOut *MutationResult, errOut error) {
	startTime := time.Now()
	if opts == nil {
		opts = &UnlockOptions{}
	}

	span := c.startKvOpTrace("Unlock", nil)
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.unlock(ctx, span.Context(), id, cas, startTime, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) unlock(ctx context.Context, tracectx requestSpanContext, id string, cas Cas,
	startTime time.Time, opts UnlockOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	retryWrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryWrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	ctrl := c.newOpManager(ctx, startTime, "Unlock")
	err = ctrl.wait(agent.UnlockEx(gocbcore.UnlockOptions{
		Key:            []byte(id),
		Cas:            gocbcore.Cas(cas),
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   tracectx,
	}, func(res *gocbcore.UnlockResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, id, false)
			ctrl.resolve()
			return
		}

		mutOut = &MutationResult{}
		mutOut.cas = Cas(res.Cas)

		if res.MutationToken.VbUuid != 0 {
			mutTok := &MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			mutOut.mt = mutTok
		}

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// TouchOptions are the options available to the Touch operation.
type TouchOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// Touch touches a document, specifying a new expiry time for it.
func (c *Collection) Touch(id string, expiry uint32, opts *TouchOptions) (mutOut *MutationResult, errOut error) {
	startTime := time.Now()
	if opts == nil {
		opts = &TouchOptions{}
	}

	span := c.startKvOpTrace("Touch", nil)
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.touch(ctx, span.Context(), id, expiry, startTime, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) touch(ctx context.Context, tracectx requestSpanContext, id string, expiry uint32,
	startTime time.Time, opts TouchOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	retryWrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryWrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	ctrl := c.newOpManager(ctx, startTime, "Touch")
	err = ctrl.wait(agent.TouchEx(gocbcore.TouchOptions{
		Key:            []byte(id),
		Expiry:         expiry,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
		RetryStrategy:  retryWrapper,
		TraceContext:   tracectx,
	}, func(res *gocbcore.TouchResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, id, false)
			ctrl.resolve()
			return
		}

		mutOut = &MutationResult{}
		mutOut.cas = Cas(res.Cas)

		if res.MutationToken.VbUuid != 0 {
			mutTok := &MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			mutOut.mt = mutTok
		}

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// Binary creates and returns a BinaryCollection object.
func (c *Collection) Binary() *BinaryCollection {
	return &BinaryCollection{collection: c}
}
