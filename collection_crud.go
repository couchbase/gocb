package gocb

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	"gopkg.in/couchbase/gocbcore.v8"
)

type kvProvider interface {
	AddEx(opts gocbcore.AddOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	SetEx(opts gocbcore.SetOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	ReplaceEx(opts gocbcore.ReplaceOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	GetEx(opts gocbcore.GetOptions, cb gocbcore.GetExCallback) (gocbcore.PendingOp, error)
	GetReplicaEx(opts gocbcore.GetReplicaOptions, cb gocbcore.GetReplicaExCallback) (gocbcore.PendingOp, error)
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
	NumReplicas() int // UNSURE THIS SHOULDNT BE ON ANOTHER INTERFACE
}

// shortestTime calculates the shortest of two times, this is used for context deadlines.
func shortestTime(first, second time.Time) time.Time {
	if first.IsZero() {
		return second
	}
	if second.IsZero() || first.Before(second) {
		return first
	}
	return second
}

// deadline calculates the shortest timeout from context, operation timeout and collection level kv timeout.
func (c *Collection) deadline(ctx context.Context, now time.Time, opTimeout time.Duration) time.Time {
	var earliest time.Time
	if opTimeout > 0 {
		earliest = now.Add(opTimeout)
	}
	if d, ok := ctx.Deadline(); ok {
		earliest = shortestTime(earliest, d)
	}
	return shortestTime(earliest, now.Add(c.sb.KvTimeout))
}

func (c *Collection) context(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	d := c.deadline(ctx, time.Now(), timeout)
	if currentD, ok := ctx.Deadline(); ok {
		if d.Before(currentD) {
			// the original context has a deadline but one of the timeouts is shorter
			return context.WithDeadline(ctx, d)
		}
	} else {
		// original context has no deadline so make one
		return context.WithDeadline(ctx, d)
	}

	return ctx, nil
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

// Cas represents the specific state of a document on the cluster.
type Cas gocbcore.Cas

type pendingOp gocbcore.PendingOp

// UpsertOptions are options that can be applied to an Upsert operation.
type UpsertOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	// The expiration length in seconds
	Expiration      uint32
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Encoder         Encode
}

// InsertOptions are options that can be applied to an Insert operation.
type InsertOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	// The expiration length in seconds
	Expiration      uint32
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Encoder         Encode
}

// Insert creates a new document in the Collection.
func (c *Collection) Insert(key string, val interface{}, opts *InsertOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &InsertOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Insert")
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.insert(ctx, span.Context(), key, val, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(durabilitySettings{
		ctx:            opts.Context,
		tracectx:       span.Context(),
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

func (c *Collection) insert(ctx context.Context, traceCtx opentracing.SpanContext, key string, val interface{}, opts InsertOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		errOut = err
		return
	}

	encoder := opts.Encoder
	if opts.Encoder == nil {
		encoder = DefaultEncode
	}

	encodeSpan := opentracing.GlobalTracer().StartSpan("Encoding", opentracing.ChildOf(traceCtx))
	bytes, flags, err := encoder(val)
	if err != nil {
		errOut = err
		return
	}
	encodeSpan.Finish()

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.AddEx(gocbcore.AddOptions{
		Key:            []byte(key),
		Value:          bytes,
		Flags:          flags,
		Expiry:         opts.Expiration,
		TraceContext:   traceCtx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = maybeEnhanceErr(err, key)
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

	span := c.startKvOpTrace(opts.ParentSpanContext, "Upsert")
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.upsert(ctx, span.Context(), key, val, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(durabilitySettings{
		ctx:            opts.Context,
		tracectx:       span.Context(),
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

func (c *Collection) upsert(ctx context.Context, traceCtx opentracing.SpanContext, key string, val interface{}, opts UpsertOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		errOut = err
		return
	}

	encoder := opts.Encoder
	if opts.Encoder == nil {
		encoder = DefaultEncode
	}

	encodeSpan := opentracing.GlobalTracer().StartSpan("Encoding", opentracing.ChildOf(traceCtx))
	bytes, flags, err := encoder(val)
	if err != nil {
		errOut = err
		return
	}
	encodeSpan.Finish()

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.SetEx(gocbcore.SetOptions{
		Key:            []byte(key),
		Value:          bytes,
		Flags:          flags,
		Expiry:         opts.Expiration,
		TraceContext:   traceCtx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = maybeEnhanceErr(err, key)
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
		return
	}

	return
}

// ReplaceOptions are the options available to a Replace operation.
type ReplaceOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	Expiration        uint32
	Cas               Cas
	PersistTo         uint
	ReplicateTo       uint
	DurabilityLevel   DurabilityLevel
	Encoder           Encode
}

// Replace updates a document in the collection.
func (c *Collection) Replace(key string, val interface{}, opts *ReplaceOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &ReplaceOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Replace")
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.replace(ctx, span.Context(), key, val, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(durabilitySettings{
		ctx:            opts.Context,
		tracectx:       span.Context(),
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

func (c *Collection) replace(ctx context.Context, traceCtx opentracing.SpanContext, key string, val interface{}, opts ReplaceOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	encoder := opts.Encoder
	if opts.Encoder == nil {
		encoder = DefaultEncode
	}

	encodeSpan := opentracing.GlobalTracer().StartSpan("Encoding", opentracing.ChildOf(traceCtx))
	bytes, flags, err := encoder(val)
	if err != nil {
		errOut = err
		return
	}
	encodeSpan.Finish()

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.ReplaceEx(gocbcore.ReplaceOptions{
		Key:            []byte(key),
		Value:          bytes,
		Flags:          flags,
		Expiry:         opts.Expiration,
		Cas:            gocbcore.Cas(opts.Cas),
		TraceContext:   traceCtx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = maybeEnhanceErr(err, key)
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
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	WithExpiry        bool
	// Project causes the Get operation to only fetch the fields indicated
	// by the paths. The result of the operation is then treated as a
	// standard GetResult.
	Project *ProjectOptions
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

	span := c.startKvOpTrace(opts.ParentSpanContext, "Get")
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	if (opts.Project == nil || (opts.Project != nil && len(opts.Project.Fields) > 16)) && !opts.WithExpiry {
		// Standard fulldoc
		doc, err := c.get(ctx, span.Context(), key, opts)
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
		ops = append(ops, spec.Get(""))
		opts.Project = &ProjectOptions{}
	} else {
		for _, path := range opts.Project.Fields {
			ops = append(ops, spec.Get(path))
		}
	}

	result, err := c.lookupIn(ctx, span.Context(), key, ops, lookupOpts)
	if err != nil {
		return nil, err
	}

	doc := &GetResult{}
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
func (c *Collection) get(ctx context.Context, traceCtx opentracing.SpanContext, key string, opts *GetOptions) (docOut *GetResult, errOut error) {
	span := c.startKvOpTrace(traceCtx, "get")
	defer span.Finish()

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.GetEx(gocbcore.GetOptions{
		Key:            []byte(key),
		TraceContext:   traceCtx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.GetResult, err error) {
		if err != nil {
			errOut = maybeEnhanceErr(err, key)
			ctrl.resolve()
			return
		}
		if res != nil {
			doc := &GetResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				contents: res.Value,
				flags:    res.Flags,
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
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// Exists checks if a document exists for the given key.
func (c *Collection) Exists(key string, opts *ExistsOptions) (docOut *ExistsResult, errOut error) {
	if opts == nil {
		opts = &ExistsOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Exists")
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.exists(ctx, span.Context(), key, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) exists(ctx context.Context, traceCtx opentracing.SpanContext, key string, opts ExistsOptions) (docOut *ExistsResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.ObserveEx(gocbcore.ObserveOptions{
		Key:            []byte(key),
		TraceContext:   traceCtx,
		ReplicaIdx:     0,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.ObserveResult, err error) {
		if err != nil {
			errOut = maybeEnhanceErr(err, key)
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

// GetFromReplicaOptions are the options available to the GetFromReplica command.
type GetFromReplicaOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// GetFromReplica returns the value of a particular document from a replica server..
func (c *Collection) GetFromReplica(key string, replicaIdx int, opts *GetFromReplicaOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetFromReplicaOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "GetFromReplica")
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.getFromReplica(ctx, span.Context(), key, replicaIdx, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) getFromReplica(ctx context.Context, traceCtx opentracing.SpanContext, key string, replicaIdx int, opts GetFromReplicaOptions) (docOut *GetResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.GetReplicaEx(gocbcore.GetReplicaOptions{
		Key:            []byte(key),
		TraceContext:   traceCtx,
		ReplicaIdx:     replicaIdx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.GetReplicaResult, err error) {
		if err != nil {
			errOut = maybeEnhanceErr(err, key)
			ctrl.resolve()
			return
		}
		if res != nil {
			doc := &GetResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				contents: res.Value,
				flags:    res.Flags,
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

// RemoveOptions are the options available to the Remove command.
type RemoveOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	Cas               Cas
	PersistTo         uint
	ReplicateTo       uint
	DurabilityLevel   DurabilityLevel
}

// Remove removes a document from the collection.
func (c *Collection) Remove(key string, opts *RemoveOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &RemoveOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Remove")
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.remove(ctx, span.Context(), key, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(durabilitySettings{
		ctx:            opts.Context,
		tracectx:       span.Context(),
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

func (c *Collection) remove(ctx context.Context, traceCtx opentracing.SpanContext, key string, opts RemoveOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.DeleteEx(gocbcore.DeleteOptions{
		Key:            []byte(key),
		Cas:            gocbcore.Cas(opts.Cas),
		TraceContext:   traceCtx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.DeleteResult, err error) {
		if err != nil {
			errOut = maybeEnhanceErr(err, key)
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
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// GetAndTouch retrieves a document and simultaneously updates its expiry time.
func (c *Collection) GetAndTouch(key string, expiration uint32, opts *GetAndTouchOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetAndTouchOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "GetAndTouch")
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.getAndTouch(ctx, span.Context(), key, expiration, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) getAndTouch(ctx context.Context, traceCtx opentracing.SpanContext, key string, expiration uint32, opts GetAndTouchOptions) (docOut *GetResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.GetAndTouchEx(gocbcore.GetAndTouchOptions{
		Key:            []byte(key),
		Expiry:         expiration,
		TraceContext:   traceCtx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.GetAndTouchResult, err error) {
		if err != nil {
			errOut = maybeEnhanceErr(err, key)
			ctrl.resolve()
			return
		}
		if res != nil {
			doc := &GetResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				contents: res.Value,
				flags:    res.Flags,
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
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// GetAndLock locks a document for a period of time, providing exclusive RW access to it.
func (c *Collection) GetAndLock(key string, expiration uint32, opts *GetAndLockOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetAndLockOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "GetAndLock")
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.getAndLock(ctx, span.Context(), key, expiration, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}
func (c *Collection) getAndLock(ctx context.Context, traceCtx opentracing.SpanContext, key string, expiration uint32, opts GetAndLockOptions) (docOut *GetResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.GetAndLockEx(gocbcore.GetAndLockOptions{
		Key:            []byte(key),
		LockTime:       expiration,
		TraceContext:   traceCtx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.GetAndLockResult, err error) {
		if err != nil {
			errOut = maybeEnhanceErr(err, key)
			ctrl.resolve()
			return
		}
		if res != nil {
			doc := &GetResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				contents: res.Value,
				flags:    res.Flags,
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
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	Cas               Cas
}

// Unlock unlocks a document which was locked with GetAndLock.
func (c *Collection) Unlock(key string, opts *UnlockOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &UnlockOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Unlock")
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.unlock(ctx, span.Context(), key, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) unlock(ctx context.Context, traceCtx opentracing.SpanContext, key string, opts UnlockOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.UnlockEx(gocbcore.UnlockOptions{
		Key:            []byte(key),
		Cas:            gocbcore.Cas(opts.Cas),
		TraceContext:   traceCtx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.UnlockResult, err error) {
		if err != nil {
			errOut = maybeEnhanceErr(err, key)
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
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// Touch touches a document, specifying a new expiry time for it.
func (c *Collection) Touch(key string, expiration uint32, opts *TouchOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &TouchOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Touch")
	defer span.Finish()

	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.touch(ctx, span.Context(), key, expiration, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) touch(ctx context.Context, traceCtx opentracing.SpanContext, key string, expiration uint32, opts TouchOptions) (mutOut *MutationResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.TouchEx(gocbcore.TouchOptions{
		Key:            []byte(key),
		Expiry:         expiration,
		TraceContext:   traceCtx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.TouchResult, err error) {
		if err != nil {
			errOut = maybeEnhanceErr(err, key)
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

// Binary creates and returns a CollectionBinary object.
func (c *Collection) Binary() *CollectionBinary {
	return &CollectionBinary{c}
}
