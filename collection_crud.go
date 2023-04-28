package gocb

import (
	"context"
	"sync"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v10"
)

// Cas represents the specific state of a document on the cluster.
type Cas gocbcore.Cas

// InsertOptions are options that can be applied to an Insert operation.
type InsertOptions struct {
	Expiry          time.Duration
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Transcoder      Transcoder
	Timeout         time.Duration
	RetryStrategy   RetryStrategy
	ParentSpan      RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		User string
	}
}

// Insert creates a new document in the Collection.
func (c *Collection) Insert(id string, val interface{}, opts *InsertOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &InsertOptions{}
	}

	opm := newKvOpManager(c, "insert", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetValue(val)
	opm.SetDuraOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	return agent.Add(opm)
}

// UpsertOptions are options that can be applied to an Upsert operation.
type UpsertOptions struct {
	Expiry          time.Duration
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Transcoder      Transcoder
	Timeout         time.Duration
	RetryStrategy   RetryStrategy
	ParentSpan      RequestSpan
	PreserveExpiry  bool

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		User string
	}
}

// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
func (c *Collection) Upsert(id string, val interface{}, opts *UpsertOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &UpsertOptions{}
	}

	opm := newKvOpManager(c, "upsert", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetValue(val)
	opm.SetDuraOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)
	opm.SetPreserveExpiry(opts.PreserveExpiry)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	return agent.Set(opm)
}

// ReplaceOptions are the options available to a Replace operation.
type ReplaceOptions struct {
	Expiry          time.Duration
	Cas             Cas
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Transcoder      Transcoder
	Timeout         time.Duration
	RetryStrategy   RetryStrategy
	ParentSpan      RequestSpan
	PreserveExpiry  bool

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		User string
	}
}

// Replace updates a document in the collection.
func (c *Collection) Replace(id string, val interface{}, opts *ReplaceOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &ReplaceOptions{}
	}

	if opts.Expiry > 0 && opts.PreserveExpiry {
		return nil, makeInvalidArgumentsError("cannot use expiry and preserve ttl together for replace")
	}

	opm := newKvOpManager(c, "replace", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetValue(val)
	opm.SetDuraOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)
	opm.SetPreserveExpiry(opts.PreserveExpiry)
	opm.SetCas(opts.Cas)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	return agent.Replace(opm)
}

// GetOptions are the options available to a Get operation.
type GetOptions struct {
	WithExpiry bool
	// Project causes the Get operation to only fetch the fields indicated
	// by the paths. The result of the operation is then treated as a
	// standard GetResult.
	Project       []string
	Transcoder    Transcoder
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		User string
	}
}

// Get performs a fetch operation against the collection. This can take 3 paths, a standard full document
// fetch, a subdocument full document fetch also fetching document expiry (when WithExpiry is set),
// or a subdocument fetch (when Project is used).
func (c *Collection) Get(id string, opts *GetOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetOptions{}
	}

	if len(opts.Project) == 0 && !opts.WithExpiry {
		return c.getDirect(id, opts)
	}

	return c.getProjected(id, opts)
}

func (c *Collection) getDirect(id string, opts *GetOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetOptions{}
	}

	opm := newKvOpManager(c, "get", opts.ParentSpan)

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	return agent.Get(opm)
}

func (c *Collection) getProjected(id string, opts *GetOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetOptions{}
	}

	opm := newKvOpManager(c, "get", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	var withFlags bool
	numProjects := len(opts.Project)
	if opts.WithExpiry {
		if numProjects == 0 {
			// This must be a full get with expiry
			withFlags = true
		}
		numProjects = 1 + numProjects
	}

	projections := opts.Project
	if numProjects > 16 {
		projections = nil
	}

	var ops []LookupInSpec

	if opts.WithExpiry {
		ops = append(ops, GetSpec("$document.exptime", &GetSpecOptions{IsXattr: true}))

		if withFlags {
			// We also need to fetch the flags, we need them for transcoding and they aren't included in a lookupin
			// response. We only need these when doing a full get with expiry.
			ops = append(ops, GetSpec("$document.flags", &GetSpecOptions{IsXattr: true}))
		}
	}

	if len(projections) == 0 {
		ops = append(ops, GetSpec("", nil))
	} else {
		for _, path := range projections {
			ops = append(ops, GetSpec(path, nil))
		}
	}

	result, err := c.LookupIn(id, ops, &LookupInOptions{
		ParentSpan: opm.TraceSpan(),
		noMetrics:  true,
		Context:    opts.Context,
	})
	if err != nil {
		return nil, err
	}

	doc := &GetResult{}
	if opts.WithExpiry {
		// if expiration was requested then extract and remove it from the results
		var expires int64
		err = result.ContentAt(0, &expires)
		if err != nil {
			return nil, err
		}

		expiryTime := time.Unix(expires, 0)
		doc.expiryTime = &expiryTime

		ops = ops[1:]
		result.contents = result.contents[1:]

		if withFlags {
			var flags uint32
			err = result.ContentAt(0, &flags)
			if err != nil {
				return nil, err
			}

			doc.flags = flags

			ops = ops[1:]
			result.contents = result.contents[1:]
		}
	}

	doc.transcoder = opm.Transcoder()
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

// ExistsOptions are the options available to the Exists command.
type ExistsOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		User string
	}
}

// Exists checks if a document exists for the given id.
func (c *Collection) Exists(id string, opts *ExistsOptions) (docOut *ExistsResult, errOut error) {
	if opts == nil {
		opts = &ExistsOptions{}
	}

	opm := newKvOpManager(c, "exists", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	return agent.Exists(opm)
}

func (c *Collection) getOneReplica(
	ctx context.Context,
	span RequestSpan,
	id string,
	replicaIdx int,
	transcoder Transcoder,
	retryStrategy RetryStrategy,
	cancelCh chan struct{},
	timeout time.Duration,
	user string,
) (*GetReplicaResult, error) {
	opm := newKvOpManager(c, "get_replica", span)

	opm.SetDocumentID(id)
	opm.SetTranscoder(transcoder)
	opm.SetRetryStrategy(retryStrategy)
	opm.SetTimeout(timeout)
	// opm.SetCancelCh(cancelCh) TODO: add cancel ch support (is this just context?)
	opm.SetImpersonate(user)
	opm.SetContext(ctx)

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	if replicaIdx == 0 {
		res, err := agent.Get(opm)
		if err != nil {
			return nil, err
		}
		docOut := &GetReplicaResult{}
		docOut.cas = Cas(res.cas)
		docOut.transcoder = opm.Transcoder()
		docOut.contents = res.contents
		docOut.flags = res.flags
		docOut.isReplica = false
		return docOut, nil

	}

	return agent.GetReplica(opm)

}

// GetAllReplicaOptions are the options available to the GetAllReplicas command.
type GetAllReplicaOptions struct {
	Transcoder    Transcoder
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		User string
	}

	noMetrics bool
}

// GetAllReplicasResult represents the results of a GetAllReplicas operation.
type GetAllReplicasResult struct {
	lock                sync.Mutex
	totalRequests       uint32
	successResults      uint32
	totalResults        uint32
	resCh               chan *GetReplicaResult
	cancelCh            chan struct{}
	span                RequestSpan
	childReqsCompleteCh chan struct{}
	valueRecorder       ValueRecorder
	startedTime         time.Time
}

func (r *GetAllReplicasResult) addFailed() {
	r.lock.Lock()

	r.totalResults++
	if r.totalResults == r.totalRequests {
		close(r.childReqsCompleteCh)
	}

	r.lock.Unlock()
}

func (r *GetAllReplicasResult) addResult(res *GetReplicaResult) {
	// We use a lock here because the alternative means that there is a race
	// between the channel writes from multiple results and the channels being
	// closed.  IE: T1-Incr, T2-Incr, T2-Send, T2-Close, T1-Send[PANIC]
	r.lock.Lock()

	r.successResults++
	resultCount := r.successResults

	if resultCount <= r.totalRequests {
		r.resCh <- res
	}

	if resultCount == r.totalRequests {
		close(r.cancelCh)
		close(r.resCh)

		r.span.End()
		if r.valueRecorder != nil {
			r.valueRecorder.RecordValue(uint64(time.Since(r.startedTime).Microseconds()))
		}
	}

	r.totalResults++
	if r.totalResults == r.totalRequests {
		close(r.childReqsCompleteCh)
	}

	r.lock.Unlock()
}

// Next fetches the next replica result.
func (r *GetAllReplicasResult) Next() *GetReplicaResult {
	return <-r.resCh
}

// Close cancels all remaining get replica requests.
func (r *GetAllReplicasResult) Close() error {
	// See addResult discussion on lock usage.
	r.lock.Lock()

	// Note that this number increment must be high enough to be clear that
	// the result set was closed, but low enough that it won't overflow if
	// additional result objects are processed after the close.
	prevResultCount := r.successResults
	r.successResults += 100000

	// We only have to close everything if the addResult method didn't already
	// close them due to already having completed every request
	var weClosed bool
	if prevResultCount < r.totalRequests {
		close(r.cancelCh)
		close(r.resCh)

		weClosed = true
	}

	r.lock.Unlock()

	if weClosed {
		// We need to wait for the child requests spans to be completed.
		<-r.childReqsCompleteCh
		r.span.End()
	}

	return nil
}

// GetAllReplicas returns the value of a particular document from all replica servers. This will return an iterable
// which streams results one at a time.
func (c *Collection) GetAllReplicas(id string, opts *GetAllReplicaOptions) (*GetAllReplicasResult, error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	return agent.GetAllReplicas(c, id, opts)
}

// GetAnyReplicaOptions are the options available to the GetAnyReplica command.
type GetAnyReplicaOptions struct {
	Transcoder    Transcoder
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		User string
	}
}

// GetAnyReplica returns the value of a particular document from a replica server.
func (c *Collection) GetAnyReplica(id string, opts *GetAnyReplicaOptions) (docOut *GetReplicaResult, errOut error) {
	if opts == nil {
		opts = &GetAnyReplicaOptions{}
	}

	start := time.Now()
	defer c.meter.ValueRecord("kv", "get_any_replica", start)

	var tracectx RequestSpanContext
	if opts.ParentSpan != nil {
		tracectx = opts.ParentSpan.Context()
	}

	span := c.startKvOpTrace("get_any_replica", tracectx, false)
	defer span.End()

	repRes, err := c.GetAllReplicas(id, &GetAllReplicaOptions{
		Timeout:       opts.Timeout,
		Transcoder:    opts.Transcoder,
		RetryStrategy: opts.RetryStrategy,
		Internal:      opts.Internal,
		ParentSpan:    span,
		noMetrics:     true,
		Context:       opts.Context,
	})
	if err != nil {
		return nil, err
	}

	// Try to fetch at least one result
	res := repRes.Next()
	if res == nil {
		return nil, &KeyValueError{
			InnerError:     ErrDocumentUnretrievable,
			BucketName:     c.bucketName(),
			ScopeName:      c.scope,
			CollectionName: c.collectionName,
		}
	}

	// Close the results channel since we don't care about any of the
	// remaining result objects at this point.
	err = repRes.Close()
	if err != nil {
		logDebugf("failed to close GetAnyReplica response: %s", err)
	}

	return res, nil
}

// RemoveOptions are the options available to the Remove command.
type RemoveOptions struct {
	Cas             Cas
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Timeout         time.Duration
	RetryStrategy   RetryStrategy
	ParentSpan      RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		User string
	}
}

// Remove removes a document from the collection.
func (c *Collection) Remove(id string, opts *RemoveOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &RemoveOptions{}
	}

	opm := newKvOpManager(c, "remove", opts.ParentSpan)

	opm.SetDocumentID(id)
	opm.SetDuraOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)
	opm.SetCas(opts.Cas)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	return agent.Delete(opm)
}

// GetAndTouchOptions are the options available to the GetAndTouch operation.
type GetAndTouchOptions struct {
	Transcoder    Transcoder
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		User string
	}
}

// GetAndTouch retrieves a document and simultaneously updates its expiry time.
func (c *Collection) GetAndTouch(id string, expiry time.Duration, opts *GetAndTouchOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetAndTouchOptions{}
	}

	opm := newKvOpManager(c, "get_and_touch", opts.ParentSpan)

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	return agent.GetAndTouch(opm)
}

// GetAndLockOptions are the options available to the GetAndLock operation.
type GetAndLockOptions struct {
	Transcoder    Transcoder
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		User string
	}
}

// GetAndLock locks a document for a period of time, providing exclusive RW access to it.
// A lockTime value of over 30 seconds will be treated as 30 seconds. The resolution used to send this value to
// the server is seconds and is calculated using uint32(lockTime/time.Second).
func (c *Collection) GetAndLock(id string, lockTime time.Duration, opts *GetAndLockOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetAndLockOptions{}
	}

	opm := newKvOpManager(c, "get_and_lock", opts.ParentSpan)

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)
	opm.SetLockTime(lockTime)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	return agent.GetAndLock(opm)
}

// UnlockOptions are the options available to the GetAndLock operation.
type UnlockOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		User string
	}
}

// Unlock unlocks a document which was locked with GetAndLock.
func (c *Collection) Unlock(id string, cas Cas, opts *UnlockOptions) (errOut error) {
	if opts == nil {
		opts = &UnlockOptions{}
	}

	opm := newKvOpManager(c, "unlock", nil)

	opm.SetDocumentID(id)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)

	if err := opm.CheckReadyForOp(); err != nil {
		return err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return err
	}

	return agent.Unlock(opm)
}

// TouchOptions are the options available to the Touch operation.
type TouchOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		User string
	}
}

// Touch touches a document, specifying a new expiry time for it.
func (c *Collection) Touch(id string, expiry time.Duration, opts *TouchOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &TouchOptions{}
	}

	opm := newKvOpManager(c, "touch", nil)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)
	opm.SetExpiry(expiry)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	return agent.Touch(opm)
}

// Binary creates and returns a BinaryCollection object.
func (c *Collection) Binary() *BinaryCollection {
	return &BinaryCollection{collection: c}
}
