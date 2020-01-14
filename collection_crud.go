package gocb

import (
	"errors"
	"sync/atomic"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

type kvProvider interface {
	AddEx(opts gocbcore.AddOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	SetEx(opts gocbcore.SetOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	ReplaceEx(opts gocbcore.ReplaceOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	GetEx(opts gocbcore.GetOptions, cb gocbcore.GetExCallback) (gocbcore.PendingOp, error)
	GetOneReplicaEx(opts gocbcore.GetOneReplicaOptions, cb gocbcore.GetReplicaExCallback) (gocbcore.PendingOp, error)
	ObserveEx(opts gocbcore.ObserveOptions, cb gocbcore.ObserveExCallback) (gocbcore.PendingOp, error)
	ObserveVbEx(opts gocbcore.ObserveVbOptions, cb gocbcore.ObserveVbExCallback) (gocbcore.PendingOp, error)
	GetMetaEx(opts gocbcore.GetMetaOptions, cb gocbcore.GetMetaExCallback) (gocbcore.PendingOp, error)
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

// Cas represents the specific state of a document on the cluster.
type Cas gocbcore.Cas

type pendingOp gocbcore.PendingOp

// InsertOptions are options that can be applied to an Insert operation.
type InsertOptions struct {
	// The expiry length in seconds
	Expiry          uint32
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Transcoder      Transcoder
	Timeout         time.Duration
	RetryStrategy   RetryStrategy
}

// Insert creates a new document in the Collection.
func (c *Collection) Insert(id string, val interface{}, opts *InsertOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &InsertOptions{}
	}

	opm := c.newKvOpManager("Insert", nil)
	defer opm.Finish()

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetValue(val)
	opm.SetDuraOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	err = opm.Wait(agent.AddEx(gocbcore.AddOptions{
		Key:                    opm.DocumentID(),
		Value:                  opm.ValueBytes(),
		Flags:                  opm.ValueFlags(),
		Expiry:                 opts.Expiry,
		CollectionName:         opm.CollectionName(),
		ScopeName:              opm.ScopeName(),
		DurabilityLevel:        opm.DurabilityLevel(),
		DurabilityLevelTimeout: opm.DurabilityTimeout(),
		RetryStrategy:          opm.RetryStrategy(),
		TraceContext:           opm.TraceSpan(),
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = opm.EnhanceErr(err)
			opm.Reject()
			return
		}

		mutOut = &MutationResult{}
		mutOut.cas = Cas(res.Cas)
		mutOut.mt = opm.EnhanceMt(res.MutationToken)

		opm.Resolve(mutOut.mt)
	}))
	if err != nil {
		errOut = err
	}
	return
}

// UpsertOptions are options that can be applied to an Upsert operation.
type UpsertOptions struct {
	// The expiry length in seconds
	Expiry          uint32
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Transcoder      Transcoder
	Timeout         time.Duration
	RetryStrategy   RetryStrategy
}

// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
func (c *Collection) Upsert(id string, val interface{}, opts *UpsertOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &UpsertOptions{}
	}

	opm := c.newKvOpManager("Upsert", nil)
	defer opm.Finish()

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetValue(val)
	opm.SetDuraOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	err = opm.Wait(agent.SetEx(gocbcore.SetOptions{
		Key:                    opm.DocumentID(),
		Value:                  opm.ValueBytes(),
		Flags:                  opm.ValueFlags(),
		Expiry:                 opts.Expiry,
		CollectionName:         opm.CollectionName(),
		ScopeName:              opm.ScopeName(),
		DurabilityLevel:        opm.DurabilityLevel(),
		DurabilityLevelTimeout: opm.DurabilityTimeout(),
		RetryStrategy:          opm.RetryStrategy(),
		TraceContext:           opm.TraceSpan(),
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = opm.EnhanceErr(err)
			opm.Reject()
			return
		}

		mutOut = &MutationResult{}
		mutOut.cas = Cas(res.Cas)
		mutOut.mt = opm.EnhanceMt(res.MutationToken)

		opm.Resolve(mutOut.mt)
	}))
	if err != nil {
		errOut = err
	}
	return
}

// ReplaceOptions are the options available to a Replace operation.
type ReplaceOptions struct {
	Expiry          uint32
	Cas             Cas
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	Transcoder      Transcoder
	Timeout         time.Duration
	RetryStrategy   RetryStrategy
}

// Replace updates a document in the collection.
func (c *Collection) Replace(id string, val interface{}, opts *ReplaceOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &ReplaceOptions{}
	}

	opm := c.newKvOpManager("Replace", nil)
	defer opm.Finish()

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetValue(val)
	opm.SetDuraOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	err = opm.Wait(agent.ReplaceEx(gocbcore.ReplaceOptions{
		Key:                    opm.DocumentID(),
		Value:                  opm.ValueBytes(),
		Flags:                  opm.ValueFlags(),
		Expiry:                 opts.Expiry,
		Cas:                    gocbcore.Cas(opts.Cas),
		CollectionName:         opm.CollectionName(),
		ScopeName:              opm.ScopeName(),
		DurabilityLevel:        opm.DurabilityLevel(),
		DurabilityLevelTimeout: opm.DurabilityTimeout(),
		RetryStrategy:          opm.RetryStrategy(),
		TraceContext:           opm.TraceSpan(),
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			errOut = opm.EnhanceErr(err)
			opm.Reject()
			return
		}

		mutOut = &MutationResult{}
		mutOut.cas = Cas(res.Cas)
		mutOut.mt = opm.EnhanceMt(res.MutationToken)

		opm.Resolve(mutOut.mt)
	}))
	if err != nil {
		errOut = err
	}
	return
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

	opm := c.newKvOpManager("Get", nil)
	defer opm.Finish()

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	err = opm.Wait(agent.GetEx(gocbcore.GetOptions{
		Key:            opm.DocumentID(),
		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		RetryStrategy:  opm.RetryStrategy(),
		TraceContext:   opm.TraceSpan(),
	}, func(res *gocbcore.GetResult, err error) {
		if err != nil {
			errOut = opm.EnhanceErr(err)
			opm.Reject()
			return
		}

		doc := &GetResult{
			Result: Result{
				cas: Cas(res.Cas),
			},
			transcoder: opm.Transcoder(),
			contents:   res.Value,
			flags:      res.Flags,
		}

		docOut = doc

		opm.Resolve(nil)
	}))
	if err != nil {
		errOut = err
	}
	return
}

func (c *Collection) getProjected(id string, opts *GetOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetOptions{}
	}

	opm := c.newKvOpManager("Get", nil)
	defer opm.Finish()

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)

	if opts.Transcoder != nil {
		return nil, errors.New("Cannot specify custom transcoder for projected gets")
	}

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	numProjects := len(opts.Project)
	if opts.WithExpiry {
		numProjects = 1 + numProjects
	}

	projections := opts.Project
	if numProjects > 16 {
		projections = nil
	}

	var ops []LookupInSpec

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

	result, err := c.internalLookupIn(opm, ops)
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
}

// Exists checks if a document exists for the given id.
func (c *Collection) Exists(id string, opts *ExistsOptions) (docOut *ExistsResult, errOut error) {
	if opts == nil {
		opts = &ExistsOptions{}
	}

	opm := c.newKvOpManager("Exists", nil)
	defer opm.Finish()

	opm.SetDocumentID(id)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	err = opm.Wait(agent.GetMetaEx(gocbcore.GetMetaOptions{
		Key:            opm.DocumentID(),
		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		RetryStrategy:  opm.RetryStrategy(),
		TraceContext:   opm.TraceSpan,
	}, func(res *gocbcore.GetMetaResult, err error) {
		if errors.Is(err, ErrDocumentNotFound) {
			docOut = &ExistsResult{
				Result: Result{
					cas: Cas(0),
				},
				docExists: false,
			}
			opm.Resolve(nil)
			return
		}

		if err != nil {
			errOut = opm.EnhanceErr(err)
			opm.Reject()
			return
		}

		if res != nil {
			docOut = &ExistsResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				docExists: true,
			}
		}

		opm.Resolve(nil)
	}))
	if err != nil {
		errOut = err
	}
	return
}

func (c *Collection) getOneReplica(
	span requestSpanContext,
	id string,
	replicaIdx int,
	transcoder Transcoder,
	retryStrategy RetryStrategy,
	cancelCh chan struct{},
) (docOut *GetReplicaResult, errOut error) {
	opm := c.newKvOpManager("getOneReplica", span)
	defer opm.Finish()

	opm.SetDocumentID(id)
	opm.SetTranscoder(transcoder)
	opm.SetRetryStrategy(retryStrategy)
	opm.SetCancelCh(cancelCh)

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	if replicaIdx == 0 {
		err = opm.Wait(agent.GetEx(gocbcore.GetOptions{
			Key:            opm.DocumentID(),
			CollectionName: opm.CollectionName(),
			ScopeName:      opm.ScopeName(),
			RetryStrategy:  opm.RetryStrategy(),
			TraceContext:   opm.TraceSpan(),
		}, func(res *gocbcore.GetResult, err error) {
			if err != nil {
				errOut = opm.EnhanceErr(err)
				opm.Reject()
				return
			}

			docOut = &GetReplicaResult{}
			docOut.cas = Cas(res.Cas)
			docOut.transcoder = opm.Transcoder()
			docOut.contents = res.Value
			docOut.flags = res.Flags
			docOut.isReplica = false

			opm.Resolve(nil)
		}))
		if err != nil {
			errOut = err
		}
		return
	}

	err = opm.Wait(agent.GetOneReplicaEx(gocbcore.GetOneReplicaOptions{
		Key:            opm.DocumentID(),
		ReplicaIdx:     replicaIdx,
		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		RetryStrategy:  opm.RetryStrategy(),
		TraceContext:   opm.TraceSpan(),
	}, func(res *gocbcore.GetReplicaResult, err error) {
		if err != nil {
			errOut = opm.EnhanceErr(err)
			opm.Reject()
			return
		}

		docOut = &GetReplicaResult{}
		docOut.cas = Cas(res.Cas)
		docOut.transcoder = opm.Transcoder()
		docOut.contents = res.Value
		docOut.flags = res.Flags
		docOut.isReplica = true

		opm.Resolve(nil)
	}))
	if err != nil {
		errOut = err
	}
	return
}

// GetAllReplicaOptions are the options available to the GetAllReplicas command.
type GetAllReplicaOptions struct {
	Transcoder    Transcoder
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// GetAllReplicasResult represents the results of a GetAllReplicas operation.
type GetAllReplicasResult struct {
	totalRequests uint32
	totalResults  uint32
	resCh         chan *GetReplicaResult
	cancelCh      chan struct{}
}

func (r *GetAllReplicasResult) addResult(res *GetReplicaResult) {
	resultCount := atomic.AddUint32(&r.totalResults, 1)
	if resultCount <= r.totalRequests {
		r.resCh <- res
	}
	if resultCount == r.totalRequests {
		close(r.cancelCh)
		close(r.resCh)
	}
}

// Next fetches the next replica result.
func (r *GetAllReplicasResult) Next() *GetReplicaResult {
	return <-r.resCh
}

// Close cancels all remaining get replica requests.
func (r *GetAllReplicasResult) Close() error {
	resultCount := atomic.SwapUint32(&r.totalResults, 0xffffffff)

	// We only have to close everything if the addResult method didn't already
	// close them due to already having completed every request
	if resultCount < r.totalRequests {
		close(r.cancelCh)
		close(r.resCh)
	}

	return nil
}

// GetAllReplicas returns the value of a particular document from all replica servers. This will return an iterable
// which streams results one at a time.
func (c *Collection) GetAllReplicas(id string, opts *GetAllReplicaOptions) (docOut *GetAllReplicasResult, errOut error) {
	if opts == nil {
		opts = &GetAllReplicaOptions{}
	}

	span := c.startKvOpTrace("GetAllReplicas", nil)
	defer span.Finish()

	// Timeout needs to be adjusted here, since we use it at the bottom of this
	// function, but the remaining options are all passed downwards and get handled
	// by those functions rather than us.
	timeout := opts.Timeout
	if timeout == 0 || timeout > c.sb.KvTimeout {
		timeout = c.sb.KvTimeout
	}

	deadline := time.Now().Add(timeout)
	transcoder := opts.Transcoder
	retryStrategy := opts.RetryStrategy

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	numServers := agent.NumReplicas() + 1
	outCh := make(chan *GetReplicaResult, numServers)
	cancelCh := make(chan struct{})

	repRes := &GetAllReplicasResult{
		totalRequests: uint32(numServers),
		resCh:         outCh,
		cancelCh:      cancelCh,
	}

	// Loop all the servers and populate the result object
	for replicaIdx := 0; replicaIdx < numServers; replicaIdx++ {
		go func(replicaIdx int) {
			res, err := c.getOneReplica(span, id, replicaIdx, transcoder, retryStrategy, cancelCh)
			if err != nil {
				logDebugf("Failed to fetch replica from replica %d: %s", replicaIdx, err)
			} else {
				repRes.addResult(res)
			}
		}(replicaIdx)
	}

	// Start a timer to close it after the deadline
	go func() {
		select {
		case <-time.After(deadline.Sub(time.Now())):
			// If we timeout, we should close the result
			repRes.Close()
			return
		case <-cancelCh:
			// If the cancel channel closes, we are done
			return
		}
	}()

	return repRes, nil
}

// GetAnyReplicaOptions are the options available to the GetAnyReplica command.
type GetAnyReplicaOptions struct {
	Transcoder    Transcoder
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// GetAnyReplica returns the value of a particular document from a replica server.
func (c *Collection) GetAnyReplica(id string, opts *GetAnyReplicaOptions) (docOut *GetReplicaResult, errOut error) {
	if opts == nil {
		opts = &GetAnyReplicaOptions{}
	}

	span := c.startKvOpTrace("GetAnyReplica", nil)
	defer span.Finish()

	repRes, err := c.GetAllReplicas(id, &GetAllReplicaOptions{
		Timeout:       opts.Timeout,
		Transcoder:    opts.Transcoder,
		RetryStrategy: opts.RetryStrategy,
	})
	if err != nil {
		return nil, err
	}

	// Try to fetch at least one result
	res := repRes.Next()
	if res == nil {
		return nil, &KeyValueError{
			InnerError:     ErrDocumentUnretrievable,
			BucketName:     c.sb.BucketName,
			ScopeName:      c.sb.ScopeName,
			CollectionName: c.sb.CollectionName,
		}
	}

	// Close the results channel since we don't care about any of the
	// remaining result objects at this point.
	repRes.Close()

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
}

// Remove removes a document from the collection.
func (c *Collection) Remove(id string, opts *RemoveOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &RemoveOptions{}
	}

	opm := c.newKvOpManager("Remove", nil)
	defer opm.Finish()

	opm.SetDocumentID(id)
	opm.SetDuraOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	err = opm.Wait(agent.DeleteEx(gocbcore.DeleteOptions{
		Key:                    opm.DocumentID(),
		Cas:                    gocbcore.Cas(opts.Cas),
		CollectionName:         opm.CollectionName(),
		ScopeName:              opm.ScopeName(),
		DurabilityLevel:        opm.DurabilityLevel(),
		DurabilityLevelTimeout: opm.DurabilityTimeout(),
		RetryStrategy:          opm.RetryStrategy(),
		TraceContext:           opm.TraceSpan(),
	}, func(res *gocbcore.DeleteResult, err error) {
		if err != nil {
			errOut = opm.EnhanceErr(err)
			opm.Reject()
			return
		}

		mutOut = &MutationResult{}
		mutOut.cas = Cas(res.Cas)
		mutOut.mt = opm.EnhanceMt(res.MutationToken)

		opm.Resolve(mutOut.mt)
	}))
	if err != nil {
		errOut = err
	}
	return
}

// GetAndTouchOptions are the options available to the GetAndTouch operation.
type GetAndTouchOptions struct {
	Transcoder    Transcoder
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// GetAndTouch retrieves a document and simultaneously updates its expiry time.
func (c *Collection) GetAndTouch(id string, expiry uint32, opts *GetAndTouchOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetAndTouchOptions{}
	}

	opm := c.newKvOpManager("GetAndTouch", nil)
	defer opm.Finish()

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	err = opm.Wait(agent.GetAndTouchEx(gocbcore.GetAndTouchOptions{
		Key:            opm.DocumentID(),
		Expiry:         expiry,
		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		RetryStrategy:  opm.RetryStrategy(),
		TraceContext:   opm.TraceSpan(),
	}, func(res *gocbcore.GetAndTouchResult, err error) {
		if err != nil {
			errOut = opm.EnhanceErr(err)
			opm.Reject()
			return
		}

		if res != nil {
			doc := &GetResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				transcoder: opm.Transcoder(),
				contents:   res.Value,
				flags:      res.Flags,
			}

			docOut = doc
		}

		opm.Resolve(nil)
	}))
	if err != nil {
		errOut = err
	}
	return
}

// GetAndLockOptions are the options available to the GetAndLock operation.
type GetAndLockOptions struct {
	Transcoder    Transcoder
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// GetAndLock locks a document for a period of time, providing exclusive RW access to it.
// A lockTime value of over 30 seconds will be treated as 30 seconds. The resolution used to send this value to
// the server is seconds and is calculated using uint32(lockTime/time.Second).
func (c *Collection) GetAndLock(id string, lockTime time.Duration, opts *GetAndLockOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetAndLockOptions{}
	}

	opm := c.newKvOpManager("GetAndLock", nil)
	defer opm.Finish()

	opm.SetDocumentID(id)
	opm.SetTranscoder(opts.Transcoder)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	err = opm.Wait(agent.GetAndLockEx(gocbcore.GetAndLockOptions{
		Key:            opm.DocumentID(),
		LockTime:       uint32(lockTime / time.Second),
		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		RetryStrategy:  opm.RetryStrategy(),
		TraceContext:   opm.TraceSpan(),
	}, func(res *gocbcore.GetAndLockResult, err error) {
		if err != nil {
			errOut = opm.EnhanceErr(err)
			opm.Reject()
			return
		}

		if res != nil {
			doc := &GetResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
				transcoder: opm.Transcoder(),
				contents:   res.Value,
				flags:      res.Flags,
			}

			docOut = doc
		}

		opm.Resolve(nil)
	}))
	if err != nil {
		errOut = err
	}
	return
}

// UnlockOptions are the options available to the GetAndLock operation.
type UnlockOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// Unlock unlocks a document which was locked with GetAndLock.
func (c *Collection) Unlock(id string, cas Cas, opts *UnlockOptions) (errOut error) {
	if opts == nil {
		opts = &UnlockOptions{}
	}

	opm := c.newKvOpManager("Unlock", nil)
	defer opm.Finish()

	opm.SetDocumentID(id)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return err
	}
	err = opm.Wait(agent.UnlockEx(gocbcore.UnlockOptions{
		Key:            opm.DocumentID(),
		Cas:            gocbcore.Cas(cas),
		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		RetryStrategy:  opm.RetryStrategy(),
		TraceContext:   opm.TraceSpan(),
	}, func(res *gocbcore.UnlockResult, err error) {
		if err != nil {
			errOut = opm.EnhanceErr(err)
			opm.Reject()
			return
		}

		mt := opm.EnhanceMt(res.MutationToken)
		opm.Resolve(mt)
	}))
	if err != nil {
		errOut = err
	}
	return
}

// TouchOptions are the options available to the Touch operation.
type TouchOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

// Touch touches a document, specifying a new expiry time for it.
func (c *Collection) Touch(id string, expiry uint32, opts *TouchOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &TouchOptions{}
	}

	opm := c.newKvOpManager("Touch", nil)
	defer opm.Finish()

	opm.SetDocumentID(id)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}
	err = opm.Wait(agent.TouchEx(gocbcore.TouchOptions{
		Key:            opm.DocumentID(),
		Expiry:         expiry,
		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		RetryStrategy:  opm.RetryStrategy(),
		TraceContext:   opm.TraceSpan(),
	}, func(res *gocbcore.TouchResult, err error) {
		if err != nil {
			errOut = opm.EnhanceErr(err)
			opm.Reject()
			return
		}

		mutOut = &MutationResult{}
		mutOut.cas = Cas(res.Cas)
		mutOut.mt = opm.EnhanceMt(res.MutationToken)

		opm.Resolve(mutOut.mt)
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
