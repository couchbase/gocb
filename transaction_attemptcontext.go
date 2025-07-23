package gocb

import (
	"errors"
	"sync"

	"github.com/couchbase/gocbcore/v10"
)

type transactionAttempt struct {
	State                 TransactionAttemptState
	PreExpiryAutoRollback bool
	Expired               bool
}

type transactionQueryState struct {
	queryTarget string
	scope       *Scope
}

// defaultTransactionTranscoder is identical to the JSONTranscoder, with the exception that it does not check the
// document flags when decoding. This is to maintain compatibility with the pre-ExtBinarySupport behaviour.
type defaultTransactionTranscoder struct {
	JSONTranscoder
}

func newDefaultTransactionTranscoder() *defaultTransactionTranscoder {
	return &defaultTransactionTranscoder{}
}

func (t *defaultTransactionTranscoder) Decode(bytes []byte, _ uint32, out interface{}) error {
	return t.JSONTranscoder.Decode(bytes, 2<<24, out)
}

// TransactionAttemptContext represents a single attempt to execute a transaction.
type TransactionAttemptContext struct {
	txn        *gocbcore.Transaction
	transcoder Transcoder
	cluster    *Cluster
	hooks      TransactionHooks

	// State applicable to when we move into query mode
	queryState *transactionQueryState
	// Pointer to satisfy go vet complaining about the hooks.
	queryStateLock *sync.Mutex
	queryConfig    TransactionQueryOptions
	logger         *transactionLogger
	attemptID      string

	preferredServerGroup string
}

func (c *TransactionAttemptContext) canCommit() bool {
	return c.txn.CanCommit()
}

func (c *TransactionAttemptContext) shouldRollback() bool {
	return c.txn.ShouldRollback()
}

func (c *TransactionAttemptContext) shouldRetry() bool {
	return c.txn.ShouldRetry()
}

func (c *TransactionAttemptContext) finalErrorToRaise() gocbcore.TransactionErrorReason {
	return c.txn.FinalErrorToRaise()
}

func (c *TransactionAttemptContext) attempt() transactionAttempt {
	a := c.txn.Attempt()
	return transactionAttempt{
		State:                 TransactionAttemptState(a.State),
		PreExpiryAutoRollback: a.PreExpiryAutoRollback,
		Expired:               c.txn.TimeRemaining() <= 0 || a.Expired,
	}
}

// Internal is used for internal dealings.
// Internal: This should never be used and is not supported.
func (c *TransactionAttemptContext) Internal() *InternalTransactionAttemptContext {
	return &InternalTransactionAttemptContext{
		ac: c,
	}
}

// InternalTransactionAttemptContext is used for internal dealings.
// Internal: This should never be used and is not supported.
type InternalTransactionAttemptContext struct {
	ac *TransactionAttemptContext
}

func (iac *InternalTransactionAttemptContext) IsExpired() bool {
	return iac.ac.txn.HasExpired()
}

// Get will attempt to fetch a document, and fail the transaction if it does not exist.
func (c *TransactionAttemptContext) Get(collection *Collection, id string) (*TransactionGetResult, error) {
	return c.GetWithOptions(collection, id, nil)
}

// GetWithOptions will attempt to fetch a document, and fail the transaction if it does not exist.
func (c *TransactionAttemptContext) GetWithOptions(collection *Collection, id string, opts *TransactionGetOptions) (*TransactionGetResult, error) {
	if opts == nil {
		opts = &TransactionGetOptions{}
	}

	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		res, err := c.getQueryMode(collection, id)
		if err != nil {
			c.logger.logInfof(c.attemptID, "Query mode get failed")
			c.txn.UpdateState(gocbcore.TransactionUpdateStateOptions{
				ShouldNotCommit: !errors.Is(err, ErrDocumentNotFound),
			})
			c.queryStateLock.Unlock()
			return nil, err
		}
		c.queryStateLock.Unlock()
		return res, nil
	}
	c.queryStateLock.Unlock()

	return c.get(collection, id, "", opts)
}

// GetReplicaFromPreferredServerGroup will attempt to fetch a document from the preferred server group, and fail the transaction if it does not exist.
//
// UNCOMMITTED: This API may change in the future.
func (c *TransactionAttemptContext) GetReplicaFromPreferredServerGroup(collection *Collection, id string) (*TransactionGetResult, error) {
	return c.GetReplicaFromPreferredServerGroupWithOptions(collection, id, nil)
}

// GetReplicaFromPreferredServerGroupWithOptions will attempt to fetch a document from the preferred server group, and fail the transaction if it does not exist.
//
// UNCOMMITTED: This API may change in the future.
func (c *TransactionAttemptContext) GetReplicaFromPreferredServerGroupWithOptions(collection *Collection, id string, opts *TransactionGetReplicaFromPreferredServerGroupOptions) (*TransactionGetResult, error) {
	if opts == nil {
		opts = &TransactionGetReplicaFromPreferredServerGroupOptions{}
	}

	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		c.queryStateLock.Unlock()
		c.updateState(transactionOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
			ErrorCause: wrapError(
				ErrFeatureNotAvailable,
				"the GetReplicaFromPreferredServerGroup operation is not available for queries",
			),
			ErrorClass:      gocbcore.TransactionErrorClassFailOther,
			ShouldNotCommit: true,
		})

		return nil, createTransactionOperationFailedError(
			wrapError(
				ErrFeatureNotAvailable,
				"the GetReplicaFromPreferredServerGroup operation is not available for queries",
			),
		)
	}
	c.queryStateLock.Unlock()

	return c.get(collection, id, c.preferredServerGroup, &TransactionGetOptions{
		Transcoder: opts.Transcoder,
	})

}

func (c *TransactionAttemptContext) get(collection *Collection, id string, serverGroup string, opts *TransactionGetOptions) (resOut *TransactionGetResult, errOut error) {
	transcoder := opts.Transcoder
	if transcoder == nil {
		transcoder = c.transcoder
	}

	a, err := collection.Bucket().Internal().IORouter()
	if err != nil {
		return nil, createTransactionOperationFailedError(err)
	}

	waitCh := make(chan struct{}, 1)
	err = c.txn.Get(gocbcore.TransactionGetOptions{
		Agent:          a,
		ScopeName:      collection.ScopeName(),
		CollectionName: collection.Name(),
		Key:            []byte(id),
		ServerGroup:    serverGroup,
	}, func(res *gocbcore.TransactionGetResult, err error) {
		if err == nil {
			resOut = &TransactionGetResult{
				collection: collection,
				docID:      id,

				transcoder: transcoder,
				flags:      res.Flags,

				coreRes: res,
			}
		}
		if errors.Is(err, ErrDocumentNotFound) {
			errOut = err
			waitCh <- struct{}{}
			return
		}
		if serverGroup != "" && (errors.Is(err, ErrDocumentUnretrievable) || errors.Is(err, ErrFeatureNotAvailable)) {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		errOut = createTransactionOperationFailedError(err)
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = createTransactionOperationFailedError(err)
		return
	}
	<-waitCh

	return
}

// Replace will replace the contents of a document, failing if the document does not already exist.
func (c *TransactionAttemptContext) Replace(doc *TransactionGetResult, value interface{}) (*TransactionGetResult, error) {
	return c.ReplaceWithOptions(doc, value, nil)
}

// ReplaceWithOptions will replace the contents of a document, failing if the document does not already exist.
func (c *TransactionAttemptContext) ReplaceWithOptions(doc *TransactionGetResult, value interface{}, opts *TransactionReplaceOptions) (*TransactionGetResult, error) {
	if opts == nil {
		opts = &TransactionReplaceOptions{}
	}

	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		res, err := c.replaceQueryMode(doc, value, opts)
		c.queryStateLock.Unlock()
		if err != nil {
			c.logger.logInfof(c.attemptID, "Query mode replace failed")
			return nil, err
		}

		return res, nil
	}
	c.queryStateLock.Unlock()

	return c.replace(doc, value, opts)
}

func (c *TransactionAttemptContext) replace(doc *TransactionGetResult, value interface{}, opts *TransactionReplaceOptions) (resOut *TransactionGetResult, errOut error) {
	transcoder := opts.Transcoder
	if transcoder == nil {
		transcoder = c.transcoder
	}

	valueBytes, flags, err := transcoder.Encode(value)
	if err != nil {
		return nil, createTransactionOperationFailedError(err)
	}

	collection := doc.collection
	id := doc.docID

	waitCh := make(chan struct{}, 1)
	err = c.txn.Replace(gocbcore.TransactionReplaceOptions{
		Document: doc.coreRes,
		Value:    valueBytes,
		Flags:    flags,
	}, func(res *gocbcore.TransactionGetResult, err error) {
		if err == nil {
			resOut = &TransactionGetResult{
				collection: collection,
				docID:      id,

				transcoder: transcoder,
				flags:      res.Flags,

				coreRes: res,
			}
		}
		errOut = createTransactionOperationFailedError(err)
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = createTransactionOperationFailedError(err)
		return
	}
	<-waitCh

	return
}

// Insert will insert a new document, failing if the document already exists.
func (c *TransactionAttemptContext) Insert(collection *Collection, id string, value interface{}) (*TransactionGetResult, error) {
	return c.InsertWithOptions(collection, id, value, nil)
}

// InsertWithOptions will insert a new document, failing if the document already exists.
func (c *TransactionAttemptContext) InsertWithOptions(collection *Collection, id string, value interface{}, opts *TransactionInsertOptions) (*TransactionGetResult, error) {
	if opts == nil {
		opts = &TransactionInsertOptions{}
	}

	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		res, err := c.insertQueryMode(collection, id, value, opts)
		c.queryStateLock.Unlock()
		if err != nil {
			c.logger.logInfof(c.attemptID, "Query mode insert failed")
			return nil, err
		}

		return res, nil
	}
	c.queryStateLock.Unlock()

	return c.insert(collection, id, value, opts)
}

func (c *TransactionAttemptContext) insert(collection *Collection, id string, value interface{}, opts *TransactionInsertOptions) (resOut *TransactionGetResult, errOut error) {
	transcoder := opts.Transcoder
	if transcoder == nil {
		transcoder = c.transcoder
	}

	valueBytes, flags, err := transcoder.Encode(value)
	if err != nil {
		return nil, createTransactionOperationFailedError(err)
	}

	a, err := collection.Bucket().Internal().IORouter()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{}, 1)
	err = c.txn.Insert(gocbcore.TransactionInsertOptions{
		Agent:          a,
		ScopeName:      collection.ScopeName(),
		CollectionName: collection.Name(),
		Key:            []byte(id),
		Value:          valueBytes,
		Flags:          flags,
	}, func(res *gocbcore.TransactionGetResult, err error) {
		if err == nil {
			resOut = &TransactionGetResult{
				collection: collection,
				docID:      id,

				transcoder: transcoder,
				flags:      res.Flags,

				coreRes: res,
			}
		}
		// Handling for ExtInsertExisting
		if errors.Is(err, gocbcore.ErrDocumentExists) {
			errOut = err
		} else {
			errOut = createTransactionOperationFailedError(err)
		}
		waitCh <- struct{}{}
	})
	if err != nil {
		resOut = nil
		errOut = createTransactionOperationFailedError(err)
		return
	}
	<-waitCh

	return
}

// Remove will delete a document.
func (c *TransactionAttemptContext) Remove(doc *TransactionGetResult) error {
	return c.RemoveWithOptions(doc, nil)
}

// RemoveWithOptions will delete a document.
func (c *TransactionAttemptContext) RemoveWithOptions(doc *TransactionGetResult, _opts *TransactionRemoveOptions) error {
	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		err := c.removeQueryMode(doc)
		c.queryStateLock.Unlock()
		if err != nil {
			c.logger.logInfof(c.attemptID, "Query mode remove failed")
			return err
		}

		return nil
	}
	c.queryStateLock.Unlock()

	return c.remove(doc)
}

func (c *TransactionAttemptContext) remove(doc *TransactionGetResult) (errOut error) {
	waitCh := make(chan struct{}, 1)
	err := c.txn.Remove(gocbcore.TransactionRemoveOptions{
		Document: doc.coreRes,
	}, func(res *gocbcore.TransactionGetResult, err error) {
		errOut = createTransactionOperationFailedError(err)
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = createTransactionOperationFailedError(err)
		return
	}
	<-waitCh

	return
}

// BulkGet fetches multiple documents at once, spending a tunable level of effort to minimize read skew.
func (c *TransactionAttemptContext) BulkGet(specs []TransactionBulkGetSpec, options *TransactionBulkGetOptions) (*TransactionBulkGetResult, error) {
	if options == nil {
		options = &TransactionBulkGetOptions{}
	}

	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		c.queryStateLock.Unlock()
		return nil, operationFailed(transactionOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
			ErrorCause: wrapError(
				ErrFeatureNotAvailable,
				"the BulkGet operation is not available in query mode",
			),
			ErrorClass:      gocbcore.TransactionErrorClassFailOther,
			ShouldNotCommit: true,
		}, c)
	}
	c.queryStateLock.Unlock()

	return c.bulkGet(specs, options)
}

func (c *TransactionAttemptContext) bulkGet(specs []TransactionBulkGetSpec, options *TransactionBulkGetOptions) (resOut *TransactionBulkGetResult, errOut error) {
	if options == nil {
		options = &TransactionBulkGetOptions{}
	}

	coreOpts := gocbcore.TransactionGetMultiOptions{
		Mode: gocbcore.TransactionGetMultiMode(options.Mode),
	}

	for _, spec := range specs {
		a, err := spec.Collection.Bucket().Internal().IORouter()
		if err != nil {
			return nil, createTransactionOperationFailedError(err)
		}

		coreOpts.Specs = append(coreOpts.Specs, gocbcore.TransactionGetMultiSpec{
			Agent:          a,
			ScopeName:      spec.Collection.ScopeName(),
			CollectionName: spec.Collection.Name(),
			Key:            []byte(spec.ID),
		})
	}

	waitCh := make(chan struct{}, 1)

	err := c.txn.GetMulti(coreOpts, func(res *gocbcore.TransactionGetMultiResult, err error) {
		errOut = createTransactionOperationFailedError(err)
		resOut = &TransactionBulkGetResult{
			specs:             specs,
			defaultTranscoder: c.transcoder,
			coreRes:           res,
		}
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = createTransactionOperationFailedError(err)
		return
	}
	<-waitCh

	return
}

// BulkGetReplicaFromPreferredServerGroup fetches multiple replicas from the preferred server group at once, spending a
// tunable level of effort to minimize read skew. The preferred server group is specified via ClusterOptions.PreferredServerGroup.
func (c *TransactionAttemptContext) BulkGetReplicaFromPreferredServerGroup(
	specs []TransactionBulkGetReplicaFromPreferredServerGroupSpec,
	options *TransactionBulkGetReplicaFromPreferredServerGroupOptions,
) (*TransactionBulkGetReplicaFromPreferredServerGroupResult, error) {

	if options == nil {
		options = &TransactionBulkGetReplicaFromPreferredServerGroupOptions{}
	}

	if c.preferredServerGroup == "" {
		return nil, operationFailed(transactionOperationFailedDef{
			ShouldNotRollback: false,
			ShouldNotRetry:    true,
			ShouldNotCommit:   true,
			Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
			ErrorCause:        errors.New("PreferredServerGroup must have previously been set in ClusterOptions"),
		}, c)
	}

	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		c.queryStateLock.Unlock()
		return nil, operationFailed(transactionOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: false,
			Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
			ErrorCause: wrapError(
				ErrFeatureNotAvailable,
				"the BulkGetReplicaFromPreferredServerGroup operation is not available in query mode",
			),
			ErrorClass:      gocbcore.TransactionErrorClassFailOther,
			ShouldNotCommit: true,
		}, c)
	}
	c.queryStateLock.Unlock()

	return c.bulkGetReplicaFromPreferredServerGroup(specs, options)
}

func (c *TransactionAttemptContext) bulkGetReplicaFromPreferredServerGroup(
	specs []TransactionBulkGetReplicaFromPreferredServerGroupSpec,
	options *TransactionBulkGetReplicaFromPreferredServerGroupOptions,
) (resOut *TransactionBulkGetReplicaFromPreferredServerGroupResult, errOut error) {
	if options == nil {
		options = &TransactionBulkGetReplicaFromPreferredServerGroupOptions{}
	}

	coreOpts := gocbcore.TransactionGetMultiOptions{
		ServerGroup: c.preferredServerGroup,
		Mode:        gocbcore.TransactionGetMultiMode(options.Mode),
	}

	for _, spec := range specs {
		a, err := spec.Collection.Bucket().Internal().IORouter()
		if err != nil {
			return nil, createTransactionOperationFailedError(err)
		}

		coreOpts.Specs = append(coreOpts.Specs, gocbcore.TransactionGetMultiSpec{
			Agent:          a,
			ScopeName:      spec.Collection.ScopeName(),
			CollectionName: spec.Collection.Name(),
			Key:            []byte(spec.ID),
		})
	}

	waitCh := make(chan struct{}, 1)

	err := c.txn.GetMulti(coreOpts, func(res *gocbcore.TransactionGetMultiResult, err error) {
		errOut = createTransactionOperationFailedError(err)
		resOut = &TransactionBulkGetReplicaFromPreferredServerGroupResult{
			specs:             specs,
			defaultTranscoder: c.transcoder,
			coreRes:           res,
		}
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = createTransactionOperationFailedError(err)
		return
	}
	<-waitCh

	return
}

func (c *TransactionAttemptContext) commit() (errOut error) {
	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		err := c.commitQueryMode()
		c.queryStateLock.Unlock()
		if err != nil {
			c.logger.logInfof(c.attemptID, "Query mode commit failed")
			return err
		}

		return nil
	}
	c.queryStateLock.Unlock()

	waitCh := make(chan struct{}, 1)
	err := c.txn.Commit(func(err error) {
		errOut = createTransactionOperationFailedError(err)
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = createTransactionOperationFailedError(err)
		return
	}
	<-waitCh
	return
}

func (c *TransactionAttemptContext) rollback() (errOut error) {
	c.queryStateLock.Lock()
	if c.queryModeLocked() {
		err := c.rollbackQueryMode()
		c.queryStateLock.Unlock()
		if err != nil {
			c.logger.logInfof(c.attemptID, "Query mode rollback failed")
			return err
		}

		return nil
	}
	c.queryStateLock.Unlock()

	waitCh := make(chan struct{}, 1)
	err := c.txn.Rollback(func(err error) {
		errOut = createTransactionOperationFailedError(err)
		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = createTransactionOperationFailedError(err)
		return
	}
	<-waitCh
	return
}
