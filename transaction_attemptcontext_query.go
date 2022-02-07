package gocb

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocbcore/v10"
)

// Query executes the query statement on the server.
func (c *TransactionAttemptContext) Query(statement string, options *TransactionQueryOptions) (*TransactionQueryResult, error) {
	var opts TransactionQueryOptions
	if options != nil {
		opts = *options
	}
	c.queryStateLock.Lock()
	res, err := c.queryWrapperWrapper(opts.Scope, statement, opts.toSDKOptions(), "query", false, true,
		nil)
	c.queryStateLock.Unlock()
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *TransactionAttemptContext) queryModeLocked() bool {
	return c.queryState != nil
}

func (c *TransactionAttemptContext) getQueryMode(collection *Collection, id string) (*TransactionGetResult, error) {
	txdata := map[string]interface{}{
		"kv": true,
	}

	b, err := json.Marshal(txdata)
	if err != nil {
		// TODO: should we be wrapping this? It really shouldn't happen...
		return nil, err
	}

	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}
		// Note: We don't return nil here if the error is doc not found (which is what the spec says to do) instead we
		// pick up that error in GetOptional if required.
		return c.operationFailed(transactionQueryOperationFailedDef{
			ShouldNotRetry: true,
			ErrorCause:     err,
			Reason:         gocbcore.TransactionErrorReasonTransactionFailed,
		})
	}

	res, err := c.queryWrapperWrapper(nil, "EXECUTE __get", QueryOptions{
		PositionalParameters: []interface{}{c.keyspace(collection), id},
		Adhoc:                true,
	}, "queryKvGet", false, true, b)
	if err != nil {
		return nil, handleErr(err)
	}

	type getQueryResult struct {
		Scas    string          `json:"scas"`
		Doc     json.RawMessage `json:"doc"`
		TxnMeta json.RawMessage `json:"txnMeta,omitempty"`
	}

	var row getQueryResult
	err = res.One(&row)
	if err != nil {
		return nil, handleErr(err)
	}

	cas, err := fromScas(row.Scas)
	if err != nil {
		return nil, handleErr(err)
	}

	return &TransactionGetResult{
		collection: collection,
		docID:      id,

		transcoder: NewJSONTranscoder(),
		flags:      2 << 24,

		txnMeta: row.TxnMeta,

		coreRes: &gocbcore.TransactionGetResult{
			Value: row.Doc,
			Cas:   cas,
		},
	}, nil
}

func (c *TransactionAttemptContext) replaceQueryMode(doc *TransactionGetResult, valueBytes json.RawMessage) (*TransactionGetResult, error) {
	txdata := map[string]interface{}{
		"kv":   true,
		"scas": toScas(doc.coreRes.Cas),
	}

	if len(doc.txnMeta) > 0 {
		txdata["txnMeta"] = doc.txnMeta
	}

	b, err := json.Marshal(txdata)
	if err != nil {
		return nil, err
	}

	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}

		if errors.Is(err, ErrDocumentNotFound) {
			return c.operationFailed(transactionQueryOperationFailedDef{
				ErrorCause:      err,
				Reason:          gocbcore.TransactionErrorReasonTransactionFailed,
				ShouldNotCommit: true,
				ErrorClass:      gocbcore.TransactionErrorClassFailDocNotFound,
			})
		} else if errors.Is(err, ErrCasMismatch) {
			return c.operationFailed(transactionQueryOperationFailedDef{
				ErrorCause:      err,
				Reason:          gocbcore.TransactionErrorReasonTransactionFailed,
				ShouldNotCommit: true,
				ErrorClass:      gocbcore.TransactionErrorClassFailCasMismatch,
			})
		}

		return c.operationFailed(transactionQueryOperationFailedDef{
			ShouldNotRetry:  true,
			ErrorCause:      err,
			Reason:          gocbcore.TransactionErrorReasonTransactionFailed,
			ShouldNotCommit: true,
		})
	}

	params := []interface{}{c.keyspace(doc.collection), doc.docID, valueBytes, json.RawMessage("{}")}

	res, err := c.queryWrapperWrapper(nil, "EXECUTE __update", QueryOptions{
		PositionalParameters: params,
		Adhoc:                true,
	}, "queryKvReplace", false, true, b)
	if err != nil {
		return nil, handleErr(err)
	}

	type replaceQueryResult struct {
		Scas string          `json:"scas"`
		Doc  json.RawMessage `json:"doc"`
	}

	var row replaceQueryResult
	err = res.One(&row)
	if err != nil {
		return nil, handleErr(c.queryMaybeTranslateToTransactionsError(err))
	}

	cas, err := fromScas(row.Scas)
	if err != nil {
		return nil, handleErr(err)
	}

	return &TransactionGetResult{
		collection: doc.collection,
		docID:      doc.docID,

		transcoder: NewJSONTranscoder(),
		flags:      2 << 24,

		coreRes: &gocbcore.TransactionGetResult{
			Value: row.Doc,
			Cas:   cas,
		},
	}, nil
}

func (c *TransactionAttemptContext) insertQueryMode(collection *Collection, id string, valueBytes json.RawMessage) (*TransactionGetResult, error) {
	txdata := map[string]interface{}{
		"kv": true,
	}

	b, err := json.Marshal(txdata)
	if err != nil {
		return nil, &TransactionOperationFailedError{
			errorCause: err,
		}
	}

	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}
		if errors.Is(err, ErrDocumentExists) {
			return err
		}

		return c.operationFailed(transactionQueryOperationFailedDef{
			ShouldNotRetry:  true,
			ErrorCause:      err,
			Reason:          gocbcore.TransactionErrorReasonTransactionFailed,
			ShouldNotCommit: true,
		})
	}

	params := []interface{}{c.keyspace(collection), id, valueBytes, json.RawMessage("{}")}

	res, err := c.queryWrapperWrapper(nil, "EXECUTE __insert", QueryOptions{
		PositionalParameters: params,
		Adhoc:                true,
	}, "queryKvInsert", false, true, b)
	if err != nil {
		return nil, handleErr(err)
	}

	type insertQueryResult struct {
		Scas string `json:"scas"`
	}

	var row insertQueryResult
	err = res.One(&row)
	if err != nil {
		return nil, handleErr(c.queryMaybeTranslateToTransactionsError(err))
	}

	cas, err := fromScas(row.Scas)
	if err != nil {
		return nil, handleErr(err)
	}

	return &TransactionGetResult{
		collection: collection,
		docID:      id,

		transcoder: NewJSONTranscoder(),
		flags:      2 << 24,

		coreRes: &gocbcore.TransactionGetResult{
			Value: valueBytes,
			Cas:   cas,
		},
	}, nil
}

func (c *TransactionAttemptContext) removeQueryMode(doc *TransactionGetResult) error {
	txdata := map[string]interface{}{
		"kv":   true,
		"scas": toScas(doc.coreRes.Cas),
	}

	if len(doc.txnMeta) > 0 {
		txdata["txnMeta"] = doc.txnMeta
	}

	b, err := json.Marshal(txdata)
	if err != nil {
		return err
	}

	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}

		if errors.Is(err, ErrDocumentNotFound) {
			return c.operationFailed(transactionQueryOperationFailedDef{
				ErrorCause:      err,
				Reason:          gocbcore.TransactionErrorReasonTransactionFailed,
				ShouldNotCommit: true,
				ErrorClass:      gocbcore.TransactionErrorClassFailDocNotFound,
			})
		} else if errors.Is(err, ErrCasMismatch) {
			return c.operationFailed(transactionQueryOperationFailedDef{
				ErrorCause:      err,
				Reason:          gocbcore.TransactionErrorReasonTransactionFailed,
				ShouldNotCommit: true,
				ErrorClass:      gocbcore.TransactionErrorClassFailCasMismatch,
			})
		}

		return c.operationFailed(transactionQueryOperationFailedDef{
			ShouldNotRetry:  true,
			ErrorCause:      err,
			Reason:          gocbcore.TransactionErrorReasonTransactionFailed,
			ShouldNotCommit: true,
		})
	}

	params := []interface{}{c.keyspace(doc.collection), doc.docID, json.RawMessage("{}")}

	_, err = c.queryWrapperWrapper(nil, "EXECUTE __delete", QueryOptions{
		PositionalParameters: params,
		Adhoc:                true,
	}, "queryKvRemove", false, true, b)
	if err != nil {
		return handleErr(err)
	}

	return nil
}

func (c *TransactionAttemptContext) commitQueryMode() error {
	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}

		if errors.Is(err, ErrAttemptExpired) {
			return c.operationFailed(transactionQueryOperationFailedDef{
				ErrorCause:        err,
				Reason:            gocbcore.TransactionErrorReasonTransactionCommitAmbiguous,
				ShouldNotRollback: true,
				ShouldNotRetry:    true,
				ErrorClass:        gocbcore.TransactionErrorClassFailExpiry,
			})
		}

		return c.operationFailed(transactionQueryOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			ErrorCause:        err,
			Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
		})
	}

	_, err := c.queryWrapperWrapper(nil, "COMMIT", QueryOptions{
		Adhoc: true,
	}, "queryCommit", false, true, nil)
	c.txn.UpdateState(gocbcore.TransactionUpdateStateOptions{
		ShouldNotCommit: true,
	})
	if err != nil {
		return handleErr(err)
	}

	c.txn.UpdateState(gocbcore.TransactionUpdateStateOptions{
		ShouldNotRollback: true,
		ShouldNotRetry:    true,
		State:             gocbcore.TransactionAttemptStateCompleted,
	})

	return nil
}

func (c *TransactionAttemptContext) rollbackQueryMode() error {
	handleErr := func(err error) error {
		var terr *TransactionOperationFailedError
		if errors.As(err, &terr) {
			return err
		}

		if errors.Is(err, ErrAttemptNotFoundOnQuery) {
			return nil
		}

		return c.operationFailed(transactionQueryOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			ErrorCause:        err,
			Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
			ShouldNotCommit:   true,
		})
	}

	_, err := c.queryWrapperWrapper(nil, "ROLLBACK", QueryOptions{
		Adhoc: true,
	}, "queryRollback", false, false, nil)
	c.txn.UpdateState(gocbcore.TransactionUpdateStateOptions{
		ShouldNotRollback: true,
		ShouldNotCommit:   true,
	})
	if err != nil {
		return handleErr(err)
	}

	c.txn.UpdateState(gocbcore.TransactionUpdateStateOptions{
		State: gocbcore.TransactionAttemptStateRolledBack,
	})

	return nil
}

type jsonTransactionOperationFailed struct {
	Cause    interface{} `json:"cause"`
	Retry    bool        `json:"retry"`
	Rollback bool        `json:"rollback"`
	Raise    string      `json:"raise"`
}

type jsonQueryTransactionOperationFailedCause struct {
	Cause   *jsonTransactionOperationFailed `json:"cause"`
	Code    uint32                          `json:"code"`
	Message string                          `json:"message"`
}

func durabilityLevelToQueryString(level gocbcore.TransactionDurabilityLevel) string {
	switch level {
	case gocbcore.TransactionDurabilityLevelUnknown:
		return "unset"
	case gocbcore.TransactionDurabilityLevelNone:
		return "none"
	case gocbcore.TransactionDurabilityLevelMajority:
		return "majority"
	case gocbcore.TransactionDurabilityLevelMajorityAndPersistToActive:
		return "majorityAndPersistActive"
	case gocbcore.TransactionDurabilityLevelPersistToMajority:
		return "persistToMajority"
	}
	return ""
}

// queryWrapperWrapper is used by any Query based calls on TransactionAttemptContext that require a non-streaming
// result. It handles converting QueryResult To TransactionQueryResult, handling any errors that occur on the stream,
// or because of a FATAL status in metadata.
func (c *TransactionAttemptContext) queryWrapperWrapper(scope *Scope, statement string, options QueryOptions, hookPoint string,
	isBeginWork bool, existingErrorCheck bool, txData []byte) (*TransactionQueryResult, error) {
	result, err := c.queryWrapper(scope, statement, options, hookPoint, isBeginWork, existingErrorCheck, txData, false)
	if err != nil {
		return nil, err
	}

	var results []json.RawMessage
	for result.Next() {
		var r json.RawMessage
		err = result.Row(&r)
		if err != nil {
			return nil, c.queryMaybeTranslateToTransactionsError(err)
		}

		results = append(results, r)
	}

	if err := result.Err(); err != nil {
		return nil, c.queryMaybeTranslateToTransactionsError(err)
	}

	meta, err := result.MetaData()
	if err != nil {
		return nil, c.queryMaybeTranslateToTransactionsError(err)
	}

	if meta.Status == QueryStatusFatal {
		return nil, c.operationFailed(transactionQueryOperationFailedDef{
			ShouldNotRetry:  true,
			Reason:          gocbcore.TransactionErrorReasonTransactionFailed,
			ShouldNotCommit: true,
		})
	}

	return newTransactionQueryResult(results, meta, result.endpoint), nil
}

// queryWrapper is used by every query based call on TransactionAttemptContext. It handles actually sending the
// query as well as begin work and setting up query mode state. It returns a streaming QueryResult, handling only
// errors that occur at query call time.
func (c *TransactionAttemptContext) queryWrapper(scope *Scope, statement string, options QueryOptions, hookPoint string,
	isBeginWork bool, existingErrorCheck bool, txData []byte, txImplicit bool) (*QueryResult, error) {

	var target string
	if !isBeginWork {
		if !c.queryModeLocked() {
			// This is quite a big lock but we can't put the context into "query mode" until we know that begin work was
			// successful. We also can't allow any further ops to happen until we know if we're in "query mode" or not.

			// queryBeginWork implicitly performs an existingErrorCheck and the call into Serialize on the gocbcore side
			// will return an error if there have been any previously failed operations.
			if err := c.queryBeginWork(); err != nil {
				return nil, err
			}
		}

		// If we've got here then transactionQueryState cannot be nil.
		target = c.queryState.queryTarget

		if !c.txn.CanCommit() && !c.txn.ShouldRollback() {
			return nil, c.operationFailed(transactionQueryOperationFailedDef{
				ShouldNotRetry:    true,
				Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
				ErrorCause:        ErrOther,
				ErrorClass:        gocbcore.TransactionErrorClassFailOther,
				ShouldNotRollback: true,
			})
		}
	}

	if existingErrorCheck {
		if !c.txn.CanCommit() {
			return nil, c.operationFailed(transactionQueryOperationFailedDef{
				ShouldNotRetry: true,
				Reason:         gocbcore.TransactionErrorReasonTransactionFailed,
				ErrorCause:     ErrPreviousOperationFailed,
				ErrorClass:     gocbcore.TransactionErrorClassFailOther,
			})
		}
	}

	expired, err := c.hooks.HasExpiredClientSideHook(*c, hookPoint, statement)
	if err != nil {
		// This isn't meant to happen...
		return nil, &TransactionOperationFailedError{
			errorCause: err,
		}
	}
	cfg := c.txn.Config()
	if cfg.ExpirationTime < 10*time.Millisecond || expired {
		return nil, c.operationFailed(transactionQueryOperationFailedDef{
			ShouldNotRetry:    true,
			ShouldNotRollback: true,
			Reason:            gocbcore.TransactionErrorReasonTransactionExpired,
			ErrorCause:        ErrAttemptExpired,
			ErrorClass:        gocbcore.TransactionErrorClassFailExpiry,
		})
	}

	options.Metrics = true
	options.Internal.Endpoint = target
	if options.Raw == nil {
		options.Raw = make(map[string]interface{})
	}
	if !isBeginWork && !txImplicit {
		options.Raw["txid"] = c.txn.Attempt().ID
	}

	if len(txData) > 0 {
		options.Raw["txdata"] = json.RawMessage(txData)
	}
	if txImplicit {
		options.Raw["tximplicit"] = true
	}
	options.Timeout = cfg.ExpirationTime + cfg.KeyValueTimeout + (1 * time.Second) // TODO: add timeout value here

	err = c.hooks.BeforeQuery(*c, statement)
	if err != nil {
		return nil, c.queryMaybeTranslateToTransactionsError(err)
	}

	var result *QueryResult
	var queryErr error
	if scope == nil {
		result, queryErr = c.cluster.Query(statement, &options)
	} else {
		result, queryErr = scope.Query(statement, &options)
	}
	if queryErr != nil {
		return nil, c.queryMaybeTranslateToTransactionsError(queryErr)
	}

	err = c.hooks.AfterQuery(*c, statement)
	if err != nil {
		return nil, c.queryMaybeTranslateToTransactionsError(err)
	}

	return result, nil
}

func (c *TransactionAttemptContext) queryBeginWork() (errOut error) {
	waitCh := make(chan struct{}, 1)
	err := c.txn.SerializeAttempt(func(txdata []byte, err error) {
		if err != nil {
			var coreErr *gocbcore.TransactionOperationFailedError
			if errors.As(err, &coreErr) {
				// Note that we purposely do not use operationFailed here, we haven't moved into query mode yet.
				// State will continue to be controlled from the gocbcore side.
				errOut = &TransactionOperationFailedError{
					shouldRetry:       coreErr.Retry(),
					shouldNotRollback: !coreErr.Rollback(),
					errorCause:        coreErr,
					shouldRaise:       coreErr.ToRaise(),
					errorClass:        coreErr.ErrorClass(),
				}
			} else {
				errOut = err
			}
			waitCh <- struct{}{}
			return
		}

		c.queryState = &transactionQueryState{}

		cfg := c.txn.Config()
		raw := make(map[string]interface{})
		raw["durability_level"] = durabilityLevelToQueryString(cfg.DurabilityLevel)
		raw["txtimeout"] = fmt.Sprintf("%dms", cfg.ExpirationTime.Milliseconds())
		if cfg.CustomATRLocation.Agent != nil {
			// Agent being non nil signifies that this was set.
			raw["atrcollection"] = fmt.Sprintf(
				"%s.%s.%s",
				cfg.CustomATRLocation.Agent.BucketName(),
				cfg.CustomATRLocation.ScopeName,
				cfg.CustomATRLocation.CollectionName,
			)
		}

		res, err := c.queryWrapperWrapper(nil, "BEGIN WORK", QueryOptions{
			ScanConsistency: c.queryConfig.ScanConsistency,
			Raw:             raw,
			Adhoc:           true,
		}, "queryBeginWork", true, false, txdata)
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		c.queryState.queryTarget = res.endpoint

		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}
	<-waitCh

	return
}

func (c *TransactionAttemptContext) keyspace(collection *Collection) string {
	return fmt.Sprintf("default:`%s`.`%s`.`%s`", collection.Bucket().Name(), collection.ScopeName(), collection.Name())
}
