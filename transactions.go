package gocb

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/couchbase/gocbcore/v10"
)

// AttemptFunc represents the lambda used by the Transactions Run function.
type AttemptFunc func(*TransactionAttemptContext) error

// Transactions can be used to perform transactions.
type Transactions struct {
	config     TransactionsConfig
	cluster    *Cluster
	transcoder Transcoder

	txns                *gocbcore.TransactionsManager
	hooksWrapper        transactionHooksWrapper
	cleanupHooksWrapper transactionCleanupHooksWrapper
	cleanupCollections  []gocbcore.TransactionLostATRLocation
}

// initTransactions will initialize the transactions library and return a Transactions
// object which can be used to perform transactions.
func (c *Cluster) initTransactions(config TransactionsConfig) (*Transactions, error) {
	// Note that gocbcore will handle a lot of default values for us.
	if config.QueryConfig.ScanConsistency == 0 {
		config.QueryConfig.ScanConsistency = QueryScanConsistencyRequestPlus
	}
	if config.DurabilityLevel == DurabilityLevelUnknown {
		config.DurabilityLevel = DurabilityLevelMajority
	}

	var hooksWrapper transactionHooksWrapper
	if config.Internal.Hooks == nil {
		hooksWrapper = &noopHooksWrapper{
			TransactionDefaultHooks: gocbcore.TransactionDefaultHooks{},
			hooks:                   transactionsDefaultHooks{},
		}
	} else {
		hooksWrapper = &coreTxnsHooksWrapper{
			hooks: config.Internal.Hooks,
		}
	}

	var cleanupHooksWrapper transactionCleanupHooksWrapper
	if config.Internal.CleanupHooks == nil {
		cleanupHooksWrapper = &noopCleanupHooksWrapper{
			TransactionDefaultCleanupHooks: gocbcore.TransactionDefaultCleanupHooks{},
		}
	} else {
		cleanupHooksWrapper = &coreTxnsCleanupHooksWrapper{
			CleanupHooks: config.Internal.CleanupHooks,
		}
	}

	var clientRecordHooksWrapper clientRecordHooksWrapper
	if config.Internal.ClientRecordHooks == nil {
		clientRecordHooksWrapper = &noopClientRecordHooksWrapper{
			TransactionDefaultCleanupHooks:      gocbcore.TransactionDefaultCleanupHooks{},
			TransactionDefaultClientRecordHooks: gocbcore.TransactionDefaultClientRecordHooks{},
		}
	} else {
		clientRecordHooksWrapper = &coreTxnsClientRecordHooksWrapper{
			coreTxnsCleanupHooksWrapper: coreTxnsCleanupHooksWrapper{
				CleanupHooks: config.Internal.CleanupHooks,
			},
			ClientRecordHooks: config.Internal.ClientRecordHooks,
		}
	}

	atrLocation := gocbcore.TransactionATRLocation{}
	if config.MetadataCollection != nil {
		customATRAgent, err := c.Bucket(config.MetadataCollection.BucketName).Internal().IORouter()
		if err != nil {
			return nil, err
		}
		atrLocation.Agent = customATRAgent
		atrLocation.CollectionName = config.MetadataCollection.CollectionName
		atrLocation.ScopeName = config.MetadataCollection.ScopeName

		// We add the custom metadata collection to the cleanup collections so that lost cleanup starts watching it
		// immediately. Note that we don't do the same for the custom metadata on TransactionOptions, this is because
		// we know that that collection will be used in a transaction.
		var alreadyInCleanup bool
		for _, keySpace := range config.CleanupConfig.CleanupCollections {
			if keySpace == *config.MetadataCollection {
				alreadyInCleanup = true
				break
			}
		}

		if !alreadyInCleanup {
			config.CleanupConfig.CleanupCollections = append(config.CleanupConfig.CleanupCollections, *config.MetadataCollection)
		}
	}

	var cleanupLocs []gocbcore.TransactionLostATRLocation
	for _, keyspace := range config.CleanupConfig.CleanupCollections {
		cleanupLocs = append(cleanupLocs, gocbcore.TransactionLostATRLocation{
			BucketName:     keyspace.BucketName,
			ScopeName:      keyspace.ScopeName,
			CollectionName: keyspace.CollectionName,
		})
	}

	t := &Transactions{
		cluster:             c,
		config:              config,
		transcoder:          NewJSONTranscoder(),
		hooksWrapper:        hooksWrapper,
		cleanupHooksWrapper: cleanupHooksWrapper,
		cleanupCollections:  cleanupLocs,
	}

	corecfg := &gocbcore.TransactionsConfig{}
	corecfg.DurabilityLevel = gocbcore.TransactionDurabilityLevel(config.DurabilityLevel)
	corecfg.BucketAgentProvider = t.agentProvider
	corecfg.LostCleanupATRLocationProvider = t.atrLocationsProvider
	corecfg.CleanupClientAttempts = !config.CleanupConfig.DisableClientAttemptCleanup
	corecfg.CleanupQueueSize = config.CleanupConfig.CleanupQueueSize
	corecfg.ExpirationTime = config.Timeout
	corecfg.CleanupWindow = config.CleanupConfig.CleanupWindow
	corecfg.CleanupLostAttempts = !config.CleanupConfig.DisableLostAttemptCleanup
	corecfg.CleanupWatchATRs = !config.CleanupConfig.DisableLostAttemptCleanup
	corecfg.CustomATRLocation = atrLocation
	corecfg.Internal.Hooks = hooksWrapper
	corecfg.Internal.CleanUpHooks = cleanupHooksWrapper
	corecfg.Internal.ClientRecordHooks = clientRecordHooksWrapper
	corecfg.Internal.NumATRs = config.Internal.NumATRs
	corecfg.KeyValueTimeout = c.timeoutsConfig.KVTimeout

	txns, err := gocbcore.InitTransactions(corecfg)
	if err != nil {
		return nil, err
	}

	t.txns = txns
	return t, nil
}

// Run runs a lambda to perform a number of operations as part of a
// singular transaction.
func (t *Transactions) Run(logicFn AttemptFunc, perConfig *TransactionOptions) (*TransactionResult, error) {
	return t.run(logicFn, perConfig, false)
}

func (t *Transactions) run(logicFn AttemptFunc, perConfig *TransactionOptions, singleQueryMode bool) (*TransactionResult, error) {
	if perConfig == nil {
		perConfig = &TransactionOptions{
			DurabilityLevel: t.config.DurabilityLevel,
			Timeout:         t.config.Timeout,
		}
	}

	scanConsistency := t.config.QueryConfig.ScanConsistency

	// Gocbcore looks at whether the location agent is nil to verify whether CustomATRLocation has been set.
	atrLocation := gocbcore.TransactionATRLocation{}
	if perConfig.MetadataCollection != nil {
		customATRAgent, err := perConfig.MetadataCollection.bucket.Internal().IORouter()
		if err != nil {
			return nil, err
		}

		atrLocation.Agent = customATRAgent
		atrLocation.CollectionName = perConfig.MetadataCollection.Name()
		atrLocation.ScopeName = perConfig.MetadataCollection.ScopeName()
	}

	logger := newTransactionLogger()

	// TODO: fill in the rest of this config
	config := &gocbcore.TransactionOptions{
		DurabilityLevel:   gocbcore.TransactionDurabilityLevel(perConfig.DurabilityLevel),
		ExpirationTime:    perConfig.Timeout,
		CustomATRLocation: atrLocation,
		TransactionLogger: logger,
	}

	hooksWrapper := t.hooksWrapper
	if perConfig.Internal.Hooks != nil {
		hooksWrapper = &coreTxnsHooksWrapper{
			hooks: perConfig.Internal.Hooks,
		}
		config.Internal.Hooks = hooksWrapper
	}

	txn, err := t.txns.BeginTransaction(config)
	if err != nil {
		return nil, err
	}

	logger.setTxnID(txn.ID())

	retries := 0
	backoffCalc := func() time.Duration {
		var max float64 = 100000000 // 100 Milliseconds
		var min float64 = 1000000   // 1 Millisecond
		retries++
		backoff := min * (math.Pow(2, float64(retries)))

		if backoff > max {
			backoff = max
		}
		if backoff < min {
			backoff = min
		}

		return time.Duration(backoff)
	}

	for {
		err = txn.NewAttempt()
		if err != nil {
			return nil, err
		}

		attemptID := txn.Attempt().ID
		logDebugf("New transaction attempt starting for %s, %s", txn.ID(), attemptID)
		logger.logInfof(attemptID, "New transaction attempt starting")

		attempt := TransactionAttemptContext{
			txn:            txn,
			transcoder:     t.transcoder,
			hooks:          hooksWrapper.Hooks(),
			cluster:        t.cluster,
			queryStateLock: new(sync.Mutex),
			queryConfig: TransactionQueryOptions{
				ScanConsistency: scanConsistency,
			},
			logger:    logger,
			attemptID: attemptID,
		}

		if hooksWrapper != nil {
			hooksWrapper.SetAttemptContext(attempt)
		}

		lambdaErr := logicFn(&attempt)

		if !singleQueryMode && lambdaErr != nil {
			logger.logInfof(attemptID, "Lambda returned error and not single query mode")
			var txnErr *TransactionOperationFailedError
			if !errors.As(lambdaErr, &txnErr) {
				// We wrap non-TOF errors in a TOF.
				lambdaErr = operationFailed(transactionQueryOperationFailedDef{
					ShouldNotRetry:    true,
					ShouldNotRollback: false,
					Reason:            gocbcore.TransactionErrorReasonTransactionFailed,
					ErrorCause:        lambdaErr,
					ShouldNotCommit:   true,
				}, &attempt)
			}
		}

		finalErr := lambdaErr
		if !singleQueryMode {
			if attempt.canCommit() {
				finalErr = attempt.commit()
			}
			if attempt.shouldRollback() {
				rollbackErr := attempt.rollback()
				if rollbackErr != nil {
					logWarnf("rollback after error failed: %s", rollbackErr)
				}
			}
		}
		toRaise := attempt.finalErrorToRaise()

		if attempt.shouldRetry() && toRaise != gocbcore.TransactionErrorReasonSuccess {
			logDebugf("retrying lambda after backoff")
			sleep := backoffCalc()
			logger.logInfof(attemptID, "Will retry lambda after %s", sleep)
			time.Sleep(sleep)
			continue
		}

		// We don't want the TOF to be the cause in the final error we return so we unwrap it. Right now it's not
		// actually possible for this error to not be a TOF but a) best to handle the case where it somehow isn't and
		// b) this logic will be required for single query transactions (where a non TOF can be surfaced) anyway.
		var finalErrCause error
		if finalErr != nil {
			var txnErr *TransactionOperationFailedError
			if errors.As(finalErr, &txnErr) {
				finalErrCause = txnErr.InternalUnwrap()
			} else {
				finalErrCause = finalErr
			}
		}

		switch toRaise {
		case gocbcore.TransactionErrorReasonSuccess:
			if singleQueryMode && finalErr != nil {
				return nil, finalErr
			}

			unstagingComplete := attempt.attempt().State == TransactionAttemptStateCompleted

			return &TransactionResult{
				TransactionID:     txn.ID(),
				UnstagingComplete: unstagingComplete,
				Logs:              logger.Logs(),
			}, nil
		case gocbcore.TransactionErrorReasonTransactionFailed:
			return nil, &TransactionFailedError{
				cause: finalErrCause,
				result: &TransactionResult{
					TransactionID:     txn.ID(),
					UnstagingComplete: false,
					Logs:              logger.Logs(),
				},
			}
		case gocbcore.TransactionErrorReasonTransactionExpired:
			// If we expired during gocbcore auto-rollback then we return failed with the error cause rather
			// than expired. This occurs when we commit itself errors and gocbcore auto rolls back the transaction.
			if attempt.attempt().PreExpiryAutoRollback {
				return nil, &TransactionFailedError{
					cause: finalErrCause,
					result: &TransactionResult{
						TransactionID:     txn.ID(),
						UnstagingComplete: false,
						Logs:              logger.Logs(),
					},
				}
			}
			return nil, &TransactionExpiredError{
				result: &TransactionResult{
					TransactionID:     txn.ID(),
					UnstagingComplete: false,
					Logs:              logger.Logs(),
				},
			}
		case gocbcore.TransactionErrorReasonTransactionCommitAmbiguous:
			return nil, &TransactionCommitAmbiguousError{
				cause: finalErrCause,
				result: &TransactionResult{
					TransactionID:     txn.ID(),
					UnstagingComplete: false,
					Logs:              logger.Logs(),
				},
			}
		case gocbcore.TransactionErrorReasonTransactionFailedPostCommit:
			return &TransactionResult{
				TransactionID:     txn.ID(),
				UnstagingComplete: false,
				Logs:              logger.Logs(),
			}, nil
		default:
			return nil, errors.New("invalid final transaction state")
		}
	}
}

// Close will shut down this Transactions object, shutting down all
// background tasks associated with it.
func (t *Transactions) close() error {
	return t.txns.Close()
}

func (t *Transactions) agentProvider(bucketName string) (*gocbcore.Agent, string, error) {
	b := t.cluster.Bucket(bucketName)
	agent, err := b.Internal().IORouter()
	if err != nil {
		return nil, "", err
	}

	return agent, "", err
}

func (t *Transactions) atrLocationsProvider() ([]gocbcore.TransactionLostATRLocation, error) {
	return t.cleanupCollections, nil
}

func (t *Transactions) singleQuery(statement string, scope *Scope, opts QueryOptions) (*QueryResult, error) {
	if opts.Context != nil {
		return nil, makeInvalidArgumentsError("cannot use context and transactions together")
	}

	config := &TransactionOptions{
		DurabilityLevel: opts.AsTransaction.DurabilityLevel,
		Timeout:         opts.Timeout,
	}
	config.Internal.Hooks = opts.AsTransaction.Internal.Hooks

	var queryRes *QueryResult
	res, err := t.run(func(context *TransactionAttemptContext) error {
		// We need to tell the core loop that autocommit and autorollback are disabled.
		// context.txn.UpdateState(gocbcore.TransactionUpdateStateOptions{
		// 	ShouldNotCommit:   true,
		// 	ShouldNotRollback: true,
		// })
		qRes, err := context.queryWrapper(scope, statement, opts, "query", false, false,
			nil, true)
		if err != nil {
			return err
		}

		queryRes = qRes
		// If the result contains rows then we can't immediately check for errors, so we need to return here.
		if len(queryRes.peekNext()) > 0 {
			// We consider this success so tell the core to not retry - any errors on stream will happen outside the
			// context of the core loop.
			// context.txn.UpdateState(gocbcore.TransactionUpdateStateOptions{
			// 	ShouldNotRetry: true,
			// })

			return nil
		}

		if err := qRes.Err(); err != nil {
			return queryMaybeTranslateToTransactionsError(err, context)
		}

		meta, err := qRes.MetaData()
		if err != nil {
			return queryMaybeTranslateToTransactionsError(err, context)
		}

		if meta.Status == QueryStatusFatal {
			return operationFailed(transactionQueryOperationFailedDef{
				ShouldNotRetry:  true,
				Reason:          gocbcore.TransactionErrorReasonTransactionFailed,
				ShouldNotCommit: true,
			}, context)
		}

		// We won't do autocommit or autorollback so tell the core loop to not retry.
		// context.txn.UpdateState(gocbcore.TransactionUpdateStateOptions{
		// 	ShouldNotRetry: true,
		// })

		return nil
	}, config, true)
	if err != nil {
		var expiredErr *TransactionExpiredError
		if errors.As(err, &expiredErr) {
			return nil, ErrUnambiguousTimeout
		}
		return nil, err
	}

	queryRes.transactionID = res.TransactionID

	return queryRes, nil
}

// TransactionsInternal exposes internal methods that are useful for testing and/or
// other forms of internal use.
type TransactionsInternal struct {
	parent *Transactions
}

// Internal returns an TransactionsInternal object which can be used for specialized
// internal use cases.
func (t *Transactions) Internal() *TransactionsInternal {
	return &TransactionsInternal{
		parent: t,
	}
}

// ForceCleanupQueue forces the transactions client cleanup queue to drain without waiting for expirations.
func (t *TransactionsInternal) ForceCleanupQueue() []TransactionCleanupAttempt {
	waitCh := make(chan []gocbcore.TransactionsCleanupAttempt, 1)
	t.parent.txns.Internal().ForceCleanupQueue(func(attempts []gocbcore.TransactionsCleanupAttempt) {
		waitCh <- attempts
	})
	coreAttempts := <-waitCh

	var attempts []TransactionCleanupAttempt
	for _, attempt := range coreAttempts {
		attempts = append(attempts, cleanupAttemptFromCore(attempt))
	}

	return attempts
}

// CleanupQueueLength returns the current length of the client cleanup queue.
func (t *TransactionsInternal) CleanupQueueLength() int32 {
	return t.parent.txns.Internal().CleanupQueueLength()
}

// ClientCleanupEnabled returns whether the client cleanup process is enabled.
func (t *TransactionsInternal) ClientCleanupEnabled() bool {
	return t.parent.txns.Config().CleanupClientAttempts
}

// CleanupLocations returns the set of locations currently being watched by the lost transactions process.
func (t *TransactionsInternal) CleanupLocations() []gocbcore.TransactionLostATRLocation {
	return t.parent.txns.Internal().CleanupLocations()
}
