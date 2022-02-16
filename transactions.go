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

	// TODO: fill in the rest of this config
	txn, err := t.txns.BeginTransaction(&gocbcore.TransactionOptions{
		DurabilityLevel:   gocbcore.TransactionDurabilityLevel(perConfig.DurabilityLevel),
		ExpirationTime:    perConfig.Timeout,
		CustomATRLocation: atrLocation,
	})
	if err != nil {
		return nil, err
	}

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

		logDebugf("New transaction attempt starting for %s, %s", txn.ID(), txn.Attempt().ID)

		attempt := TransactionAttemptContext{
			txn:            txn,
			transcoder:     t.transcoder,
			hooks:          t.hooksWrapper.Hooks(),
			cluster:        t.cluster,
			queryStateLock: new(sync.Mutex),
			queryConfig: TransactionQueryOptions{
				ScanConsistency: scanConsistency,
			},
		}

		if t.hooksWrapper != nil {
			t.hooksWrapper.SetAttemptContext(attempt)
		}

		lambdaErr := logicFn(&attempt)

		var autoRollback bool
		var finalErr error
		if lambdaErr == nil {
			if attempt.canCommit() {
				finalErr = attempt.commit()
			} else if attempt.shouldRollback() {
				autoRollback = true
				finalErr = attempt.rollback()
			}
		} else {
			finalErr = lambdaErr

			if attempt.shouldRollback() {
				rollbackErr := attempt.rollback()
				if rollbackErr != nil {
					logWarnf("rollback after error failed: %s", rollbackErr)
				}
			}
		}

		var finalErrCause error
		var wasUserError bool
		if finalErr != nil {
			var txnErr *TransactionOperationFailedError
			if errors.As(finalErr, &txnErr) {
				finalErrCause = txnErr.Unwrap()
			} else {
				wasUserError = true
				finalErrCause = finalErr
			}
		}

		a := attempt.attempt()

		if !a.Expired && attempt.shouldRetry() && !wasUserError {
			logDebugf("retrying lambda after backoff")
			time.Sleep(backoffCalc())
			continue
		}

		switch a.State {
		case TransactionAttemptStateNothingWritten:
			fallthrough
		case TransactionAttemptStatePending:
			fallthrough
		case TransactionAttemptStateAborted:
			fallthrough
		case TransactionAttemptStateRolledBack:
			// If we rolled back then an error must have happened and not been propagated so we can't return
			// success.
			if finalErr == nil && !autoRollback {
				return &TransactionResult{
					TransactionID: txn.ID(),
				}, nil
			}

			if a.Expired && !a.PreExpiryAutoRollback && !wasUserError {
				return nil, &TransactionExpiredError{
					result: &TransactionResult{
						TransactionID:     txn.ID(),
						UnstagingComplete: false,
					},
				}
			}

			return nil, &TransactionFailedError{
				cause: finalErrCause,
				result: &TransactionResult{
					TransactionID:     txn.ID(),
					UnstagingComplete: false,
				},
			}
		case TransactionAttemptStateCommitting:
			return nil, &TransactionCommitAmbiguousError{
				cause: finalErrCause,
				result: &TransactionResult{
					TransactionID:     txn.ID(),
					UnstagingComplete: false,
				},
			}
		case TransactionAttemptStateCommitted:
			fallthrough
		case TransactionAttemptStateCompleted:
			unstagingComplete := a.State == TransactionAttemptStateCompleted

			return &TransactionResult{
				TransactionID:     txn.ID(),
				UnstagingComplete: unstagingComplete,
			}, nil
		default:
			return nil, errors.New("invalid final transaction state")
		}
	}
}

//
// func (t *Transactions) Query(statement string, options *SingleQueryTransactionConfig) (*SingleQueryTransactionResult, error) {
// 	if options == nil {
// 		options = &SingleQueryTransactionConfig{}
// 	}
// 	var qResult SingleQueryTransactionResult
// 	tResult, err := t.Run(func(context *TransactionAttemptContext) error {
// 		res, err := context.query(statement, options.QueryOptions, true)
// 		if err != nil {
// 			return err
// 		}
//
// 		qResult.wrapped = res
// 		return nil
// 	}, &TransactionOptions{
// 		DurabilityLevel: options.DurabilityLevel,
// 		Timeout:  options.Timeout,
// 	})
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	qResult.unstagingComplete = tResult.UnstagingComplete
//
// 	return &qResult, nil
// }

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
