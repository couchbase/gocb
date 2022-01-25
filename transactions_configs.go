package gocb

import (
	"time"

	"github.com/couchbase/gocbcore/v10"
)

// TransactionDurabilityLevel specifies the level of synchronous replication to use for a transaction.
// UNCOMMITTED: This API may change in the future.
type TransactionDurabilityLevel uint8

const (
	// TransactionDurabilityLevelUnknown indicates to use the default level.
	TransactionDurabilityLevelUnknown = TransactionDurabilityLevel(gocbcore.TransactionDurabilityLevelUnknown)

	// TransactionDurabilityLevelNone indicates that no durability is needed.
	TransactionDurabilityLevelNone = TransactionDurabilityLevel(gocbcore.TransactionDurabilityLevelNone)

	// TransactionDurabilityLevelMajority indicates the operation must be replicated to the majority.
	TransactionDurabilityLevelMajority = TransactionDurabilityLevel(gocbcore.TransactionDurabilityLevelMajority)

	// TransactionDurabilityLevelMajorityAndPersistToActive indicates the operation must be replicated
	// to the majority and persisted to the active server.
	TransactionDurabilityLevelMajorityAndPersistToActive = TransactionDurabilityLevel(gocbcore.TransactionDurabilityLevelMajorityAndPersistToActive)

	// TransactionDurabilityLevelPersistToMajority indicates the operation must be persisted to the active server.
	TransactionDurabilityLevelPersistToMajority = TransactionDurabilityLevel(gocbcore.TransactionDurabilityLevelPersistToMajority)
)

// TransactionsConfig specifies various tunable options related to transactions.
// UNCOMMITTED: This API may change in the future.
type TransactionsConfig struct {
	// MetadataCollection specifies a specific location to place meta-data.
	MetadataCollection *TransactionKeyspace

	// ExpirationTime sets the maximum time that transactions created
	// by this Transactions object can run for, before expiring.
	ExpirationTime time.Duration

	// DurabilityLevel specifies the durability level that should be used
	// for all write operations performed by this Transactions object.
	DurabilityLevel TransactionDurabilityLevel

	// CleanupWindow specifies how often to the cleanup process runs
	// attempting to garbage collection transactions that have failed but
	// were not cleaned up by the previous client.
	CleanupWindow time.Duration

	// CleanupClientAttempts controls where any transaction attempts made
	// by this client are automatically removed.
	CleanupClientAttempts bool

	// CleanupLostAttempts controls where a background process is created
	// to cleanup any ‘lost’ transaction attempts.
	CleanupLostAttempts bool

	// CleanupQueueSize controls the maximum queue size for the cleanup thread.
	CleanupQueueSize uint32

	// QueryConfig specifies any query configuration to use in transactions.
	QueryConfig TransactionsQueryConfig

	// Internal specifies a set of options for internal use.
	// Internal: This should never be used and is not supported.
	Internal struct {
		Hooks             TransactionHooks
		CleanupHooks      TransactionCleanupHooks
		ClientRecordHooks TransactionClientRecordHooks
		NumATRs           int
	}
}

// TransactionOptions specifies options which can be overridden on a per transaction basis.
// UNCOMMITTED: This API may change in the future.
type TransactionOptions struct {
	// DurabilityLevel specifies the durability level that should be used
	// for all write operations performed by this transaction.
	DurabilityLevel TransactionDurabilityLevel
	ExpirationTime  time.Duration
}

// TransactionsQueryConfig specifies various tunable query options related to transactions.
// UNCOMMITTED: This API may change in the future.
type TransactionsQueryConfig struct {
	ScanConsistency QueryScanConsistency
}

// SingleQueryTransactionConfig specifies various tunable query options related to single query transactions.
// type SingleQueryTransactionConfig struct {
// 	QueryOptions    QueryOptions
// 	DurabilityLevel TransactionDurabilityLevel
// 	ExpirationTime  time.Duration
// }

// TransactionKeyspace specifies a specific location where ATR entries should be
// placed when performing transactions.
// UNCOMMITTED: This API may change in the future.
type TransactionKeyspace struct {
	BucketName     string
	ScopeName      string
	CollectionName string
}

// TransactionQueryOptions specifies the set of options available when running queries as a part of a transaction.
// This is a subset of QueryOptions.
// UNCOMMITTED: This API may change in the future.
type TransactionQueryOptions struct {
	ScanConsistency QueryScanConsistency
	Profile         QueryProfileMode

	// ScanCap is the maximum buffered channel size between the indexer connectionManager and the query service for index scans.
	ScanCap uint32

	// PipelineBatch controls the number of items execution operators can batch for Fetch from the KV.
	PipelineBatch uint32

	// PipelineCap controls the maximum number of items each execution operator can buffer between various operators.
	PipelineCap uint32

	// ScanWait is how long the indexer is allowed to wait until it can satisfy ScanConsistency/ConsistentWith criteria.
	ScanWait time.Duration
	Readonly bool

	// ClientContextID provides a unique ID for this query which can be used matching up requests between connectionManager and
	// server. If not provided will be assigned a uuid value.
	ClientContextID      string
	PositionalParameters []interface{}
	NamedParameters      map[string]interface{}

	// FlexIndex tells the query engine to use a flex index (utilizing the search service).
	FlexIndex bool

	// Raw provides a way to provide extra parameters in the request body for the query.
	Raw map[string]interface{}

	Prepared bool

	Scope *Scope
}

func (qo *TransactionQueryOptions) toSDKOptions() QueryOptions {
	scanc := qo.ScanConsistency
	if scanc == 0 {
		scanc = QueryScanConsistencyRequestPlus
	}

	return QueryOptions{
		ScanConsistency:      scanc,
		Profile:              qo.Profile,
		ScanCap:              qo.ScanCap,
		PipelineBatch:        qo.PipelineBatch,
		PipelineCap:          qo.PipelineCap,
		ScanWait:             qo.ScanWait,
		Readonly:             qo.Readonly,
		ClientContextID:      qo.ClientContextID,
		PositionalParameters: qo.PositionalParameters,
		NamedParameters:      qo.NamedParameters,
		Raw:                  qo.Raw,
		Adhoc:                !qo.Prepared,
		FlexIndex:            qo.FlexIndex,
	}
}
