package gocb

import (
	"github.com/couchbase/gocbcore/v10"
)

type TransactionAttemptState int

const (
	AttemptStateNothingWritten = TransactionAttemptState(gocbcore.TransactionAttemptStateNothingWritten)
	AttemptStatePending        = TransactionAttemptState(gocbcore.TransactionAttemptStatePending)
	AttemptStateCommitting     = TransactionAttemptState(gocbcore.TransactionAttemptStateCommitting)
	AttemptStateCommitted      = TransactionAttemptState(gocbcore.TransactionAttemptStateCommitted)
	AttemptStateCompleted      = TransactionAttemptState(gocbcore.TransactionAttemptStateCompleted)
	AttemptStateAborted        = TransactionAttemptState(gocbcore.TransactionAttemptStateAborted)
	AttemptStateRolledBack     = TransactionAttemptState(gocbcore.TransactionAttemptStateRolledBack)
)

// TransactionResult represents the result of a transaction which was executed.
type TransactionResult struct {
	// TransactionID represents the UUID assigned to this transaction
	TransactionID string

	// UnstagingComplete indicates whether the transaction was succesfully
	// unstaged, or if a later cleanup job will be responsible.
	UnstagingComplete bool

	// Serialized represents the serialized data from this transaction if
	// the transaction was serialized as opposed to being executed.
	Serialized *TransactionSerializedContext
}
