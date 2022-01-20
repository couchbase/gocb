package gocb

import (
	"github.com/couchbase/gocbcore/v10"
)

// TransactionQueryResult allows access to the results of a query.
type TransactionQueryResult struct {
	wrapped *QueryResult
	context *TransactionAttemptContext

	err error
}

func newTransactionQueryResult(wrapped *QueryResult, context *TransactionAttemptContext) *TransactionQueryResult {
	return &TransactionQueryResult{
		wrapped: wrapped,
		context: context,
	}
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *TransactionQueryResult) Next() bool {
	if r.wrapped == nil {
		return false
	}

	hasNext := r.wrapped.Next()
	if hasNext {
		return true
	}

	meta, err := r.wrapped.MetaData()
	if err != nil {
		r.err = err
	}

	if err := r.wrapped.Err(); err != nil {
		r.err = r.context.queryMaybeTranslateToTransactionsError(err)
		return false
	}

	if meta.Status == QueryStatusFatal {
		r.err = r.context.operationFailed(transactionQueryOperationFailedDef{
			ShouldNotRetry:  true,
			Reason:          gocbcore.TransactionErrorReasonTransactionFailed,
			ShouldNotCommit: true,
		})
		return false
	}

	return false
}

// Row returns the contents of the current row
func (r *TransactionQueryResult) Row(valuePtr interface{}) error {
	return r.wrapped.Row(valuePtr)
}

// Err returns any errors that have occurred on the stream
func (r *TransactionQueryResult) Err() error {
	if r.err != nil {
		return r.err
	}

	return r.wrapped.Err()
}

// Close marks the results as closed, returning any errors that occurred during reading the results.
func (r *TransactionQueryResult) Close() error {
	err := r.wrapped.Close()
	if r.err != nil {
		return r.err
	}

	return err
}

// One assigns the first value from the results into the value pointer.
// It will close the results but not before iterating through all remaining
// results, as such this should only be used for very small resultsets - ideally
// of, at most, length 1.
func (r *TransactionQueryResult) One(valuePtr interface{}) error {
	// Prime the row
	if !r.Next() {
		if r.err != nil {
			return r.err
		}

		return ErrNoResult
	}

	err := r.Row(valuePtr)
	if err != nil {
		return err
	}

	for r.Next() {
	}
	if r.err != nil {
		return r.err
	}

	return nil
}

// MetaData returns any meta-data that was available from this query.  Note that
// the meta-data will only be available once the object has been closed (either
// implicitly or explicitly).
func (r *TransactionQueryResult) MetaData() (*QueryMetaData, error) {
	return r.wrapped.MetaData()
}
