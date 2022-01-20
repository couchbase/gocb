package gocb

import (
	"encoding/json"
	"errors"
	"github.com/couchbase/gocbcore/v10"
)

func (c *TransactionAttemptContext) queryErrorCodeToError(code uint32) error {
	switch code {
	case 1065:
		return c.operationFailed(transactionQueryOperationFailedDef{
			ShouldNotRetry: true,
			Reason:         gocbcore.TransactionErrorReasonTransactionFailed,
			ErrorCause:     ErrFeatureNotAvailable,
		})
	case 1080:
		return c.operationFailed(transactionQueryOperationFailedDef{
			ShouldNotRetry:    true,
			Reason:            gocbcore.TransactionErrorReasonTransactionExpired,
			ErrorCause:        gocbcore.ErrAttemptExpired,
			ErrorClass:        gocbcore.TransactionErrorClassFailExpiry,
			ShouldNotRollback: true,
		})
	case 17004:
		return ErrAttemptNotFoundOnQuery
	case 17010:
		return c.operationFailed(transactionQueryOperationFailedDef{
			ShouldNotRetry:    true,
			Reason:            gocbcore.TransactionErrorReasonTransactionExpired,
			ErrorCause:        gocbcore.ErrAttemptExpired,
			ErrorClass:        gocbcore.TransactionErrorClassFailExpiry,
			ShouldNotRollback: true,
		})
	case 17012:
		return ErrDocumentExists
	case 17014:
		return ErrDocumentNotFound
	case 17015:
		return ErrCasMismatch
	default:
		return nil
	}
}

func (c *TransactionAttemptContext) queryCauseToOperationFailedError(queryErr *QueryError) error {

	var operationFailedErrs []jsonQueryTransactionOperationFailedCause
	if err := json.Unmarshal([]byte(queryErr.ErrorText), &operationFailedErrs); err == nil {
		for _, operationFailedErr := range operationFailedErrs {
			if operationFailedErr.Cause != nil {
				if operationFailedErr.Code >= 17000 && operationFailedErr.Code <= 18000 {
					if err := c.queryErrorCodeToError(operationFailedErr.Code); err != nil {
						return err
					}
				}

				return c.operationFailed(transactionQueryOperationFailedDef{
					ShouldNotRetry:    !operationFailedErr.Cause.Retry,
					ShouldNotRollback: !operationFailedErr.Cause.Rollback,
					Reason:            errorReasonFromString(operationFailedErr.Cause.Raise),
					ErrorCause:        queryErr,
					ShouldNotCommit:   true,
				})
			}
		}
	}
	return nil
}

func (c *TransactionAttemptContext) queryMaybeTranslateToTransactionsError(err error) error {
	if errors.Is(err, ErrTimeout) {
		return c.operationFailed(transactionQueryOperationFailedDef{
			ShouldNotRetry: true,
			Reason:         gocbcore.TransactionErrorReasonTransactionExpired,
			ErrorCause:     gocbcore.ErrAttemptExpired,
		})
	}

	var queryErr *QueryError
	if !errors.As(err, &queryErr) {
		return err
	}

	if len(queryErr.Errors) == 0 {
		return queryErr
	}

	// If an error contains a cause field, use that error.
	// Otherwise, if an error has code between 17000 and 18000 inclusive, it is a transactions-related error. Use that.
	// Otherwise, fallback to using the first error.
	if err := c.queryCauseToOperationFailedError(queryErr); err != nil {
		return err
	}

	for _, e := range queryErr.Errors {
		if e.Code >= 17000 && e.Code <= 18000 {
			if err := c.queryErrorCodeToError(e.Code); err != nil {
				return err
			}
		}
	}

	if err := c.queryErrorCodeToError(queryErr.Errors[0].Code); err != nil {
		return err
	}

	return queryErr
}

type transactionQueryOperationFailedDef struct {
	ShouldNotRetry    bool
	ShouldNotRollback bool
	Reason            gocbcore.TransactionErrorReason
	ErrorCause        error
	ErrorClass        gocbcore.TransactionErrorClass
	ShouldNotCommit   bool
}

func (c *TransactionAttemptContext) operationFailed(def transactionQueryOperationFailedDef) *TransactionOperationFailedError {
	err := &TransactionOperationFailedError{
		shouldRetry:       !def.ShouldNotRetry,
		shouldNotRollback: def.ShouldNotRollback,
		errorCause:        def.ErrorCause,
		shouldRaise:       def.Reason,
		errorClass:        def.ErrorClass,
	}

	opts := gocbcore.TransactionUpdateStateOptions{}
	if def.ShouldNotRollback {
		opts.ShouldNotRollback = true
	}
	if def.ShouldNotRetry {
		opts.ShouldNotRetry = true
	}
	if def.Reason == gocbcore.TransactionErrorReasonTransactionExpired {
		opts.HasExpired = true
	}
	if def.ShouldNotCommit {
		opts.ShouldNotCommit = true
	}
	c.txn.UpdateState(opts)

	return err
}
