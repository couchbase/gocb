package twoway

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
	protoTransactions "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/transactions"
	txnerrors "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/transactions/errors"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (txn *Transaction) verifyContent(content *shared.ContentTypes, contentAsValidation *shared.ContentAsPerformerValidation) error {
	if len(contentAsValidation.GetExpectedContentBytes()) == 0 {
		return nil
	}

	switch contentAsValidation.GetContentAs().GetAs().(type) {
	case *shared.ContentAs_AsJsonObject:
		var actual map[string]interface{}
		err := json.Unmarshal(content.GetContentAsBytes(), &actual)
		if err != nil {
			err := status.Error(codes.FailedPrecondition, "couldn't reparse content as json object")
			txn.fatalError = err
			return err
		}
		var expected map[string]interface{}
		err = json.Unmarshal(contentAsValidation.ExpectedContentBytes, &expected)
		if err != nil {
			err := status.Error(codes.FailedPrecondition, "couldn't parse expected content as json object")
			txn.fatalError = err
			return err
		}
		if !reflect.DeepEqual(expected, actual) {
			txn.logger.Logf(logrus.WarnLevel, "Get result did not match expected. Expected: %s, was :%s",
				string(contentAsValidation.ExpectedContentBytes), string(content.GetContentAsBytes()))
			err := status.Errorf(codes.FailedPrecondition, "get result did not match expected, expected: %s, was :%s",
				string(contentAsValidation.ExpectedContentBytes), string(content.GetContentAsBytes()))
			txn.fatalError = err
			return err
		}
	case *shared.ContentAs_AsJsonArray:
		var actual []interface{}
		err := json.Unmarshal(content.GetContentAsBytes(), &actual)
		if err != nil {
			err := status.Error(codes.FailedPrecondition, "couldn't reparse content as json array")
			txn.fatalError = err
			return err
		}
		var expected []interface{}
		err = json.Unmarshal(contentAsValidation.ExpectedContentBytes, &expected)
		if err != nil {
			err := status.Error(codes.FailedPrecondition, "couldn't parse expected content as json array")
			txn.fatalError = err
			return err
		}
		if !reflect.DeepEqual(expected, actual) {
			txn.logger.Logf(logrus.WarnLevel, "Get result did not match expected. Expected: %s, was :%s",
				string(contentAsValidation.ExpectedContentBytes), string(content.GetContentAsBytes()))
			err := status.Errorf(codes.FailedPrecondition, "get result did not match expected, expected: %s, was :%s",
				string(contentAsValidation.ExpectedContentBytes), string(content.GetContentAsBytes()))
			txn.fatalError = err
			return err
		}
	case *shared.ContentAs_AsByteArray:
		if !slices.Equal(contentAsValidation.GetExpectedContentBytes(), content.GetContentAsBytes()) {
			txn.logger.Logf(logrus.WarnLevel, "Get result did not match expected binary. Expected: %s, was :%s",
				contentAsValidation.ExpectedContentBytes, content.GetContentAsBytes())
			err := status.Errorf(codes.FailedPrecondition, "get result did not match expected binary, expected: %s, was :%s",
				contentAsValidation.ExpectedContentBytes, content.GetContentAsBytes())
			txn.fatalError = err
			return err
		}
	default:
		err := status.Error(codes.FailedPrecondition, "can't verify content with given ContentAs type")
		txn.fatalError = err
	}
	return nil
}

// verifyGetResultExpectedContentJSON is the 'legacy' content verification. Now superseded by ContentAsValidation, but
// tests still use it for now.
func (txn *Transaction) verifyGetResultExpectedContentJSON(res *gocb.TransactionGetResult, expectedJSON string) error {
	if len(expectedJSON) > 0 {
		var actualJSON json.RawMessage
		err := res.Content(&actualJSON)
		if err != nil {
			txn.logger.Logf(logrus.WarnLevel, "Get result failed to be read: %v", err)
			txn.fatalError = txnerrors.ErrInternal
			return txnerrors.ErrInternal
		}

		if string(actualJSON) != expectedJSON {
			txn.logger.Logf(logrus.WarnLevel, "Get result did not match expected. Expected: %s, was :%s",
				expectedJSON, string(actualJSON))
			txn.fatalError = txnerrors.ErrTestFailed
			return txnerrors.ErrTestFailed
		}
	}

	return nil
}

// Both gocb.TransactionBulkGetResult and gocb.TransactionBulkGetReplicaFromPreferredServerGroupResult currently have identical APIs.
// Using this interface to DRY the verification code.
type bulkGetResult interface {
	Exists(idx uint) bool
	ContentAt(idx uint, valuePtr interface{}) error
}

func (txn *Transaction) verifyBulkGetResult(res bulkGetResult, protoSpecs []*protoTransactions.CommandGetMulti_TransactionGetMultiSpec) error {
	for idx, protoSpec := range protoSpecs {
		if res.Exists(uint(idx)) != protoSpec.GetExpectPresent() {
			txn.logger.Logf(logrus.WarnLevel, "Expected Exists at index %d to be %v, was %v", idx, protoSpec.GetExpectPresent(), res.Exists(uint(idx)))
		}

		if protoSpec.ContentAsValidation == nil {
			continue
		}

		content, err := helpers.ParseContentAs(protoSpec.ContentAsValidation.ContentAs, func(valuePtr interface{}) error {
			return res.ContentAt(uint(idx), valuePtr)
		})
		if protoSpec.ContentAsValidation.ExpectSuccess {
			if err != nil {
				err := status.Error(codes.FailedPrecondition, fmt.Sprintf("content as parsing failed: %v", err))
				txn.fatalError = err
				return err
			}

			err := txn.verifyContent(content, protoSpec.ContentAsValidation)
			if err != nil {
				return err
			}
		} else {
			if err == nil {
				err := status.Error(codes.FailedPrecondition, "content as parsing succeeded")
				txn.fatalError = err
				return err
			}
		}
	}

	return nil
}

func (txn *Transaction) verifyGetResult(res *gocb.TransactionGetResult, contentAsValidation *shared.ContentAsPerformerValidation) error {
	if contentAsValidation == nil || contentAsValidation.ContentAs == nil {
		return nil
	}

	content, err := helpers.ParseContentAs(contentAsValidation.ContentAs, func(valuePtr interface{}) error {
		return res.Content(valuePtr)
	})
	if contentAsValidation.ExpectSuccess {
		if err != nil {
			err := status.Error(codes.FailedPrecondition, fmt.Sprintf("content as parsing failed: %v", err))
			txn.fatalError = err
			return err
		}

		err := txn.verifyContent(content, contentAsValidation)
		if err != nil {
			return err
		}
	} else {
		if err == nil {
			err := status.Error(codes.FailedPrecondition, "content as parsing succeeded")
			txn.fatalError = err
			return err
		}
	}

	return nil
}

func (txn *Transaction) verifyQueryResult(res *gocb.TransactionQueryResult,
	resErr error, c *protoTransactions.CommandQuery, doNotPropogateError bool) error {

	rows, err := txn.compoundQueryErrorsAndIterateRows("Query", res, resErr)
	if err := txn.verifyExpectations("Query Stream "+c.Statement, c.ExpectedResult, err, doNotPropogateError); err != nil {
		return err
	}
	if res == nil {
		// If verify didn't return error and we don't have a result then we must be (purposely) not returning the error.
		return nil
	}

	meta, err := res.MetaData()
	if err != nil {
		txn.logger.Logf(logrus.WarnLevel, "Query metadata failed to be read: %v", err)
		txn.fatalError = txnerrors.ErrInternal
		return txnerrors.ErrInternal
	}

	if err := txn.verifyQueryRows(c, rows, meta); err != nil {
		return err
	}

	return nil
}

func (txn *Transaction) verifySingleQueryResult(res *gocb.QueryResult,
	resErr error, c *protoTransactions.CommandQuery) (*protoTransactions.TransactionSingleQueryResponse, error) {

	buildResponse := func(err error, errDuringStream bool) *protoTransactions.TransactionSingleQueryResponse {
		var raise gocb.TransactionErrorReason = 255
		switch err.(type) {
		case *gocb.TransactionFailedError:
			raise = gocb.TransactionErrorReasonTransactionFailed
		case *gocb.TransactionExpiredError:
			raise = gocb.TransactionErrorReasonTransactionExpired
		case *gocb.TransactionCommitAmbiguousError:
			raise = gocb.TransactionErrorReasonTransactionCommitAmbiguous
		case *gocb.TransactionFailedPostCommit:
			raise = gocb.TransactionErrorReasonTransactionFailedPostCommit
		}
		res := &protoTransactions.TransactionSingleQueryResponse{}
		if errDuringStream {
			res.ExceptionDuringStreaming = txnRaiseToProtocolRaise(raise)
			res.ExceptionCauseDuringStreaming = mapFinalErrorCause(err)
		} else {
			res.Exception = txnRaiseToProtocolRaise(raise)
			res.ExceptionCause = mapFinalErrorCause(err)
		}

		return res
	}

	var errDuringStream bool
	var rows []map[string]interface{}
	if resErr == nil {
		for res.Next() {
			var row map[string]interface{}
			if err := res.Row(&row); err != nil {
				txn.logger.Logf(logrus.WarnLevel, c.Statement+" result failed to be read: %v", err)
				txn.fatalError = err
				resErr = err
				errDuringStream = true
				continue
			}

			rows = append(rows, row)
		}

		if err := res.Err(); err != nil {
			txn.logger.Logf(logrus.WarnLevel, c.Statement+" result returned error during streaming: %v", err)
			txn.fatalError = err
			resErr = err
			errDuringStream = true
		}
	}

	if err := txn.verifyExpectations("Single Query Stream "+c.Statement, c.ExpectedResult, resErr, false); err != nil {
		return buildResponse(err, errDuringStream), nil
	}
	if res == nil {
		// If verify didn't return error and we don't have a result then we must be (purposely) not returning the error.
		return &protoTransactions.TransactionSingleQueryResponse{}, nil
	}

	meta, err := res.MetaData()
	if err != nil {
		txn.logger.Logf(logrus.WarnLevel, "Query metadata failed to be read: %v", err)
		txn.fatalError = txnerrors.ErrInternal
		return nil, txnerrors.ErrInternal
	}

	if err := txn.verifyQueryRows(c, rows, meta); err != nil {
		return buildResponse(err, errDuringStream), nil
	}

	return &protoTransactions.TransactionSingleQueryResponse{}, nil
}

func (txn *Transaction) verifyQueryRows(request *protoTransactions.CommandQuery, rows []map[string]interface{}, meta *gocb.QueryMetaData) error {
	if request.CheckRowCount {
		if len(rows) != int(request.ExpectedRowCount) {
			txn.logger.Logf(logrus.WarnLevel, "Query result did not match expected. Expected: %d rows, was :%d",
				request.ExpectedRowCount, len(rows))
			txn.fatalError = txnerrors.ErrTestFailed
			return txnerrors.ErrTestFailed
		}
	}

	if request.CheckMutations {
		if meta.Metrics.MutationCount != uint64(request.ExpectedMutations) {
			txn.logger.Logf(logrus.WarnLevel, "Query result did not match expected. Expected: %d mutations, was :%d",
				request.ExpectedMutations, meta.Metrics.MutationCount)
			txn.fatalError = txnerrors.ErrTestFailed
			return txnerrors.ErrTestFailed
		}
	}

	if request.CheckRowContent {
		if len(rows) != int(request.ExpectedRowCount) {
			txn.logger.Logf(logrus.WarnLevel, "Query result did not match expected. Expected: %d rows, was :%d",
				request.ExpectedRowCount, len(rows))
			txn.fatalError = txnerrors.ErrTestFailed
			return txnerrors.ErrTestFailed
		}

		for i := 0; i < len(rows); i++ {
			var expectedRow map[string]interface{}
			if err := json.Unmarshal([]byte(request.ExpectedRows[i]), &expectedRow); err != nil {
				txn.logger.Logf(logrus.WarnLevel, "Failed to unmarshal expected row: %s: %v", request.ExpectedRows[i], err)
				txn.fatalError = txnerrors.ErrInternal
				return txnerrors.ErrInternal
			}

			actualRow := rows[i]
			if len(actualRow) != len(expectedRow) {
				txn.logger.Logf(logrus.WarnLevel, "Query result row did not match expected - mismatched len. Expected: %#v, was :%#v",
					expectedRow, actualRow)
				txn.fatalError = txnerrors.ErrTestFailed
				return txnerrors.ErrTestFailed
			}

			for field, value := range expectedRow {
				actual, ok := actualRow[field]
				if !ok {
					txn.logger.Logf(logrus.WarnLevel, "Query result row did not match expected - mismatched field. Expected: %#v, was :%#v - field: %s",
						expectedRow, actualRow, field)
					txn.fatalError = txnerrors.ErrTestFailed
					return txnerrors.ErrTestFailed
				}

				if actual != value {
					txn.logger.Logf(logrus.WarnLevel, "Query result row did not match expected - mismatched value. Expected: %#v, was :%#v - field: %s",
						expectedRow, actualRow, field)
					txn.fatalError = txnerrors.ErrTestFailed
					return txnerrors.ErrTestFailed
				}
			}
		}
	}

	return nil
}

func (txn *Transaction) verifyExpectations(opName string, expectations []*protoTransactions.ExpectedResult, err error,
	doNotPropagate bool) error {

	maybePropagateError := func() error {
		if doNotPropagate {
			txn.logger.Logf(logrus.InfoLevel, "Not propagating the error, as requested by test")
			return nil
		}
		return err
	}

	if len(expectations) == 0 {
		// No validation to be done.
		return maybePropagateError()
	}

	for _, expectation := range expectations {
		switch t := expectation.Result.(type) {
		case *protoTransactions.ExpectedResult_Success:
			if err == nil {
				return maybePropagateError()
			}
		case *protoTransactions.ExpectedResult_AnythingAllowed:
			return maybePropagateError()
		case *protoTransactions.ExpectedResult_Error:
			var txnErr *gocb.TransactionOperationFailedError
			if errors.As(err, &txnErr) {
				errWrapper := &protoTransactions.ErrorWrapper{
					RetryTransaction:    txnErr.Retry(),
					AutoRollbackAttempt: txnErr.Rollback(),
					ToRaise:             txnRaiseToProtocolRaise(txnErr.ToRaise()),
				}

				if t.Error.RetryTransaction == errWrapper.RetryTransaction &&
					t.Error.AutoRollbackAttempt == errWrapper.AutoRollbackAttempt && t.Error.ToRaise == errWrapper.ToRaise {
					switch cause := t.Error.Cause.Cause.(type) {
					case *protoTransactions.ExpectedCause_DoNotCheck:
						return maybePropagateError()
					case *protoTransactions.ExpectedCause_Exception:
						mappedCause := mapTOFCause(txnErr)
						if cause.Exception == mappedCause {
							return maybePropagateError()
						}
					}
				}
			}
		case *protoTransactions.ExpectedResult_Exception:
			if mapFinalErrorCause(err) == t.Exception {
				return maybePropagateError()
			}
		}
	}

	txn.logger.Logf(logrus.WarnLevel, "Operation %s failed unexpectedly: expected: %+v, got: %+v", opName, expectations, err)
	txn.fatalError = err

	return maybePropagateError()
}

func (txn *Transaction) compoundQueryErrorsAndIterateRows(op string, result *gocb.TransactionQueryResult,
	resErr error) ([]map[string]interface{}, error) {
	if resErr != nil {
		txn.logger.Logf(logrus.WarnLevel, op+" returned error: %v", resErr)
		return nil, resErr
	}
	var rows []map[string]interface{}
	for result.Next() {
		var row map[string]interface{}
		if err := result.Row(&row); err != nil {
			txn.logger.Logf(logrus.WarnLevel, op+" result failed to be read: %v", err)
			txn.fatalError = txnerrors.ErrInternal
			return nil, txnerrors.ErrInternal
		}

		rows = append(rows, row)
	}

	return rows, nil
}
