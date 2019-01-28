package gocb

import (
	"testing"

	"github.com/pkg/errors"
)

func TestQueryErrors(t *testing.T) {
	errs := []QueryError{
		queryError{
			ErrorCode:    4000,
			ErrorMessage: "an error occurred",
		},
		queryError{
			ErrorCode:    4002,
			ErrorMessage: "another error occurred",
		},
	}

	err := queryMultiError{
		errors:     errs,
		contextID:  "contextID",
		endpoint:   "http://localhost:8093",
		httpStatus: 400,
	}

	wrappedErr := errors.Wrap(err, "some extra context")

	causeErr, ok := ErrorCause(wrappedErr).(QueryErrors)
	if !ok {
		t.Fatalf("Expected error cause to be QueryErrors")
	}

	if causeErr.ContextID() != err.contextID {
		t.Fatalf("Expected error context ID to be %s but was %s", err.contextID, causeErr.ContextID())
	}

	if causeErr.Endpoint() != err.endpoint {
		t.Fatalf("Expected error context ID to be %s but was %s", err.endpoint, causeErr.Endpoint())
	}

	if causeErr.HTTPStatus() != err.httpStatus {
		t.Fatalf("Expected error http status to be %d but was %d", err.httpStatus, causeErr.HTTPStatus())
	}

	expectedMessage := "[4000] an error occurred, [4002] another error occurred"
	if causeErr.Error() != expectedMessage {
		t.Fatalf("Expected error error message to be %s but was %s", expectedMessage, causeErr.Error())
	}

	if len(causeErr.Errors()) != len(errs) {
		t.Fatalf("Expected error errors length to be %d but was %d", len(errs), len(err.Errors()))
	}

	for i, e := range causeErr.Errors() {
		if e.Code() != errs[i].Code() {
			t.Fatalf("Expected error code to be %d but was %d", e.Code(), errs[i].Code())
		}

		if e.Message() != errs[i].Message() {
			t.Fatalf("Expected error message to be %s but was %s", e.Message(), errs[i].Message())
		}

		expectedMessage = errs[i].Error()
		if e.Error() != expectedMessage {
			t.Fatalf("Expected error error message to be %s but was %s", expectedMessage, e.Error())
		}
	}

	if isRetryableError(err) {
		t.Fatalf("Query error shouldn't have been retryable")
	}

	retryable := queryMultiError{
		errors: []QueryError{
			queryError{ErrorCode: 4050},
			queryError{ErrorCode: 400},
		},
		contextID:  "contextID",
		endpoint:   "http://localhost:8093",
		httpStatus: 400,
	}

	if !isRetryableError(retryable) {
		t.Fatalf("Query multi error should have been retryable")
	}

	singleRetryable := queryError{
		ErrorCode: 4050,
	}

	if !isRetryableError(singleRetryable) {
		t.Fatalf("Query error should have been retryable")
	}

	singleRetryable = queryError{
		ErrorCode: 4070,
	}

	if !isRetryableError(singleRetryable) {
		t.Fatalf("Query error should have been retryable")
	}

	singleRetryable = queryError{
		ErrorCode: 5000,
	}

	if !isRetryableError(singleRetryable) {
		t.Fatalf("Query error should have been retryable")
	}
}
