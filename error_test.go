package gocb

import (
	"testing"

	"github.com/couchbase/gocbcore/v8"
)

func TestIsCasMismatchError(t *testing.T) {
	err := &gocbcore.KvError{
		Code: gocbcore.StatusKeyExists,
	}

	mismatchErr := maybeEnhanceKVErr(err, "myfakekey", true)
	if IsCasMismatchError(mismatchErr) {
		t.Fatalf("Error should not have been cas mismatch")
	}

	notMismatchErr := maybeEnhanceKVErr(err, "myfakekey", false)
	if !IsCasMismatchError(notMismatchErr) {
		t.Fatalf("Error should have been cas mismatch")
	}

	errValTooLarge := &gocbcore.KvError{
		Code: gocbcore.StatusTooBig,
	}

	notMismatchErr = maybeEnhanceKVErr(errValTooLarge, "myfakekey", true)
	if IsCasMismatchError(notMismatchErr) {
		t.Fatalf("Error should not have been cas mismatch")
	}
}

func TestNilEnhanceError(t *testing.T) {
	enhancedErr := maybeEnhanceKVErr(nil, "myfakekey", false)
	if enhancedErr != nil {
		t.Fatalf("Expected enhanced error to be nil but was %v", enhancedErr)
	}
}

func TestKVIsRetryable(t *testing.T) {
	err := &gocbcore.KvError{
		Code: gocbcore.StatusTmpFail,
	}

	enhancedErr := maybeEnhanceKVErr(err, "myfakekey", false)
	if !IsRetryableError(enhancedErr) {
		t.Fatalf("StatusTmpFail error should have been retryable")
	}

	err = &gocbcore.KvError{
		Code: gocbcore.StatusOutOfMemory,
	}

	enhancedErr = maybeEnhanceKVErr(err, "myfakekey", false)
	if !IsRetryableError(enhancedErr) {
		t.Fatalf("StatusOutOfMemory error should have been retryable")
	}

	err = &gocbcore.KvError{
		Code: gocbcore.StatusBusy,
	}

	enhancedErr = maybeEnhanceKVErr(err, "myfakekey", false)
	if !IsRetryableError(enhancedErr) {
		t.Fatalf("StatusBusy error should have been retryable")
	}

	err = &gocbcore.KvError{
		Code: gocbcore.StatusTooBig,
	}

	enhancedErr = maybeEnhanceKVErr(err, "myfakekey", false)
	if IsRetryableError(enhancedErr) {
		t.Fatalf("StatusTooBig error should not have been retryable")
	}
}
