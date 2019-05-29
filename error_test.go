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
	if !IsCasMismatchError(mismatchErr) {
		t.Fatalf("Error should have been cas mismatch")
	}

	notMismatchErr := maybeEnhanceKVErr(err, "myfakekey", false)
	if IsCasMismatchError(notMismatchErr) {
		t.Fatalf("Error should not have been cas mismatch")
	}

	errValTooLarge := &gocbcore.KvError{
		Code: gocbcore.StatusTooBig,
	}

	notMismatchErr = maybeEnhanceKVErr(errValTooLarge, "myfakekey", true)
	if IsCasMismatchError(notMismatchErr) {
		t.Fatalf("Error should not have been cas mismatch")
	}
}
