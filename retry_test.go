package gocb

import (
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v8"
)

type mockGocbcoreRequest struct {
	attempts   uint32
	identifier string
	idempotent bool
	reasons    []gocbcore.RetryReason
	cancelSet  bool
	gocbcore.RetryRequest
}

func (mgr *mockGocbcoreRequest) RetryAttempts() uint32 {
	return mgr.attempts
}

func (mgr *mockGocbcoreRequest) Identifier() string {
	return mgr.identifier
}

func (mgr *mockGocbcoreRequest) Idempotent() bool {
	return mgr.idempotent
}

func (mgr *mockGocbcoreRequest) RetryReasons() []gocbcore.RetryReason {
	return mgr.reasons
}

type mockRetryRequest struct {
	attempts   uint32
	identifier string
	idempotent bool
	reasons    []RetryReason
}

func (mgr *mockRetryRequest) RetryAttempts() uint32 {
	return mgr.attempts
}

func (mgr *mockRetryRequest) IncrementRetryAttempts() {
	mgr.attempts++
}

func (mgr *mockRetryRequest) Identifier() string {
	return mgr.identifier
}

func (mgr *mockRetryRequest) Idempotent() bool {
	return mgr.idempotent
}

func (mgr *mockRetryRequest) RetryReasons() []RetryReason {
	return mgr.reasons
}

type mockRetryStrategy struct {
	retried bool
	action  RetryAction
}

func (mrs *mockRetryStrategy) RetryAfter(req RetryRequest, reason RetryReason) RetryAction {
	mrs.retried = true
	return mrs.action
}

func mockBackoffCalculator(retryAttempts uint32) time.Duration {
	return time.Millisecond * time.Duration(retryAttempts)
}

func TestRetryWrapper_ForwardsAttempt(t *testing.T) {
	expectedAction := &NoRetryRetryAction{}
	strategy := newRetryStrategyWrapper(&mockRetryStrategy{action: expectedAction})

	request := &mockGocbcoreRequest{
		reasons: []gocbcore.RetryReason{gocbcore.KVCollectionOutdatedRetryReason, gocbcore.UnknownRetryReason},
	}
	action := strategy.RetryAfter(request, gocbcore.UnknownRetryReason)
	if action != expectedAction {
		t.Fatalf("Expected retry action to be %v but was %v", expectedAction, action)
	}
}

func TestBestEffortRetryStrategy_RetryAfterNoRetry(t *testing.T) {
	strategy := NewBestEffortRetryStrategy(mockBackoffCalculator)
	action := strategy.RetryAfter(&mockRetryRequest{}, RetryReason(gocbcore.UnknownRetryReason))
	if action.Duration() != 0 {
		t.Fatalf("Expected duration to be %d but was %d", 0, action.Duration())
	}
}

func TestBestEffortRetryStrategy_RetryAfterAlwaysRetry(t *testing.T) {
	strategy := NewBestEffortRetryStrategy(mockBackoffCalculator)
	action := strategy.RetryAfter(&mockRetryRequest{}, RetryReason(gocbcore.KVCollectionOutdatedRetryReason))
	if action.Duration() != 0 {
		t.Fatalf("Expected duration to be %d but was %d", 0, action.Duration())
	}

	action = strategy.RetryAfter(&mockRetryRequest{attempts: 5}, RetryReason(gocbcore.KVCollectionOutdatedRetryReason))
	if action.Duration() != 5*time.Millisecond {
		t.Fatalf("Expected duration to be %d but was %d", 5*time.Millisecond, action.Duration())
	}
}

func TestBestEffortRetryStrategy_RetryAfterAllowsNonIdempotent(t *testing.T) {
	strategy := NewBestEffortRetryStrategy(mockBackoffCalculator)
	action := strategy.RetryAfter(&mockRetryRequest{}, RetryReason(gocbcore.KVLockedRetryReason))
	if action.Duration() != 0 {
		t.Fatalf("Expected duration to be %d but was %d", 0, action.Duration())
	}

	action = strategy.RetryAfter(&mockRetryRequest{attempts: 5}, RetryReason(gocbcore.KVLockedRetryReason))
	if action.Duration() != 5*time.Millisecond {
		t.Fatalf("Expected duration to be %d but was %d", 5*time.Millisecond, action.Duration())
	}
}

func TestBestEffortRetryStrategy_RetryAfterDefaultCalculator(t *testing.T) {
	strategy := NewBestEffortRetryStrategy(nil)
	action := strategy.RetryAfter(&mockRetryRequest{}, RetryReason(gocbcore.KVCollectionOutdatedRetryReason))
	if action.Duration() != 1*time.Millisecond {
		t.Fatalf("Expected duration to be %d but was %d", 1*time.Millisecond, action.Duration())
	}

	action = strategy.RetryAfter(&mockRetryRequest{attempts: 5}, RetryReason(gocbcore.KVLockedRetryReason))
	if action.Duration() != 1000*time.Millisecond {
		t.Fatalf("Expected duration to be %d but was %d", 1000*time.Millisecond, action.Duration())
	}
}

func TestFailFastRetryStrategy_RetryAfterNoRetry(t *testing.T) {
	strategy := newFailFastRetryStrategy()
	action := strategy.RetryAfter(&mockRetryRequest{}, RetryReason(gocbcore.UnknownRetryReason))
	if action.Duration() != 0 {
		t.Fatalf("Expected duration to be %d but was %d", 0, action.Duration())
	}
}

func TestFailFastRetryStrategy_RetryAfterAlwaysRetry(t *testing.T) {
	strategy := newFailFastRetryStrategy()
	action := strategy.RetryAfter(&mockRetryRequest{}, RetryReason(gocbcore.KVCollectionOutdatedRetryReason))
	if action.Duration() != 0 {
		t.Fatalf("Expected duration to be %d but was %d", 0, action.Duration())
	}
}

func TestFailFastRetryStrategy_RetryAfterAllowsNonIdempotent(t *testing.T) {
	strategy := newFailFastRetryStrategy()
	action := strategy.RetryAfter(&mockRetryRequest{}, RetryReason(gocbcore.KVLockedRetryReason))
	if action.Duration() != 0 {
		t.Fatalf("Expected duration to be %d but was %d", 0, action.Duration())
	}
}
