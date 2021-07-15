package gocb

import (
	"time"

	"github.com/couchbase/gocbcore/v10"
)

// failFastRetryStrategy represents a strategy that will never retry.
type failFastRetryStrategy struct {
}

// newFailFastRetryStrategy returns a new FailFastRetryStrategy.
func newFailFastRetryStrategy() *failFastRetryStrategy {
	return &failFastRetryStrategy{}
}

// RetryAfter calculates and returns a RetryAction describing how long to wait before retrying an operation.
func (rs *failFastRetryStrategy) RetryAfter(req RetryRequest, reason RetryReason) RetryAction {
	return &NoRetryRetryAction{}
}

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

func (suite *UnitTestSuite) TestRetryWrapper_ForwardsAttempt() {
	expectedAction := &NoRetryRetryAction{}
	strategy := newRetryStrategyWrapper(&mockRetryStrategy{action: expectedAction})

	request := &mockGocbcoreRequest{
		reasons: []gocbcore.RetryReason{gocbcore.KVCollectionOutdatedRetryReason, gocbcore.UnknownRetryReason},
	}
	action := strategy.RetryAfter(request, gocbcore.UnknownRetryReason)
	if action != expectedAction {
		suite.T().Fatalf("Expected retry action to be %v but was %v", expectedAction, action)
	}
}

func (suite *UnitTestSuite) TestBestEffortRetryStrategy_RetryAfterNoRetry() {
	strategy := NewBestEffortRetryStrategy(mockBackoffCalculator)
	action := strategy.RetryAfter(&mockRetryRequest{}, RetryReason(gocbcore.UnknownRetryReason))
	if action.Duration() != 0 {
		suite.T().Fatalf("Expected duration to be %d but was %d", 0, action.Duration())
	}
}

func (suite *UnitTestSuite) TestBestEffortRetryStrategy_RetryAfterAlwaysRetry() {
	strategy := NewBestEffortRetryStrategy(mockBackoffCalculator)
	action := strategy.RetryAfter(&mockRetryRequest{}, RetryReason(gocbcore.KVCollectionOutdatedRetryReason))
	if action.Duration() != 0 {
		suite.T().Fatalf("Expected duration to be %d but was %d", 0, action.Duration())
	}

	action = strategy.RetryAfter(&mockRetryRequest{attempts: 5}, RetryReason(gocbcore.KVCollectionOutdatedRetryReason))
	if action.Duration() != 5*time.Millisecond {
		suite.T().Fatalf("Expected duration to be %d but was %d", 5*time.Millisecond, action.Duration())
	}
}

func (suite *UnitTestSuite) TestBestEffortRetryStrategy_RetryAfterAllowsNonIdempotent() {
	strategy := NewBestEffortRetryStrategy(mockBackoffCalculator)
	action := strategy.RetryAfter(&mockRetryRequest{}, RetryReason(gocbcore.KVLockedRetryReason))
	if action.Duration() != 0 {
		suite.T().Fatalf("Expected duration to be %d but was %d", 0, action.Duration())
	}

	action = strategy.RetryAfter(&mockRetryRequest{attempts: 5}, RetryReason(gocbcore.KVLockedRetryReason))
	if action.Duration() != 5*time.Millisecond {
		suite.T().Fatalf("Expected duration to be %d but was %d", 5*time.Millisecond, action.Duration())
	}
}

func (suite *UnitTestSuite) TestBestEffortRetryStrategy_RetryAfterDefaultCalculator() {
	strategy := NewBestEffortRetryStrategy(nil)
	action := strategy.RetryAfter(&mockRetryRequest{}, RetryReason(gocbcore.KVCollectionOutdatedRetryReason))
	if action.Duration() != 1*time.Millisecond {
		suite.T().Fatalf("Expected duration to be %d but was %d", 1*time.Millisecond, action.Duration())
	}

	action = strategy.RetryAfter(&mockRetryRequest{attempts: 5}, RetryReason(gocbcore.KVLockedRetryReason))
	if action.Duration() != 32*time.Millisecond {
		suite.T().Fatalf("Expected duration to be %d but was %d", 32*time.Millisecond, action.Duration())
	}
}

func (suite *UnitTestSuite) TestFailFastRetryStrategy_RetryAfterNoRetry() {
	strategy := newFailFastRetryStrategy()
	action := strategy.RetryAfter(&mockRetryRequest{}, RetryReason(gocbcore.UnknownRetryReason))
	if action.Duration() != 0 {
		suite.T().Fatalf("Expected duration to be %d but was %d", 0, action.Duration())
	}
}

func (suite *UnitTestSuite) TestFailFastRetryStrategy_RetryAfterAlwaysRetry() {
	strategy := newFailFastRetryStrategy()
	action := strategy.RetryAfter(&mockRetryRequest{}, RetryReason(gocbcore.KVCollectionOutdatedRetryReason))
	if action.Duration() != 0 {
		suite.T().Fatalf("Expected duration to be %d but was %d", 0, action.Duration())
	}
}

func (suite *UnitTestSuite) TestFailFastRetryStrategy_RetryAfterAllowsNonIdempotent() {
	strategy := newFailFastRetryStrategy()
	action := strategy.RetryAfter(&mockRetryRequest{}, RetryReason(gocbcore.KVLockedRetryReason))
	if action.Duration() != 0 {
		suite.T().Fatalf("Expected duration to be %d but was %d", 0, action.Duration())
	}
}
