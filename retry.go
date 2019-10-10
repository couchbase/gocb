package gocb

import (
	"time"

	"github.com/couchbase/gocbcore/v8"
)

// RetryRequest is a request that can possibly be retried.
type RetryRequest interface {
	RetryAttempts() uint32
	Identifier() string
	Idempotent() bool
	RetryReasons() []RetryReason
}

type retryRequest struct {
	attempts   uint32
	identifier string
	idempotent bool
	reasons    []RetryReason
}

func (mgr *retryRequest) RetryAttempts() uint32 {
	return mgr.attempts
}

func (mgr *retryRequest) IncrementRetryAttempts() {
	mgr.attempts++
}

func (mgr *retryRequest) Identifier() string {
	return mgr.identifier
}

func (mgr *retryRequest) Idempotent() bool {
	return mgr.idempotent
}

func (mgr *retryRequest) RetryReasons() []RetryReason {
	return mgr.reasons
}

// RetryReason represents the reason for an operation possibly being retried.
type RetryReason interface {
	AllowsNonIdempotentRetry() bool
	AlwaysRetry() bool
	Description() string
}

var (
	// UnknownRetryReason indicates that the operation failed for an unknown reason.
	UnknownRetryReason = RetryReason(gocbcore.UnknownRetryReason)

	// SocketNotAvailableRetryReason indicates that the operation failed because the underlying socket was not available.
	SocketNotAvailableRetryReason = RetryReason(gocbcore.SocketNotAvailableRetryReason)

	// ServiceNotAvailableRetryReason indicates that the operation failed because the requested service was not available.
	ServiceNotAvailableRetryReason = RetryReason(gocbcore.ServiceNotAvailableRetryReason)

	// NodeNotAvailableRetryReason indicates that the operation failed because the requested node was not available.
	NodeNotAvailableRetryReason = RetryReason(gocbcore.NodeNotAvailableRetryReason)

	// KVNotMyVBucketRetryReason indicates that the operation failed because it was sent to the wrong node for the vbucket.
	KVNotMyVBucketRetryReason = RetryReason(gocbcore.KVNotMyVBucketRetryReason)

	// KVCollectionOutdatedRetryReason indicates that the operation failed because the collection ID on the request is outdated.
	KVCollectionOutdatedRetryReason = RetryReason(gocbcore.KVCollectionOutdatedRetryReason)

	// KVErrMapRetryReason indicates that the operation failed for an unsupported reason but the KV error map indicated
	// that the operation can be retried.
	KVErrMapRetryReason = RetryReason(gocbcore.KVErrMapRetryReason)

	// KVLockedRetryReason indicates that the operation failed because the document was locked.
	KVLockedRetryReason = RetryReason(gocbcore.KVLockedRetryReason)

	// KVTemporaryFailureRetryReason indicates that the operation failed because of a temporary failure.
	KVTemporaryFailureRetryReason = RetryReason(gocbcore.KVTemporaryFailureRetryReason)

	// KVSyncWriteInProgressRetryReason indicates that the operation failed because a sync write is in progress.
	KVSyncWriteInProgressRetryReason = RetryReason(gocbcore.KVSyncWriteInProgressRetryReason)

	// KVSyncWriteRecommitInProgressRetryReason indicates that the operation failed because a sync write recommit is in progress.
	KVSyncWriteRecommitInProgressRetryReason = RetryReason(gocbcore.KVSyncWriteRecommitInProgressRetryReason)

	// ServiceResponseCodeIndicatedRetryReason indicates that the operation failed and the service responded stating that
	// the request should be retried.
	ServiceResponseCodeIndicatedRetryReason = RetryReason(gocbcore.ServiceResponseCodeIndicatedRetryReason)

	// SocketCloseInFlightRetryReason indicates that the operation failed because the socket was closed whilst the operation
	// was in flight.
	SocketCloseInFlightRetryReason = RetryReason(gocbcore.SocketCloseInFlightRetryReason)
)

// RetryAction is used by a RetryStrategy to calculate the duration to wait before retrying an operation.
// Returning a value of 0 indicates to not retry.
type RetryAction interface {
	Duration() time.Duration
}

// NoRetryRetryAction represents an action that indicates to not retry.
type NoRetryRetryAction struct {
}

// Duration is the length of time to wait before retrying an operation.
func (ra *NoRetryRetryAction) Duration() time.Duration {
	return 0
}

// WithDurationRetryAction represents an action that indicates to retry with a given duration.
type WithDurationRetryAction struct {
	WithDuration time.Duration
}

// Duration is the length of time to wait before retrying an operation.
func (ra *WithDurationRetryAction) Duration() time.Duration {
	return ra.WithDuration
}

// RetryStrategy is to determine if an operation should be retried, and if so how long to wait before retrying.
type RetryStrategy interface {
	RetryAfter(req RetryRequest, reason RetryReason) RetryAction
}

func newRetryStrategyWrapper(strategy RetryStrategy) *retryStrategyWrapper {
	return &retryStrategyWrapper{
		wrapped: strategy,
	}
}

type retryStrategyWrapper struct {
	wrapped RetryStrategy
}

// RetryAfter calculates and returns a RetryAction describing how long to wait before retrying an operation.
func (rs *retryStrategyWrapper) RetryAfter(req gocbcore.RetryRequest, reason gocbcore.RetryReason) gocbcore.RetryAction {
	gocbRequest := &retryRequest{
		attempts:   req.RetryAttempts(),
		identifier: req.Identifier(),
		idempotent: req.Idempotent(),
	}
	for _, retryReason := range req.RetryReasons() {
		gocbReason, ok := retryReason.(RetryReason)
		if !ok {
			logErrorf("Failed to assert gocbcore retry reason to gocb retry reason: %v", reason)
			continue
		}
		gocbRequest.reasons = append(gocbRequest.reasons, gocbReason)
	}

	wrappedAction := rs.wrapped.RetryAfter(gocbRequest, RetryReason(reason))
	return wrappedAction
}

// FailFastRetryStrategy represents a strategy that will never retry.
type FailFastRetryStrategy struct {
}

// NewFailFastRetryStrategy returns a new FailFastRetryStrategy.
func NewFailFastRetryStrategy() *FailFastRetryStrategy {
	return &FailFastRetryStrategy{}
}

// RetryAfter calculates and returns a RetryAction describing how long to wait before retrying an operation.
func (rs *FailFastRetryStrategy) RetryAfter(req RetryRequest, reason RetryReason) RetryAction {
	return &NoRetryRetryAction{}
}

// BackoffCalculator defines how backoff durations will be calculated by the retry API.g
type BackoffCalculator func(retryAttempts uint32) time.Duration

// BestEffortRetryStrategy represents a strategy that will keep retrying until it succeeds (or the caller times out
// the request).
type BestEffortRetryStrategy struct {
	backoffCalculator BackoffCalculator
}

// NewBestEffortRetryStrategy returns a new BestEffortRetryStrategy which will use the supplied calculator function
// to calculate retry durations. If calculator is nil then a controlled backoff will be used.
func NewBestEffortRetryStrategy(calculator BackoffCalculator) *BestEffortRetryStrategy {
	if calculator == nil {
		calculator = gocbcore.ControlledBackoff
	}

	return &BestEffortRetryStrategy{backoffCalculator: calculator}
}

// RetryAfter calculates and returns a RetryAction describing how long to wait before retrying an operation.
func (rs *BestEffortRetryStrategy) RetryAfter(req RetryRequest, reason RetryReason) RetryAction {
	if req.Idempotent() || reason.AllowsNonIdempotentRetry() {
		return &WithDurationRetryAction{WithDuration: rs.backoffCalculator(req.RetryAttempts())}
	}

	return &NoRetryRetryAction{}
}
