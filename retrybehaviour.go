package gocb

import (
	"math"
	"time"
)

// RetryBehavior defines the behavior to be used for retries
type RetryBehavior interface {
	NextInterval(retries uint) time.Duration
	CanRetry(retries uint) bool
}

// RetryDelayFunction is called to get the next try delay
type RetryDelayFunction func(retryDelay uint, retries uint) time.Duration

// LinearDelayFunction provides retry delay durations (ms) following a linear increment pattern
func LinearDelayFunction(retryDelay uint, retries uint) time.Duration {
	return time.Duration(retryDelay*retries) * time.Millisecond
}

// ExponentialDelayFunction provides retry delay durations (ms) following an exponential increment pattern
func ExponentialDelayFunction(retryDelay uint, retries uint) time.Duration {
	pow := math.Pow(float64(retryDelay), float64(retries))
	return time.Duration(pow) * time.Millisecond
}

// DelayRetryBehavior provides the behavior to use when retrying queries with a backoff delay
type DelayRetryBehavior struct {
	maxRetries uint
	retryDelay uint
	delayLimit time.Duration
	delayFunc  RetryDelayFunction
}

// StandardDelayRetryBehavior provides a DelayRetryBehavior that will retry at most maxRetries number of times and
// with an initial retry delay of retryDelay (ms) up to a maximum delay of delayLimit
func StandardDelayRetryBehavior(maxRetries uint, retryDelay uint, delayLimit time.Duration, delayFunc RetryDelayFunction) *DelayRetryBehavior {
	return &DelayRetryBehavior{
		retryDelay: retryDelay,
		maxRetries: maxRetries,
		delayLimit: delayLimit,
		delayFunc:  delayFunc,
	}
}

// NextInterval calculates what the next retry interval (ms) should be given how many
// retries there have been already
func (rb *DelayRetryBehavior) NextInterval(retries uint) time.Duration {
	interval := rb.delayFunc(rb.retryDelay, retries)
	if interval > rb.delayLimit {
		interval = rb.delayLimit
	}

	return interval
}

// CanRetry determines whether or not the query can be retried according to the behavior
func (rb *DelayRetryBehavior) CanRetry(retries uint) bool {
	return retries < rb.maxRetries
}
