package gocb

import (
	"math"
	"time"
)

// retryBehavior defines the behavior to be used for retries
// Volatile: This API is subject to change at any time.
type retryBehavior interface {
	NextInterval(retries uint) time.Duration
	CanRetry(retries uint) bool
}

// retryDelayFunction is called to get the next try delay
type retryDelayFunction func(retryDelay uint, retries uint) time.Duration

// linearDelayFunction provides retry delay durations (ms) following a linear increment pattern
func linearDelayFunction(retryDelay uint, retries uint) time.Duration {
	return time.Duration(retryDelay*retries) * time.Millisecond
}

// exponentialDelayFunction provides retry delay durations (ms) following an exponential increment pattern
func exponentialDelayFunction(retryDelay uint, retries uint) time.Duration {
	pow := math.Pow(float64(retryDelay), float64(retries))
	return time.Duration(pow) * time.Millisecond
}

// delayRetryBehavior provides the behavior to use when retrying queries with a backoff delay
type delayRetryBehavior struct {
	maxRetries uint
	retryDelay uint
	delayLimit time.Duration
	delayFunc  retryDelayFunction
}

// standardDelayRetryBehavior provides a delayRetryBehavior that will retry at most maxRetries number of times and
// with an initial retry delay of retryDelay (ms) up to a maximum delay of delayLimit
func standardDelayRetryBehavior(maxRetries uint, retryDelay uint, delayLimit time.Duration, delayFunc retryDelayFunction) *delayRetryBehavior {
	return &delayRetryBehavior{
		retryDelay: retryDelay,
		maxRetries: maxRetries,
		delayLimit: delayLimit,
		delayFunc:  delayFunc,
	}
}

// NextInterval calculates what the next retry interval (ms) should be given how many
// retries there have been already
func (rb *delayRetryBehavior) NextInterval(retries uint) time.Duration {
	interval := rb.delayFunc(rb.retryDelay, retries)
	if interval > rb.delayLimit {
		interval = rb.delayLimit
	}

	return interval
}

// CanRetry determines whether or not the query can be retried according to the behavior
func (rb *delayRetryBehavior) CanRetry(retries uint) bool {
	return retries < rb.maxRetries
}
