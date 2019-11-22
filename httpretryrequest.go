package gocb

import (
	"sort"
	"sync/atomic"
)

type httpRetryRequest struct {
	IsIdempotent  bool
	UniqueID      string
	RetryStrategy RetryStrategy

	retryCount   uint32
	retryReasons []RetryReason
}

func (hr *httpRetryRequest) retryStrategy() RetryStrategy {
	return hr.RetryStrategy
}

func (hr *httpRetryRequest) RetryAttempts() uint32 {
	return atomic.LoadUint32(&hr.retryCount)
}

func (hr *httpRetryRequest) Identifier() string {
	return hr.UniqueID
}

func (hr *httpRetryRequest) Idempotent() bool {
	return hr.IsIdempotent
}

func (hr *httpRetryRequest) RetryReasons() []RetryReason {
	return hr.retryReasons
}

func (hr *httpRetryRequest) incrementRetryAttempts() {
	atomic.AddUint32(&hr.retryCount, 1)
}

func (hr *httpRetryRequest) addRetryReason(reason RetryReason) {
	idx := sort.Search(len(hr.retryReasons), func(i int) bool {
		return hr.retryReasons[i] == reason
	})

	// if idx is out of the range of retryReasons then it wasn't found.
	if idx > len(hr.retryReasons)-1 {
		hr.retryReasons = append(hr.retryReasons, reason)
	}
}
