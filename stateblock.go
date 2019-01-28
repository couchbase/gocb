package gocb

import (
	"fmt"
	"time"
)

type clientStateBlock struct {
	BucketName        string
	UseMutationTokens bool
}

func (sb *clientStateBlock) Hash() string {
	return fmt.Sprintf("%s-%t",
		sb.BucketName,
		sb.UseMutationTokens)
}

type stateBlock struct {
	cachedClient client

	clientStateBlock

	ScopeName      string
	CollectionName string

	ConnectTimeout  time.Duration
	KvTimeout       time.Duration
	DuraTimeout     time.Duration
	DuraPollTimeout time.Duration
	PersistTo       uint
	ReplicateTo     uint

	N1qlRetryBehavior      RetryBehavior
	AnalyticsRetryBehavior RetryBehavior
	SearchRetryBehavior    RetryBehavior

	QueryTimeout     time.Duration
	AnalyticsTimeout time.Duration
	SearchTimeout    time.Duration
	ViewTimeout      time.Duration

	useMutationTokens bool

	client func(*clientStateBlock) client
}

func (sb *stateBlock) getCachedClient() client {
	if sb.cachedClient == nil {
		panic("attempted to fetch client from incomplete state block")
	}

	return sb.cachedClient
}

func (sb *stateBlock) cacheClient() {
	sb.cachedClient = sb.client(&sb.clientStateBlock)
}
