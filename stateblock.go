package gocb

import (
	"fmt"
	"time"
)

type clientStateBlock struct {
	BucketName string
}

func (sb *clientStateBlock) Hash() string {
	return fmt.Sprintf("%s", sb.BucketName)
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

	N1qlRetryBehavior      retryBehavior
	AnalyticsRetryBehavior retryBehavior
	SearchRetryBehavior    retryBehavior

	QueryTimeout      time.Duration
	AnalyticsTimeout  time.Duration
	SearchTimeout     time.Duration
	ViewTimeout       time.Duration
	ManagementTimeout time.Duration

	UseMutationTokens bool

	Transcoder Transcoder
	Serializer JSONSerializer
}

func (sb *stateBlock) getCachedClient() client {
	if sb.cachedClient == nil {
		panic("attempted to fetch client from incomplete state block")
	}

	return sb.cachedClient
}

func (sb *stateBlock) cacheClient(cli client) {
	sb.cachedClient = cli
}
