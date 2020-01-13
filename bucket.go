package gocb

import (
	gocbcore "github.com/couchbase/gocbcore/v8"
)

// Bucket represents a single bucket within a cluster.
type Bucket struct {
	sb stateBlock
}

func newBucket(sb *stateBlock, bucketName string) *Bucket {
	return &Bucket{
		sb: stateBlock{
			clientStateBlock: clientStateBlock{
				BucketName: bucketName,
			},
			QueryTimeout:      sb.QueryTimeout,
			SearchTimeout:     sb.SearchTimeout,
			AnalyticsTimeout:  sb.AnalyticsTimeout,
			KvTimeout:         sb.KvTimeout,
			ViewTimeout:       sb.ViewTimeout,
			ConnectTimeout:    sb.ConnectTimeout,
			DuraTimeout:       sb.DuraTimeout,
			DuraPollTimeout:   sb.DuraPollTimeout,
			ManagementTimeout: sb.ManagementTimeout,

			Transcoder: sb.Transcoder,

			RetryStrategyWrapper: sb.RetryStrategyWrapper,

			Tracer: sb.Tracer,

			UseServerDurations: sb.UseServerDurations,
			UseMutationTokens:  sb.UseMutationTokens,
		},
	}
}

func (b *Bucket) hash() string {
	return b.sb.Hash()
}

func (b *Bucket) cacheClient(cli client) {
	b.sb.cacheClient(cli)
}

func (b *Bucket) clone() *Bucket {
	newB := *b
	return &newB
}

// Name returns the name of the bucket.
func (b *Bucket) Name() string {
	return b.sb.BucketName
}

// Scope returns an instance of a Scope.
// VOLATILE: This API is subject to change at any time.
func (b *Bucket) Scope(scopeName string) *Scope {
	return newScope(b, scopeName)
}

// DefaultScope returns an instance of the default scope.
// VOLATILE: This API is subject to change at any time.
func (b *Bucket) DefaultScope() *Scope {
	return b.Scope("_default")
}

// Collection returns an instance of a collection from within the default scope.
// VOLATILE: This API is subject to change at any time.
func (b *Bucket) Collection(collectionName string) *Collection {
	return b.DefaultScope().Collection(collectionName)
}

// DefaultCollection returns an instance of the default collection.
func (b *Bucket) DefaultCollection() *Collection {
	return b.DefaultScope().Collection("_default")
}

func (b *Bucket) stateBlock() stateBlock {
	return b.sb
}

// ViewIndexes returns a ViewIndexManager instance for managing views.
func (b *Bucket) ViewIndexes() *ViewIndexManager {
	return &ViewIndexManager{
		bucket: b,
		tracer: b.sb.Tracer,
	}
}

type bucketHTTPWrapper struct {
	b *Bucket
}

func (bw bucketHTTPWrapper) DoHTTPRequest(req *gocbcore.HTTPRequest) (*gocbcore.HTTPResponse, error) {
	provider, err := bw.b.sb.getCachedClient().getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return provider.DoHTTPRequest(req)
}

// Collections provides functions for managing collections.
func (b *Bucket) Collections() *CollectionManager {
	provider := bucketHTTPWrapper{b}

	return &CollectionManager{
		httpClient:           provider,
		bucketName:           b.Name(),
		globalTimeout:        b.sb.ManagementTimeout,
		defaultRetryStrategy: b.sb.RetryStrategyWrapper,
		tracer:               b.sb.Tracer,
	}
}
