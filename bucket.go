package gocb

import (
	"time"

	"github.com/couchbase/gocbcore/v9"
	"github.com/pkg/errors"
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
			KvDurableTimeout:  sb.KvDurableTimeout,
			ViewTimeout:       sb.ViewTimeout,
			ConnectTimeout:    sb.ConnectTimeout,
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
		mgmtProvider: b,
		bucketName:   b.Name(),
		tracer:       b.sb.Tracer,
	}
}

// Collections provides functions for managing collections.
func (b *Bucket) Collections() *CollectionManager {
	cli := b.sb.getCachedClient()

	return &CollectionManager{
		collectionsSupported: cli.supportsCollections(),
		mgmtProvider:         b,
		bucketName:           b.Name(),
		tracer:               b.sb.Tracer,
	}
}

// WaitUntilReady will wait for the bucket object to be ready for use.
// At present this will wait until memd connections have been established with the server and are ready
// to be used.
func (b *Bucket) WaitUntilReady(timeout time.Duration, opts *WaitUntilReadyOptions) error {
	if opts == nil {
		opts = &WaitUntilReadyOptions{}
	}

	cli := b.sb.getCachedClient()
	if cli == nil {
		return errors.New("bucket is not connected")
	}

	err := cli.getBootstrapError()
	if err != nil {
		return err
	}

	provider, err := cli.getWaitUntilReadyProvider()
	if err != nil {
		return err
	}

	desiredState := opts.DesiredState
	if desiredState == 0 {
		desiredState = ClusterStateOnline
	}

	err = provider.WaitUntilReady(
		time.Now().Add(timeout),
		gocbcore.WaitUntilReadyOptions{
			DesiredState: gocbcore.ClusterState(desiredState),
		},
	)
	if err != nil {
		return err
	}

	return nil
}
