package gocb

// Bucket represents a single bucket within a cluster.
type Bucket struct {
	sb stateBlock
}

type bucketOptions struct {
	DisableMutationTokens bool
}

func newBucket(sb *stateBlock, bucketName string, opts bucketOptions) *Bucket {
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
			UseMutationTokens: sb.UseMutationTokens,
			ManagementTimeout: sb.ManagementTimeout,

			Transcoder: sb.Transcoder,

			RetryStrategyWrapper: sb.RetryStrategyWrapper,

			UseServerDurations: sb.UseServerDurations,
			Tracer:             sb.Tracer,
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
// Volatile: This API is subject to change at any time.
func (b *Bucket) Scope(scopeName string) *Scope {
	return newScope(b, scopeName)
}

func (b *Bucket) defaultScope() *Scope {
	return b.Scope("_default")
}

// Collection returns an instance of a collection from within the default scope.
// Volatile: This API is subject to change at any time.
func (b *Bucket) Collection(collectionName string) *Collection {
	return b.defaultScope().Collection(collectionName)
}

// DefaultCollection returns an instance of the default collection.
func (b *Bucket) DefaultCollection() *Collection {
	return b.defaultScope().Collection("_default")
}

func (b *Bucket) stateBlock() stateBlock {
	return b.sb
}

// ViewIndexes returns a ViewIndexManager instance for managing views.
// Volatile: This API is subject to change at any time.
func (b *Bucket) ViewIndexes() (*ViewIndexManager, error) {
	return &ViewIndexManager{
		bucket: b,
		tracer: b.sb.Tracer,
	}, nil
}

// CollectionManager provides functions for managing collections.
// Volatile: This API is subject to change at any time.
func (b *Bucket) CollectionManager() (*CollectionManager, error) {
	provider, err := b.sb.getCachedClient().getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return &CollectionManager{
		httpClient:           provider,
		bucketName:           b.Name(),
		globalTimeout:        b.sb.ManagementTimeout,
		defaultRetryStrategy: b.sb.RetryStrategyWrapper,
		tracer:               b.sb.Tracer,
	}, nil
}
