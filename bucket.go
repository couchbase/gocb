package gocb

// Bucket represents a single bucket within a cluster.
type Bucket struct {
	sb stateBlock
}

// BucketOptions are the options available when connecting to a Bucket.
type BucketOptions struct {
	UseMutationTokens bool
}

func newBucket(sb *stateBlock, bucketName string, opts BucketOptions) *Bucket {
	return &Bucket{
		sb: stateBlock{
			clientStateBlock: clientStateBlock{
				BucketName:        bucketName,
				UseMutationTokens: opts.UseMutationTokens,
			},
			QueryTimeout:     sb.QueryTimeout,
			SearchTimeout:    sb.SearchTimeout,
			AnalyticsTimeout: sb.AnalyticsTimeout,
			KvTimeout:        sb.KvTimeout,
			ViewTimeout:      sb.ViewTimeout,
			ConnectTimeout:   sb.ConnectTimeout,
			DuraTimeout:      sb.DuraTimeout,
			DuraPollTimeout:  sb.DuraPollTimeout,

			client: sb.client,
		},
	}
}

func (b *Bucket) connect() {
	b.sb.cacheClient()
	cli := b.sb.getCachedClient()
	cli.connect()
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
func (b *Bucket) Scope(scopeName string) *Scope {
	return newScope(b, scopeName)
}

func (b *Bucket) defaultScope() *Scope {
	return b.Scope("_default")
}

// Collection returns an instance of a collection.
func (b *Bucket) Collection(scopeName string, collectionName string, opts *CollectionOptions) *Collection {
	return b.Scope(scopeName).Collection(collectionName, opts)
}

// DefaultCollection returns an instance of the default collection.
func (b *Bucket) DefaultCollection(opts *CollectionOptions) *Collection {
	return b.defaultScope().DefaultCollection(opts)
}

func (b *Bucket) stateBlock() stateBlock {
	return b.sb
}
