package gocb

// Collection represents a single collection.
type Collection struct {
	collectionName string
	scope          string
	bucket         *Bucket

	timeoutsConfig TimeoutsConfig

	transcoder           Transcoder
	retryStrategyWrapper *coreRetryStrategyWrapper
	tracer               RequestTracer
	meter                *meterWrapper

	useMutationTokens bool

	getKvProvider         func() (kvProvider, error)
	getQueryIndexProvider func() (queryIndexProvider, error)
	getQueryProvider      func() (queryProvider, error)
}

func newCollection(scope *Scope, collectionName string) *Collection {
	return &Collection{
		collectionName: collectionName,
		scope:          scope.Name(),
		bucket:         scope.bucket,

		timeoutsConfig: scope.timeoutsConfig,

		transcoder:           scope.transcoder,
		retryStrategyWrapper: scope.retryStrategyWrapper,
		tracer:               scope.tracer,
		meter:                scope.meter,

		useMutationTokens: scope.useMutationTokens,

		getKvProvider:         scope.getKvProvider,
		getQueryIndexProvider: scope.getQueryIndexProvider,
		getQueryProvider:      scope.getQueryProvider,
	}
}

func (c *Collection) name() string {
	return c.collectionName
}

// ScopeName returns the name of the scope to which this collection belongs.
func (c *Collection) ScopeName() string {
	return c.scope
}

// Bucket returns the bucket to which this collection belongs.
// UNCOMMITTED: This API may change in the future.
func (c *Collection) Bucket() *Bucket {
	return c.bucket
}

// Name returns the name of the collection.
func (c *Collection) Name() string {
	return c.collectionName
}

// QueryIndexes returns a CollectionQueryIndexManager for managing query indexes.
// UNCOMMITTED: This API may change in the future.
func (c *Collection) QueryIndexes() *CollectionQueryIndexManager {
	return &CollectionQueryIndexManager{
		getProvider: c.getQueryIndexProvider,

		c: c,
	}
}

func (c *Collection) startKvOpTrace(operationName string, tracectx RequestSpanContext, noAttributes bool) RequestSpan {
	span := c.tracer.RequestSpan(tracectx, operationName)
	if !noAttributes {
		span.SetAttribute(spanAttribDBNameKey, c.bucket.Name())
		span.SetAttribute(spanAttribDBCollectionNameKey, c.Name())
		span.SetAttribute(spanAttribDBScopeNameKey, c.ScopeName())
		span.SetAttribute(spanAttribServiceKey, "kv")
		span.SetAttribute(spanAttribOperationKey, operationName)
	}
	span.SetAttribute(spanAttribDBSystemKey, spanAttribDBSystemValue)

	return span
}

func (c *Collection) bucketName() string {
	return c.bucket.Name()
}
