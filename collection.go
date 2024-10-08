package gocb

// Collection represents a single collection.
type Collection struct {
	collectionName string
	scope          string
	bucket         *Bucket

	timeoutsConfig TimeoutsConfig

	transcoder           Transcoder
	retryStrategyWrapper *coreRetryStrategyWrapper
	compressor           *compressor

	useMutationTokens bool

	keyspace keyspace

	opController opController

	getKvProvider         func() (kvProvider, error)
	getKvBulkProvider     func() (kvBulkProvider, error)
	getQueryIndexProvider func() (queryIndexProvider, error)
}

func newCollection(scope *Scope, collectionName string) *Collection {
	return &Collection{
		collectionName: collectionName,
		scope:          scope.Name(),
		bucket:         scope.bucket,

		timeoutsConfig: scope.timeoutsConfig,

		transcoder:           scope.transcoder,
		retryStrategyWrapper: scope.retryStrategyWrapper,
		compressor:           scope.compressor,

		useMutationTokens: scope.useMutationTokens,

		keyspace: keyspace{
			bucketName:     scope.BucketName(),
			scopeName:      scope.Name(),
			collectionName: collectionName,
		},

		opController: scope.opController,

		getKvProvider:         scope.getKvProvider,
		getKvBulkProvider:     scope.getKvBulkProvider,
		getQueryIndexProvider: scope.getQueryIndexProvider,
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
		controller: &providerController[queryIndexProvider]{
			get:          c.getQueryIndexProvider,
			opController: c.opController,

			meter:    c.bucket.connectionManager.getMeter(),
			service:  serviceValueManagement,
			keyspace: &c.keyspace,
		},

		c: c,
	}
}

func (c *Collection) startKvOpTrace(operationName string, parentSpan RequestSpan, tracer *tracerWrapper, noAttributes bool) RequestSpan {
	var span RequestSpan
	if noAttributes {
		span = tracer.createSpan(parentSpan, operationName, "")
	} else {
		span = tracer.createSpan(parentSpan, operationName, "kv")
		span.SetAttribute(spanAttribOperationKey, operationName)
		span.SetAttribute(spanAttribDBNameKey, c.bucket.Name())
		span.SetAttribute(spanAttribDBCollectionNameKey, c.Name())
		span.SetAttribute(spanAttribDBScopeNameKey, c.ScopeName())
	}
	return span
}

func (c *Collection) bucketName() string {
	return c.bucket.Name()
}

func (c *Collection) isDefault() bool {
	return (c.scope == "" || c.scope == "_default") && (c.collectionName == "" || c.collectionName == "_default")
}

func (c *Collection) kvController() *providerController[kvProvider] {
	var meter *meterWrapper
	if c.bucket.connectionManager != nil {
		meter = c.bucket.connectionManager.getMeter()
	}

	return &providerController[kvProvider]{
		get:          c.getKvProvider,
		opController: c.opController,

		meter:    meter,
		service:  serviceValueKV,
		keyspace: &c.keyspace,
	}
}

func (c *Collection) kvBulkController() *providerController[kvBulkProvider] {
	return &providerController[kvBulkProvider]{
		get:          c.getKvBulkProvider,
		opController: c.opController,
	}
}
