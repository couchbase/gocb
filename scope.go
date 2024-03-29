package gocb

// Scope represents a single scope within a bucket.
type Scope struct {
	scopeName string
	bucket    *Bucket

	timeoutsConfig TimeoutsConfig

	transcoder           Transcoder
	retryStrategyWrapper *coreRetryStrategyWrapper
	tracer               RequestTracer
	meter                *meterWrapper
	compressor           *compressor

	useMutationTokens bool

	getKvProvider                 func() (kvProvider, error)
	getKvBulkProvider             func() (kvBulkProvider, error)
	getQueryProvider              func() (queryProvider, error)
	getQueryIndexProvider         func() (queryIndexProvider, error)
	getSearchProvider             func() (searchProvider, error)
	getSearchIndexProvider        func() (searchIndexProvider, error)
	getAnalyticsProvider          func() (analyticsProvider, error)
	getEventingManagementProvider func() (eventingManagementProvider, error)
	getTransactions               func() *Transactions
}

func newScope(bucket *Bucket, scopeName string) *Scope {
	return &Scope{
		scopeName: scopeName,
		bucket:    bucket,

		timeoutsConfig: bucket.timeoutsConfig,

		transcoder:           bucket.transcoder,
		retryStrategyWrapper: bucket.retryStrategyWrapper,
		tracer:               bucket.tracer,
		meter:                bucket.meter,
		compressor:           bucket.compressor,

		useMutationTokens: bucket.useMutationTokens,

		getKvProvider:                 bucket.getKvProvider,
		getKvBulkProvider:             bucket.getKvBulkProvider,
		getQueryProvider:              bucket.getQueryProvider,
		getQueryIndexProvider:         bucket.getQueryIndexProvider,
		getSearchProvider:             bucket.getSearchProvider,
		getSearchIndexProvider:        bucket.getSearchIndexProvider,
		getAnalyticsProvider:          bucket.getAnalyticsProvider,
		getEventingManagementProvider: bucket.getEventingManagementProvider,
		getTransactions:               bucket.getTransactions,
	}
}

// Name returns the name of the scope.
func (s *Scope) Name() string {
	return s.scopeName
}

// BucketName returns the name of the bucket to which this collection belongs.
// UNCOMMITTED: This API may change in the future.
func (s *Scope) BucketName() string {
	return s.bucket.Name()
}

// Collection returns an instance of a collection.
func (s *Scope) Collection(collectionName string) *Collection {
	return newCollection(s, collectionName)
}

// SearchIndexes returns a ScopeSearchIndexManager for managing scope-level search indexes.
func (s *Scope) SearchIndexes() *ScopeSearchIndexManager {
	return &ScopeSearchIndexManager{
		getProvider: s.getSearchIndexProvider,

		scope: s,
	}
}

// EventingFunctions returns a ScopeEventingFunctionManager for managing scope-level eventing functions.
//
// # UNCOMMITTED
//
// This API is UNCOMMITTED and may change in the future.
func (s *Scope) EventingFunctions() *ScopeEventingFunctionManager {
	return &ScopeEventingFunctionManager{
		getProvider: s.getEventingManagementProvider,

		scope: s,
	}
}
