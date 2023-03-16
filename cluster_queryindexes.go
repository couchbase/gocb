package gocb

import (
	"context"
	"fmt"
	"time"
)

// QueryIndexManager provides methods for performing Couchbase query index management.
type QueryIndexManager struct {
	base *baseQueryIndexManager
}

func (qm *QueryIndexManager) maybeAddScopeCollectionToBucket(bucketName, scope, collection string) string {
	if scope != "" && collection != "" {
		bucketName = fmt.Sprintf("`%s`.`%s`.`%s`", bucketName, scope, collection)
	} else if collection == "" && scope != "" {
		bucketName = fmt.Sprintf("`%s`.`%s`.`_default", bucketName, scope)
	} else if collection != "" && scope == "" {
		bucketName = fmt.Sprintf("`%s`.`_default`.`%s", bucketName, collection)
	} else {
		bucketName = "`" + bucketName + "`"
	}

	return bucketName
}

func (qm *QueryIndexManager) validateScopeCollection(scope, collection string) error {
	if scope == "" && collection != "" {
		return makeInvalidArgumentsError("if collection is set then scope must be set")
	} else if scope != "" && collection == "" {
		return makeInvalidArgumentsError("if scope is set then collection must be set")
	}

	return nil
}

// CreateQueryIndexOptions is the set of options available to the query indexes CreateIndex operation.
type CreateQueryIndexOptions struct {
	IgnoreIfExists bool
	Deferred       bool
	NumReplicas    int

	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	ScopeName      string
	CollectionName string

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

// CreateIndex creates an index over the specified fields.
// The SDK will automatically escape the provided index keys. For more advanced use cases like index keys using keywords
// cluster.Query or scope.Query should be used with the query directly.
func (qm *QueryIndexManager) CreateIndex(bucketName, indexName string, keys []string, opts *CreateQueryIndexOptions) error {
	if opts == nil {
		opts = &CreateQueryIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{
			message: "an invalid index name was specified",
		}
	}
	if len(keys) <= 0 {
		return invalidArgumentsError{
			message: "you must specify at least one index-key to index",
		}
	}
	if err := qm.validateScopeCollection(opts.ScopeName, opts.CollectionName); err != nil {
		return err
	}

	return qm.base.CreateIndex(
		opts.Context,
		opts.ParentSpan,
		qm.maybeAddScopeCollectionToBucket(bucketName, opts.ScopeName, opts.CollectionName),
		indexName,
		keys,
		createQueryIndexOptions{
			IgnoreIfExists: opts.IgnoreIfExists,
			Deferred:       opts.Deferred,
			Timeout:        opts.Timeout,
			RetryStrategy:  opts.RetryStrategy,
			NumReplicas:    opts.NumReplicas,
		},
	)
}

// CreatePrimaryQueryIndexOptions is the set of options available to the query indexes CreatePrimaryIndex operation.
type CreatePrimaryQueryIndexOptions struct {
	IgnoreIfExists bool
	Deferred       bool
	CustomName     string
	NumReplicas    int

	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	ScopeName      string
	CollectionName string

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

// CreatePrimaryIndex creates a primary index.  An empty customName uses the default naming.
func (qm *QueryIndexManager) CreatePrimaryIndex(bucketName string, opts *CreatePrimaryQueryIndexOptions) error {
	if opts == nil {
		opts = &CreatePrimaryQueryIndexOptions{}
	}
	if err := qm.validateScopeCollection(opts.ScopeName, opts.CollectionName); err != nil {
		return err
	}

	return qm.base.CreateIndex(
		opts.Context,
		opts.ParentSpan,
		qm.maybeAddScopeCollectionToBucket(bucketName, opts.ScopeName, opts.CollectionName),
		opts.CustomName,
		nil,
		createQueryIndexOptions{
			IgnoreIfExists: opts.IgnoreIfExists,
			Deferred:       opts.Deferred,
			Timeout:        opts.Timeout,
			RetryStrategy:  opts.RetryStrategy,
			NumReplicas:    opts.NumReplicas,
		})
}

// DropQueryIndexOptions is the set of options available to the query indexes DropIndex operation.
type DropQueryIndexOptions struct {
	IgnoreIfNotExists bool

	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	ScopeName      string
	CollectionName string

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

// DropIndex drops a specific index by name.
func (qm *QueryIndexManager) DropIndex(bucketName, indexName string, opts *DropQueryIndexOptions) error {
	if opts == nil {
		opts = &DropQueryIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{
			message: "an invalid index name was specified",
		}
	}
	if err := qm.validateScopeCollection(opts.ScopeName, opts.CollectionName); err != nil {
		return err
	}

	return qm.base.DropIndex(
		opts.Context,
		opts.ParentSpan,
		qm.maybeAddScopeCollectionToBucket(bucketName, opts.ScopeName, opts.CollectionName),
		indexName,
		dropQueryIndexOptions{
			IgnoreIfNotExists:    opts.IgnoreIfNotExists,
			Timeout:              opts.Timeout,
			RetryStrategy:        opts.RetryStrategy,
			UseCollectionsSyntax: opts.ScopeName != "" || opts.CollectionName != "",
		})
}

// DropPrimaryQueryIndexOptions is the set of options available to the query indexes DropPrimaryIndex operation.
type DropPrimaryQueryIndexOptions struct {
	IgnoreIfNotExists bool
	CustomName        string

	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	ScopeName      string
	CollectionName string

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

// DropPrimaryIndex drops the primary index.  Pass an empty customName for unnamed primary indexes.
func (qm *QueryIndexManager) DropPrimaryIndex(bucketName string, opts *DropPrimaryQueryIndexOptions) error {
	if opts == nil {
		opts = &DropPrimaryQueryIndexOptions{}
	}
	if err := qm.validateScopeCollection(opts.ScopeName, opts.CollectionName); err != nil {
		return err
	}

	return qm.base.DropIndex(
		opts.Context,
		opts.ParentSpan,
		qm.maybeAddScopeCollectionToBucket(bucketName, opts.ScopeName, opts.CollectionName),
		opts.CustomName,
		dropQueryIndexOptions{
			IgnoreIfNotExists:    opts.IgnoreIfNotExists,
			Timeout:              opts.Timeout,
			RetryStrategy:        opts.RetryStrategy,
			UseCollectionsSyntax: opts.ScopeName != "" || opts.CollectionName != "",
		})
}

// GetAllQueryIndexesOptions is the set of options available to the query indexes GetAllIndexes operation.
type GetAllQueryIndexesOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	ScopeName      string
	CollectionName string

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

func (qm *QueryIndexManager) buildGetAllIndexesWhereClause(
	bucketName,
	scopeName,
	collectionName string,
) (string, map[string]interface{}) {
	bucketCond := "bucket_id = $bucketName"
	scopeCond := "(" + bucketCond + " AND scope_id = $scopeName)"
	collectionCond := "(" + scopeCond + " AND keyspace_id = $collectionName)"
	params := map[string]interface{}{
		"bucketName": bucketName,
	}

	var where string
	if collectionName != "" {
		where = collectionCond
		params["scopeName"] = scopeName
		params["collectionName"] = collectionName
	} else if scopeName != "" {
		where = scopeCond
		params["scopeName"] = scopeName
	} else {
		where = bucketCond
	}

	if collectionName == "_default" || collectionName == "" {
		defaultColCond := "(bucket_id IS MISSING AND keyspace_id = $bucketName)"
		where = "(" + where + " OR " + defaultColCond + ")"
	}

	return where, params
}

// GetAllIndexes returns a list of all currently registered indexes.
func (qm *QueryIndexManager) GetAllIndexes(bucketName string, opts *GetAllQueryIndexesOptions) ([]QueryIndex, error) {
	if opts == nil {
		opts = &GetAllQueryIndexesOptions{}
	}

	where, params := qm.buildGetAllIndexesWhereClause(bucketName, opts.ScopeName, opts.CollectionName)

	return qm.base.GetAllIndexes(
		opts.Context,
		opts.ParentSpan,
		where,
		params,
		getAllQueryIndexesOptions{
			Timeout:       opts.Timeout,
			RetryStrategy: opts.RetryStrategy,
			ParentSpan:    opts.ParentSpan,
			Context:       opts.Context,
		})
}

// BuildDeferredQueryIndexOptions is the set of options available to the query indexes BuildDeferredIndexes operation.
type BuildDeferredQueryIndexOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	ScopeName      string
	CollectionName string

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

// BuildDeferredIndexes builds all indexes which are currently in deferred state.
// If no collection and scope names are specified in the options then *only* indexes created on the bucket directly
// will be built.
func (qm *QueryIndexManager) BuildDeferredIndexes(bucketName string, opts *BuildDeferredQueryIndexOptions) ([]string, error) {
	if opts == nil {
		opts = &BuildDeferredQueryIndexOptions{}
	}
	if err := qm.validateScopeCollection(opts.ScopeName, opts.CollectionName); err != nil {
		return nil, err
	}

	var where string
	params := make(map[string]interface{})
	if opts.CollectionName == "" {
		where = "(keyspace_id = $bucketName AND bucket_id IS MISSING)"
		params["bucketName"] = bucketName
	} else {
		where = "bucket_id = $bucketName AND scope_id = $scopeName AND keyspace_id = $collectionName"
		params["bucketName"] = bucketName
		params["scopeName"] = opts.ScopeName
		params["collectionName"] = opts.CollectionName
	}

	return qm.base.BuildDeferredIndexes(
		qm.maybeAddScopeCollectionToBucket(bucketName, opts.ScopeName, opts.CollectionName),
		where,
		params,
		buildDeferredQueryIndexOptions{
			Timeout:       opts.Timeout,
			RetryStrategy: opts.RetryStrategy,
			ParentSpan:    opts.ParentSpan,
			Context:       opts.Context,
		},
	)
}

// WatchQueryIndexOptions is the set of options available to the query indexes Watch operation.
type WatchQueryIndexOptions struct {
	WatchPrimary bool

	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	ScopeName      string
	CollectionName string

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

// WatchIndexes waits for a set of indexes to come online.
func (qm *QueryIndexManager) WatchIndexes(bucketName string, watchList []string, timeout time.Duration, opts *WatchQueryIndexOptions) error {
	if opts == nil {
		opts = &WatchQueryIndexOptions{}
	}
	if err := qm.validateScopeCollection(opts.ScopeName, opts.CollectionName); err != nil {
		return err
	}

	where, params := qm.buildGetAllIndexesWhereClause(bucketName, opts.ScopeName, opts.CollectionName)

	return qm.base.WatchIndexes(
		where,
		params,
		watchList,
		timeout,
		watchQueryIndexOptions{
			WatchPrimary:  opts.WatchPrimary,
			RetryStrategy: opts.RetryStrategy,
			ParentSpan:    opts.ParentSpan,
			Context:       opts.Context,
		},
	)
}
