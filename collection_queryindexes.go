package gocb

import (
	"fmt"
	"time"
)

// CollectionQueryIndexManager provides methods for performing Couchbase query index management against collections.
// UNCOMMITTED: This API may change in the future.
type CollectionQueryIndexManager struct {
	base *baseQueryIndexManager

	bucketName     string
	scopeName      string
	collectionName string
}

func (qm *CollectionQueryIndexManager) makeKeyspace() string {
	return fmt.Sprintf("`%s`.`%s`.`%s`", qm.bucketName, qm.scopeName, qm.collectionName)
}

func (qm *CollectionQueryIndexManager) validateScopeCollection(scope, collection string) error {
	if scope != "" || collection != "" {
		return makeInvalidArgumentsError("cannot use scope or collection with collection query index manager")
	}
	return nil
}

// CreateIndex creates an index over the specified fields.
// The SDK will automatically escape the provided index keys. For more advanced use cases like index keys using keywords
// scope.Query should be used with the query directly.
func (qm *CollectionQueryIndexManager) CreateIndex(indexName string, keys []string, opts *CreateQueryIndexOptions) error {
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
		qm.makeKeyspace(),
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

// CreatePrimaryIndex creates a primary index.  An empty customName uses the default naming.
func (qm *CollectionQueryIndexManager) CreatePrimaryIndex(opts *CreatePrimaryQueryIndexOptions) error {
	if opts == nil {
		opts = &CreatePrimaryQueryIndexOptions{}
	}
	if err := qm.validateScopeCollection(opts.ScopeName, opts.CollectionName); err != nil {
		return err
	}

	return qm.base.CreateIndex(
		opts.Context,
		opts.ParentSpan,
		qm.makeKeyspace(),
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

// DropIndex drops a specific index by name.
func (qm *CollectionQueryIndexManager) DropIndex(indexName string, opts *DropQueryIndexOptions) error {
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
		qm.makeKeyspace(),
		indexName,
		dropQueryIndexOptions{
			IgnoreIfNotExists:    opts.IgnoreIfNotExists,
			Timeout:              opts.Timeout,
			RetryStrategy:        opts.RetryStrategy,
			UseCollectionsSyntax: qm.scopeName != "" || qm.collectionName != "",
		})
}

// DropPrimaryIndex drops the primary index.  Pass an empty customName for unnamed primary indexes.
func (qm *CollectionQueryIndexManager) DropPrimaryIndex(opts *DropPrimaryQueryIndexOptions) error {
	if opts == nil {
		opts = &DropPrimaryQueryIndexOptions{}
	}
	if err := qm.validateScopeCollection(opts.ScopeName, opts.CollectionName); err != nil {
		return err
	}

	return qm.base.DropIndex(
		opts.Context,
		opts.ParentSpan,
		qm.makeKeyspace(),
		opts.CustomName,
		dropQueryIndexOptions{
			IgnoreIfNotExists:    opts.IgnoreIfNotExists,
			Timeout:              opts.Timeout,
			RetryStrategy:        opts.RetryStrategy,
			UseCollectionsSyntax: qm.scopeName != "" || qm.collectionName != "",
		})
}

func (qm *CollectionQueryIndexManager) buildGetAllIndexesWhereClause() (string, map[string]interface{}) {
	var where string
	if qm.collectionName == "_default" && qm.scopeName == "_default" {
		where = "((bucket_id=$bucketName AND scope_id=$scopeName AND keyspace_id=$collectionName) OR (bucket_id IS MISSING and keyspace_id=$bucketName)) "
	} else {
		where = "(bucket_id=$bucketName AND scope_id=$scopeName AND keyspace_id=$collectionName)"
	}

	return where, map[string]interface{}{
		"bucketName":     qm.bucketName,
		"scopeName":      qm.scopeName,
		"collectionName": qm.collectionName,
	}
}

// GetAllIndexes returns a list of all currently registered indexes.
func (qm *CollectionQueryIndexManager) GetAllIndexes(opts *GetAllQueryIndexesOptions) ([]QueryIndex, error) {
	if opts == nil {
		opts = &GetAllQueryIndexesOptions{}
	}
	if err := qm.validateScopeCollection(opts.ScopeName, opts.CollectionName); err != nil {
		return nil, err
	}

	where, params := qm.buildGetAllIndexesWhereClause()

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

// BuildDeferredIndexes builds all indexes which are currently in deferred state.
// If no collection and scope names are specified in the options then *only* indexes created on the bucket directly
// will be built.
func (qm *CollectionQueryIndexManager) BuildDeferredIndexes(opts *BuildDeferredQueryIndexOptions) ([]string, error) {
	if opts == nil {
		opts = &BuildDeferredQueryIndexOptions{}
	}
	if err := qm.validateScopeCollection(opts.ScopeName, opts.CollectionName); err != nil {
		return nil, err
	}

	where, params := qm.buildGetAllIndexesWhereClause()

	return qm.base.BuildDeferredIndexes(
		qm.makeKeyspace(),
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

// WatchIndexes waits for a set of indexes to come online.
func (qm *CollectionQueryIndexManager) WatchIndexes(watchList []string, timeout time.Duration, opts *WatchQueryIndexOptions) error {
	if opts == nil {
		opts = &WatchQueryIndexOptions{}
	}
	if err := qm.validateScopeCollection(opts.ScopeName, opts.CollectionName); err != nil {
		return err
	}

	where, params := qm.buildGetAllIndexesWhereClause()

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
