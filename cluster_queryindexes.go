package gocb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// QueryIndexManager provides methods for performing Couchbase query index management.
type QueryIndexManager struct {
	provider queryIndexQueryProvider

	globalTimeout time.Duration
	tracer        RequestTracer
	meter         *meterWrapper
}

type queryIndexQueryProvider interface {
	Query(statement string, opts *QueryOptions) (*QueryResult, error)
}

func (qm *QueryIndexManager) tryParseErrorMessage(err error) error {
	var qErr *QueryError
	if !errors.As(err, &qErr) {
		return err
	}

	if len(qErr.Errors) == 0 {
		return err
	}

	firstErr := qErr.Errors[0]
	var innerErr error
	if firstErr.Code == 12016 {
		innerErr = ErrIndexNotFound
	} else if firstErr.Code == 4300 {
		innerErr = ErrIndexExists
	} else {
		// Older server versions don't return meaningful error codes when it comes to index management
		// so we need to go spelunking.
		msg := strings.ToLower(firstErr.Message)
		if match, err := regexp.MatchString(".*?ndex .*? not found.*", msg); err == nil && match {
			innerErr = ErrIndexNotFound
		} else if match, err := regexp.MatchString(".*?ndex .*? already exists.*", msg); err == nil && match {
			innerErr = ErrIndexExists
		}
	}

	if innerErr == nil {
		return err
	}

	return QueryError{
		InnerError:      innerErr,
		Statement:       qErr.Statement,
		ClientContextID: qErr.ClientContextID,
		Errors:          qErr.Errors,
		Endpoint:        qErr.Endpoint,
		RetryReasons:    qErr.RetryReasons,
		RetryAttempts:   qErr.RetryAttempts,
	}
}

func (qm *QueryIndexManager) doQuery(q string, opts *QueryOptions) ([][]byte, error) {
	if opts.Timeout == 0 {
		opts.Timeout = qm.globalTimeout
	}

	result, err := qm.provider.Query(q, opts)
	if err != nil {
		return nil, qm.tryParseErrorMessage(err)
	}

	var rows [][]byte
	for result.Next() {
		var row json.RawMessage
		err := result.Row(&row)
		if err != nil {
			logWarnf("management operation failed to read row: %s", err)
		} else {
			rows = append(rows, row)
		}
	}
	err = result.Err()
	if err != nil {
		return nil, qm.tryParseErrorMessage(err)
	}

	return rows, nil
}

type jsonQueryIndex struct {
	Name      string         `json:"name"`
	IsPrimary bool           `json:"is_primary"`
	Type      QueryIndexType `json:"using"`
	State     string         `json:"state"`
	Keyspace  string         `json:"keyspace_id"`
	Namespace string         `json:"namespace_id"`
	IndexKey  []string       `json:"index_key"`
	Condition string         `json:"condition"`
	Partition string         `json:"partition"`
	Scope     string         `json:"scope_id"`
	Bucket    string         `json:"bucket_id"`
}

// QueryIndex represents a Couchbase GSI index.
type QueryIndex struct {
	Name           string
	IsPrimary      bool
	Type           QueryIndexType
	State          string
	Keyspace       string
	Namespace      string
	IndexKey       []string
	Condition      string
	Partition      string
	CollectionName string
	ScopeName      string
	BucketName     string
}

func (index *QueryIndex) fromData(data jsonQueryIndex) error {
	index.Name = data.Name
	index.IsPrimary = data.IsPrimary
	index.Type = data.Type
	index.State = data.State
	index.Keyspace = data.Keyspace
	index.Namespace = data.Namespace
	index.IndexKey = data.IndexKey
	index.Condition = data.Condition
	index.Partition = data.Partition
	index.ScopeName = data.Scope
	if data.Bucket == "" {
		index.BucketName = data.Keyspace
	} else {
		index.BucketName = data.Bucket
	}
	if data.Scope != "" {
		index.CollectionName = data.Keyspace
	}

	return nil
}

type createQueryIndexOptions struct {
	IgnoreIfExists bool
	Deferred       bool

	Timeout       time.Duration
	RetryStrategy RetryStrategy

	Scope      string
	Collection string
}

func (qm *QueryIndexManager) createIndex(
	ctx context.Context,
	parent RequestSpan,
	bucketName, indexName string,
	fields []string,
	opts createQueryIndexOptions,
) error {
	var qs string

	spanName := "manager_query_create_index"

	bucketName = qm.maybeAddScopeCollectionToBucket(bucketName, opts.Scope, opts.Collection)

	if len(fields) == 0 {
		spanName = "manager_query_create_primary_index"
		qs += "CREATE PRIMARY INDEX"
	} else {
		qs += "CREATE INDEX"
	}
	if indexName != "" {
		qs += " `" + indexName + "`"
	}
	qs += " ON " + bucketName
	if len(fields) > 0 {
		qs += " ("
		for i := 0; i < len(fields); i++ {
			if i > 0 {
				qs += ", "
			}
			qs += "`" + fields[i] + "`"
		}
		qs += ")"
	}
	if opts.Deferred {
		qs += " WITH {\"defer_build\": true}"
	}

	start := time.Now()
	defer qm.meter.ValueRecord(meterValueServiceManagement, spanName, start)

	span := createSpan(qm.tracer, parent, spanName, "management")
	defer span.End()

	_, err := qm.doQuery(qs, &QueryOptions{
		Timeout:       opts.Timeout,
		RetryStrategy: opts.RetryStrategy,
		Adhoc:         true,
		ParentSpan:    span,
		Context:       ctx,
	})
	if err == nil {
		return nil
	}

	if opts.IgnoreIfExists && errors.Is(err, ErrIndexExists) {
		return nil
	}

	return err
}

func (qm *QueryIndexManager) maybeAddScopeCollectionToBucket(bucketName, scope, collection string) string {
	if scope != "" && collection != "" {
		bucketName = fmt.Sprintf("`%s`.`%s`.`%s`", bucketName, scope, collection)
	} else if collection == "" && scope != "" {
		bucketName = fmt.Sprintf("`%s`.`%s`.`%s`", bucketName, scope, collection)
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

	return qm.createIndex(opts.Context, opts.ParentSpan, bucketName, indexName, keys, createQueryIndexOptions{
		IgnoreIfExists: opts.IgnoreIfExists,
		Deferred:       opts.Deferred,
		Timeout:        opts.Timeout,
		RetryStrategy:  opts.RetryStrategy,
		Scope:          opts.ScopeName,
		Collection:     opts.CollectionName,
	})
}

// CreatePrimaryQueryIndexOptions is the set of options available to the query indexes CreatePrimaryIndex operation.
type CreatePrimaryQueryIndexOptions struct {
	IgnoreIfExists bool
	Deferred       bool
	CustomName     string

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

	return qm.createIndex(
		opts.Context,
		opts.ParentSpan,
		bucketName,
		opts.CustomName,
		nil,
		createQueryIndexOptions{
			IgnoreIfExists: opts.IgnoreIfExists,
			Deferred:       opts.Deferred,
			Timeout:        opts.Timeout,
			RetryStrategy:  opts.RetryStrategy,
			Scope:          opts.ScopeName,
			Collection:     opts.CollectionName,
		})
}

type dropQueryIndexOptions struct {
	IgnoreIfNotExists bool

	Timeout        time.Duration
	RetryStrategy  RetryStrategy
	ScopeName      string
	CollectionName string
}

func (qm *QueryIndexManager) dropIndex(
	ctx context.Context,
	parent RequestSpan,
	bucketName, indexName string,
	opts dropQueryIndexOptions,
) error {
	var qs string

	bucketName = qm.maybeAddScopeCollectionToBucket(bucketName, opts.ScopeName, opts.CollectionName)

	spanName := "manager_query_drop_index"
	if indexName == "" {
		spanName = "manager_query_drop_primary_index"
		qs += "DROP PRIMARY INDEX ON " + bucketName
	} else {
		if opts.ScopeName != "" || opts.CollectionName != "" {
			qs += "DROP INDEX `" + indexName + "` ON " + bucketName
		} else {
			qs += "DROP INDEX " + bucketName + ".`" + indexName + "`"
		}
	}

	start := time.Now()
	defer qm.meter.ValueRecord(meterValueServiceManagement, spanName, start)

	span := createSpan(qm.tracer, parent, spanName, "management")
	defer span.End()

	_, err := qm.doQuery(qs, &QueryOptions{
		Timeout:       opts.Timeout,
		RetryStrategy: opts.RetryStrategy,
		Adhoc:         true,
		ParentSpan:    span,
		Context:       ctx,
	})
	if err == nil {
		return nil
	}

	if opts.IgnoreIfNotExists && errors.Is(err, ErrIndexNotFound) {
		return nil
	}

	return err
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
	// if err := qm.validateScopeCollection(opts.ScopeName, opts.CollectionName); err != nil {
	// 	return err
	// }

	return qm.dropIndex(
		opts.Context,
		opts.ParentSpan,
		bucketName,
		indexName,
		dropQueryIndexOptions{
			IgnoreIfNotExists: opts.IgnoreIfNotExists,
			Timeout:           opts.Timeout,
			RetryStrategy:     opts.RetryStrategy,
			ScopeName:         opts.ScopeName,
			CollectionName:    opts.CollectionName,
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

	return qm.dropIndex(
		opts.Context,
		opts.ParentSpan,
		bucketName,
		opts.CustomName,
		dropQueryIndexOptions{
			IgnoreIfNotExists: opts.IgnoreIfNotExists,
			Timeout:           opts.Timeout,
			RetryStrategy:     opts.RetryStrategy,
			ScopeName:         opts.ScopeName,
			CollectionName:    opts.CollectionName,
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

// GetAllIndexes returns a list of all currently registered indexes.
func (qm *QueryIndexManager) GetAllIndexes(bucketName string, opts *GetAllQueryIndexesOptions) ([]QueryIndex, error) {
	if opts == nil {
		opts = &GetAllQueryIndexesOptions{}
	}

	start := time.Now()
	defer qm.meter.ValueRecord(meterValueServiceManagement, "manager_query_get_all_indexes", start)

	return qm.getAllIndexes(opts.Context, opts.ParentSpan, bucketName, opts)
}

func (qm *QueryIndexManager) getAllIndexes(
	ctx context.Context,
	parent RequestSpan,
	bucketName string,
	opts *GetAllQueryIndexesOptions,
) ([]QueryIndex, error) {
	span := createSpan(qm.tracer, parent, "manager_query_get_all_indexes", "management")
	defer span.End()

	bucketCond := "bucket_id = $bucketName"
	scopeCond := "(" + bucketCond + " AND scope_id = $scopeName)"
	collectionCond := "(" + scopeCond + " AND keyspace_id = $collectionName)"
	params := map[string]interface{}{
		"bucketName":     bucketName,
		"scopeName":      opts.ScopeName,
		"collectionName": opts.CollectionName,
	}

	var where string
	if opts.CollectionName != "" {
		where = collectionCond
	} else if opts.ScopeName != "" {
		where = scopeCond
	} else {
		where = bucketCond
	}

	if opts.CollectionName == "_default" || opts.CollectionName == "" {
		defaultColCond := "(bucket_id IS MISSING AND keyspace_id = $bucketName)"
		where = "(" + where + " OR " + defaultColCond + ")"
	}

	q := "SELECT `idx`.* FROM system:indexes AS idx WHERE " + where + " AND `using` = \"gsi\" " +
		"ORDER BY is_primary DESC, name ASC"

	rows, err := qm.doQuery(q, &QueryOptions{
		NamedParameters: params,
		Readonly:        true,
		Timeout:         opts.Timeout,
		RetryStrategy:   opts.RetryStrategy,
		Adhoc:           true,
		ParentSpan:      span,
		Context:         ctx,
	})
	if err != nil {
		return nil, err
	}

	var indexes []QueryIndex
	for _, row := range rows {
		var jsonIdx jsonQueryIndex
		err := json.Unmarshal(row, &jsonIdx)
		if err != nil {
			return nil, err
		}

		var index QueryIndex
		err = index.fromData(jsonIdx)
		if err != nil {
			return nil, err
		}

		indexes = append(indexes, index)
	}

	return indexes, nil
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

	start := time.Now()
	defer qm.meter.ValueRecord(meterValueServiceManagement, "manager_query_build_deferred_indexes", start)

	span := createSpan(qm.tracer, opts.ParentSpan, "manager_query_build_deferred_indexes", "management")
	defer span.End()

	var query string
	var params []interface{}
	if opts.CollectionName == "" {
		query = "SELECT RAW name from system:indexes WHERE (keyspace_id = ? AND bucket_id IS MISSING) AND state = \"deferred\""
		params = append(params, bucketName)
	} else {
		query = "SELECT RAW name from system:indexes WHERE bucket_id = ? AND scope_id = ? AND keyspace_id = ? AND state = \"deferred\""
		params = append(params, bucketName, opts.ScopeName, opts.CollectionName)
	}

	indexesRes, err := qm.doQuery(query, &QueryOptions{
		Timeout:              opts.Timeout,
		RetryStrategy:        opts.RetryStrategy,
		Adhoc:                true,
		ParentSpan:           span,
		Context:              opts.Context,
		PositionalParameters: params,
	})
	if err != nil {
		return nil, err
	}

	var deferredList []string
	for _, row := range indexesRes {
		var name string
		err := json.Unmarshal(row, &name)
		if err != nil {
			return nil, err
		}

		deferredList = append(deferredList, name)
	}

	if len(deferredList) == 0 {
		// Don't try to build an empty index list
		return nil, nil
	}

	bucketName = qm.maybeAddScopeCollectionToBucket(bucketName, opts.ScopeName, opts.CollectionName)

	var qs string
	qs += "BUILD INDEX ON " + bucketName + "("
	for i := 0; i < len(deferredList); i++ {
		if i > 0 {
			qs += ", "
		}
		qs += "`" + deferredList[i] + "`"
	}
	qs += ")"

	_, err = qm.doQuery(qs, &QueryOptions{
		Timeout:       opts.Timeout,
		RetryStrategy: opts.RetryStrategy,
		Adhoc:         true,
		ParentSpan:    span,
		Context:       opts.Context,
	})
	if err != nil {
		return nil, err
	}

	return deferredList, nil
}

func checkIndexesActive(indexes []QueryIndex, checkList []string) (bool, error) {
	var checkIndexes []QueryIndex
	for i := 0; i < len(checkList); i++ {
		indexName := checkList[i]

		for j := 0; j < len(indexes); j++ {
			if indexes[j].Name == indexName {
				checkIndexes = append(checkIndexes, indexes[j])
				break
			}
		}
	}

	if len(checkIndexes) != len(checkList) {
		return false, ErrIndexNotFound
	}

	for i := 0; i < len(checkIndexes); i++ {
		if checkIndexes[i].State != "online" {
			logDebugf("Index not online: %s is in state %s", checkIndexes[i].Name, checkIndexes[i].State)
			return false, nil
		}
	}
	return true, nil
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

	start := time.Now()
	defer qm.meter.ValueRecord(meterValueServiceManagement, "manager_query_watch_indexes", start)

	span := createSpan(qm.tracer, opts.ParentSpan, "manager_query_watch_indexes", "management")
	defer span.End()

	if opts.WatchPrimary {
		watchList = append(watchList, "#primary")
	}

	deadline := time.Now().Add(timeout)

	curInterval := 50 * time.Millisecond
	for {
		if deadline.Before(time.Now()) {
			return ErrUnambiguousTimeout
		}

		indexes, err := qm.getAllIndexes(
			opts.Context,
			span,
			bucketName,
			&GetAllQueryIndexesOptions{
				Timeout:        time.Until(deadline),
				RetryStrategy:  opts.RetryStrategy,
				ScopeName:      opts.ScopeName,
				CollectionName: opts.CollectionName,
			})
		if err != nil {
			return err
		}

		allOnline, err := checkIndexesActive(indexes, watchList)
		if err != nil {
			return err
		}

		if allOnline {
			break
		}

		curInterval += 500 * time.Millisecond
		if curInterval > 1000 {
			curInterval = 1000
		}

		// Make sure we don't sleep past our overall deadline, if we adjust the
		// deadline then it will be caught at the top of this loop as a timeout.
		sleepDeadline := time.Now().Add(curInterval)
		if sleepDeadline.After(deadline) {
			sleepDeadline = deadline
		}

		// wait till our next poll interval
		time.Sleep(time.Until(sleepDeadline))
	}

	return nil
}
