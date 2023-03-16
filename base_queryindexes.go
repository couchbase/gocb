package gocb

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type baseQueryIndexManager struct {
	provider queryIndexQueryProvider

	globalTimeout time.Duration
	tracer        RequestTracer
	meter         *meterWrapper
}

type queryIndexQueryProvider interface {
	Query(statement string, opts *QueryOptions) (*QueryResult, error)
}

func (qm *baseQueryIndexManager) tryParseErrorMessage(err error) error {
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

func (qm *baseQueryIndexManager) doQuery(q string, opts *QueryOptions) ([][]byte, error) {
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
	NumReplicas    int

	Timeout       time.Duration
	RetryStrategy RetryStrategy
}

func (qm *baseQueryIndexManager) CreateIndex(
	ctx context.Context,
	parent RequestSpan,
	keyspace, indexName string,
	fields []string,
	opts createQueryIndexOptions,
) error {
	var qs string

	spanName := "manager_query_create_index"

	if len(fields) == 0 {
		spanName = "manager_query_create_primary_index"
		qs += "CREATE PRIMARY INDEX"
	} else {
		qs += "CREATE INDEX"
	}
	if indexName != "" {
		qs += " `" + indexName + "`"
	}
	qs += " ON " + keyspace
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

	var with []string
	if opts.Deferred {
		with = append(with, `"defer_build":true`)
	}
	if opts.NumReplicas > 0 {
		with = append(with, `"num_replica":`+strconv.Itoa(opts.NumReplicas))
	}

	if len(with) > 0 {
		withStr := strings.Join(with, ",")
		qs += " WITH {" + withStr + "}"
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

type dropQueryIndexOptions struct {
	IgnoreIfNotExists bool

	Timeout       time.Duration
	RetryStrategy RetryStrategy

	UseCollectionsSyntax bool
}

func (qm *baseQueryIndexManager) DropIndex(
	ctx context.Context,
	parent RequestSpan,
	keyspace, indexName string,
	opts dropQueryIndexOptions,
) error {
	var qs string

	spanName := "manager_query_drop_index"
	if indexName == "" {
		spanName = "manager_query_drop_primary_index"
		qs += "DROP PRIMARY INDEX ON " + keyspace
	} else {
		if opts.UseCollectionsSyntax {
			qs += "DROP INDEX `" + indexName + "` ON " + keyspace
		} else {
			qs += "DROP INDEX " + keyspace + ".`" + indexName + "`"
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

type getAllQueryIndexesOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

func (qm *baseQueryIndexManager) GetAllIndexes(
	ctx context.Context,
	parent RequestSpan,
	whereClause string,
	params map[string]interface{},
	opts getAllQueryIndexesOptions,
) ([]QueryIndex, error) {
	start := time.Now()
	defer qm.meter.ValueRecord(meterValueServiceManagement, "manager_query_get_all_indexes", start)

	return qm.getAllIndexes(ctx, parent, whereClause, params, opts)
}

func (qm *baseQueryIndexManager) getAllIndexes(
	ctx context.Context,
	parent RequestSpan,
	whereClause string,
	params map[string]interface{},
	opts getAllQueryIndexesOptions,
) ([]QueryIndex, error) {
	span := createSpan(qm.tracer, parent, "manager_query_get_all_indexes", "management")
	defer span.End()

	q := "SELECT `idx`.* FROM system:indexes AS idx WHERE " + whereClause + " AND `using` = \"gsi\" " +
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

type buildDeferredQueryIndexOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

func (qm *baseQueryIndexManager) BuildDeferredIndexes(
	keyspace string,
	getIndexesWhereClause string,
	getIndexesWhereParams map[string]interface{},
	opts buildDeferredQueryIndexOptions,
) ([]string, error) {
	start := time.Now()
	defer qm.meter.ValueRecord(meterValueServiceManagement, "manager_query_build_deferred_indexes", start)

	span := createSpan(qm.tracer, opts.ParentSpan, "manager_query_build_deferred_indexes", "management")
	defer span.End()

	query := "SELECT RAW name from system:indexes WHERE " + getIndexesWhereClause + " AND state = \"deferred\""

	indexesRes, err := qm.doQuery(query, &QueryOptions{
		Timeout:         opts.Timeout,
		RetryStrategy:   opts.RetryStrategy,
		Adhoc:           true,
		ParentSpan:      span,
		Context:         opts.Context,
		NamedParameters: getIndexesWhereParams,
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

	var qs string
	qs += "BUILD INDEX ON " + keyspace + "("
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

type watchQueryIndexOptions struct {
	WatchPrimary bool

	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context
}

func (qm *baseQueryIndexManager) WatchIndexes(
	getAllWhereClause string,
	getAllWhereParams map[string]interface{},
	watchList []string,
	timeout time.Duration,
	opts watchQueryIndexOptions,
) error {
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
			getAllWhereClause,
			getAllWhereParams,
			getAllQueryIndexesOptions{
				Timeout:       time.Until(deadline),
				RetryStrategy: opts.RetryStrategy,
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
