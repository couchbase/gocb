package gocb

import (
	"context"
	"strings"
	"time"
)

// QueryIndexManager provides methods for performing Couchbase N1ql index management.
// Volatile: This API is subject to change at any time.
type QueryIndexManager struct {
	executeQuery func(statement string, opts *QueryOptions) (*QueryResults, error)
}

// QueryIndex represents a Couchbase GSI index.
type QueryIndex struct {
	Name      string    `json:"name"`
	IsPrimary bool      `json:"is_primary"`
	Type      IndexType `json:"using"`
	State     string    `json:"state"`
	Keyspace  string    `json:"keyspace_id"`
	Namespace string    `json:"namespace_id"`
	IndexKey  []string  `json:"index_key"`
}

type createQueryIndexOptions struct {
	Context context.Context

	IgnoreIfExists bool
	Deferred       bool
}

func (qm *QueryIndexManager) createIndex(bucketName, indexName string, fields []string, opts createQueryIndexOptions) error {
	var qs string

	if len(fields) == 0 {
		qs += "CREATE PRIMARY INDEX"
	} else {
		qs += "CREATE INDEX"
	}
	if indexName != "" {
		qs += " `" + indexName + "`"
	}
	qs += " ON `" + bucketName + "`"
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

	rows, err := qm.executeQuery(qs, &QueryOptions{
		Context: opts.Context,
	})
	if err != nil {
		if strings.Contains(err.Error(), "already exist") {
			if opts.IgnoreIfExists {
				return nil
			}
			return queryIndexError{
				indexExists: true,
				message:     "the index specified already exists",
			}
		}
		return err
	}

	return rows.Close()
}

// CreateQueryIndexOptions is the set of options available to the query indexes Create operation.
type CreateQueryIndexOptions struct {
	Timeout time.Duration
	Context context.Context

	IgnoreIfExists bool
	Deferred       bool
}

// Create creates an index over the specified fields.
func (qm *QueryIndexManager) Create(bucketName, indexName string, fields []string, opts *CreateQueryIndexOptions) error {
	if indexName == "" {
		return invalidArgumentsError{
			message: "an invalid index name was specified",
		}
	}
	if len(fields) <= 0 {
		return invalidArgumentsError{
			message: "you must specify at least one field to index",
		}
	}

	if opts == nil {
		opts = &CreateQueryIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	return qm.createIndex(bucketName, indexName, fields, createQueryIndexOptions{
		IgnoreIfExists: opts.IgnoreIfExists,
		Deferred:       opts.Deferred,
		Context:        ctx,
	})
}

// CreatePrimaryQueryIndexOptions is the set of options available to the query indexes CreatePrimary operation.
type CreatePrimaryQueryIndexOptions struct {
	Timeout time.Duration
	Context context.Context

	IgnoreIfExists bool
	Deferred       bool
	CustomName     string
}

// CreatePrimary creates a primary index.  An empty customName uses the default naming.
func (qm *QueryIndexManager) CreatePrimary(bucketName string, opts *CreatePrimaryQueryIndexOptions) error {
	if opts == nil {
		opts = &CreatePrimaryQueryIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	return qm.createIndex(bucketName, opts.CustomName, nil, createQueryIndexOptions{
		IgnoreIfExists: opts.IgnoreIfExists,
		Deferred:       opts.Deferred,
		Context:        ctx,
	})
}

type dropQueryIndexOptions struct {
	Context context.Context

	IgnoreIfNotExists bool
}

func (qm *QueryIndexManager) dropIndex(bucketName, indexName string, opts dropQueryIndexOptions) error {
	var qs string

	if indexName == "" {
		qs += "DROP PRIMARY INDEX ON `" + bucketName + "`"
	} else {
		qs += "DROP INDEX `" + bucketName + "`.`" + indexName + "`"
	}

	rows, err := qm.executeQuery(qs, &QueryOptions{
		Context: opts.Context,
	})
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			if opts.IgnoreIfNotExists {
				return nil
			}
			return queryIndexError{
				indexExists: true,
				message:     "the index specified does not exist",
			}
		}
		return err
	}

	return rows.Close()
}

// DropQueryIndexOptions is the set of options available to the query indexes Drop operation.
type DropQueryIndexOptions struct {
	Timeout time.Duration
	Context context.Context

	IgnoreIfNotExists bool
}

// Drop drops a specific index by name.
func (qm *QueryIndexManager) Drop(bucketName, indexName string, opts *DropQueryIndexOptions) error {
	if indexName == "" {
		return invalidArgumentsError{
			message: "an invalid index name was specified",
		}
	}

	if opts == nil {
		opts = &DropQueryIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	return qm.dropIndex(bucketName, indexName, dropQueryIndexOptions{
		Context:           ctx,
		IgnoreIfNotExists: opts.IgnoreIfNotExists,
	})
}

// DropPrimaryQueryIndexOptions is the set of options available to the query indexes DropPrimary operation.
type DropPrimaryQueryIndexOptions struct {
	Timeout time.Duration
	Context context.Context

	IgnoreIfNotExists bool
	CustomName        string
}

// DropPrimary drops the primary index.  Pass an empty customName for unnamed primary indexes.
func (qm *QueryIndexManager) DropPrimary(bucketName string, opts *DropPrimaryQueryIndexOptions) error {
	if opts == nil {
		opts = &DropPrimaryQueryIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	return qm.dropIndex(bucketName, opts.CustomName, dropQueryIndexOptions{
		IgnoreIfNotExists: opts.IgnoreIfNotExists,
		Context:           ctx,
	})
}

// GetAllQueryIndexesOptions is the set of options available to the query indexes GetAll operation.
type GetAllQueryIndexesOptions struct {
	Timeout time.Duration
	Context context.Context
}

// GetAll returns a list of all currently registered indexes.
func (qm *QueryIndexManager) GetAll(bucketName string, opts *GetAllQueryIndexesOptions) ([]QueryIndex, error) {
	if opts == nil {
		opts = &GetAllQueryIndexesOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	q := "SELECT `indexes`.* FROM system:indexes WHERE keyspace_id=?"
	queryOpts := &QueryOptions{
		Context:              ctx,
		PositionalParameters: []interface{}{bucketName},
	}

	rows, err := qm.executeQuery(q, queryOpts)
	if err != nil {
		return nil, err
	}

	var indexes []QueryIndex
	var index QueryIndex
	for rows.Next(&index) {
		indexes = append(indexes, index)
		index = QueryIndex{}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	return indexes, nil
}

// BuildDeferredQueryIndexOptions is the set of options available to the query indexes BuildDeferred operation.
type BuildDeferredQueryIndexOptions struct {
	Timeout time.Duration
	Context context.Context
}

// BuildDeferred builds all indexes which are currently in deferred state.
func (qm *QueryIndexManager) BuildDeferred(bucketName string, opts *BuildDeferredQueryIndexOptions) ([]string, error) {
	if opts == nil {
		opts = &BuildDeferredQueryIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	indexList, err := qm.GetAll(bucketName, &GetAllQueryIndexesOptions{
		Context: ctx,
	})
	if err != nil {
		return nil, err
	}

	var deferredList []string
	for i := 0; i < len(indexList); i++ {
		var index = indexList[i]
		if index.State == "deferred" || index.State == "pending" {
			deferredList = append(deferredList, index.Name)
		}
	}

	if len(deferredList) == 0 {
		// Don't try to build an empty index list
		return nil, nil
	}

	var qs string
	qs += "BUILD INDEX ON `" + bucketName + "`("
	for i := 0; i < len(deferredList); i++ {
		if i > 0 {
			qs += ", "
		}
		qs += "`" + deferredList[i] + "`"
	}
	qs += ")"

	rows, err := qm.executeQuery(qs, &QueryOptions{
		Context: ctx,
	})
	if err != nil {
		return nil, err
	}

	if err := rows.Close(); err != nil {
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
		return false, queryIndexError{
			indexMissing: true,
			message:      "the index specified does not exist",
		}
	}

	for i := 0; i < len(checkIndexes); i++ {
		if checkIndexes[i].State != "online" {
			return false, nil
		}
	}
	return true, nil
}

// WatchQueryIndexOptions is the set of options available to the query indexes Watch operation.
type WatchQueryIndexOptions struct {
	WatchPrimary bool
}

// WatchQueryIndexTimeout is used for setting a timeout value for the query indexes Watch operation.
type WatchQueryIndexTimeout struct {
	Timeout time.Duration
	Context context.Context
}

// Watch waits for a set of indexes to come online.
func (qm *QueryIndexManager) Watch(bucketName string, watchList []string, timeout WatchQueryIndexTimeout, opts *WatchQueryIndexOptions) error {
	if timeout.Context == nil && timeout.Timeout == 0 {
		return invalidArgumentsError{
			message: "either a context or a timeout value must be supplied to watch",
		}
	}

	if opts == nil {
		opts = &WatchQueryIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(timeout.Context, timeout.Timeout)
	if cancel != nil {
		defer cancel()
	}

	if opts.WatchPrimary {
		watchList = append(watchList, "#primary")
	}

	curInterval := 50 * time.Millisecond
	for {
		indexes, err := qm.GetAll(bucketName, &GetAllQueryIndexesOptions{
			Context: ctx,
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

		// This can only be !ok if the user has set context to something like Background so let's just keep running.
		d, ok := ctx.Deadline()
		if ok {
			if time.Now().Add(curInterval).After(d) {
				return timeoutError{}
			}
		}

		// wait till our next poll interval
		time.Sleep(curInterval)
	}

	return nil
}
