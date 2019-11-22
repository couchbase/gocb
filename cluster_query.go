package gocb

import (
	"encoding/json"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

type queryCacheEntry struct {
	enhanced    bool
	name        string
	encodedPlan string
}

type jsonQueryMetrics struct {
	ElapsedTime   string `json:"elapsedTime"`
	ExecutionTime string `json:"executionTime"`
	ResultCount   uint   `json:"resultCount"`
	ResultSize    uint   `json:"resultSize"`
	MutationCount uint   `json:"mutationCount,omitempty"`
	SortCount     uint   `json:"sortCount,omitempty"`
	ErrorCount    uint   `json:"errorCount,omitempty"`
	WarningCount  uint   `json:"warningCount,omitempty"`
}

type jsonQueryWarning struct {
	Code    uint32 `json:"code"`
	Message string `json:"msg"`
}

type jsonQueryResponse struct {
	RequestID       string             `json:"requestID"`
	ClientContextID string             `json:"clientContextID"`
	Status          string             `json:"status"`
	Warnings        []jsonQueryWarning `json:"warnings"`
	Metrics         jsonQueryMetrics   `json:"metrics"`
	Profile         interface{}        `json:"profile"`
	Signature       interface{}        `json:"signature"`
	Prepared        string             `json:"prepared"`
}

type jsonQueryPrepData struct {
	EncodedPlan string `json:"encoded_plan"`
	Name        string `json:"name"`
}

// QueryMetrics encapsulates various metrics gathered during a queries execution.
type QueryMetrics struct {
	ElapsedTime   time.Duration
	ExecutionTime time.Duration
	ResultCount   uint
	ResultSize    uint
	MutationCount uint
	SortCount     uint
	ErrorCount    uint
	WarningCount  uint
}

func (metrics *QueryMetrics) fromData(data jsonQueryMetrics) error {
	elapsedTime, err := time.ParseDuration(data.ElapsedTime)
	if err != nil {
		logDebugf("Failed to parse query metrics elapsed time: %s", err)
	}

	executionTime, err := time.ParseDuration(data.ExecutionTime)
	if err != nil {
		logDebugf("Failed to parse query metrics execution time: %s", err)
	}

	metrics.ElapsedTime = elapsedTime
	metrics.ExecutionTime = executionTime
	metrics.ResultCount = data.ResultCount
	metrics.ResultSize = data.ResultSize
	metrics.MutationCount = data.MutationCount
	metrics.SortCount = data.SortCount
	metrics.ErrorCount = data.ErrorCount
	metrics.WarningCount = data.WarningCount

	return nil
}

// QueryWarning encapsulates any warnings returned by a query.
type QueryWarning struct {
	Code    uint32
	Message string
}

func (warning *QueryWarning) fromData(data jsonQueryWarning) error {
	warning.Code = data.Code
	warning.Message = data.Message

	return nil
}

// QueryMetaData provides access to the meta-data properties of a N1QL query result.
type QueryMetaData struct {
	RequestID       string
	ClientContextID string
	Metrics         QueryMetrics
	Signature       interface{}
	Warnings        []QueryWarning
	Profile         interface{}

	preparedName string
}

func (meta *QueryMetaData) fromData(data jsonQueryResponse) error {
	metrics := QueryMetrics{}
	if err := metrics.fromData(data.Metrics); err != nil {
		return err
	}

	warnings := make([]QueryWarning, len(data.Warnings))
	for wIdx, jsonWarning := range data.Warnings {
		warnings[wIdx].fromData(jsonWarning)
	}

	meta.RequestID = data.RequestID
	meta.ClientContextID = data.ClientContextID
	meta.Metrics = metrics
	meta.Signature = data.Signature
	meta.Warnings = warnings
	meta.Profile = data.Profile
	meta.preparedName = data.Prepared

	return nil
}

// QueryResult allows access to the results of a N1QL query.
type QueryResult struct {
	reader *gocbcore.N1QLRowReader

	rowBytes []byte
}

func newQueryResult(reader *gocbcore.N1QLRowReader) (*QueryResult, error) {
	return &QueryResult{
		reader: reader,
	}, nil
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *QueryResult) Next() bool {
	rowBytes := r.reader.NextRow()
	if rowBytes == nil {
		return false
	}

	r.rowBytes = rowBytes
	return true
}

// Value returns the value of the current row
func (r *QueryResult) Value(valuePtr interface{}) error {
	if r.rowBytes == nil {
		return ErrNoResult
	}

	if bytesPtr, ok := valuePtr.(*json.RawMessage); ok {
		*bytesPtr = r.rowBytes
		return nil
	}

	return json.Unmarshal(r.rowBytes, valuePtr)
}

// Err returns any errors that have occurred on the stream
func (r *QueryResult) Err() error {
	return r.reader.Err()
}

// Close marks the results as closed, returning any errors that occurred during reading the results.
func (r *QueryResult) Close() error {
	return r.reader.Close()
}

// One assigns the first value from the results into the value pointer.
// It will close the results but not before iterating through all remaining
// results, as such this should only be used for very small resultsets - ideally
// of, at most, length 1.
func (r *QueryResult) One(valuePtr interface{}) error {
	// Read the bytes from the first row
	valueBytes := r.reader.NextRow()
	if valueBytes == nil {
		return ErrNoResult
	}

	// Skip through the remaining rows
	for r.reader.NextRow() != nil {
		// do nothing with the row
	}

	return json.Unmarshal(valueBytes, valuePtr)
}

// MetaData returns any meta-data that was available from this query.  Note that
// the meta-data will only be available once the object has been closed (either
// implicitly or explicitly).
func (r *QueryResult) MetaData() (*QueryMetaData, error) {
	metaDataBytes, err := r.reader.MetaData()
	if err != nil {
		return nil, err
	}

	var jsonResp jsonQueryResponse
	err = json.Unmarshal(metaDataBytes, &jsonResp)
	if err != nil {
		return nil, err
	}

	var metaData QueryMetaData
	err = metaData.fromData(jsonResp)
	if err != nil {
		return nil, err
	}

	return &metaData, nil
}

// Query executes the N1QL query statement on the server.
func (c *Cluster) Query(statement string, opts *QueryOptions) (*QueryResult, error) {
	if opts == nil {
		opts = &QueryOptions{}
	}

	span := c.sb.Tracer.StartSpan("Query", opts.parentSpan).
		SetTag("couchbase.service", "query")
	defer span.Finish()

	timeout := c.sb.QueryTimeout
	if opts.Timeout != 0 && opts.Timeout < timeout {
		timeout = opts.Timeout
	}
	deadline := time.Now().Add(timeout)

	retryStrategy := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryStrategy = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	queryOpts, err := opts.toMap()
	if err != nil {
		return nil, QueryError{
			InnerError:      wrapError(err, "failed to generate query options"),
			Statement:       statement,
			ClientContextID: opts.ClientContextID,
		}
	}

	queryOpts["statement"] = statement

	if !opts.AdHoc {
		return c.execPreparedN1qlQuery(span, queryOpts, deadline, retryStrategy)
	}

	return c.execN1qlQuery(span, queryOpts, deadline, retryStrategy)
}

func maybeGetQueryOption(options map[string]interface{}, name string) string {
	if value, ok := options[name].(string); ok {
		return value
	}
	return ""
}

func (c *Cluster) execN1qlQuery(
	span requestSpan,
	options map[string]interface{},
	deadline time.Time,
	retryStrategy *retryStrategyWrapper,
) (*QueryResult, error) {
	provider, err := c.getQueryProvider()
	if err != nil {
		return nil, QueryError{
			InnerError:      wrapError(err, "failed to get query provider"),
			Statement:       maybeGetQueryOption(options, "statement"),
			ClientContextID: maybeGetQueryOption(options, "client_context_id"),
		}
	}

	reqBytes, err := json.Marshal(options)
	if err != nil {
		return nil, QueryError{
			InnerError:      wrapError(err, "failed to marshall query body"),
			Statement:       maybeGetQueryOption(options, "statement"),
			ClientContextID: maybeGetQueryOption(options, "client_context_id"),
		}
	}

	res, err := provider.N1QLQuery(gocbcore.N1QLQueryOptions{
		Payload:       reqBytes,
		RetryStrategy: retryStrategy,
		Deadline:      deadline,
	})
	if err != nil {
		return nil, maybeEnhanceQueryError(err)
	}

	return newQueryResult(res)

}

func (c *Cluster) execPreparedN1qlQuery(
	span requestSpan,
	options map[string]interface{},
	deadline time.Time,
	retryStrategy *retryStrategyWrapper,
) (*QueryResult, error) {
	return c.execOldPreparedN1qlQuery(span, options, deadline, retryStrategy)
}

func (c *Cluster) execEnhPreparedN1qlQuery(
	span requestSpan,
	options map[string]interface{},
	deadline time.Time,
	retryStrategy *retryStrategyWrapper,
) (*QueryResult, error) {
	statement, stmtOk := options["statement"].(string)
	if !stmtOk {
		return nil, newCliInternalError("statement was not a string")
	}

	options["statement"] = "PREPARE " + statement
	options["auto_execute"] = true

	return c.execN1qlQuery(span, options, deadline, retryStrategy)
}

func (c *Cluster) execOldPreparedN1qlQuery(
	span requestSpan,
	options map[string]interface{},
	deadline time.Time,
	retryStrategy *retryStrategyWrapper,
) (*QueryResult, error) {
	statement, stmtOk := options["statement"].(string)
	if !stmtOk {
		return nil, newCliInternalError("statement was not a string")
	}

	c.clusterLock.RLock()
	cachedStmt := c.queryCache[statement]
	c.clusterLock.RUnlock()

	// Try to execute the cached query
	if cachedStmt != nil {
		// Attempt to execute our cached query plan
		delete(options, "statement")
		options["prepared"] = cachedStmt.name
		options["encoded_plan"] = cachedStmt.encodedPlan

		results, err := c.execN1qlQuery(span, options, deadline, retryStrategy)
		if err == nil {
			return results, nil
		}
	}

	// Try to prepare the query
	delete(options, "prepared")
	delete(options, "encoded_plan")
	delete(options, "auto_execute")
	options["statement"] = "PREPARE " + statement

	cacheRes, err := c.execN1qlQuery(span, options, deadline, retryStrategy)
	if err != nil {
		return nil, err
	}

	var prepData jsonQueryPrepData
	err = cacheRes.One(&prepData)
	if err != nil {
		return nil, err
	}

	cachedStmt = &queryCacheEntry{}
	cachedStmt.name = prepData.Name
	cachedStmt.encodedPlan = prepData.EncodedPlan

	c.clusterLock.Lock()
	c.queryCache[statement] = cachedStmt
	c.clusterLock.Unlock()

	// Attempt to execute our cached query plan
	delete(options, "statement")
	options["prepared"] = cachedStmt.name
	options["encoded_plan"] = cachedStmt.encodedPlan

	return c.execN1qlQuery(span, options, deadline, retryStrategy)
}
