package gocb

import (
	"context"
	"encoding/json"
	"net/url"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
	"github.com/pkg/errors"
)

type n1qlCache struct {
	enhanced    bool
	name        string
	encodedPlan string
}

type n1qlResponseMetrics struct {
	ElapsedTime   string `json:"elapsedTime"`
	ExecutionTime string `json:"executionTime"`
	ResultCount   uint   `json:"resultCount"`
	ResultSize    uint   `json:"resultSize"`
	MutationCount uint   `json:"mutationCount,omitempty"`
	SortCount     uint   `json:"sortCount,omitempty"`
	ErrorCount    uint   `json:"errorCount,omitempty"`
	WarningCount  uint   `json:"warningCount,omitempty"`
}

type n1qlResponse struct {
	RequestID       string              `json:"requestID"`
	ClientContextID string              `json:"clientContextID"`
	Results         []json.RawMessage   `json:"results,omitempty"`
	Errors          []queryError        `json:"errors,omitempty"`
	Status          string              `json:"status"`
	Metrics         n1qlResponseMetrics `json:"metrics"`
	Warnings        []QueryWarning      `json:"warnings"`
}

// QueryWarning is the representation of any warnings that occurred during query execution.
type QueryWarning struct {
	Code    uint32 `json:"code"`
	Message string `json:"msg"`
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

// QueryMetadata provides access to the metadata properties of a N1QL query result.
type QueryMetadata struct {
	requestID       string
	clientContextID string
	metrics         *QueryMetrics
	signature       interface{}
	warnings        []QueryWarning
	sourceAddr      string
	profile         interface{}
}

// QueryResult allows access to the results of a N1QL query.
type QueryResult struct {
	metadata     QueryMetadata
	preparedName string
	err          error
	httpStatus   int

	streamResult       *streamingResult
	cancel             context.CancelFunc
	ctx                context.Context
	enhancedStatements bool

	serializer JSONSerializer
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *QueryResult) Next(valuePtr interface{}) bool {
	if r.err != nil {
		return false
	}

	row := r.NextBytes()
	if row == nil {
		return false
	}

	r.err = r.serializer.Deserialize(row, valuePtr)
	if r.err != nil {
		return false
	}

	return true
}

// NextBytes returns the next result from the results as a byte array.
func (r *QueryResult) NextBytes() []byte {
	if r.streamResult.Closed() {
		return nil
	}

	raw, err := r.streamResult.NextBytes()
	if err != nil {
		r.err = err
		return nil
	}

	return raw
}

// Close marks the results as closed, returning any errors that occurred during reading the results.
func (r *QueryResult) Close() error {
	if r.streamResult.Closed() {
		return r.err
	}

	err := r.streamResult.Close()
	ctxErr := r.ctx.Err()
	if r.cancel != nil {
		r.cancel()
	}
	if ctxErr == context.DeadlineExceeded {
		return timeoutError{}
	}
	if r.err != nil {
		return r.err
	}
	return err
}

// One assigns the first value from the results into the value pointer.
// It will close the results but not before iterating through all remaining
// results, as such this should only be used for very small resultsets - ideally
// of, at most, length 1.
func (r *QueryResult) One(valuePtr interface{}) error {
	if !r.Next(valuePtr) {
		err := r.Close()
		if err != nil {
			return err
		}
		return noResultsError{}
	}

	// We have to purge the remaining rows in order to get to the remaining
	// response attributes
	for r.NextBytes() != nil {
	}

	err := r.Close()
	if err != nil {
		return err
	}

	return nil
}

// Metadata returns metadata for this result.
func (r *QueryResult) Metadata() (*QueryMetadata, error) {
	if !r.streamResult.Closed() {
		return nil, clientError{message: "result must be closed before accessing meta-data"}
	}

	return &r.metadata, nil
}

// RequestID returns the request ID used for this query.
func (r *QueryMetadata) RequestID() string {
	return r.requestID
}

// Profile returns the profile generated for this query.
func (r *QueryMetadata) Profile() interface{} {
	return r.profile
}

// ClientContextID returns the context ID used for this query.
func (r *QueryMetadata) ClientContextID() string {
	return r.clientContextID
}

// Metrics returns metrics about execution of this result.
func (r *QueryMetadata) Metrics() *QueryMetrics {
	return r.metrics
}

// Warnings returns any warnings that were generated during execution of the query.
func (r *QueryMetadata) Warnings() []QueryWarning {
	return r.warnings
}

// Signature returns the schema of the results.
func (r *QueryMetadata) Signature() interface{} {
	return r.signature
}

func (r *QueryResult) readAttribute(decoder *json.Decoder, t json.Token) (bool, error) {
	switch t {
	case "requestID":
		err := decoder.Decode(&r.metadata.requestID)
		if err != nil {
			return false, err
		}
	case "clientContextID":
		err := decoder.Decode(&r.metadata.clientContextID)
		if err != nil {
			return false, err
		}
	case "prepared":
		err := decoder.Decode(&r.preparedName)
		if err != nil {
			return false, err
		}
	case "metrics":
		var metrics n1qlResponseMetrics
		err := decoder.Decode(&metrics)
		if err != nil {
			return false, err
		}
		elapsedTime, err := time.ParseDuration(metrics.ElapsedTime)
		if err != nil {
			logDebugf("Failed to parse elapsed time duration (%s)", err)
		}

		executionTime, err := time.ParseDuration(metrics.ExecutionTime)
		if err != nil {
			logDebugf("Failed to parse execution time duration (%s)", err)
		}

		r.metadata.metrics = &QueryMetrics{
			ElapsedTime:   elapsedTime,
			ExecutionTime: executionTime,
			ResultCount:   metrics.ResultCount,
			ResultSize:    metrics.ResultSize,
			MutationCount: metrics.MutationCount,
			SortCount:     metrics.SortCount,
			ErrorCount:    metrics.ErrorCount,
			WarningCount:  metrics.WarningCount,
		}
	case "errors":
		var respErrs []queryError
		err := decoder.Decode(&respErrs)
		if err != nil {
			return false, err
		}
		if len(respErrs) > 0 {
			// this isn't an error that we want to bail on so store it and keep going
			respErr := respErrs[0]
			respErr.enhancedStmtSupported = r.enhancedStatements
			respErr.endpoint = r.metadata.sourceAddr
			respErr.httpStatus = r.httpStatus
			respErr.contextID = r.metadata.clientContextID
			r.err = respErr
		}
	case "results":
		// read the opening [, this prevents the decoder from loading the entire results array into memory
		t, err := decoder.Token()
		if err != nil {
			return false, err
		}
		if delim, ok := t.(json.Delim); !ok || delim != '[' {
			return false, clientError{message: "expected results opening token to be [ but was " + string(delim)}
		}

		return true, nil
	case "warnings":
		err := decoder.Decode(&r.metadata.warnings)
		if err != nil {
			return false, err
		}
	case "signature":
		err := decoder.Decode(&r.metadata.signature)
		if err != nil {
			return false, err
		}
	case "profile":
		err := decoder.Decode(&r.metadata.profile)
		if err != nil {
			return false, err
		}
	default:
		var ignore interface{}
		err := decoder.Decode(&ignore)
		if err != nil {
			return false, err
		}
	}

	return false, nil
}

type httpProvider interface {
	DoHttpRequest(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error)
}

type clusterCapabilityProvider interface {
	SupportsClusterCapability(capability gocbcore.ClusterCapability) bool
}

type doQueryFn func(ctx context.Context, opts map[string]interface{},
	provider httpProvider, cancel context.CancelFunc) (*QueryResult, error)

// Query executes the N1QL query statement on the server n1qlEp.
// This function assumes that `opts` already contains all the required
// settings. This function will inject any additional connection or request-level
// settings into the `opts` map (currently this is only the timeout).
func (c *Cluster) Query(statement string, opts *QueryOptions) (*QueryResult, error) {
	if opts == nil {
		opts = &QueryOptions{}
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	provider, err := c.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return c.query(ctx, statement, opts, provider)
}

func (c *Cluster) query(ctx context.Context, statement string, opts *QueryOptions,
	provider httpProvider) (*QueryResult, error) {

	queryOpts, err := opts.toMap(statement)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse query options")
	}

	// Work out which timeout to use, the cluster level default or query specific one
	timeout := c.sb.QueryTimeout
	tmostr, castok := queryOpts["timeout"].(string)
	if castok {
		timeout, err = time.ParseDuration(tmostr)
		if err != nil {
			return nil, errors.Wrap(err, "could not parse timeout value")
		}
	}

	if ctx == nil {
		ctx = context.Background()
	}

	// We need to try to create the context with timeout + 1 second so that the server closes the connection rather
	// than us. This is just a better user experience.
	timeoutPlusBuffer := timeout + time.Second
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeoutPlusBuffer)

	now := time.Now()
	d, _ := ctx.Deadline()
	newTimeout := d.Sub(now)

	// We need to take the shorter of the timeouts here so that the server can try to timeout first, if the context
	// already had a shorter deadline then there's not much we can do about it.
	if newTimeout > timeout {
		queryOpts["timeout"] = timeout.String()
	} else {
		queryOpts["timeout"] = newTimeout.String()
	}

	if opts.Serializer == nil {
		opts.Serializer = c.sb.Serializer
	}

	var res *QueryResult
	if opts.AdHoc {
		res, err = c.doPreparedN1qlQuery(ctx, queryOpts, provider, cancel, opts.Serializer)
	} else {
		res, err = c.doRetryableQuery(ctx, queryOpts, provider, cancel, opts.Serializer)
	}

	if err != nil {
		// only cancel on error, if we cancel when things have gone to plan then we'll prematurely close the stream
		if cancel != nil {
			cancel()
		}
		return nil, err
	}

	return res, nil
}

func (c *Cluster) doPreparedN1qlQuery(ctx context.Context, queryOpts map[string]interface{},
	provider httpProvider, cancel context.CancelFunc, serializer JSONSerializer) (*QueryResult, error) {
	if capabilitySupporter, ok := provider.(clusterCapabilityProvider); ok {
		if !c.supportsEnhancedPreparedStatements() &&
			capabilitySupporter.SupportsClusterCapability(gocbcore.ClusterCapabilityEnhancedPreparedStatements) {
			c.setSupportsEnhancedPreparedStatements(true)
			c.clusterLock.Lock()
			c.queryCache = make(map[string]*n1qlCache)
			c.clusterLock.Unlock()
		}
	}

	stmtStr, isStr := queryOpts["statement"].(string)
	if !isStr {
		return nil, invalidArgumentsError{message: "query statement could not be parsed"}
	}

	c.clusterLock.RLock()
	cachedStmt := c.queryCache[stmtStr]
	c.clusterLock.RUnlock()

	if cachedStmt != nil {
		// Attempt to execute our cached query plan
		delete(queryOpts, "statement")
		queryOpts["prepared"] = cachedStmt.name
		if !cachedStmt.enhanced {
			queryOpts["encoded_plan"] = cachedStmt.encodedPlan
		}

		results, err := c.doRetryableQuery(ctx, queryOpts, provider, cancel, serializer)
		if err == nil {
			return results, nil
		}

		// If we get error 4050, 4070 or 5000, we should attempt
		//   to re-prepare the statement immediately before failing.
		if !IsRetryableError(err) {
			return nil, err
		}
	}

	// Prepare the query
	if c.supportsEnhancedPreparedStatements() {
		results, err := c.prepareEnhancedN1qlQuery(ctx, queryOpts, provider, cancel, serializer)
		if err != nil {
			return nil, err
		}

		c.clusterLock.Lock()
		c.queryCache[stmtStr] = &n1qlCache{enhanced: true, name: results.preparedName}
		c.clusterLock.Unlock()

		return results, nil
	}

	var err error
	cachedStmt, err = c.prepareN1qlQuery(ctx, queryOpts, provider)
	if err != nil {
		return nil, err
	}

	// Save new cached statement
	c.clusterLock.Lock()
	c.queryCache[stmtStr] = cachedStmt
	c.clusterLock.Unlock()

	// Update with new prepared data
	delete(queryOpts, "statement")
	queryOpts["prepared"] = cachedStmt.name
	queryOpts["encoded_plan"] = cachedStmt.encodedPlan

	return c.doRetryableQuery(ctx, queryOpts, provider, cancel, serializer)
}

func (c *Cluster) prepareEnhancedN1qlQuery(ctx context.Context, opts map[string]interface{},
	provider httpProvider, cancel context.CancelFunc, serializer JSONSerializer) (*QueryResult, error) {

	prepOpts := make(map[string]interface{})
	for k, v := range opts {
		prepOpts[k] = v
	}
	prepOpts["statement"] = "PREPARE " + opts["statement"].(string)
	prepOpts["auto_execute"] = true

	return c.doRetryableQuery(ctx, prepOpts, provider, cancel, serializer)
}

func (c *Cluster) prepareN1qlQuery(ctx context.Context, opts map[string]interface{},
	provider httpProvider) (*n1qlCache, error) {

	prepOpts := make(map[string]interface{})
	for k, v := range opts {
		prepOpts[k] = v
	}
	prepOpts["statement"] = "PREPARE " + opts["statement"].(string)

	// There's no need to pass cancel here, if there's an error then we'll cancel further up the stack
	// and if there isn't then we run another query later where we will cancel
	prepRes, err := c.doRetryableQuery(ctx, prepOpts, provider, nil, &DefaultJSONSerializer{})
	if err != nil {
		return nil, err
	}

	var preped n1qlPrepData
	err = prepRes.One(&preped)
	if err != nil {
		return nil, err
	}

	return &n1qlCache{
		name:        preped.Name,
		encodedPlan: preped.EncodedPlan,
	}, nil
}

func (c *Cluster) doRetryableQuery(ctx context.Context, queryOpts map[string]interface{},
	provider httpProvider, cancel context.CancelFunc, serializer JSONSerializer) (*QueryResult, error) {
	var res *QueryResult
	var err error
	var retries uint
	var endpoint string
	enhancedStatements := c.supportsEnhancedPreparedStatements()
	for {
		retries++
		res, err = c.executeN1qlQuery(ctx, queryOpts, provider, cancel, endpoint, serializer)
		if err == nil {
			break
		}

		if !IsRetryableError(err) || c.sb.N1qlRetryBehavior == nil || !c.sb.N1qlRetryBehavior.CanRetry(retries) {
			break
		}

		if enhancedStatements {
			qErr, ok := err.(QueryError)
			if ok {
				endpoint = qErr.Endpoint()
			}
		}

		time.Sleep(c.sb.N1qlRetryBehavior.NextInterval(retries))
	}

	return res, err
}

type n1qlPrepData struct {
	EncodedPlan string `json:"encoded_plan"`
	Name        string `json:"name"`
}

// Executes the N1QL query (in opts) on the server n1qlEp.
// This function assumes that `opts` already contains all the required
// settings. This function will inject any additional connection or request-level
// settings into the `opts` map.
func (c *Cluster) executeN1qlQuery(ctx context.Context, opts map[string]interface{},
	provider httpProvider, cancel context.CancelFunc, endpoint string, serializer JSONSerializer) (*QueryResult, error) {
	reqJSON, err := json.Marshal(opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal query request body")
	}

	req := &gocbcore.HttpRequest{
		Service:  gocbcore.N1qlService,
		Path:     "/query/service",
		Method:   "POST",
		Context:  ctx,
		Body:     reqJSON,
		Endpoint: endpoint,
	}

	resp, err := provider.DoHttpRequest(req)
	if err != nil {
		if err == gocbcore.ErrNoN1qlService {
			return nil, serviceNotAvailableError{message: gocbcore.ErrNoN1qlService.Error()}
		}

		// as we're effectively manually timing out the request using cancellation we need
		// to check if the original context has timed out as err itself will only show as canceled
		if ctx.Err() == context.DeadlineExceeded {
			return nil, timeoutError{}
		}
		return nil, errors.Wrap(err, "could not complete query http request")
	}

	epInfo, err := url.Parse(resp.Endpoint)
	if err != nil {
		logWarnf("Failed to parse N1QL source address")
		epInfo = &url.URL{
			Host: "",
		}
	}

	queryResults := &QueryResult{
		metadata: QueryMetadata{
			sourceAddr: epInfo.Host,
		},
		httpStatus:         resp.StatusCode,
		serializer:         serializer,
		enhancedStatements: c.supportsEnhancedPreparedStatements(),
	}

	streamResult, err := newStreamingResults(resp.Body, queryResults.readAttribute)
	if err != nil {
		return nil, err
	}

	err = streamResult.readAttributes()
	if err != nil {
		bodyErr := streamResult.Close()
		if bodyErr != nil {
			logDebugf("Failed to close socket (%s)", bodyErr.Error())
		}
		return nil, err
	}

	queryResults.streamResult = streamResult

	if streamResult.HasRows() {
		queryResults.cancel = cancel
		queryResults.ctx = ctx
	} else {
		bodyErr := streamResult.Close()
		if bodyErr != nil {
			logDebugf("Failed to close response body, %s", bodyErr.Error())
		}

		// There are no rows and there are errors so fast fail
		if queryResults.err != nil {
			return nil, queryResults.err
		}
	}
	return queryResults, nil
}
