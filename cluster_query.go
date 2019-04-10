package gocb

import (
	"context"
	"encoding/json"
	"net/url"
	"time"

	"github.com/couchbase/gocbcore/v8"
	"github.com/pkg/errors"

	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
)

type n1qlCache struct {
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

// QueryResultMetrics encapsulates various metrics gathered during a queries execution.
type QueryResultMetrics struct {
	ElapsedTime   time.Duration
	ExecutionTime time.Duration
	ResultCount   uint
	ResultSize    uint
	MutationCount uint
	SortCount     uint
	ErrorCount    uint
	WarningCount  uint
}

// QueryResultsMetadata provides access to the metadata properties of a N1QL query result.
type QueryResultsMetadata struct {
	requestID       string
	clientContextID string
	metrics         QueryResultMetrics
	signature       interface{}
	warnings        []QueryWarning
	sourceAddr      string
}

// QueryResults allows access to the results of a N1QL query.
type QueryResults struct {
	metadata   QueryResultsMetadata
	err        error
	httpStatus int

	streamResult *streamingResult
	strace       opentracing.Span
	cancel       context.CancelFunc
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *QueryResults) Next(valuePtr interface{}) bool {
	if r.err != nil {
		return false
	}

	row := r.NextBytes()
	if row == nil {
		return false
	}

	r.err = json.Unmarshal(row, valuePtr)
	if r.err != nil {
		return false
	}

	return true
}

// NextBytes returns the next result from the results as a byte array.
func (r *QueryResults) NextBytes() []byte {
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
func (r *QueryResults) Close() error {
	if r.streamResult.Closed() {
		return r.err
	}

	err := r.streamResult.Close()
	if r.strace != nil {
		r.strace.Finish()
	}
	if r.cancel != nil {
		r.cancel()
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
func (r *QueryResults) One(valuePtr interface{}) error {
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
func (r *QueryResults) Metadata() (*QueryResultsMetadata, error) {
	if !r.streamResult.Closed() {
		return nil, errors.New("result must be closed before accessing meta-data")
	}

	return &r.metadata, nil
}

// SourceEndpoint returns the endpoint used for execution of this query.
// VOLATILE
func (r *QueryResultsMetadata) SourceEndpoint() string {
	return r.sourceAddr
}

// RequestID returns the request ID used for this query.
func (r *QueryResultsMetadata) RequestID() string {
	return r.requestID
}

// ClientContextID returns the context ID used for this query.
func (r *QueryResultsMetadata) ClientContextID() string {
	return r.clientContextID
}

// Metrics returns metrics about execution of this result.
func (r *QueryResultsMetadata) Metrics() QueryResultMetrics {
	return r.metrics
}

// Warnings returns any warnings that were generated during execution of the query.
func (r *QueryResultsMetadata) Warnings() []QueryWarning {
	return r.warnings
}

// Signature returns the schema of the results.
func (r *QueryResultsMetadata) Signature() interface{} {
	return r.signature
}

func (r *QueryResults) readAttribute(decoder *json.Decoder, t json.Token) (bool, error) {
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

		r.metadata.metrics = QueryResultMetrics{
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
			errs := make([]QueryError, len(respErrs))
			for i, e := range respErrs {
				errs[i] = e
			}
			// this isn't an error that we want to bail on so store it and keep going
			r.err = queryMultiError{
				errors:     errs,
				endpoint:   r.metadata.sourceAddr,
				httpStatus: r.httpStatus,
				contextID:  r.metadata.clientContextID,
			}
		}
	case "results":
		// read the opening [, this prevents the decoder from loading the entire results array into memory
		t, err := decoder.Token()
		if err != nil {
			return false, err
		}
		if delim, ok := t.(json.Delim); !ok || delim != '[' {
			return false, errors.New("expected results opening token to be [ but was " + string(delim))
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

// Query executes the N1QL query statement on the server n1qlEp.
// This function assumes that `opts` already contains all the required
// settings. This function will inject any additional connection or request-level
// settings into the `opts` map (currently this is only the timeout).
func (c *Cluster) Query(statement string, opts *QueryOptions) (*QueryResults, error) {
	if opts == nil {
		opts = &QueryOptions{}
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	var span opentracing.Span
	if opts.ParentSpanContext == nil {
		span = opentracing.GlobalTracer().StartSpan("ExecuteN1QLQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "n1ql"})
	} else {
		span = opentracing.GlobalTracer().StartSpan("ExecuteN1QLQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "n1ql"}, opentracing.ChildOf(opts.ParentSpanContext))
	}
	defer span.Finish()

	provider, err := c.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return c.query(ctx, span.Context(), statement, opts, provider)
}

func (c *Cluster) query(ctx context.Context, traceCtx opentracing.SpanContext, statement string, opts *QueryOptions,
	provider httpProvider) (*QueryResults, error) {

	queryOpts, err := opts.toMap(statement)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse query options")
	}

	_, castok := queryOpts["client_context_id"]
	if !castok {
		queryOpts["client_context_id"] = uuid.New()
	}

	// Work out which timeout to use, the cluster level default or query specific one
	timeout := c.sb.QueryTimeout
	var optTimeout time.Duration
	tmostr, castok := queryOpts["timeout"].(string)
	if castok {
		optTimeout, err = time.ParseDuration(tmostr)
		if err != nil {
			return nil, errors.Wrap(err, "could not parse timeout value")
		}
	}
	if optTimeout > 0 && optTimeout < timeout {
		timeout = optTimeout
	}

	now := time.Now()
	d, ok := ctx.Deadline()

	// If we don't need to then we don't touch the original ctx value so that the Done channel is set
	// in a predictable manner.
	if !ok || now.Add(timeout).Before(d) {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	} else {
		timeout = d.Sub(now)
	}

	queryOpts["timeout"] = timeout.String()

	var retries uint
	var res *QueryResults
	for {
		retries++
		if opts.Prepared {
			etrace := opentracing.GlobalTracer().StartSpan("execute", opentracing.ChildOf(traceCtx))
			res, err = c.doPreparedN1qlQuery(ctx, traceCtx, queryOpts, provider)
			etrace.Finish()
		} else {
			etrace := opentracing.GlobalTracer().StartSpan("execute", opentracing.ChildOf(traceCtx))
			res, err = c.executeN1qlQuery(ctx, traceCtx, queryOpts, provider)
			etrace.Finish()
		}
		if err != nil {
			break
		}

		if !res.streamResult.Closed() {
			// the stream is open so there's no way we have query errors already
			break
		}

		// There were no rows so it's likely there was an error
		resErr := res.err
		if resErr == nil {
			break
		}
		if !isRetryableError(resErr) || c.sb.N1qlRetryBehavior == nil || !c.sb.N1qlRetryBehavior.CanRetry(retries) {
			break
		}

		time.Sleep(c.sb.N1qlRetryBehavior.NextInterval(retries))

	}

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Cluster) doPreparedN1qlQuery(ctx context.Context, traceCtx opentracing.SpanContext, queryOpts map[string]interface{},
	provider httpProvider) (*QueryResults, error) {

	stmtStr, isStr := queryOpts["statement"].(string)
	if !isStr {
		return nil, configurationError{message: "query statement could not be parsed"}
	}

	c.clusterLock.RLock()
	cachedStmt := c.queryCache[stmtStr]
	c.clusterLock.RUnlock()

	if cachedStmt != nil {
		// Attempt to execute our cached query plan
		delete(queryOpts, "statement")
		queryOpts["prepared"] = cachedStmt.name
		queryOpts["encoded_plan"] = cachedStmt.encodedPlan

		etrace := opentracing.GlobalTracer().StartSpan("execute", opentracing.ChildOf(traceCtx))

		results, err := c.executeN1qlQuery(ctx, etrace.Context(), queryOpts, provider)
		if err == nil {
			etrace.Finish()
			return results, nil
		}

		etrace.Finish()

		// If we get error 4050, 4070 or 5000, we should attempt
		//   to re-prepare the statement immediately before failing.
		if !isRetryableError(err) {
			return nil, err
		}
	}

	// Prepare the query
	ptrace := opentracing.GlobalTracer().StartSpan("prepare", opentracing.ChildOf(traceCtx))

	var err error
	cachedStmt, err = c.prepareN1qlQuery(ctx, ptrace.Context(), queryOpts, provider)
	if err != nil {
		ptrace.Finish()
		return nil, err
	}

	ptrace.Finish()

	// Save new cached statement
	c.clusterLock.Lock()
	c.queryCache[stmtStr] = cachedStmt
	c.clusterLock.Unlock()

	// Update with new prepared data
	delete(queryOpts, "statement")
	queryOpts["prepared"] = cachedStmt.name
	queryOpts["encoded_plan"] = cachedStmt.encodedPlan

	etrace := opentracing.GlobalTracer().StartSpan("execute", opentracing.ChildOf(traceCtx))
	defer etrace.Finish()

	return c.executeN1qlQuery(ctx, etrace.Context(), queryOpts, provider)
}

func (c *Cluster) prepareN1qlQuery(ctx context.Context, traceCtx opentracing.SpanContext, opts map[string]interface{},
	provider httpProvider) (*n1qlCache, error) {

	prepOpts := make(map[string]interface{})
	for k, v := range opts {
		prepOpts[k] = v
	}
	prepOpts["statement"] = "PREPARE " + opts["statement"].(string)

	prepRes, err := c.executeN1qlQuery(ctx, traceCtx, prepOpts, provider)
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

type n1qlPrepData struct {
	EncodedPlan string `json:"encoded_plan"`
	Name        string `json:"name"`
}

func (c *Cluster) runContextTimeout(ctx context.Context, reqCancel context.CancelFunc, doneChan chan struct{}) {
	select {
	case <-ctx.Done():
		reqCancel()
		<-doneChan
	case <-doneChan:

	}
}

// Executes the N1QL query (in opts) on the server n1qlEp.
// This function assumes that `opts` already contains all the required
// settings. This function will inject any additional connection or request-level
// settings into the `opts` map.
func (c *Cluster) executeN1qlQuery(ctx context.Context, traceCtx opentracing.SpanContext, opts map[string]interface{},
	provider httpProvider) (*QueryResults, error) {

	reqJSON, err := json.Marshal(opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal query request body")
	}

	// we only want ctx to timeout the initial connection rather than the stream
	reqCtx, reqCancel := context.WithCancel(context.Background())
	doneChan := make(chan struct{})
	go c.runContextTimeout(ctx, reqCancel, doneChan)

	req := &gocbcore.HttpRequest{
		Service: gocbcore.N1qlService,
		Path:    "/query/service",
		Method:  "POST",
		Context: reqCtx,
		Body:    reqJSON,
	}

	dtrace := opentracing.GlobalTracer().StartSpan("dispatch", opentracing.ChildOf(traceCtx))

	resp, err := provider.DoHttpRequest(req)
	doneChan <- struct{}{}
	if err != nil {
		dtrace.Finish()
		if err == gocbcore.ErrNoN1qlService {
			return nil, serviceNotFoundError{}
		}

		// as we're effectively manually timing out the request using cancellation we need
		// to check if the original context has timed out as err itself will only show as canceled
		if ctx.Err() == context.DeadlineExceeded {
			return nil, timeoutError{}
		}
		return nil, errors.Wrap(err, "could not complete query http request")
	}

	dtrace.Finish()

	// TODO(brett19): place the server_duration in the right place...
	// srvDuration, _ := time.ParseDuration(n1qlResp.Metrics.ExecutionTime)
	// strace.SetTag("server_duration", srvDuration)

	epInfo, err := url.Parse(resp.Endpoint)
	if err != nil {
		logWarnf("Failed to parse N1QL source address")
		epInfo = &url.URL{
			Host: "",
		}
	}

	strace := opentracing.GlobalTracer().StartSpan("streaming", opentracing.ChildOf(traceCtx))

	queryResults := &QueryResults{
		metadata: QueryResultsMetadata{
			sourceAddr: epInfo.Host,
		},
		httpStatus: resp.StatusCode,
	}

	streamResult, err := newStreamingResults(resp.Body, queryResults.readAttribute)
	if err != nil {
		reqCancel()
		strace.Finish()
		return nil, err
	}

	err = streamResult.readAttributes()
	if err != nil {
		bodyErr := streamResult.Close()
		if bodyErr != nil {
			logDebugf("Failed to close socket (%s)", bodyErr.Error())
		}
		reqCancel()
		strace.Finish()
		return nil, err
	}

	queryResults.streamResult = streamResult

	strace.SetTag("couchbase.operation_id", queryResults.metadata.requestID)

	if streamResult.HasRows() {
		queryResults.strace = strace
		queryResults.cancel = reqCancel
	} else {
		bodyErr := streamResult.Close()
		if bodyErr != nil {
			logDebugf("Failed to close response body, %s", bodyErr.Error())
		}
		reqCancel()
		strace.Finish()
	}
	return queryResults, nil
}
