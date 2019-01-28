package gocb

import (
	"context"
	"encoding/json"
	"net/url"
	"time"

	"github.com/pkg/errors"

	"github.com/opentracing/opentracing-go"
	"gopkg.in/couchbase/gocbcore.v8"
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

// QueryResults allows access to the results of a N1QL query.
type QueryResults struct {
	closed          bool
	index           int
	rows            []json.RawMessage
	err             error
	requestID       string
	clientContextID string
	metrics         QueryResultMetrics
	sourceAddr      string
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
	if r.err != nil {
		return nil
	}

	if r.index+1 >= len(r.rows) {
		r.closed = true
		return nil
	}
	r.index++

	return r.rows[r.index]
}

// Close marks the results as closed, returning any errors that occurred during reading the results.
func (r *QueryResults) Close() error {
	r.closed = true
	return r.err
}

// One assigns the first value from the results into the value pointer.
func (r *QueryResults) One(valuePtr interface{}) error {
	if !r.Next(valuePtr) {
		err := r.Close()
		if err != nil {
			return err
		}
		// return ErrNoResults TODO
	}

	// Ignore any errors occurring after we already have our result
	err := r.Close()
	if err != nil {
		// Return no error as we got the one result already.
		return nil
	}

	return nil
}

// SourceEndpoint returns the endpoint used for execution of this query.
// VOLATILE
func (r *QueryResults) SourceEndpoint() string {
	return r.sourceAddr
}

// RequestID returns the request ID used for this query.
func (r *QueryResults) RequestID() string {
	if !r.closed {
		panic("Result must be closed before accessing meta-data")
	}

	return r.requestID
}

// ClientContextID returns the context ID used for this query.
func (r *QueryResults) ClientContextID() string {
	if !r.closed {
		panic("Result must be closed before accessing meta-data")
	}

	return r.clientContextID
}

// Metrics returns metrics about execution of this result.
func (r *QueryResults) Metrics() QueryResultMetrics {
	if !r.closed {
		panic("Result must be closed before accessing meta-data")
	}

	return r.metrics
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
			res, err = c.executeN1qlQuery(ctx, traceCtx, queryOpts, provider)
		}
		if err == nil {
			return res, err
		}

		if !isRetryableError(err) || c.sb.N1qlRetryBehavior == nil || !c.sb.N1qlRetryBehavior.CanRetry(retries) {
			return res, err
		}

		time.Sleep(c.sb.N1qlRetryBehavior.NextInterval(retries))
	}
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

	req := &gocbcore.HttpRequest{
		Service: gocbcore.N1qlService,
		Path:    "/query/service",
		Method:  "POST",
		Context: ctx,
		Body:    reqJSON,
	}

	dtrace := opentracing.GlobalTracer().StartSpan("dispatch", opentracing.ChildOf(traceCtx))

	resp, err := provider.DoHttpRequest(req)
	if err != nil {
		dtrace.Finish()
		if err == gocbcore.ErrNoN1qlService {
			return nil, serviceNotFoundError{}
		}

		if err == context.DeadlineExceeded {
			return nil, timeoutError{}
		}
		return nil, errors.Wrap(err, "could not complete query http request")
	}

	dtrace.Finish()

	strace := opentracing.GlobalTracer().StartSpan("streaming", opentracing.ChildOf(traceCtx))

	n1qlResp := n1qlResponse{}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&n1qlResp)
	if err != nil {
		strace.Finish()
		return nil, errors.Wrap(err, "failed to decode query response body")
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	// TODO(brett19): place the server_duration in the right place...
	// srvDuration, _ := time.ParseDuration(n1qlResp.Metrics.ExecutionTime)
	// strace.SetTag("server_duration", srvDuration)

	strace.SetTag("couchbase.operation_id", n1qlResp.RequestID)
	strace.Finish()

	epInfo, err := url.Parse(resp.Endpoint)
	if err != nil {
		logWarnf("Failed to parse N1QL source address")
		epInfo = &url.URL{
			Host: "",
		}
	}

	if len(n1qlResp.Errors) > 0 {
		errs := make([]QueryError, len(n1qlResp.Errors))
		for i, e := range n1qlResp.Errors {
			errs[i] = e
		}
		return nil, queryMultiError{
			errors:     errs,
			endpoint:   epInfo.Host,
			httpStatus: resp.StatusCode,
			contextID:  n1qlResp.ClientContextID,
		}
	}

	if resp.StatusCode != 200 {
		return nil, &httpError{
			statusCode: resp.StatusCode,
		}
	}

	elapsedTime, err := time.ParseDuration(n1qlResp.Metrics.ElapsedTime)
	if err != nil {
		logDebugf("Failed to parse elapsed time duration (%s)", err)
	}

	executionTime, err := time.ParseDuration(n1qlResp.Metrics.ExecutionTime)
	if err != nil {
		logDebugf("Failed to parse execution time duration (%s)", err)
	}

	return &QueryResults{
		sourceAddr:      epInfo.Host,
		requestID:       n1qlResp.RequestID,
		clientContextID: n1qlResp.ClientContextID,
		index:           -1,
		rows:            n1qlResp.Results,
		metrics: QueryResultMetrics{
			ElapsedTime:   elapsedTime,
			ExecutionTime: executionTime,
			ResultCount:   n1qlResp.Metrics.ResultCount,
			ResultSize:    n1qlResp.Metrics.ResultSize,
			MutationCount: n1qlResp.Metrics.MutationCount,
			SortCount:     n1qlResp.Metrics.SortCount,
			ErrorCount:    n1qlResp.Metrics.ErrorCount,
			WarningCount:  n1qlResp.Metrics.WarningCount,
		},
	}, nil
}
