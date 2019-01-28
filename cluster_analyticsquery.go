package gocb

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"gopkg.in/couchbase/gocbcore.v8"
)

// AnalyticsWarning represents any warning generating during the execution of an Analytics query.
type AnalyticsWarning struct {
	Code    uint32 `json:"code"`
	Message string `json:"msg"`
}

type analyticsResponse struct {
	RequestID       string                   `json:"requestID"`
	ClientContextID string                   `json:"clientContextID"`
	Results         []json.RawMessage        `json:"results,omitempty"`
	Errors          []analyticsQueryError    `json:"errors,omitempty"`
	Warnings        []AnalyticsWarning       `json:"warnings,omitempty"`
	Status          string                   `json:"status,omitempty"`
	Signature       interface{}              `json:"signature,omitempty"`
	Metrics         analyticsResponseMetrics `json:"metrics,omitempty"`
	Handle          string                   `json:"handle,omitempty"`
}

type analyticsResponseMetrics struct {
	ElapsedTime      string `json:"elapsedTime"`
	ExecutionTime    string `json:"executionTime"`
	ResultCount      uint   `json:"resultCount"`
	ResultSize       uint   `json:"resultSize"`
	MutationCount    uint   `json:"mutationCount,omitempty"`
	SortCount        uint   `json:"sortCount,omitempty"`
	ErrorCount       uint   `json:"errorCount,omitempty"`
	WarningCount     uint   `json:"warningCount,omitempty"`
	ProcessedObjects uint   `json:"processedObjects,omitempty"`
}

type analyticsResponseHandle struct {
	Status string `json:"status,omitempty"`
	Handle string `json:"handle,omitempty"`
}

// AnalyticsResultMetrics encapsulates various metrics gathered during a queries execution.
type AnalyticsResultMetrics struct {
	ElapsedTime      time.Duration
	ExecutionTime    time.Duration
	ResultCount      uint
	ResultSize       uint
	MutationCount    uint
	SortCount        uint
	ErrorCount       uint
	WarningCount     uint
	ProcessedObjects uint
}

type analyticsRows struct {
	closed bool
	index  int
	rows   []json.RawMessage
}

// AnalyticsResults allows access to the results of a Analytics query.
type AnalyticsResults struct {
	rows            *analyticsRows
	err             error
	requestID       string
	clientContextID string
	status          string
	warnings        []AnalyticsWarning
	signature       interface{}
	metrics         AnalyticsResultMetrics
	handle          AnalyticsDeferredResultHandle
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *AnalyticsResults) Next(valuePtr interface{}) bool {
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
func (r *AnalyticsResults) NextBytes() []byte {
	if r.err != nil {
		return nil
	}

	return r.rows.NextBytes()
}

// Close marks the results as closed, returning any errors that occurred during reading the results.
func (r *AnalyticsResults) Close() error {
	r.rows.Close()
	return r.err
}

// One assigns the first value from the results into the value pointer.
func (r *AnalyticsResults) One(valuePtr interface{}) error {
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

// Warnings returns any warnings that occurred during query execution.
func (r *AnalyticsResults) Warnings() []AnalyticsWarning {
	return r.warnings
}

// Status returns the status for the results.
func (r *AnalyticsResults) Status() string {
	return r.status
}

// Signature returns TODO
func (r *AnalyticsResults) Signature() interface{} {
	return r.signature
}

// Metrics returns metrics about execution of this result.
func (r *AnalyticsResults) Metrics() AnalyticsResultMetrics {
	return r.metrics
}

// RequestID returns the request ID used for this query.
func (r *AnalyticsResults) RequestID() string {
	if !r.rows.closed {
		panic("Result must be closed before accessing meta-data")
	}

	return r.requestID
}

// ClientContextID returns the context ID used for this query.
func (r *AnalyticsResults) ClientContextID() string {
	if !r.rows.closed {
		panic("Result must be closed before accessing meta-data")
	}

	return r.clientContextID
}

// Handle returns a deferred result handle. This can be polled to verify whether
// the result is ready to be read.
//
// Experimental: This API is subject to change at any time.
func (r *AnalyticsResults) Handle() AnalyticsDeferredResultHandle {
	return r.handle
}

// NextBytes returns the next result from the rows as a byte array.
func (r *analyticsRows) NextBytes() []byte {
	if r.index+1 >= len(r.rows) {
		r.closed = true
		return nil
	}
	r.index++

	return r.rows[r.index]
}

// Close marks the rows as closed.
func (r *analyticsRows) Close() {
	r.closed = true
}

// AnalyticsQuery performs an analytics query and returns a list of rows or an error.
func (c *Cluster) AnalyticsQuery(statement string, opts *AnalyticsQueryOptions) (*AnalyticsResults, error) {
	if opts == nil {
		opts = &AnalyticsQueryOptions{}
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	var span opentracing.Span
	if opts.ParentSpanContext == nil {
		span = opentracing.GlobalTracer().StartSpan("ExecuteAnalyticsQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "cbas"})
	} else {
		span = opentracing.GlobalTracer().StartSpan("ExecuteAnalyticsQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "cbas"}, opentracing.ChildOf(opts.ParentSpanContext))
	}
	defer span.Finish()

	provider, err := c.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return c.analyticsQuery(ctx, span.Context(), statement, opts, provider)
}

func (c *Cluster) analyticsQuery(ctx context.Context, traceCtx opentracing.SpanContext, statement string, opts *AnalyticsQueryOptions,
	provider httpProvider) (resultsOut *AnalyticsResults, errOut error) {

	queryOpts, err := opts.toMap(statement)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse query options")
	}

	timeout := c.sb.AnalyticsTimeout
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

	if !ok || now.Add(timeout).Before(d) {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	} else {
		timeout = d.Sub(now)
	}

	queryOpts["timeout"] = timeout.String()

	// TODO: clientcontextid?

	var retries uint
	for {
		retries++
		var res *AnalyticsResults
		res, err = c.executeAnalyticsQuery(ctx, traceCtx, queryOpts, provider)
		if err == nil {
			return res, err
		}

		if !isRetryableError(err) || c.sb.AnalyticsRetryBehavior == nil || !c.sb.AnalyticsRetryBehavior.CanRetry(retries) {
			return res, err
		}

		time.Sleep(c.sb.AnalyticsRetryBehavior.NextInterval(retries))
	}
}

func (c *Cluster) executeAnalyticsQuery(ctx context.Context, traceCtx opentracing.SpanContext, opts map[string]interface{},
	provider httpProvider) (*AnalyticsResults, error) {

	// priority is sent as a header not in the body
	priority, priorityCastOK := opts["priority"].(int)
	if priorityCastOK {
		delete(opts, "priority")
	}

	reqJSON, err := json.Marshal(opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal query request body")
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.CbasService,
		Path:    "/analytics/service",
		Method:  "POST",
		Context: ctx,
		Body:    reqJSON,
	}

	if priorityCastOK {
		req.Headers = make(map[string]string)
		req.Headers["Analytics-Priority"] = strconv.Itoa(priority)
	}

	dtrace := opentracing.GlobalTracer().StartSpan("dispatch", opentracing.ChildOf(traceCtx))

	resp, err := provider.DoHttpRequest(req)
	if err != nil {
		dtrace.Finish()
		if err == gocbcore.ErrNoCbasService {
			return nil, serviceNotFoundError{}
		}

		if err == context.DeadlineExceeded {
			return nil, timeoutError{}
		}
		return nil, errors.Wrap(err, "could not complete query http request")
	}

	dtrace.Finish()

	strace := opentracing.GlobalTracer().StartSpan("streaming", opentracing.ChildOf(traceCtx))

	analyticsResp := analyticsResponse{}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&analyticsResp)
	if err != nil {
		strace.Finish()
		return nil, errors.Wrap(err, "failed to decode query response body")
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	strace.SetTag("couchbase.operation_id", analyticsResp.RequestID)
	strace.Finish()

	elapsedTime, err := time.ParseDuration(analyticsResp.Metrics.ElapsedTime)
	if err != nil {
		logDebugf("Failed to parse elapsed time duration (%s)", err)
	}

	executionTime, err := time.ParseDuration(analyticsResp.Metrics.ExecutionTime)
	if err != nil {
		logDebugf("Failed to parse execution time duration (%s)", err)
	}

	if len(analyticsResp.Errors) > 0 {
		errs := make([]AnalyticsQueryError, len(analyticsResp.Errors))
		for i, e := range analyticsResp.Errors {
			errs[i] = e
		}
		return nil, analyticsQueryMultiError{
			errors:     errs,
			endpoint:   resp.Endpoint,
			httpStatus: resp.StatusCode,
			contextID:  analyticsResp.ClientContextID,
		}
	}

	if resp.StatusCode != 200 {
		return nil, &httpError{
			statusCode: resp.StatusCode,
		}
	}

	return &AnalyticsResults{
		requestID:       analyticsResp.RequestID,
		clientContextID: analyticsResp.ClientContextID,
		rows: &analyticsRows{
			rows:  analyticsResp.Results,
			index: -1,
		},
		signature: analyticsResp.Signature,
		status:    analyticsResp.Status,
		warnings:  analyticsResp.Warnings,
		metrics: AnalyticsResultMetrics{
			ElapsedTime:      elapsedTime,
			ExecutionTime:    executionTime,
			ResultCount:      analyticsResp.Metrics.ResultCount,
			ResultSize:       analyticsResp.Metrics.ResultSize,
			MutationCount:    analyticsResp.Metrics.MutationCount,
			SortCount:        analyticsResp.Metrics.SortCount,
			ErrorCount:       analyticsResp.Metrics.ErrorCount,
			WarningCount:     analyticsResp.Metrics.WarningCount,
			ProcessedObjects: analyticsResp.Metrics.ProcessedObjects,
		},
		handle: &analyticsDeferredResultHandle{
			handleUri: analyticsResp.Handle,
			rows: &analyticsRows{
				index: -1,
			},
			status:   analyticsResp.Status,
			provider: provider,
		},
	}, nil
}
