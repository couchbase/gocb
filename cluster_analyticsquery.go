package gocb

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"time"

	"github.com/couchbase/gocbcore/v8"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
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

// AnalyticsResults allows access to the results of a Analytics query.
type AnalyticsResults struct {
	err             error
	requestID       string
	clientContextID string
	status          string
	warnings        []AnalyticsWarning
	signature       interface{}
	metrics         AnalyticsResultMetrics
	handle          *AnalyticsDeferredResultHandle
	sourceAddr      string
	httpStatus      int

	streamResult *streamingResult
	cancel       context.CancelFunc
	strace       opentracing.Span
	httpProvider httpProvider
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
func (r *AnalyticsResults) Close() error {
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
func (r *AnalyticsResults) One(valuePtr interface{}) error {
	if !r.Next(valuePtr) {
		err := r.Close()
		if err != nil {
			return err
		}
		return ErrNoResults // TODO
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

// Warnings returns any warnings that occurred during query execution.
func (r *AnalyticsResults) Warnings() []AnalyticsWarning {
	if !r.streamResult.Closed() {
		panic("Result must be closed before accessing meta-data")
	}

	return r.warnings
}

// Status returns the status for the results.
func (r *AnalyticsResults) Status() string {
	if !r.streamResult.Closed() {
		panic("Result must be closed before accessing meta-data")
	}

	return r.status
}

// Signature returns TODO
func (r *AnalyticsResults) Signature() interface{} {
	if !r.streamResult.Closed() {
		panic("Result must be closed before accessing meta-data")
	}

	return r.signature
}

// Metrics returns metrics about execution of this result.
func (r *AnalyticsResults) Metrics() AnalyticsResultMetrics {
	if !r.streamResult.Closed() {
		panic("Result must be closed before accessing meta-data")
	}

	return r.metrics
}

// RequestID returns the request ID used for this query.
func (r *AnalyticsResults) RequestID() string {
	if !r.streamResult.Closed() {
		panic("Result must be closed before accessing meta-data")
	}

	return r.requestID
}

// ClientContextID returns the context ID used for this query.
func (r *AnalyticsResults) ClientContextID() string {
	if !r.streamResult.Closed() {
		panic("Result must be closed before accessing meta-data")
	}

	return r.clientContextID
}

// Handle returns a deferred result handle. This can be polled to verify whether
// the result is ready to be read.
//
// Experimental: This API is subject to change at any time.
func (r *AnalyticsResults) Handle() *AnalyticsDeferredResultHandle {
	return r.handle
}

func (r *AnalyticsResults) readAttribute(decoder *json.Decoder, t json.Token) (bool, error) {
	switch t {
	case "requestID":
		err := decoder.Decode(&r.requestID)
		if err != nil {
			return false, err
		}
	case "clientContextID":
		err := decoder.Decode(&r.clientContextID)
		if err != nil {
			return false, err
		}
	case "metrics":
		var metrics analyticsResponseMetrics
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

		r.metrics = AnalyticsResultMetrics{
			ElapsedTime:      elapsedTime,
			ExecutionTime:    executionTime,
			ResultCount:      metrics.ResultCount,
			ResultSize:       metrics.ResultSize,
			MutationCount:    metrics.MutationCount,
			SortCount:        metrics.SortCount,
			ErrorCount:       metrics.ErrorCount,
			WarningCount:     metrics.WarningCount,
			ProcessedObjects: metrics.ProcessedObjects,
		}
	case "errors":
		var respErrs []analyticsQueryError
		err := decoder.Decode(&respErrs)
		if err != nil {
			return false, err
		}
		if len(respErrs) > 0 {
			errs := make([]AnalyticsQueryError, len(respErrs))
			for i, e := range respErrs {
				errs[i] = e
			}
			// this isn't an error that we want to bail on so store it and keep going
			r.err = analyticsQueryMultiError{
				errors:     errs,
				endpoint:   r.sourceAddr,
				httpStatus: 200, // this can only be 200, for now at least
				contextID:  r.clientContextID,
			}
		}
	case "results":
		// read the opening [, this prevents the decoder from loading the entire results array into memory
		t, err := decoder.Token()
		if err != nil {
			return false, err
		}
		if delim, ok := t.(json.Delim); !ok || delim != '[' {
			return false, errors.New("expected results opening token to be [ but was " + t.(string))
		}

		return true, nil
	case "warnings":
		err := decoder.Decode(&r.warnings)
		if err != nil {
			return false, err
		}
	case "signature":
		err := decoder.Decode(&r.signature)
		if err != nil {
			return false, err
		}
	case "status":
		err := decoder.Decode(&r.status)
		if err != nil {
			return false, err
		}
	case "handle":
		var handle string
		err := decoder.Decode(&handle)
		if err != nil {
			return false, err
		}
		r.handle = &AnalyticsDeferredResultHandle{
			handleUri: handle,
			provider:  r.httpProvider,
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
	provider httpProvider) (*AnalyticsResults, error) {

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
	var res *AnalyticsResults
	for {
		retries++
		etrace := opentracing.GlobalTracer().StartSpan("execute", opentracing.ChildOf(traceCtx))
		res, err = c.executeAnalyticsQuery(ctx, traceCtx, queryOpts, provider)
		etrace.Finish()
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
		if !isRetryableError(resErr) || c.sb.AnalyticsRetryBehavior == nil || !c.sb.AnalyticsRetryBehavior.CanRetry(retries) {
			break
		}

		time.Sleep(c.sb.AnalyticsRetryBehavior.NextInterval(retries))
	}

	if err != nil {
		return nil, err
	}

	return res, nil
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

	// we only want ctx to timeout the initial connection rather than the stream
	reqCtx, reqCancel := context.WithCancel(context.Background())
	doneChan := make(chan struct{})
	go c.runContextTimeout(ctx, reqCancel, doneChan)

	req := &gocbcore.HttpRequest{
		Service: gocbcore.CbasService,
		Path:    "/analytics/service",
		Method:  "POST",
		Context: reqCtx,
		Body:    reqJSON,
	}

	if priorityCastOK {
		req.Headers = make(map[string]string)
		req.Headers["Analytics-Priority"] = strconv.Itoa(priority)
	}

	dtrace := opentracing.GlobalTracer().StartSpan("dispatch", opentracing.ChildOf(traceCtx))

	resp, err := provider.DoHttpRequest(req)
	doneChan <- struct{}{}
	if err != nil {
		dtrace.Finish()
		if err == gocbcore.ErrNoCbasService {
			return nil, serviceNotFoundError{}
		}

		// as we're effectively manually timing out the request using cancellation we need
		// to check if the original context has timed out as err itself will only show as canceled
		if ctx.Err() == context.DeadlineExceeded {
			return nil, timeoutError{}
		}
		return nil, errors.Wrap(err, "could not complete analytics http request")
	}

	dtrace.Finish()

	epInfo, err := url.Parse(resp.Endpoint)
	if err != nil {
		logWarnf("Failed to parse N1QL source address")
		epInfo = &url.URL{
			Host: "",
		}
	}

	strace := opentracing.GlobalTracer().StartSpan("streaming", opentracing.ChildOf(traceCtx))

	queryResults := &AnalyticsResults{
		sourceAddr:   epInfo.Host,
		httpStatus:   resp.StatusCode,
		httpProvider: provider,
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

	strace.SetTag("couchbase.operation_id", queryResults.requestID)

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
