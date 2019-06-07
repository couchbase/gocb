package gocb

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"time"

	"github.com/couchbase/gocbcore/v8"
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

// AnalyticsResultsMetadata provides access to the metadata properties of an Analytics query result.
type AnalyticsResultsMetadata struct {
	requestID       string
	clientContextID string
	status          string
	warnings        []AnalyticsWarning
	signature       interface{}
	metrics         AnalyticsResultMetrics
	sourceAddr      string
}

// AnalyticsResults allows access to the results of an Analytics query.
type AnalyticsResults struct {
	metadata   AnalyticsResultsMetadata
	err        error
	httpStatus int

	handle       *AnalyticsDeferredResultHandle
	streamResult *streamingResult
	cancel       context.CancelFunc
	httpProvider httpProvider
	ctx          context.Context

	serializer JSONSerializer
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
func (r *AnalyticsResults) One(valuePtr interface{}) error {
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
func (r *AnalyticsResults) Metadata() (*AnalyticsResultsMetadata, error) {
	if !r.streamResult.Closed() {
		return nil, errors.New("result must be closed before accessing meta-data")
	}

	return &r.metadata, nil
}

// Warnings returns any warnings that occurred during query execution.
func (r *AnalyticsResultsMetadata) Warnings() []AnalyticsWarning {
	return r.warnings
}

// Status returns the status for the results.
func (r *AnalyticsResultsMetadata) Status() string {
	return r.status
}

// Signature returns TODO
func (r *AnalyticsResultsMetadata) Signature() interface{} {
	return r.signature
}

// Metrics returns metrics about execution of this result.
func (r *AnalyticsResultsMetadata) Metrics() AnalyticsResultMetrics {
	return r.metrics
}

// RequestID returns the request ID used for this query.
func (r *AnalyticsResultsMetadata) RequestID() string {
	return r.requestID
}

// ClientContextID returns the context ID used for this query.
func (r *AnalyticsResultsMetadata) ClientContextID() string {
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

		r.metadata.metrics = AnalyticsResultMetrics{
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
			// this isn't an error that we want to bail on so store it and keep going
			respErr := respErrs[0]
			respErr.endpoint = r.metadata.sourceAddr
			respErr.httpStatus = 200
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
			return false, errors.New("expected results opening token to be [ but was " + t.(string))
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
	case "status":
		err := decoder.Decode(&r.metadata.status)
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

	provider, err := c.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return c.analyticsQuery(ctx, statement, opts, provider)
}

func (c *Cluster) analyticsQuery(ctx context.Context, statement string, opts *AnalyticsQueryOptions,
	provider httpProvider) (*AnalyticsResults, error) {

	queryOpts, err := opts.toMap(statement)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse query options")
	}

	timeout := c.sb.AnalyticsTimeout
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

	// TODO: clientcontextid?

	if opts.Serializer == nil {
		opts.Serializer = c.sb.Serializer
	}

	var retries uint
	var res *AnalyticsResults
	for {
		retries++
		res, err = c.executeAnalyticsQuery(ctx, queryOpts, provider, cancel, opts.Serializer)
		if err == nil {
			break
		}

		if !IsRetryableError(err) || c.sb.AnalyticsRetryBehavior == nil || !c.sb.AnalyticsRetryBehavior.CanRetry(retries) {
			break
		}

		time.Sleep(c.sb.AnalyticsRetryBehavior.NextInterval(retries))
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

func (c *Cluster) executeAnalyticsQuery(ctx context.Context, opts map[string]interface{},
	provider httpProvider, cancel context.CancelFunc, serializer JSONSerializer) (*AnalyticsResults, error) {
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

	resp, err := provider.DoHttpRequest(req)
	if err != nil {
		if err == gocbcore.ErrNoCbasService {
			return nil, serviceNotAvailableError{message: gocbcore.ErrNoCbasService.Error()}
		}

		// as we're effectively manually timing out the request using cancellation we need
		// to check if the original context has timed out as err itself will only show as canceled
		if ctx.Err() == context.DeadlineExceeded {
			return nil, timeoutError{}
		}
		return nil, errors.Wrap(err, "could not complete analytics http request")
	}

	epInfo, err := url.Parse(resp.Endpoint)
	if err != nil {
		logWarnf("Failed to parse N1QL source address")
		epInfo = &url.URL{
			Host: "",
		}
	}

	queryResults := &AnalyticsResults{
		metadata: AnalyticsResultsMetadata{
			sourceAddr: epInfo.Host,
		},
		httpStatus:   resp.StatusCode,
		httpProvider: provider,
		serializer:   serializer,
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
