package gocb

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"

	gocbcore "github.com/couchbase/gocbcore/v8"
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

// AnalyticsMetrics encapsulates various metrics gathered during a queries execution.
type AnalyticsMetrics struct {
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

// AnalyticsMetadata provides access to the metadata properties of an Analytics query result.
type AnalyticsMetadata struct {
	requestID       string
	clientContextID string
	status          string
	warnings        []AnalyticsWarning
	signature       interface{}
	metrics         AnalyticsMetrics
	sourceAddr      string
}

// AnalyticsResult allows access to the results of an Analytics query.
type AnalyticsResult struct {
	metadata   AnalyticsMetadata
	err        error
	httpStatus int
	startTime  time.Time

	streamResult *streamingResult
	cancel       context.CancelFunc
	httpProvider httpProvider
	ctx          context.Context

	serializer JSONSerializer
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *AnalyticsResult) Next(valuePtr interface{}) bool {
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
func (r *AnalyticsResult) NextBytes() []byte {
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
func (r *AnalyticsResult) Close() error {
	if r.streamResult.Closed() {
		return r.err
	}

	err := r.streamResult.Close()
	ctxErr := r.ctx.Err()
	if r.cancel != nil {
		r.cancel()
	}
	if ctxErr == context.DeadlineExceeded {
		return timeoutError{
			operationID: r.metadata.clientContextID,
			elapsed:     time.Now().Sub(r.startTime),
			remote:      r.metadata.sourceAddr,
			operation:   "cbas",
		}
	}
	if r.err != nil {
		if qErr, ok := r.err.(AnalyticsQueryError); ok {
			if qErr.Code() == 21002 {
				return timeoutError{
					operationID: r.metadata.clientContextID,
					elapsed:     time.Now().Sub(r.startTime),
					remote:      r.metadata.sourceAddr,
					operation:   "cbas",
				}
			}
		}

		return r.err
	}
	return err
}

// One assigns the first value from the results into the value pointer.
// It will close the results but not before iterating through all remaining
// results, as such this should only be used for very small resultsets - ideally
// of, at most, length 1.
func (r *AnalyticsResult) One(valuePtr interface{}) error {
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
func (r *AnalyticsResult) Metadata() (*AnalyticsMetadata, error) {
	if !r.streamResult.Closed() {
		return nil, clientError{message: "result must be closed before accessing meta-data"}
	}

	return &r.metadata, nil
}

// Warnings returns any warnings that occurred during query execution.
func (r *AnalyticsMetadata) Warnings() []AnalyticsWarning {
	return r.warnings
}

// Status returns the status for the results.
func (r *AnalyticsMetadata) Status() string {
	return r.status
}

// Signature returns TODO
func (r *AnalyticsMetadata) Signature() interface{} {
	return r.signature
}

// Metrics returns metrics about execution of this result.
func (r *AnalyticsMetadata) Metrics() AnalyticsMetrics {
	return r.metrics
}

// RequestID returns the request ID used for this query.
func (r *AnalyticsMetadata) RequestID() string {
	return r.requestID
}

// ClientContextID returns the context ID used for this query.
func (r *AnalyticsMetadata) ClientContextID() string {
	return r.clientContextID
}

func (r *AnalyticsResult) readAttribute(decoder *json.Decoder, t json.Token) (bool, error) {
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

		r.metadata.metrics = AnalyticsMetrics{
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
			return false, clientError{message: "expected results opening token to be [ but was " + t.(string)}
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
func (c *Cluster) AnalyticsQuery(statement string, opts *AnalyticsOptions) (*AnalyticsResult, error) {
	if opts == nil {
		opts = &AnalyticsOptions{}
	}

	provider, err := c.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return c.analyticsQuery(statement, opts, provider)
}

func (c *Cluster) analyticsQuery(statement string, opts *AnalyticsOptions,
	provider httpProvider) (*AnalyticsResult, error) {

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

	if opts.Context == nil {
		opts.Context = context.Background()
	}

	ctx, cancel := context.WithTimeout(opts.Context, timeout)

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

	retryWrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		retryWrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	res, err := c.executeAnalyticsQuery(ctx, queryOpts, provider, cancel, opts.ReadOnly, opts.Serializer, retryWrapper)
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
	provider httpProvider, cancel context.CancelFunc, idempotent bool, serializer JSONSerializer,
	retryWrapper *retryStrategyWrapper) (*AnalyticsResult, error) {
	startTime := time.Now()
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
		Service:       gocbcore.CbasService,
		Path:          "/analytics/service",
		Method:        "POST",
		Context:       ctx,
		Body:          reqJSON,
		IsIdempotent:  idempotent,
		RetryStrategy: retryWrapper,
	}

	contextID, ok := opts["client_context_id"].(string)
	if ok {
		req.UniqueId = contextID
	} else {
		req.UniqueId = uuid.New().String()
		logWarnf("Failed to assert analytics options client_context_id to string. Replacing with %s", req.UniqueId)
	}

	if priorityCastOK {
		req.Headers = make(map[string]string)
		req.Headers["Analytics-Priority"] = strconv.Itoa(priority)
	}

	for {
		resp, err := provider.DoHttpRequest(req)
		if err != nil {
			if err == gocbcore.ErrNoCbasService {
				return nil, serviceNotAvailableError{message: gocbcore.ErrNoCbasService.Error()}
			}

			if err == context.DeadlineExceeded {
				return nil, timeoutError{
					operationID:   req.Identifier(),
					retryReasons:  req.RetryReasons(),
					retryAttempts: req.RetryAttempts(),
					elapsed:       time.Now().Sub(startTime),
					remote:        req.Endpoint,
					operation:     "cbas",
				}
			}

			return nil, err
		}

		epInfo, err := url.Parse(resp.Endpoint)
		if err != nil {
			logWarnf("Failed to parse N1QL source address")
			epInfo = &url.URL{
				Host: "",
			}
		}

		results := &AnalyticsResult{
			metadata: AnalyticsMetadata{
				sourceAddr: epInfo.Host,
			},
			httpStatus:   resp.StatusCode,
			httpProvider: provider,
			serializer:   serializer,
			startTime:    startTime,
		}

		streamResult, err := newStreamingResults(resp.Body, results.readAttribute)
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

		results.streamResult = streamResult

		if streamResult.HasRows() {
			results.cancel = cancel
			results.ctx = ctx
		} else {
			bodyErr := streamResult.Close()
			if bodyErr != nil {
				logDebugf("Failed to close response body, %s", bodyErr.Error())
			}

			// If this isn't retryable then return immediately, otherwise attempt a retry. If that fails then return
			// immediately.
			if IsRetryableError(results.err) {
				shouldRetry, retryErr := shouldRetryHTTPRequest(ctx, req, gocbcore.ServiceResponseCodeIndicatedRetryReason,
					retryWrapper, provider, startTime)
				if shouldRetry {
					continue
				}

				if retryErr != nil {
					return nil, retryErr
				}
			}

			// If there are no rows then must be an error but we'll just make sure that users can't ever
			// end up in a state where result and error is nil.
			if results.err == nil {
				return nil, errors.New("Unknown error")
			}
			return nil, results.err
		}

		return results, nil
	}
}

func shouldRetryHTTPRequest(ctx context.Context, req *gocbcore.HttpRequest, reason gocbcore.RetryReason,
	retryWrapper *retryStrategyWrapper, provider httpProvider, startTime time.Time) (bool, error) {
	waitCh := make(chan struct{})
	retried := provider.MaybeRetryRequest(req, reason, retryWrapper, func() {
		waitCh <- struct{}{}
	})
	if retried {
		select {
		case <-waitCh:
			return true, nil
		case <-ctx.Done():
			if req.CancelRetry() {
				// Read the channel so that we don't leave it hanging
				<-waitCh
			}

			if ctx.Err() == context.DeadlineExceeded {
				return false, timeoutError{
					operationID:   req.Identifier(),
					retryReasons:  req.RetryReasons(),
					retryAttempts: req.RetryAttempts(),
					elapsed:       time.Now().Sub(startTime),
					remote:        req.Endpoint,
					operation:     "cbas",
				}
			}

			return false, ctx.Err()
		}
	}
	return false, nil
}
