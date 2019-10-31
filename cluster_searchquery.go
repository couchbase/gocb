package gocb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
	"github.com/couchbaselabs/jsonx"
	"github.com/pkg/errors"
)

// RowLocation holds the location of a hit in a list of search results.
type RowLocation struct {
	Position       int    `json:"position,omitempty"`
	Start          int    `json:"start,omitempty"`
	End            int    `json:"end,omitempty"`
	ArrayPositions []uint `json:"array_positions,omitempty"`
}

// SearchRow holds a single hit in a list of search results.
type SearchRow struct {
	Index       string                              `json:"index,omitempty"`
	ID          string                              `json:"id,omitempty"`
	Score       float64                             `json:"score,omitempty"`
	Explanation map[string]interface{}              `json:"explanation,omitempty"`
	Locations   map[string]map[string][]RowLocation `json:"locations,omitempty"`
	Fragments   map[string][]string                 `json:"fragments,omitempty"`

	fields     json.RawMessage
	serializer JSONSerializer
}

// TermFacetResult holds the results of a term facet in search results.
type TermFacetResult struct {
	Term  string `json:"term,omitempty"`
	Count int    `json:"count,omitempty"`
}

// NumericFacetResult holds the results of a numeric facet in search results.
type NumericFacetResult struct {
	Name  string  `json:"name,omitempty"`
	Min   float64 `json:"min,omitempty"`
	Max   float64 `json:"max,omitempty"`
	Count int     `json:"count,omitempty"`
}

// DateFacetResult holds the results of a date facet in search results.
type DateFacetResult struct {
	Name  string `json:"name,omitempty"`
	Min   string `json:"min,omitempty"`
	Max   string `json:"max,omitempty"`
	Count int    `json:"count,omitempty"`
}

// FacetResult holds the results of a specified facet in search results.
type FacetResult struct {
	Field         string               `json:"field,omitempty"`
	Total         int                  `json:"total,omitempty"`
	Missing       int                  `json:"missing,omitempty"`
	Other         int                  `json:"other,omitempty"`
	Terms         []TermFacetResult    `json:"terms,omitempty"`
	NumericRanges []NumericFacetResult `json:"numeric_ranges,omitempty"`
	DateRanges    []DateFacetResult    `json:"date_ranges,omitempty"`
}

// The response from the server can contain errors as either array or object so we use this as an intermediary
// between response and result.
type searchResponseStatus struct {
	Total      int         `json:"total,omitempty"`
	Failed     int         `json:"failed,omitempty"`
	Successful int         `json:"successful,omitempty"`
	Errors     interface{} `json:"errors,omitempty"`
}

// SearchStatus contains information about the status of the result.
type SearchStatus struct {
	status searchResponseStatus
}

// SearchMetrics contains metrics about the result.
type SearchMetrics struct {
	totalHits int
	took      uint
	maxScore  float64
}

// SearchMetadata provides access to the metadata properties of a search query result.
type SearchMetadata struct {
	status     searchResponseStatus
	metrics    SearchMetrics
	sourceAddr string
}

// SearchResult allows access to the results of a search query.
type SearchResult struct {
	metadata  SearchMetadata
	err       error
	facets    map[string]FacetResult
	startTime time.Time

	httpStatus   int
	streamResult *streamingResult
	cancel       context.CancelFunc
	ctx          context.Context

	serializer JSONSerializer
}

// Fields are any fields that were requested as a part of the search query.
func (row *SearchRow) Fields(valuePtr interface{}) error {
	if row.fields == nil {
		return errors.New("no fields to scan")
	}

	err := row.serializer.Deserialize(row.fields, valuePtr)
	if err != nil {
		return err
	}

	return nil
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *SearchResult) Next(rowPtr *SearchRow) bool {
	if r.err != nil {
		return false
	}

	row := r.NextBytes()
	if row == nil {
		return false
	}

	decoder := json.NewDecoder(bytes.NewBuffer(row))
	for decoder.More() {
		t, err := decoder.Token()
		if err != nil {
			r.err = err
			return false
		}

		if t == json.Delim('{') || t == json.Delim('}') {
			continue
		}

		switch t {
		case "index":
			r.err = decoder.Decode(&rowPtr.Index)
			if r.err != nil {
				return false
			}
		case "id":
			r.err = decoder.Decode(&rowPtr.ID)
			if r.err != nil {
				return false
			}
		case "score":
			r.err = decoder.Decode(&rowPtr.Score)
			if r.err != nil {
				return false
			}
		case "explanation":
			r.err = decoder.Decode(&rowPtr.Explanation)
			if r.err != nil {
				return false
			}
		case "locations":
			r.err = decoder.Decode(&rowPtr.Locations)
			if r.err != nil {
				return false
			}
		case "fragments":
			r.err = decoder.Decode(&rowPtr.Fragments)
			if r.err != nil {
				return false
			}
		case "fields":
			rowPtr.serializer = r.serializer
			r.err = decoder.Decode(&rowPtr.fields)
			if r.err != nil {
				return false
			}
		default:
			var ignore interface{}
			err := decoder.Decode(&ignore)
			if err != nil {
				return false
			}
		}
	}

	return true
}

// NextBytes returns the next result from the results as a byte array.
func (r *SearchResult) NextBytes() []byte {
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
func (r *SearchResult) Close() error {
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
			elapsed:   time.Now().Sub(r.startTime),
			remote:    r.metadata.sourceAddr,
			operation: "fts",
		}
	}
	if r.err != nil {
		return r.err
	}
	return err
}

// One assigns the first value from the results into the value pointer.
// It will close the results but not before iterating through all remaining
// results, as such this should only be used for very small resultsets - ideally
func (r *SearchResult) One(rowPtr *SearchRow) error {
	if !r.Next(rowPtr) {
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

// Facets contains the information relative to the facets requested in the search query.
func (r SearchResult) Facets() (map[string]FacetResult, error) {
	if !r.streamResult.Closed() {
		return nil, clientError{message: "result must be closed before accessing meta-data"}
	}

	return r.facets, nil
}

// Metadata returns metadata for this result.
func (r *SearchResult) Metadata() (*SearchMetadata, error) {
	if !r.streamResult.Closed() {
		return nil, clientError{message: "result must be closed before accessing meta-data"}
	}

	return &r.metadata, nil
}

// Status returns a SearchStatus containing information about the result.
func (r SearchMetadata) Status() SearchStatus {
	return SearchStatus{status: r.status}
}

// Metrics returns a SearchMetrics containing metrics about the result.
func (r SearchMetadata) Metrics() SearchMetrics {
	return r.metrics
}

// SuccessCount is the number of successes for the results.
func (r SearchStatus) SuccessCount() int {
	return r.status.Successful
}

// ErrorCount is the number of errors for the results.
func (r SearchStatus) ErrorCount() int {
	return r.status.Failed
}

// TotalCount is the total number of results returning, including errors.
func (r SearchStatus) TotalCount() int {
	return r.status.Total
}

// IsSuccess verifies whether or not the search request was successful.
func (r SearchStatus) IsSuccess() bool {
	return r.status.Failed == 0
}

// Took returns the time taken to execute the search.
func (r SearchMetrics) Took() time.Duration {
	return time.Duration(r.took) / time.Nanosecond
}

// MaxScore returns the highest score of all documents for this query.
func (r SearchMetrics) MaxScore() float64 {
	return r.maxScore
}

// TotalRows is the actual number of rows before the limit was applied.
func (r SearchMetrics) TotalRows() int {
	return r.totalHits
}

func (r *SearchResult) readAttribute(decoder *json.Decoder, t json.Token) (bool, error) {
	switch t {
	case "status":
		if r.httpStatus != 200 {
			// helpfully if the status code is not 200 then the status in the response body is a string not an object
			var ignore interface{}
			err := decoder.Decode(&ignore)
			if err != nil {
				return false, err
			}
			return false, nil
		}

		err := decoder.Decode(&r.metadata.status)
		if err != nil {
			return false, err
		}

		if r.metadata.status.Errors == nil {
			return false, nil
		}

		var statusErrors []string
		if statusError, ok := r.metadata.status.Errors.([]string); ok {
			statusErrors = statusError
		} else if statusError, ok := r.metadata.status.Errors.(map[string]interface{}); ok {
			for k, v := range statusError {
				msg, ok := v.(string)
				if !ok {
					return false, clientError{message: "could not parse errors"}
				}
				statusErrors = append(statusErrors, fmt.Sprintf("%s-%s", k, msg))
			}
		} else {
			return false, clientError{message: "could not parse errors"}
		}

		if len(statusErrors) > 0 {
			errs := make([]SearchError, len(statusErrors))
			for i, err := range statusErrors {
				errs[i] = searchError{
					message: err,
				}
			}
			r.err = searchMultiError{
				errors:     errs,
				endpoint:   r.metadata.sourceAddr,
				httpStatus: r.httpStatus,
			}
		}
	case "total_hits":
		err := decoder.Decode(&r.metadata.metrics.totalHits)
		if err != nil {
			return false, err
		}
	case "facets":
		err := decoder.Decode(&r.facets)
		if err != nil {
			return false, err
		}
	case "took":
		err := decoder.Decode(&r.metadata.metrics.took)
		if err != nil {
			return false, err
		}
	case "max_score":
		err := decoder.Decode(&r.metadata.metrics.maxScore)
		if err != nil {
			return false, err
		}
	case "hits":
		// read the opening [, this prevents the decoder from loading the entire results array into memory
		t, err := decoder.Token()
		if err != nil {
			return false, err
		}
		delim, ok := t.(json.Delim)
		if !ok {
			// hits can be null
			return false, nil
		}

		if delim != '[' {
			return false, errors.New("expected results opening token to be [ but was " + string(delim))
		}

		return true, nil
	default:
		var ignore interface{}
		err := decoder.Decode(&ignore)
		if err != nil {
			return false, err
		}
	}

	return false, nil
}

// SearchQuery performs a n1ql query and returns a list of rows or an error.
func (c *Cluster) SearchQuery(indexName string, q SearchQuery, opts *SearchOptions) (*SearchResult, error) {
	startTime := time.Now()
	if opts == nil {
		opts = &SearchOptions{}
	}

	span := c.sb.Tracer.StartSpan("SearchQuery", nil).SetTag("couchbase.service", "fts")
	defer span.Finish()

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	provider, err := c.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return c.searchQuery(ctx, span.Context(), indexName, q, opts, provider, startTime)
}

func (c *Cluster) searchQuery(ctx context.Context, tracectx requestSpanContext, qIndexName string, q interface{}, opts *SearchOptions,
	provider httpProvider, startTime time.Time) (*SearchResult, error) {

	optsData, err := opts.toOptionsData()
	if err != nil {
		return nil, err
	}

	espan := c.sb.Tracer.StartSpan("encode", tracectx)
	qBytes, err := json.Marshal(*optsData)
	espan.Finish()
	if err != nil {
		return nil, err
	}

	var queryData jsonx.DelayedObject
	err = json.Unmarshal(qBytes, &queryData)
	if err != nil {
		return nil, err
	}

	var ctlData jsonx.DelayedObject
	if queryData.Has("ctl") {
		err = queryData.Get("ctl", &ctlData)
		if err != nil {
			return nil, err
		}
	}

	timeout := c.sb.SearchTimeout
	if ctlData.Has("timeout") {
		err = ctlData.Get("timeout", &timeout)
		if err != nil {
			return nil, err
		}
	}
	// We don't make the client timeout longer for this as pindexes can timeout
	// individually rather than the entire connection. Server side timeouts are also hard to detect.
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeout*time.Millisecond)

	now := time.Now()
	d, _ := ctx.Deadline()
	opTimeout := jsonMillisecondDuration(d.Sub(now))

	err = ctlData.Set("timeout", opTimeout)
	if err != nil {
		cancel()
		return nil, err
	}

	err = queryData.Set("ctl", ctlData)
	if err != nil {
		cancel()
		return nil, err
	}

	err = queryData.Set("query", q)
	if err != nil {
		cancel()
		return nil, err
	}

	if opts.Serializer == nil {
		opts.Serializer = &DefaultJSONSerializer{}
	}

	wrapper := c.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		wrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	res, err := c.executeSearchQuery(ctx, tracectx, queryData, qIndexName, provider, cancel, opts.Serializer, wrapper, startTime)
	if err != nil {
		// only cancel on error, if we cancel when things have gone to plan then we'll prematurely close the stream
		if cancel != nil {
			cancel()
		}
		return nil, err
	}

	return res, nil
}

func (c *Cluster) executeSearchQuery(ctx context.Context, tracectx requestSpanContext, query jsonx.DelayedObject,
	qIndexName string, provider httpProvider, cancel context.CancelFunc, serializer JSONSerializer,
	wrapper *retryStrategyWrapper, startTime time.Time) (*SearchResult, error) {

	qBytes, err := json.Marshal(query)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse query options")
	}

	req := &gocbcore.HttpRequest{
		Service:       gocbcore.FtsService,
		Path:          fmt.Sprintf("/api/index/%s/query", qIndexName),
		Method:        "POST",
		Context:       ctx,
		Body:          qBytes,
		IsIdempotent:  true,
		RetryStrategy: wrapper,
	}

	for {
		dspan := c.sb.Tracer.StartSpan("dispatch", tracectx)
		resp, err := provider.DoHttpRequest(req)
		dspan.Finish()
		if err != nil {
			if err == gocbcore.ErrNoFtsService {
				return nil, serviceNotAvailableError{message: gocbcore.ErrNoFtsService.Error()}
			}

			if err == context.DeadlineExceeded {
				return nil, timeoutError{
					retryReasons:  req.RetryReasons(),
					retryAttempts: req.RetryAttempts(),
					elapsed:       time.Now().Sub(startTime),
					remote:        req.Endpoint,
					operation:     "fts",
				}
			}

			return nil, errors.Wrap(err, "could not complete search http request")
		}

		epInfo, err := url.Parse(resp.Endpoint)
		if err != nil {
			logWarnf("Failed to parse N1QL source address")
			epInfo = &url.URL{
				Host: "",
			}
		}

		switch resp.StatusCode {
		case 400:
			// This goes against the FTS RFC but makes a better experience in Go
			buf := new(bytes.Buffer)
			_, err := buf.ReadFrom(resp.Body)
			if err != nil {
				return nil, err
			}
			respErrs := []string{buf.String()}
			var errs []SearchError
			for _, err := range respErrs {
				errs = append(errs, searchError{
					message: err,
				})
			}
			return nil, searchMultiError{
				errors:     errs,
				endpoint:   epInfo.Host,
				httpStatus: resp.StatusCode,
			}
		case 401:
			// This goes against the FTS RFC but makes a better experience in Go
			return nil, searchMultiError{
				errors: []SearchError{
					searchError{
						message: "The requested consistency level could not be satisfied before the timeout was reached",
					},
				},
				endpoint:   epInfo.Host,
				httpStatus: resp.StatusCode,
			}
		case 429:
			shouldRetry, retryErr := shouldRetryHTTPRequest(ctx, req, gocbcore.ServiceResponseCodeIndicatedRetryReason,
				wrapper, provider, startTime)
			if shouldRetry {
				continue
			}

			if retryErr != nil {
				return nil, retryErr
			}

			// Drop out here and we'll create an error below.
		}

		if resp.StatusCode != 200 {
			errMsg := "An unknown error occurred"
			errBytes, bodyErr := ioutil.ReadAll(resp.Body)
			if bodyErr == nil {
				errMsg = string(errBytes)
			} else {
				logDebugf("Failed to ready message from body (%s)", bodyErr.Error())
			}

			err = searchMultiError{
				errors: []SearchError{
					searchError{
						message: errMsg,
					},
				},
				endpoint:   epInfo.Host,
				httpStatus: resp.StatusCode,
			}

			return nil, err
		}

		queryResults := &SearchResult{
			metadata: SearchMetadata{
				sourceAddr: epInfo.Host,
			},
			httpStatus: resp.StatusCode,
			serializer: serializer,
			startTime:  startTime,
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

			// We've already handled the retry case above.

			return nil, queryResults.err
		}
		return queryResults, nil
	}
}
