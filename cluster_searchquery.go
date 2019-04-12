package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
	"github.com/couchbaselabs/jsonx"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// SearchResultLocation holds the location of a hit in a list of search results.
type SearchResultLocation struct {
	Position       int    `json:"position,omitempty"`
	Start          int    `json:"start,omitempty"`
	End            int    `json:"end,omitempty"`
	ArrayPositions []uint `json:"array_positions,omitempty"`
}

// SearchResultHit holds a single hit in a list of search results.
type SearchResultHit struct {
	Index       string                                       `json:"index,omitempty"`
	Id          string                                       `json:"id,omitempty"`
	Score       float64                                      `json:"score,omitempty"`
	Explanation map[string]interface{}                       `json:"explanation,omitempty"`
	Locations   map[string]map[string][]SearchResultLocation `json:"locations,omitempty"`
	Fragments   map[string][]string                          `json:"fragments,omitempty"`
	Fields      map[string]interface{}                       `json:"fields,omitempty"`
}

// SearchResultTermFacet holds the results of a term facet in search results.
type SearchResultTermFacet struct {
	Term  string `json:"term,omitempty"`
	Count int    `json:"count,omitempty"`
}

// SearchResultNumericFacet holds the results of a numeric facet in search results.
type SearchResultNumericFacet struct {
	Name  string  `json:"name,omitempty"`
	Min   float64 `json:"min,omitempty"`
	Max   float64 `json:"max,omitempty"`
	Count int     `json:"count,omitempty"`
}

// SearchResultDateFacet holds the results of a date facet in search results.
type SearchResultDateFacet struct {
	Name  string `json:"name,omitempty"`
	Min   string `json:"min,omitempty"`
	Max   string `json:"max,omitempty"`
	Count int    `json:"count,omitempty"`
}

// SearchResultFacet holds the results of a specified facet in search results.
type SearchResultFacet struct {
	Field         string                     `json:"field,omitempty"`
	Total         int                        `json:"total,omitempty"`
	Missing       int                        `json:"missing,omitempty"`
	Other         int                        `json:"other,omitempty"`
	Terms         []SearchResultTermFacet    `json:"terms,omitempty"`
	NumericRanges []SearchResultNumericFacet `json:"numeric_ranges,omitempty"`
	DateRanges    []SearchResultDateFacet    `json:"date_ranges,omitempty"`
}

// SearchResultStatus holds the status information for an executed search query.
type SearchResultStatus struct {
	Total      int `json:"total,omitempty"`
	Failed     int `json:"failed,omitempty"`
	Successful int `json:"successful,omitempty"`
}

type searchResultStatus struct {
	Total      int      `json:"total,omitempty"`
	Failed     int      `json:"failed,omitempty"`
	Successful int      `json:"successful,omitempty"`
	Errors     []string `json:"errors,omitempty"`
}

// SearchResultsMetadata provides access to the metadata properties of a search query result.
type SearchResultsMetadata struct {
	status     SearchResultStatus
	totalHits  int
	took       uint
	maxScore   float64
	sourceAddr string
}

// SearchResults allows access to the results of a search query.
type SearchResults struct {
	metadata SearchResultsMetadata
	err      error
	facets   map[string]SearchResultFacet

	httpStatus   int
	streamResult *streamingResult
	strace       opentracing.Span
	cancel       context.CancelFunc
	ctx          context.Context
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *SearchResults) Next(hitPtr *SearchResultHit) bool {
	if r.err != nil {
		return false
	}

	row := r.NextBytes()
	if row == nil {
		return false
	}

	r.err = json.Unmarshal(row, hitPtr)
	if r.err != nil {
		return false
	}

	return true
}

// NextBytes returns the next result from the results as a byte array.
func (r *SearchResults) NextBytes() []byte {
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
func (r *SearchResults) Close() error {
	if r.streamResult.Closed() {
		return r.err
	}

	err := r.streamResult.Close()
	if r.strace != nil {
		r.strace.Finish()
	}
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
func (r *SearchResults) One(hitPtr *SearchResultHit) error {
	if !r.Next(hitPtr) {
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
func (r *SearchResults) Metadata() (*SearchResultsMetadata, error) {
	if !r.streamResult.Closed() {
		return nil, errors.New("result must be closed before accessing meta-data")
	}

	return &r.metadata, nil
}

// SuccessCount is the number of successes for the results.
func (r SearchResultsMetadata) SuccessCount() int {
	return r.status.Successful
}

// ErrorCount is the number of errors for the results.
func (r SearchResultsMetadata) ErrorCount() int {
	return r.status.Failed
}

// TotalHits is the actual number of hits before the limit was applied.
func (r SearchResultsMetadata) TotalHits() int {
	return r.totalHits
}

// Facets contains the information relative to the facets requested in the search query.
func (r SearchResults) Facets() (map[string]SearchResultFacet, error) {
	if !r.streamResult.Closed() {
		return nil, errors.New("result must be closed before accessing meta-data")
	}

	return r.facets, nil
}

// Took returns the time taken to execute the search.
func (r SearchResultsMetadata) Took() time.Duration {
	return time.Duration(r.took) / time.Nanosecond
}

// MaxScore returns the highest score of all documents for this query.
func (r SearchResultsMetadata) MaxScore() float64 {
	return r.maxScore
}

func (r *SearchResults) readAttribute(decoder *json.Decoder, t json.Token) (bool, error) {
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

		var status searchResultStatus
		err := decoder.Decode(&status)
		if err != nil {
			return false, err
		}

		r.metadata.status.Total = status.Total
		r.metadata.status.Successful = status.Successful
		r.metadata.status.Failed = status.Failed

		if len(status.Errors) > 0 {
			errs := make([]SearchError, len(status.Errors))
			for _, err := range status.Errors {
				errs = append(errs, searchError{
					message: err,
				})
			}
			r.err = searchMultiError{
				errors:     errs,
				endpoint:   r.metadata.sourceAddr,
				httpStatus: r.httpStatus,
			}
		}
	case "total_hits":
		err := decoder.Decode(&r.metadata.totalHits)
		if err != nil {
			return false, err
		}
	case "facets":
		err := decoder.Decode(&r.facets)
		if err != nil {
			return false, err
		}
	case "took":
		err := decoder.Decode(&r.metadata.took)
		if err != nil {
			return false, err
		}
	case "max_score":
		err := decoder.Decode(&r.metadata.maxScore)
		if err != nil {
			return false, err
		}
	case "errors":
		var respErrs []searchError
		err := decoder.Decode(&respErrs)
		if err != nil {
			return false, err
		}
		if len(respErrs) > 0 {
			errs := make([]SearchError, len(respErrs))
			for i, e := range respErrs {
				errs[i] = e
			}
			// this isn't an error that we want to bail on so store it and keep going
			r.err = searchMultiError{
				errors:     errs,
				endpoint:   r.metadata.sourceAddr,
				httpStatus: r.httpStatus,
			}
		}
	case "error":
		var sErr string
		err := decoder.Decode(&sErr)
		if err != nil {
			return false, err
		}
		r.err = searchMultiError{
			errors: []SearchError{
				searchError{
					message: sErr,
				},
			},
			endpoint:   r.metadata.sourceAddr,
			httpStatus: r.httpStatus,
		}
	case "hits":
		// read the opening [, this prevents the decoder from loading the entire results array into memory
		t, err := decoder.Token()
		if err != nil {
			return false, err
		}
		if delim, ok := t.(json.Delim); !ok || delim != '[' {
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
func (c *Cluster) SearchQuery(q SearchQuery, opts *SearchQueryOptions) (*SearchResults, error) {
	if opts == nil {
		opts = &SearchQueryOptions{}
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	var span opentracing.Span
	if opts.ParentSpanContext == nil {
		span = opentracing.GlobalTracer().StartSpan("ExecuteSearchQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "fts"})
	} else {
		span = opentracing.GlobalTracer().StartSpan("ExecuteSearchQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "fts"}, opentracing.ChildOf(opts.ParentSpanContext))
	}
	defer span.Finish()

	provider, err := c.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	return c.searchQuery(ctx, span.Context(), q, opts, provider)
}

func (c *Cluster) searchQuery(ctx context.Context, traceCtx opentracing.SpanContext, q SearchQuery, opts *SearchQueryOptions,
	provider httpProvider) (*SearchResults, error) {

	qIndexName := q.indexName()
	optsData, err := opts.toOptionsData()
	if err != nil {
		return nil, err
	}

	qBytes, err := json.Marshal(*optsData)
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
	opTimeout := jsonMillisecondDuration(timeout)
	if ctlData.Has("timeout") {
		err = ctlData.Get("timeout", &opTimeout)
		if err != nil {
			return nil, err
		}
		if opTimeout <= 0 || time.Duration(opTimeout) > timeout {
			opTimeout = jsonMillisecondDuration(timeout)
		}
	}

	now := time.Now()
	d, ok := ctx.Deadline()

	// If we don't need to then we don't touch the original ctx value so that the Done channel is set
	// in a predictable manner. We don't make the client timeout longer for this as pindexes can timeout
	// individually rather than the entire connection. Server side timeouts are also hard to detect.
	var cancel context.CancelFunc
	if !ok || now.Add(time.Duration(opTimeout)).Before(d) {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(opTimeout))
	} else {
		opTimeout = jsonMillisecondDuration(d.Sub(now))
	}

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

	dq, err := q.toSearchQueryData()
	if err != nil {
		cancel()
		return nil, err
	}

	err = queryData.Set("query", dq.Query)
	if err != nil {
		cancel()
		return nil, err
	}

	var retries uint
	var res *SearchResults
	for {
		retries++
		res, err = c.executeSearchQuery(ctx, traceCtx, queryData, qIndexName, provider, cancel)
		if err == nil {
			break
		}

		if !isRetryableError(err) || c.sb.SearchRetryBehavior == nil || !c.sb.SearchRetryBehavior.CanRetry(retries) {
			break
		}

		time.Sleep(c.sb.SearchRetryBehavior.NextInterval(retries))

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

func (c *Cluster) executeSearchQuery(ctx context.Context, traceCtx opentracing.SpanContext, query jsonx.DelayedObject,
	qIndexName string, provider httpProvider, cancel context.CancelFunc) (*SearchResults, error) {

	qBytes, err := json.Marshal(query)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse query options")
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.FtsService,
		Path:    fmt.Sprintf("/api/index/%s/query", qIndexName),
		Method:  "POST",
		Context: ctx,
		Body:    qBytes,
	}

	dtrace := opentracing.GlobalTracer().StartSpan("dispatch", opentracing.ChildOf(traceCtx))

	resp, err := provider.DoHttpRequest(req)
	if err != nil {
		dtrace.Finish()
		if err == gocbcore.ErrNoFtsService {
			return nil, serviceNotFoundError{}
		}

		// as we're effectively manually timing out the request using cancellation we need
		// to check if the original context has timed out as err itself will only show as canceled
		if ctx.Err() == context.DeadlineExceeded {
			return nil, timeoutError{}
		}
		return nil, errors.Wrap(err, "could not complete search http request")
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

	queryResults := &SearchResults{
		metadata: SearchResultsMetadata{
			sourceAddr: epInfo.Host,
		},
		httpStatus: resp.StatusCode,
	}

	errHandled := false
	switch resp.StatusCode {
	case 400:
		queryResults.metadata.status.Total = 1
		queryResults.metadata.status.Failed = 1
		errHandled = true
	case 401:
		queryResults.metadata.status.Total = 1
		queryResults.metadata.status.Failed = 1
		queryResults.err = searchMultiError{
			errors: []SearchError{
				searchError{
					message: "The requested consistency level could not be satisfied before the timeout was reached",
				},
			},
			endpoint:   epInfo.Host,
			httpStatus: resp.StatusCode,
		}
		errHandled = true
	}

	if resp.StatusCode != 200 && !errHandled {
		err = searchMultiError{
			errors: []SearchError{
				searchError{
					message: "An unknown error occurred",
				},
			},
			endpoint:   epInfo.Host,
			httpStatus: resp.StatusCode,
		}

		strace.Finish()
		return nil, err
	}

	streamResult, err := newStreamingResults(resp.Body, queryResults.readAttribute)
	if err != nil {
		strace.Finish()
		return nil, err
	}

	err = streamResult.readAttributes()
	if err != nil {
		bodyErr := streamResult.Close()
		if bodyErr != nil {
			logDebugf("Failed to close socket (%s)", bodyErr.Error())
		}
		strace.Finish()
		return nil, err
	}

	queryResults.streamResult = streamResult

	if streamResult.HasRows() {
		queryResults.strace = strace
		queryResults.cancel = cancel
		queryResults.ctx = ctx
	} else {
		bodyErr := streamResult.Close()
		if bodyErr != nil {
			logDebugf("Failed to close response body, %s", bodyErr.Error())
		}
		strace.Finish()

		// There are no rows and there are errors so fast fail
		if queryResults.err != nil {
			return nil, queryResults.err
		}
	}
	return queryResults, nil
}
