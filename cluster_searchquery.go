package gocb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/couchbase/gocbcore.v8"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"gopkg.in/couchbaselabs/jsonx.v1"
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

type searchResponse struct {
	Status    SearchResultStatus           `json:"status,omitempty"`
	Errors    []string                     `json:"errors,omitempty"`
	TotalHits int                          `json:"total_hits,omitempty"`
	Hits      []SearchResultHit            `json:"hits,omitempty"`
	Facets    map[string]SearchResultFacet `json:"facets,omitempty"`
	Took      uint                         `json:"took,omitempty"`
	MaxScore  float64                      `json:"max_score,omitempty"`
}

// SearchResults allows access to the results of a search query.
type SearchResults struct {
	data *searchResponse
}

// Status is the status information for the results.
func (r SearchResults) Status() SearchResultStatus {
	return r.data.Status
}

// TotalHits is the actual number of hits before the limit was applied.
func (r SearchResults) TotalHits() int {
	return r.data.TotalHits
}

// Hits are the matches for the search query.
func (r SearchResults) Hits() []SearchResultHit {
	return r.data.Hits
}

// Facets contains the information relative to the facets requested in the search query.
func (r SearchResults) Facets() map[string]SearchResultFacet {
	return r.data.Facets
}

// Took returns the time taken to execute the search.
func (r SearchResults) Took() time.Duration {
	return time.Duration(r.data.Took) / time.Nanosecond
}

// MaxScore returns the highest score of all documents for this query.
func (r SearchResults) MaxScore() float64 {
	return r.data.MaxScore
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
	// in a predictable manner.
	if !ok || now.Add(time.Duration(opTimeout)).Before(d) {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(opTimeout))
		defer cancel()
	} else {
		opTimeout = jsonMillisecondDuration(d.Sub(now))
	}

	err = ctlData.Set("timeout", opTimeout)
	if err != nil {
		return nil, err
	}

	err = queryData.Set("ctl", ctlData)
	if err != nil {
		return nil, err
	}

	dq, err := q.toSearchQueryData()
	if err != nil {
		return nil, err
	}

	err = queryData.Set("query", dq.Query)
	if err != nil {
		return nil, err
	}

	var retries uint
	for {
		retries++
		var res *SearchResults
		res, err = c.executeSearchQuery(ctx, traceCtx, queryData, qIndexName, provider)
		if err == nil {
			return res, err
		}

		if !isRetryableError(err) || c.sb.SearchRetryBehavior == nil || !c.sb.SearchRetryBehavior.CanRetry(retries) {
			return res, err
		}

		time.Sleep(c.sb.SearchRetryBehavior.NextInterval(retries))
	}
}

func (c *Cluster) executeSearchQuery(ctx context.Context, traceCtx opentracing.SpanContext, query jsonx.DelayedObject,
	qIndexName string, provider httpProvider) (*SearchResults, error) {

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

		if err == context.DeadlineExceeded {
			return nil, timeoutError{}
		} // TODO: test this...
		return nil, errors.Wrap(err, "could not complete query http request")
	}

	dtrace.Finish()

	strace := opentracing.GlobalTracer().StartSpan("streaming",
		opentracing.ChildOf(traceCtx))

	// TODO : Errors(). Partial search results.
	ftsResp := searchResponse{}
	errHandled := false
	switch resp.StatusCode {
	case 200:
		jsonDec := json.NewDecoder(resp.Body)
		err = jsonDec.Decode(&ftsResp)
		if err != nil {
			strace.Finish()
			return nil, errors.Wrap(err, "failed to decode query response body")
		}
	case 400:
		ftsResp.Status.Total = 1
		ftsResp.Status.Failed = 1
		buf := new(bytes.Buffer)
		_, err := buf.ReadFrom(resp.Body)
		if err != nil {
			strace.Finish()
			return nil, err
		}
		ftsResp.Errors = []string{buf.String()}
		errHandled = true
	case 401:
		ftsResp.Status.Total = 1
		ftsResp.Status.Failed = 1
		ftsResp.Errors = []string{"The requested consistency level could not be satisfied before the timeout was reached"}
		errHandled = true
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	strace.Finish()

	if resp.StatusCode != 200 && !errHandled {
		errOut := &httpError{
			statusCode: resp.StatusCode,
		}
		if resp.StatusCode == 429 {
			errOut.isRetryable = true
		}

		return nil, errOut
	}

	var multiErr searchMultiError
	if len(ftsResp.Errors) > 0 {
		errs := make([]SearchError, len(ftsResp.Errors))
		for i, e := range ftsResp.Errors {
			errs[i] = searchError{
				message: e,
			}
		}
		multiErr = searchMultiError{
			errors:     errs,
			endpoint:   resp.Endpoint,
			httpStatus: resp.StatusCode,
			// contextID:  resp.ClientContextID, TODO?
		}
		if ftsResp.Status.Failed != ftsResp.Status.Total {
			multiErr.partial = true
		}
	}

	return &SearchResults{
		data: &ftsResp,
	}, multiErr
}
