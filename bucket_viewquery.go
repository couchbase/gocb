package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/pkg/errors"

	"github.com/couchbase/gocbcore/v8"
	"github.com/opentracing/opentracing-go"
)

type viewResponse struct {
	TotalRows int               `json:"total_rows,omitempty"`
	Rows      []json.RawMessage `json:"rows,omitempty"`
	Error     string            `json:"error,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	Errors    []viewError       `json:"errors,omitempty"`
}

// ViewResultsMetadata provides access to the metadata properties of a view query result.
type ViewResultsMetadata struct {
	totalRows int
}

// ViewResults implements an iterator interface which can be used to iterate over the rows of the query results.
type ViewResults struct {
	metadata   ViewResultsMetadata
	errReason  string
	errMessage string

	cancel       context.CancelFunc
	streamResult *streamingResult
	strace       opentracing.Span
	err          error
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *ViewResults) Next(valuePtr interface{}) bool {
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
func (r *ViewResults) NextBytes() []byte {
	if r.streamResult == nil || r.streamResult.Closed() {
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
func (r *ViewResults) Close() error {
	if r.streamResult == nil || r.streamResult.Closed() {
		return r.makeError()
	}

	err := r.streamResult.Close()
	if r.strace != nil {
		r.strace.Finish()
	}
	if r.cancel != nil {
		r.cancel()
	}
	if vErr := r.makeError(); vErr != nil {
		return vErr
	}
	return err
}

func (r *ViewResults) makeError() error {
	if r.errReason != "" || r.errMessage != "" {
		err := viewError{
			ErrorMessage: r.errMessage,
			ErrorReason:  r.errReason,
		}
		return viewMultiError{
			errors: []ViewQueryError{err},
		}
	}

	return r.err
}

func (r *ViewResults) readAttribute(decoder *json.Decoder, t json.Token) (bool, error) {
	switch t {
	case "total_rows":
		err := decoder.Decode(&r.metadata.totalRows)
		if err != nil {
			return false, err
		}
	case "error":
		err := decoder.Decode(&r.errMessage)
		if err != nil {
			return false, err
		}
	case "reason":
		err := decoder.Decode(&r.errReason)
		if err != nil {
			return false, err
		}
	case "errors":
		var respErrs []viewError
		err := decoder.Decode(&respErrs)
		if err != nil {
			return false, err
		}
		if len(respErrs) > 0 {
			errs := make([]ViewQueryError, len(respErrs))
			for i, e := range errs {
				errs[i] = e
			}
			endErrs := viewMultiError{
				errors: errs,
			}

			r.err = endErrs
		}
	case "rows":
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

// One assigns the first value from the results into the value pointer.
// It will close the results but not before iterating through all remaining
// results, as such this should only be used for very small resultsets - ideally
// of, at most, length 1.
func (r *ViewResults) One(valuePtr interface{}) error {
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
func (r *ViewResults) Metadata() (*ViewResultsMetadata, error) {
	if r.streamResult != nil && !r.streamResult.Closed() {
		return nil, errors.New("result must be closed before accessing meta-data")
	}

	return &r.metadata, nil
}

// TotalRows returns the total number of rows in the view, can be greater than the number of rows returned.
func (r *ViewResultsMetadata) TotalRows() int {
	return r.totalRows
}

func (b *Bucket) runContextTimeout(ctx context.Context, reqCancel context.CancelFunc, doneChan chan struct{}) {
	select {
	case <-ctx.Done():
		reqCancel()
		<-doneChan
	case <-doneChan:

	}
}

// ViewQuery performs a view query and returns a list of rows or an error.
func (b *Bucket) ViewQuery(designDoc string, viewName string, opts *ViewOptions) (*ViewResults, error) {
	if opts == nil {
		opts = &ViewOptions{}
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	var span opentracing.Span
	if opts.ParentSpanContext == nil {
		span = opentracing.GlobalTracer().StartSpan("ExecuteViewQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "views"})
	} else {
		span = opentracing.GlobalTracer().StartSpan("ExecuteViewQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "views"}, opentracing.ChildOf(opts.ParentSpanContext))
	}
	defer span.Finish()

	cli := b.sb.getCachedClient()
	provider, err := cli.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	designDoc = b.maybePrefixDevDocument(opts.Development, designDoc)

	urlValues, err := opts.toURLValues()
	if err != nil {
		return nil, errors.Wrap(err, "could not parse query options")
	}

	return b.executeViewQuery(ctx, span.Context(), "_view", designDoc, viewName, *urlValues, provider)
}

// SpatialViewQuery performs a spatial query and returns a list of rows or an error.
func (b *Bucket) SpatialViewQuery(designDoc string, viewName string, opts *SpatialViewOptions) (*ViewResults, error) {
	if opts == nil {
		opts = &SpatialViewOptions{}
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	var span opentracing.Span
	if opts.ParentSpanContext == nil {
		span = opentracing.GlobalTracer().StartSpan("ExecuteSpatialQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "views"})
	} else {
		span = opentracing.GlobalTracer().StartSpan("ExecuteSpatialQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "views"}, opentracing.ChildOf(opts.ParentSpanContext))
	}
	defer span.Finish()

	cli := b.sb.getCachedClient()
	provider, err := cli.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	designDoc = b.maybePrefixDevDocument(opts.Development, designDoc)

	urlValues, err := opts.toURLValues()
	if err != nil {
		return nil, errors.Wrap(err, "could not parse query options")
	}

	return b.executeViewQuery(ctx, span.Context(), "_spatial", designDoc, viewName, *urlValues, provider)
}

func (b *Bucket) executeViewQuery(ctx context.Context, traceCtx opentracing.SpanContext, viewType, ddoc, viewName string,
	options url.Values, provider httpProvider) (*ViewResults, error) {

	// we only want ctx to timeout the initial connection rather than the stream
	reqCtx, reqCancel := context.WithCancel(context.Background())
	doneChan := make(chan struct{})
	go b.runContextTimeout(ctx, reqCancel, doneChan)

	reqUri := fmt.Sprintf("/_design/%s/%s/%s?%s", ddoc, viewType, viewName, options.Encode())
	req := &gocbcore.HttpRequest{
		Service: gocbcore.CapiService,
		Path:    reqUri,
		Method:  "GET",
		Context: reqCtx,
	}

	dtrace := opentracing.GlobalTracer().StartSpan("dispatch", opentracing.ChildOf(traceCtx))

	resp, err := provider.DoHttpRequest(req)
	if err != nil {
		dtrace.Finish()
		if err == gocbcore.ErrNoCapiService {
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

	queryResults := &ViewResults{}

	strace := opentracing.GlobalTracer().StartSpan("streaming", opentracing.ChildOf(traceCtx))

	if resp.StatusCode == 500 {
		// We have to handle the views 500 case as a special case because the body can be of form [] or {}
		defer reqCancel()
		defer strace.Finish()
		defer func() {
			err := resp.Body.Close()
			if err != nil {
				logDebugf("Failed to close socket (%s)", err.Error())
			}
		}()

		decoder := json.NewDecoder(resp.Body)
		t, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return nil, errors.New("could not read response body, no data found")
		}
		if delim == '[' {
			errMsg, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			queryResults.err = viewMultiError{
				errors: []ViewQueryError{
					viewError{
						ErrorMessage: errMsg.(string),
						ErrorReason:  fmt.Sprintf("%d", resp.StatusCode),
					},
				},
				httpStatus: resp.StatusCode,
				endpoint:   resp.Endpoint,
			}
		} else if t == '{' {
			queryResults.streamResult = &streamingResult{
				decoder:     decoder,
				stream:      resp.Body,
				attributeCb: queryResults.readAttribute,
			}
		}

		return queryResults, nil
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

func (b *Bucket) maybePrefixDevDocument(val bool, ddoc string) string {
	designDoc := ddoc
	if val {
		if !strings.HasPrefix(ddoc, "dev_") {
			designDoc = "dev_" + ddoc
		}
	} else {
		designDoc = strings.TrimPrefix(ddoc, "dev_")
	}

	return designDoc
}
