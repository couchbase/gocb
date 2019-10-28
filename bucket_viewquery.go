package gocb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
	"github.com/pkg/errors"
)

type viewResponse struct {
	TotalRows int               `json:"total_rows,omitempty"`
	Rows      []json.RawMessage `json:"rows,omitempty"`
	Error     string            `json:"error,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	Errors    []viewError       `json:"errors,omitempty"`
}

// ViewRow provides access to a single view query row.
type ViewRow struct {
	ID    string
	key   json.RawMessage
	value json.RawMessage

	serializer JSONSerializer
}

// ViewMetadata provides access to the metadata properties of a view query result.
type ViewMetadata struct {
	totalRows int
	debug     interface{}
}

// ViewResult implements an iterator interface which can be used to iterate over the rows of the query results.
type ViewResult struct {
	metadata   ViewMetadata
	errReason  string
	errMessage string
	startTime  time.Time
	endpoint   string

	ctx          context.Context
	cancel       context.CancelFunc
	streamResult *streamingResult
	err          error

	serializer JSONSerializer
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *ViewResult) Next(rowPtr *ViewRow) bool {
	if r.err != nil {
		return false
	}

	row := r.NextBytes()
	if row == nil {
		return false
	}

	decoder := json.NewDecoder(bytes.NewBuffer(row))
	rowPtr.serializer = r.serializer
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
		case "id":
			r.err = decoder.Decode(&rowPtr.ID)
			if r.err != nil {
				return false
			}
		case "key":
			r.err = decoder.Decode(&rowPtr.key)
			if r.err != nil {
				return false
			}
		case "value":
			r.err = decoder.Decode(&rowPtr.value)
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

// Key assigns the current result key from the results into the value pointer, returning any error that occurred.
func (r *ViewRow) Key(keyPtr interface{}) error {
	if r.value == nil {
		return clientError{message: "no data to scan"}
	}

	err := r.serializer.Deserialize(r.key, keyPtr)
	if err != nil {
		return err
	}

	return nil
}

// Value assigns the current result value from the results into the value pointer, returning any error that occurred.
func (r *ViewRow) Value(valuePtr interface{}) error {
	if r.value == nil {
		return clientError{message: "no data to scan"}
	}

	err := r.serializer.Deserialize(r.value, valuePtr)
	if err != nil {
		return err
	}

	return nil
}

// NextBytes returns the next result from the results as a byte array.
func (r *ViewResult) NextBytes() []byte {
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
func (r *ViewResult) Close() error {
	if r.streamResult == nil || r.streamResult.Closed() {
		return r.makeError()
	}

	err := r.streamResult.Close()
	ctxErr := r.ctx.Err()
	if r.cancel != nil {
		r.cancel()
	}
	if ctxErr == context.DeadlineExceeded {
		return timeoutError{
			elapsed:   time.Now().Sub(r.startTime),
			remote:    r.endpoint,
			operation: "fts",
		}
	}
	if vErr := r.makeError(); vErr != nil {
		return vErr
	}
	return err
}

func (r *ViewResult) makeError() error {
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

func (r *ViewResult) readAttribute(decoder *json.Decoder, t json.Token) (bool, error) {
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
			return false, clientError{message: "expected results opening token to be [ but was " + string(delim)}
		}

		return true, nil
	case "debug_info":
		err := decoder.Decode(&r.metadata.debug)
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

// One assigns the first value from the results into the value pointer.
// It will close the results but not before iterating through all remaining
// results, as such this should only be used for very small resultsets - ideally
// of, at most, length 1.
func (r *ViewResult) One(rowPtr *ViewRow) error {
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

// Metadata returns metadata for this result.
func (r *ViewResult) Metadata() (*ViewMetadata, error) {
	if r.streamResult != nil && !r.streamResult.Closed() {
		return nil, clientError{message: "result must be closed before accessing meta-data"}
	}

	return &r.metadata, nil
}

// TotalRows returns the total number of rows in the view, can be greater than the number of rows returned.
func (r *ViewMetadata) TotalRows() int {
	return r.totalRows
}

// Debug returns the debug information associated with the query, if requested.
func (r *ViewMetadata) Debug() interface{} {
	return r.debug
}

// ViewQuery performs a view query and returns a list of rows or an error.
func (b *Bucket) ViewQuery(designDoc string, viewName string, opts *ViewOptions) (*ViewResult, error) {
	startTime := time.Now()
	if opts == nil {
		opts = &ViewOptions{}
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	cli := b.sb.getCachedClient()
	provider, err := cli.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	designDoc = b.maybePrefixDevDocument(opts.Namespace, designDoc)

	timeout := b.sb.ViewTimeout
	if opts.Timeout > 0 {
		timeout = opts.Timeout
	}

	// We don't make the client timeout longer for this as pindexes can timeout
	// individually rather than the entire connection. Server side timeouts are also hard to detect.
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeout)

	urlValues, err := opts.toURLValues()
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "could not parse query options")
	}

	if opts.Serializer == nil {
		opts.Serializer = b.sb.Serializer
	}

	wrapper := b.sb.RetryStrategyWrapper
	if opts.RetryStrategy != nil {
		wrapper = newRetryStrategyWrapper(opts.RetryStrategy)
	}

	res, err := b.executeViewQuery(ctx, "_view", designDoc, viewName, *urlValues, provider, cancel,
		opts.Serializer, wrapper, startTime)
	if err != nil {
		cancel()
		return nil, err
	}

	return res, nil
}

func (b *Bucket) executeViewQuery(ctx context.Context, viewType, ddoc, viewName string,
	options url.Values, provider httpProvider, cancel context.CancelFunc, serializer JSONSerializer,
	wrapper *retryStrategyWrapper, startTime time.Time) (*ViewResult, error) {
	reqUri := fmt.Sprintf("/_design/%s/%s/%s?%s", ddoc, viewType, viewName, options.Encode())
	req := &gocbcore.HttpRequest{
		Service:       gocbcore.CapiService,
		Path:          reqUri,
		Method:        "GET",
		Context:       ctx,
		IsIdempotent:  true,
		RetryStrategy: wrapper,
	}

	resp, err := provider.DoHttpRequest(req)
	if err != nil {
		if err == gocbcore.ErrNoCapiService {
			return nil, serviceNotAvailableError{message: gocbcore.ErrNoCapiService.Error()}
		}

		if err == context.DeadlineExceeded {
			return nil, timeoutError{
				operationID:   req.Identifier(),
				retryReasons:  req.RetryReasons(),
				retryAttempts: req.RetryAttempts(),
				elapsed:       time.Now().Sub(startTime),
				remote:        req.Endpoint,
				operation:     "view",
			}
		}

		return nil, err
	}

	queryResults := &ViewResult{
		serializer: serializer,
		startTime:  startTime,
		endpoint:   resp.Endpoint,
	}

	if resp.StatusCode == 500 {
		// We have to handle the views 500 case as a special case because the body can be of form [] or {}
		defer func() {
		}()

		decoder := json.NewDecoder(resp.Body)
		t, err := decoder.Token()
		if err != nil {
			err := resp.Body.Close()
			if err != nil {
				logDebugf("Failed to close socket (%s)", err.Error())
			}
			return nil, err
		}
		delim, ok := t.(json.Delim)
		if !ok {
			err := resp.Body.Close()
			if err != nil {
				logDebugf("Failed to close socket (%s)", err.Error())
			}
			return nil, clientError{message: "could not read response body, no data found"}
		}
		if delim == '[' {
			errMsg, err := decoder.Token()
			if err != nil {
				err := resp.Body.Close()
				if err != nil {
					logDebugf("Failed to close socket (%s)", err.Error())
				}
				return nil, err
			}
			err = viewMultiError{
				errors: []ViewQueryError{
					viewError{
						ErrorMessage: errMsg.(string),
						ErrorReason:  fmt.Sprintf("%d", resp.StatusCode),
					},
				},
				httpStatus: resp.StatusCode,
				endpoint:   resp.Endpoint,
			}

			return nil, err
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
		queryResults.ctx = ctx
		queryResults.cancel = cancel
	} else {
		bodyErr := streamResult.Close()
		if bodyErr != nil {
			logDebugf("Failed to close response body, %s", bodyErr.Error())
		}

		// No retries for views

		err = queryResults.makeError()
		if err != nil {
			return nil, err
		}
	}
	return queryResults, nil
}

func (b *Bucket) maybePrefixDevDocument(namespace DesignDocumentNamespace, ddoc string) string {
	designDoc := ddoc
	if namespace {
		designDoc = strings.TrimPrefix(ddoc, "dev_")
	} else {
		if !strings.HasPrefix(ddoc, "dev_") {
			designDoc = "dev_" + ddoc
		}
	}

	return designDoc
}
