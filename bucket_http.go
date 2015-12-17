package gocb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type n1qlCache struct {
	name        string
	encodedPlan string
}

type viewResponse struct {
	TotalRows int               `json:"total_rows,omitempty"`
	Rows      []json.RawMessage `json:"rows,omitempty"`
	Error     string            `json:"error,omitempty"`
	Reason    string            `json:"reason,omitempty"`
}

type viewError struct {
	Message string `json:"message"`
	Reason  string `json:"reason"`
}

func (e *viewError) Error() string {
	return e.Message + " - " + e.Reason
}

type ViewResults interface {
	One(valuePtr interface{}) error
	Next(valuePtr interface{}) bool
	Close() error
}

type viewResults struct {
	index int
	rows  []json.RawMessage
	err   error
}

func (r *viewResults) Next(valuePtr interface{}) bool {
	if r.err != nil {
		return false
	}
	if r.index+1 >= len(r.rows) {
		return false
	}
	r.index++

	row := r.rows[r.index]
	r.err = json.Unmarshal(row, valuePtr)
	if r.err != nil {
		return false
	}

	return true
}

func (r *viewResults) Close() error {
	return r.err
}

func (r *viewResults) One(valuePtr interface{}) error {
	if !r.Next(valuePtr) {
		err := r.Close()
		if err != nil {
			return err
		}
		return ErrNoResults
	}
	// Ignore any errors occuring after we already have our result
	r.Close()
	// Return no error as we got the one result already.
	return nil
}

// Performs a view query and returns a list of rows or an error.
func (b *Bucket) ExecuteViewQuery(q *ViewQuery) (ViewResults, error) {
	capiEp, err := b.getViewEp()
	if err != nil {
		return nil, err
	}

	reqUri := fmt.Sprintf("%s/_design/%s/_view/%s?%s", capiEp, q.ddoc, q.name, q.options.Encode())

	req, err := http.NewRequest("GET", reqUri, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(b.name, b.password)

	resp, err := doHttpWithTimeout(b.client.HttpClient(), req, b.viewTimeout)
	if err != nil {
		return nil, err
	}

	viewResp := viewResponse{}
	jsonDec := json.NewDecoder(resp.Body)
	jsonDec.Decode(&viewResp)

	resp.Body.Close()

	if resp.StatusCode != 200 {
		if viewResp.Error != "" {
			return nil, &viewError{
				Message: viewResp.Error,
				Reason:  viewResp.Reason,
			}
		}

		return nil, &viewError{
			Message: "HTTP Error",
			Reason:  fmt.Sprintf("Status code was %d.", resp.StatusCode),
		}
	}

	return &viewResults{
		index: -1,
		rows:  viewResp.Rows,
	}, nil
}

// Performs a spatial query and returns a list of rows or an error.
func (b *Bucket) ExecuteSpatialQuery(q *SpatialQuery) (ViewResults, error) {
	capiEp, err := b.getViewEp()
	if err != nil {
		return nil, err
	}

	reqUri := fmt.Sprintf("%s/_design/%s/_spatial/%s?%s", capiEp, q.ddoc, q.name, q.options.Encode())

	req, err := http.NewRequest("GET", reqUri, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(b.name, b.password)

	resp, err := doHttpWithTimeout(b.client.HttpClient(), req, b.viewTimeout)
	if err != nil {
		return nil, err
	}

	viewResp := viewResponse{}
	jsonDec := json.NewDecoder(resp.Body)
	jsonDec.Decode(&viewResp)

	resp.Body.Close()

	if resp.StatusCode != 200 {
		if viewResp.Error != "" {
			return nil, &viewError{
				Message: viewResp.Error,
				Reason:  viewResp.Reason,
			}
		}

		return nil, &viewError{
			Message: "HTTP Error",
			Reason:  fmt.Sprintf("Status code was %d.", resp.StatusCode),
		}
	}

	return &viewResults{
		index: -1,
		rows:  viewResp.Rows,
	}, nil
}

type n1qlError struct {
	Code    uint32 `json:"code"`
	Message string `json:"msg"`
}

func (e *n1qlError) Error() string {
	return fmt.Sprintf("[%d] %s", e.Code, e.Message)
}

type n1qlResponse struct {
	RequestId string            `json:"requestID"`
	Results   []json.RawMessage `json:"results,omitempty"`
	Errors    []n1qlError       `json:"errors,omitempty"`
	Status    string            `json:"status"`
}

type n1qlMultiError []n1qlError

func (e *n1qlMultiError) Error() string {
	return (*e)[0].Error()
}

func (e *n1qlMultiError) Code() uint32 {
	return (*e)[0].Code
}

type QueryResults interface {
	One(valuePtr interface{}) error
	Next(valuePtr interface{}) bool
	Close() error
}

type n1qlResults struct {
	index int
	rows  []json.RawMessage
	err   error
}

func (r *n1qlResults) Next(valuePtr interface{}) bool {
	if r.err != nil {
		return false
	}
	if r.index+1 >= len(r.rows) {
		return false
	}
	r.index++

	row := r.rows[r.index]
	r.err = json.Unmarshal(row, valuePtr)
	if r.err != nil {
		return false
	}

	return true
}

func (r *n1qlResults) Close() error {
	return r.err
}

func (r *n1qlResults) One(valuePtr interface{}) error {
	if !r.Next(valuePtr) {
		err := r.Close()
		if err != nil {
			return err
		}
		return ErrNoResults
	}
	// Ignore any errors occuring after we already have our result
	r.Close()
	// Return no error as we got the one result already.
	return nil
}

// Wrapper around net.http.Client.Do().
// This allows a per-request timeout without setting the timeout on the Client object
// directly.
// The third parameter is the duration for the request itself.
func doHttpWithTimeout(cli *http.Client, req *http.Request, timeout time.Duration) (resp *http.Response, err error) {
	if timeout.Seconds() == 0 {
		// No timeout
		resp, err = cli.Do(req)
		return
	}

	tmoch := make(chan struct{})
	timer := time.AfterFunc(timeout, func() {
		tmoch <- struct{}{}
	})

	req.Cancel = tmoch
	resp, err = cli.Do(req)
	timer.Stop()
	return
}

// Executes the N1QL query (in opts) on the server n1qlEp.
// This function assumes that `opts` already contains all the required
// settings. This function will inject any additional connection or request-level
// settings into the `opts` map (currently this is only the timeout).
func (b *Bucket) executeN1qlQuery(n1qlEp string, opts map[string]interface{}) (ViewResults, error) {
	reqUri := fmt.Sprintf("%s/query/service", n1qlEp)

	reqJson, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	var timeout time.Duration = b.n1qlTimeout
	tmostr, castok := opts["timeout"].(string)
	if castok {
		timeout, err = time.ParseDuration(tmostr)
	} else {
		// Set the timeout string to its default variant
		opts["timeout"] = timeout.String()
	}

	req, err := http.NewRequest("POST", reqUri, bytes.NewBuffer(reqJson))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(b.name, b.password)

	resp, err := doHttpWithTimeout(b.client.HttpClient(), req, timeout)
	if err != nil {
		return nil, err
	}

	n1qlResp := n1qlResponse{}
	jsonDec := json.NewDecoder(resp.Body)
	jsonDec.Decode(&n1qlResp)

	resp.Body.Close()

	if resp.StatusCode != 200 {
		if len(n1qlResp.Errors) > 0 {
			return nil, (*n1qlMultiError)(&n1qlResp.Errors)
		}

		return nil, &viewError{
			Message: "HTTP Error",
			Reason:  fmt.Sprintf("Status code was %d.", resp.StatusCode),
		}
	}

	return &n1qlResults{
		index: -1,
		rows:  n1qlResp.Results,
	}, nil
}

func (b *Bucket) prepareN1qlQuery(n1qlEp string, opts map[string]interface{}) (*n1qlCache, error) {
	prepOpts := make(map[string]interface{})
	for k, v := range opts {
		prepOpts[k] = v
	}
	prepOpts["statement"] = "PREPARE " + opts["statement"].(string)

	prepRes, err := b.executeN1qlQuery(n1qlEp, prepOpts)
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

// Performs a spatial query and returns a list of rows or an error.
func (b *Bucket) ExecuteN1qlQuery(q *N1qlQuery, params interface{}) (ViewResults, error) {
	n1qlEp, err := b.getN1qlEp()
	if err != nil {
		return nil, err
	}

	execOpts := make(map[string]interface{})
	for k, v := range q.options {
		execOpts[k] = v
	}
	if params != nil {
		args, isArray := params.([]interface{})
		if isArray {
			execOpts["args"] = args
		} else {
			mapArgs, isMap := params.(map[string]interface{})
			if isMap {
				for key, value := range mapArgs {
					execOpts["$"+key] = value
				}
			} else {
				panic("Invalid params argument passed")
			}
		}
	}

	if q.adHoc {
		return b.executeN1qlQuery(n1qlEp, execOpts)
	}

	// Do Prepared Statement Logic
	var cachedStmt *n1qlCache

	stmtStr := q.options["statement"].(string)

	b.queryCacheLock.RLock()
	cachedStmt = b.queryCache[stmtStr]
	b.queryCacheLock.RUnlock()

	if cachedStmt != nil {
		// Attempt to execute our cached query plan
		delete(execOpts, "statement")
		execOpts["prepared"] = cachedStmt.name
		execOpts["encoded_plan"] = cachedStmt.encodedPlan

		results, err := b.executeN1qlQuery(n1qlEp, execOpts)
		if err == nil {
			return results, nil
		}

		// If we get error 4050, 4070 or 5000, we should attempt
		//   to reprepare the statement immediately before failing.
		n1qlErr, isN1qlErr := err.(*n1qlMultiError)
		if !isN1qlErr {
			return nil, err
		}
		if n1qlErr.Code() != 4050 && n1qlErr.Code() != 4070 && n1qlErr.Code() != 5000 {
			return nil, err
		}
	}

	// Prepare the query
	cachedStmt, err = b.prepareN1qlQuery(n1qlEp, q.options)
	if err != nil {
		return nil, err
	}

	// Save new cached statement
	b.queryCacheLock.Lock()
	b.queryCache[stmtStr] = cachedStmt
	b.queryCacheLock.Unlock()

	// Update with new prepared data
	delete(execOpts, "statement")
	execOpts["prepared"] = cachedStmt.name
	execOpts["encoded_plan"] = cachedStmt.encodedPlan

	return b.executeN1qlQuery(n1qlEp, execOpts)
}
