package gocb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type n1qlCache struct {
	name string
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
		return clientError{"No results returned"}
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

	resp, err := b.client.HttpClient().Do(req)
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

	resp, err := b.client.HttpClient().Do(req)
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
		return clientError{"No results returned"}
	}
	// Ignore any errors occuring after we already have our result
	r.Close()
	// Return no error as we got the one result already.
	return nil
}

func (b *Bucket) executeN1qlQuery(n1qlEp string, opts map[string]interface{}) (ViewResults, error) {
	reqUri := fmt.Sprintf("%s/query/service", n1qlEp)

	reqJson, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", reqUri, bytes.NewBuffer(reqJson))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(b.name, b.password)

	resp, err := b.client.HttpClient().Do(req)
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

type n1qlPrepData struct {
	EncodedPlan string `json:"encoded_plan"`
	Name string `json:"name"`
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
		execOpts["args"] = params
	}

	if !q.adHoc {
		stmtStr := q.options["statement"].(string);

		var cachedStmt *n1qlCache
		b.queryCacheLock.RLock()
		cachedStmt = b.queryCache[stmtStr]
		b.queryCacheLock.RUnlock()

		if cachedStmt == nil {
			prepOpts := make(map[string]interface{})
			for k, v := range q.options {
				prepOpts[k] = v
			}
			prepOpts["statement"] = "PREPARE " + stmtStr;

			prepRes, err := b.executeN1qlQuery(n1qlEp, prepOpts)
			if err != nil {
				return nil, err
			}

			var preped n1qlPrepData
			err = prepRes.One(&preped)
			if (err != nil) {
				return nil, err
			}

			cachedStmt = &n1qlCache{
				name: preped.Name,
				encodedPlan: preped.EncodedPlan,
			}

			b.queryCacheLock.Lock()
			b.queryCache[stmtStr] = cachedStmt
			b.queryCacheLock.Unlock()
		}

		delete(execOpts, "statement")
		execOpts["prepared"] = cachedStmt.name
		execOpts["encoded_plan"] = cachedStmt.encodedPlan
	}

	return b.executeN1qlQuery(n1qlEp, execOpts)
}
