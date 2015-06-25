package gocb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

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
func (b *Bucket) ExecuteViewQuery(q *ViewQuery) ViewResults {
	capiEp, err := b.getViewEp()
	if err != nil {
		return &viewResults{err: err}
	}

	reqUri := fmt.Sprintf("%s/_design/%s/_view/%s?%s", capiEp, q.ddoc, q.name, q.options.Encode())

	req, err := http.NewRequest("GET", reqUri, nil)
	if err != nil {
		return &viewResults{err: err}
	}
	req.SetBasicAuth(b.name, b.password)

	resp, err := b.client.HttpClient().Do(req)
	if err != nil {
		return &viewResults{err: err}
	}

	viewResp := viewResponse{}
	jsonDec := json.NewDecoder(resp.Body)
	jsonDec.Decode(&viewResp)

	resp.Body.Close()

	if resp.StatusCode != 200 {
		if viewResp.Error != "" {
			return &viewResults{
				err: &viewError{
					Message: viewResp.Error,
					Reason:  viewResp.Reason,
				},
			}
		}

		return &viewResults{
			err: &viewError{
				Message: "HTTP Error",
				Reason:  fmt.Sprintf("Status code was %d.", resp.StatusCode),
			},
		}
	}

	return &viewResults{
		index: -1,
		rows:  viewResp.Rows,
	}
}

// Performs a spatial query and returns a list of rows or an error.
func (b *Bucket) ExecuteSpatialQuery(q *SpatialQuery) ViewResults {
	capiEp, err := b.getViewEp()
	if err != nil {
		return &viewResults{err: err}
	}

	reqUri := fmt.Sprintf("%s/_design/%s/_spatial/%s?%s", capiEp, q.ddoc, q.name, q.options.Encode())

	req, err := http.NewRequest("GET", reqUri, nil)
	if err != nil {
		return &viewResults{err: err}
	}
	req.SetBasicAuth(b.name, b.password)

	resp, err := b.client.HttpClient().Do(req)
	if err != nil {
		return &viewResults{err: err}
	}

	viewResp := viewResponse{}
	jsonDec := json.NewDecoder(resp.Body)
	jsonDec.Decode(&viewResp)

	resp.Body.Close()

	if resp.StatusCode != 200 {
		if viewResp.Error != "" {
			return &viewResults{
				err: &viewError{
					Message: viewResp.Error,
					Reason:  viewResp.Reason,
				},
			}
		}

		return &viewResults{
			err: &viewError{
				Message: "HTTP Error",
				Reason:  fmt.Sprintf("Status code was %d.", resp.StatusCode),
			},
		}
	}

	return &viewResults{
		index: -1,
		rows:  viewResp.Rows,
	}
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

// Performs a spatial query and returns a list of rows or an error.
func (b *Bucket) ExecuteN1qlQuery(q *N1qlQuery, params interface{}) ViewResults {
	n1qlEp, err := b.getN1qlEp()
	if err != nil {
		return &viewResults{err: err}
	}

	reqOpts := make(map[string]interface{})
	for k, v := range q.options {
		reqOpts[k] = v
	}
	if params != nil {
		reqOpts["args"] = params
	}

	reqUri := fmt.Sprintf("%s/query/service", n1qlEp)

	reqJson, err := json.Marshal(reqOpts)
	if err != nil {
		return &viewResults{err: err}
	}

	req, err := http.NewRequest("POST", reqUri, bytes.NewBuffer(reqJson))
	if err != nil {
		return &viewResults{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(b.name, b.password)

	fmt.Printf("URI: %s\n", reqUri)
	fmt.Printf("Data: %s\n", reqJson)

	resp, err := b.client.HttpClient().Do(req)
	if err != nil {
		return &viewResults{err: err}
	}

	n1qlResp := n1qlResponse{}
	jsonDec := json.NewDecoder(resp.Body)
	jsonDec.Decode(&n1qlResp)

	resp.Body.Close()

	if resp.StatusCode != 200 {
		if len(n1qlResp.Errors) > 0 {
			return &n1qlResults{
				err: (*n1qlMultiError)(&n1qlResp.Errors),
			}
		}

		return &n1qlResults{
			err: &viewError{
				Message: "HTTP Error",
				Reason:  fmt.Sprintf("Status code was %d.", resp.StatusCode),
			},
		}
	}

	return &n1qlResults{
		index: -1,
		rows:  n1qlResp.Results,
	}
}
