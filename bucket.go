package gocb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/gocb/gocbcore"
	"math/rand"
	"net/http"
	"time"
)

// An interface representing a single bucket within a cluster.
type Bucket struct {
	name       string
	password   string
	httpCli    *http.Client
	client     *gocbcore.Agent
	transcoder Transcoder
}

func (b *Bucket) SetTranscoder(transcoder Transcoder) {
	b.transcoder = transcoder
}

func (b *Bucket) afterOpTimeout() <-chan time.Time {
	return time.After(10 * time.Second)
}

type pendingOp gocbcore.PendingOp

type ioGetCallback func([]byte, uint32, uint64, error)
type ioCasCallback func(uint64, error)
type ioCtrCallback func(uint64, uint64, error)

type hlpGetHandler func(ioGetCallback) (pendingOp, error)

func (b *Bucket) hlpGetExec(valuePtr interface{}, execFn hlpGetHandler) (casOut uint64, errOut error) {
	signal := make(chan bool, 1)
	op, err := execFn(func(bytes []byte, flags uint32, cas uint64, err error) {
		go func() {
			if err != nil {
				errOut = err
			} else {
				err = b.transcoder.Decode(bytes, flags, valuePtr)
				if err != nil {
					errOut = err
				} else {
					casOut = cas
				}

			}
			signal <- true
		}()
	})
	if err != nil {
		return 0, err
	}

	select {
	case <-signal:
		return
	case <-b.afterOpTimeout():
		op.Cancel()
		return 0, timeoutError{}
	}
}

type hlpCasHandler func(ioCasCallback) (pendingOp, error)

func (b *Bucket) hlpCasExec(execFn hlpCasHandler) (casOut uint64, errOut error) {
	signal := make(chan bool, 1)
	op, err := execFn(func(cas uint64, err error) {
		go func() {
			if err != nil {
				errOut = err
			} else {
				casOut = cas
			}
			signal <- true
		}()
	})
	if err != nil {
		return 0, err
	}

	select {
	case <-signal:
		return
	case <-b.afterOpTimeout():
		op.Cancel()
		return 0, timeoutError{}
	}
}

type hlpCtrHandler func(ioCtrCallback) (pendingOp, error)

func (b *Bucket) hlpCtrExec(execFn hlpCtrHandler) (valOut uint64, casOut uint64, errOut error) {
	signal := make(chan bool, 1)
	op, err := execFn(func(value uint64, cas uint64, err error) {
		go func() {
			if err != nil {
				errOut = err
			} else {
				valOut = value
				casOut = cas
			}
			signal <- true
		}()
	})
	if err != nil {
		return 0, 0, err
	}

	select {
	case <-signal:
		return
	case <-b.afterOpTimeout():
		op.Cancel()
		return 0, 0, timeoutError{}
	}
}

// Retrieves a document from the bucket
func (b *Bucket) Get(key string, valuePtr interface{}) (uint64, error) {
	return b.hlpGetExec(valuePtr, func(cb ioGetCallback) (pendingOp, error) {
		op, err := b.client.Get([]byte(key), gocbcore.GetCallback(cb))
		return op, err
	})
}

// Retrieves a document and simultaneously updates its expiry time.
func (b *Bucket) GetAndTouch(key string, expiry uint32, valuePtr interface{}) (uint64, error) {
	return b.hlpGetExec(valuePtr, func(cb ioGetCallback) (pendingOp, error) {
		op, err := b.client.GetAndTouch([]byte(key), expiry, gocbcore.GetCallback(cb))
		return op, err
	})
}

// Locks a document for a period of time, providing exclusive RW access to it.
func (b *Bucket) GetAndLock(key string, lockTime uint32, valuePtr interface{}) (uint64, error) {
	return b.hlpGetExec(valuePtr, func(cb ioGetCallback) (pendingOp, error) {
		op, err := b.client.GetAndLock([]byte(key), lockTime, gocbcore.GetCallback(cb))
		return op, err
	})
}

// Unlocks a document which was locked with GetAndLock.
func (b *Bucket) Unlock(key string, cas uint64) (casOut uint64, errOut error) {
	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Unlock([]byte(key), cas, gocbcore.UnlockCallback(cb))
		return op, err
	})
}

// Returns the value of a particular document from a replica server.
func (b *Bucket) GetReplica(key string, valuePtr interface{}, replicaIdx int) (uint64, error) {
	return b.hlpGetExec(valuePtr, func(cb ioGetCallback) (pendingOp, error) {
		op, err := b.client.GetReplica([]byte(key), replicaIdx, gocbcore.GetCallback(cb))
		return op, err
	})
}

// Touches a document, specifying a new expiry time for it.
func (b *Bucket) Touch(key string, cas uint64, expiry uint32) (uint64, error) {
	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Touch([]byte(key), cas, expiry, gocbcore.TouchCallback(cb))
		return op, err
	})
}

// Removes a document from the bucket.
func (b *Bucket) Remove(key string, cas uint64) (casOut uint64, errOut error) {
	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Remove([]byte(key), cas, gocbcore.RemoveCallback(cb))
		return op, err
	})
}

// Inserts or replaces a document in the bucket.
func (b *Bucket) Upsert(key string, value interface{}, expiry uint32) (casOut uint64, errOut error) {
	bytes, flags, err := b.transcoder.Encode(value)
	if err != nil {
		return 0, err
	}

	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Set([]byte(key), bytes, flags, expiry, gocbcore.StoreCallback(cb))
		return op, err
	})
}

// Inserts a new document to the bucket.
func (b *Bucket) Insert(key string, value interface{}, expiry uint32) (uint64, error) {
	bytes, flags, err := b.transcoder.Encode(value)
	if err != nil {
		return 0, err
	}

	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Add([]byte(key), bytes, flags, expiry, gocbcore.StoreCallback(cb))
		return op, err
	})
}

// Replaces a document in the bucket.
func (b *Bucket) Replace(key string, value interface{}, cas uint64, expiry uint32) (uint64, error) {
	bytes, flags, err := b.transcoder.Encode(value)
	if err != nil {
		return 0, err
	}

	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Replace([]byte(key), bytes, flags, cas, expiry, gocbcore.StoreCallback(cb))
		return op, err
	})
}

// Appends a string value to a document.
func (b *Bucket) Append(key, value string) (uint64, error) {
	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Append([]byte(key), []byte(value), gocbcore.StoreCallback(cb))
		return op, err
	})
}

// Prepends a string value to a document.
func (b *Bucket) Prepend(key, value string) (uint64, error) {
	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Prepend([]byte(key), []byte(value), gocbcore.StoreCallback(cb))
		return op, err
	})
}

// Performs an atomic addition or subtraction for an integer document.
func (b *Bucket) Counter(key string, delta, initial int64, expiry uint32) (uint64, uint64, error) {
	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if initial > 0 {
		realInitial = uint64(initial)
	}

	if delta > 0 {
		return b.hlpCtrExec(func(cb ioCtrCallback) (pendingOp, error) {
			op, err := b.client.Increment([]byte(key), uint64(delta), realInitial, expiry, gocbcore.CounterCallback(cb))
			return op, err
		})
	} else if delta < 0 {
		return b.hlpCtrExec(func(cb ioCtrCallback) (pendingOp, error) {
			op, err := b.client.Decrement([]byte(key), uint64(-delta), realInitial, expiry, gocbcore.CounterCallback(cb))
			return op, err
		})
	} else {
		return 0, 0, clientError{"Delta must be a non-zero value."}
	}
}

// Returns a CAPI endpoint.  Guarenteed to return something for now...
func (b *Bucket) getViewEp() (string, error) {
	capiEps := b.client.CapiEps()
	if len(capiEps) == 0 {
		return "", &clientError{"No available view nodes."}
	}
	return capiEps[rand.Intn(len(capiEps))], nil
}

func (b *Bucket) getMgmtEp() (string, error) {
	mgmtEps := b.client.MgmtEps()
	if len(mgmtEps) == 0 {
		return "", &clientError{"No available management nodes."}
	}
	return mgmtEps[rand.Intn(len(mgmtEps))], nil
}

func (b *Bucket) getN1qlEp() (string, error) {
	n1qlEps := b.client.N1qlEps()
	if len(n1qlEps) == 0 {
		return "", &clientError{"No available N1QL nodes."}
	}
	return n1qlEps[rand.Intn(len(n1qlEps))], nil
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

	resp, err := b.httpCli.Do(req)
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

	resp, err := b.httpCli.Do(req)
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
	/*
		n1qlEp, err := b.getN1qlEp()
	*/
	n1qlEp := "http://query.pub.couchbase.com"
	var err error
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

	resp, err := b.httpCli.Do(req)
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

func (b *Bucket) IoRouter() *gocbcore.Agent {
	return b.client
}

func (b *Bucket) Manager(username, password string) *BucketManager {
	return &BucketManager{
		bucket:   b,
		username: username,
		password: password,
	}
}
