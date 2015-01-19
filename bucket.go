package gocouchbase

import (
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/gocouchbaseio"
	"math/rand"
	"net/http"
	"time"
)

// An interface representing a single bucket within a cluster.
type Bucket struct {
	httpCli *http.Client
	client  *gocouchbaseio.Agent
}

func (b *Bucket) afterOpTimeout() <-chan time.Time {
	return time.After(10 * time.Second)
}

type pendingOp gocouchbaseio.PendingOp

type ioGetCallback func([]byte, uint32, uint64, error)
type ioCasCallback func(uint64, error)
type ioCtrCallback func(uint64, uint64, error)

type hlpGetHandler func(ioGetCallback) (pendingOp, error)

func (b *Bucket) hlpGetExec(valuePtr interface{}, execFn hlpGetHandler) (valOut interface{}, casOut uint64, errOut error) {
	signal := make(chan bool, 1)
	op, err := execFn(func(bytes []byte, flags uint32, cas uint64, err error) {
		go func() {
			if err != nil {
				errOut = err
			} else {
				value, err := b.decodeValue(bytes, flags, valuePtr)
				if err != nil {
					errOut = err
				} else {
					valOut = value
					casOut = cas
				}

			}
			signal <- true
		}()
	})
	if err != nil {
		return nil, 0, err
	}

	select {
	case <-signal:
		return
	case <-b.afterOpTimeout():
		op.Cancel()
		return nil, 0, timeoutError{}
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
func (b *Bucket) Get(key string, valuePtr interface{}) (interface{}, uint64, error) {
	return b.hlpGetExec(valuePtr, func(cb ioGetCallback) (pendingOp, error) {
		op, err := b.client.Get([]byte(key), gocouchbaseio.GetCallback(cb))
		return op, err
	})
}

// Retrieves a document and simultaneously updates its expiry time.
func (b *Bucket) GetAndTouch(key string, expiry uint32, valuePtr interface{}) (interface{}, uint64, error) {
	return b.hlpGetExec(valuePtr, func(cb ioGetCallback) (pendingOp, error) {
		op, err := b.client.GetAndTouch([]byte(key), expiry, gocouchbaseio.GetCallback(cb))
		return op, err
	})
}

// Locks a document for a period of time, providing exclusive RW access to it.
func (b *Bucket) GetAndLock(key string, lockTime uint32, valuePtr interface{}) (interface{}, uint64, error) {
	return b.hlpGetExec(valuePtr, func(cb ioGetCallback) (pendingOp, error) {
		op, err := b.client.GetAndLock([]byte(key), lockTime, gocouchbaseio.GetCallback(cb))
		return op, err
	})
}

// Unlocks a document which was locked with GetAndLock.
func (b *Bucket) Unlock(key string, cas uint64) (casOut uint64, errOut error) {
	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Unlock([]byte(key), cas, gocouchbaseio.UnlockCallback(cb))
		return op, err
	})
}

// Returns the value of a particular document from a replica server.
func (b *Bucket) GetReplica(key string, valuePtr interface{}, replicaIdx int) (interface{}, uint64, error) {
	panic("GetReplica not yet supported")
}

// Touches a document, specifying a new expiry time for it.
func (b *Bucket) Touch(key string, expiry uint32) (uint64, error) {
	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Touch([]byte(key), expiry, gocouchbaseio.TouchCallback(cb))
		return op, err
	})
}

// Removes a document from the bucket.
func (b *Bucket) Remove(key string, cas uint64) (casOut uint64, errOut error) {
	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Remove([]byte(key), cas, gocouchbaseio.RemoveCallback(cb))
		return op, err
	})
}

// Inserts or replaces a document in the bucket.
func (b *Bucket) Upsert(key string, value interface{}, expiry uint32) (casOut uint64, errOut error) {
	bytes, flags, err := b.encodeValue(value)
	if err != nil {
		return 0, err
	}

	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Set([]byte(key), bytes, flags, expiry, gocouchbaseio.StoreCallback(cb))
		return op, err
	})
}

// Inserts a new document to the bucket.
func (b *Bucket) Insert(key string, value interface{}, expiry uint32) (uint64, error) {
	bytes, flags, err := b.encodeValue(value)
	if err != nil {
		return 0, err
	}

	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Add([]byte(key), bytes, flags, expiry, gocouchbaseio.StoreCallback(cb))
		return op, err
	})
}

// Replaces a document in the bucket.
func (b *Bucket) Replace(key string, value interface{}, cas uint64, expiry uint32) (uint64, error) {
	bytes, flags, err := b.encodeValue(value)
	if err != nil {
		return 0, err
	}

	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Replace([]byte(key), bytes, flags, cas, expiry, gocouchbaseio.StoreCallback(cb))
		return op, err
	})
}

// Appends a string value to a document.
func (b *Bucket) Append(key, value string) (uint64, error) {
	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Append([]byte(key), []byte(value), gocouchbaseio.StoreCallback(cb))
		return op, err
	})
}

// Prepends a string value to a document.
func (b *Bucket) Prepend(key, value string) (uint64, error) {
	return b.hlpCasExec(func(cb ioCasCallback) (pendingOp, error) {
		op, err := b.client.Prepend([]byte(key), []byte(value), gocouchbaseio.StoreCallback(cb))
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
			op, err := b.client.Increment([]byte(key), uint64(delta), realInitial, expiry, gocouchbaseio.CounterCallback(cb))
			return op, err
		})
	} else if delta < 0 {
		return b.hlpCtrExec(func(cb ioCtrCallback) (pendingOp, error) {
			op, err := b.client.Decrement([]byte(key), uint64(-delta), realInitial, expiry, gocouchbaseio.CounterCallback(cb))
			return op, err
		})
	} else {
		return 0, 0, clientError{"Delta must be a non-zero value."}
	}
}

// Returns a CAPI endpoint.  Guarenteed to return something for now...
func (b *Bucket) getViewEp() string {
	capiEps := b.client.CapiEps()
	return capiEps[rand.Intn(len(capiEps))]
}

type viewItem struct {
	Bytes []byte
}

func (i *viewItem) UnmarshalJSON(data []byte) error {
	i.Bytes = make([]byte, len(data))
	copy(i.Bytes, data)
	return nil
}

type viewResponse struct {
	TotalRows int        `json:"total_rows,omitempty"`
	Rows      []viewItem `json:"rows,omitempty"`
	Error     string     `json:"error,omitempty"`
	Reason    string     `json:"reason,omitempty"`
}

type viewError struct {
	message string
	reason  string
}

func (e *viewError) Error() string {
	return e.message + " - " + e.reason
}

type ViewResults interface {
	Next(valuePtr interface{}) bool
	Close() error
}

type viewResults struct {
	index int
	rows  []viewItem
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
	r.err = json.Unmarshal(row.Bytes, valuePtr)
	if r.err != nil {
		return false
	}

	return true
}
func (r *viewResults) Close() error {
	return r.err
}

// Performs a view query and returns a list of rows or an error.
func (b *Bucket) ExecuteViewQuery(q *ViewQuery) ViewResults {
	capiEp := b.getViewEp()

	reqUri := fmt.Sprintf("%s/_design/%s/_view/%s?%s", capiEp, q.ddoc, q.name, q.options.Encode())

	resp, err := b.httpCli.Get(reqUri)
	if err != nil {
		return &viewResults{err: err}
	}

	viewResp := viewResponse{}
	jsonDec := json.NewDecoder(resp.Body)
	jsonDec.Decode(&viewResp)

	if resp.StatusCode != 200 {
		if viewResp.Error != "" {
			return &viewResults{
				err: &viewError{
					message: viewResp.Error,
					reason:  viewResp.Reason,
				},
			}
		}
		return &viewResults{
			err: &viewError{
				message: "HTTP Error",
				reason:  fmt.Sprintf("Status code was %d.", resp.StatusCode),
			},
		}
	}

	return &viewResults{
		index: -1,
		rows:  viewResp.Rows,
	}
}

func (b *Bucket) IoRouter() *gocouchbaseio.Agent {
	return b.client
}
