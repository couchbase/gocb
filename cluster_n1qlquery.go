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

type n1qlError struct {
	Code    uint32 `json:"code"`
	Message string `json:"msg"`
}

func (e *n1qlError) Error() string {
	return fmt.Sprintf("[%d] %s", e.Code, e.Message)
}

type n1qlResponseMetrics struct {
	ElapsedTime   string `json:"elapsedTime"`
	ExecutionTime string `json:"executionTime"`
	ResultCount   uint   `json:"resultCount"`
	ResultSize    uint   `json:"resultSize"`
	MutationCount uint   `json:"mutationCount,omitempty"`
	SortCount     uint   `json:"sortCount,omitempty"`
	ErrorCount    uint   `json:"errorCount,omitempty"`
	WarningCount  uint   `json:"warningCount,omitempty"`
}

type n1qlResponse struct {
	RequestId       string              `json:"requestID"`
	ClientContextId string              `json:"clientContextID"`
	Results         []json.RawMessage   `json:"results,omitempty"`
	Errors          []n1qlError         `json:"errors,omitempty"`
	Status          string              `json:"status"`
	Metrics         n1qlResponseMetrics `json:"metrics"`
}

type n1qlMultiError []n1qlError

func (e *n1qlMultiError) Error() string {
	return (*e)[0].Error()
}

func (e *n1qlMultiError) Code() uint32 {
	return (*e)[0].Code
}

// QueryResultMetrics encapsulates various metrics gathered during a queries execution.
type QueryResultMetrics struct {
	ElapsedTime   time.Duration
	ExecutionTime time.Duration
	ResultCount   uint
	ResultSize    uint
	MutationCount uint
	SortCount     uint
	ErrorCount    uint
	WarningCount  uint
}

// QueryResults allows access to the results of a N1QL query.
type QueryResults interface {
	One(valuePtr interface{}) error
	Next(valuePtr interface{}) bool
	GetBytes(valuePtr *[]byte) bool
	NextBytes() []byte
	Close() error

	RequestId() string
	ClientContextId() string
	Metrics() QueryResultMetrics
}

type n1qlResults struct {
	closed          bool
	index           int
	rows            []json.RawMessage
	err             error
	requestId       string
	clientContextId string
	metrics         QueryResultMetrics
}

func (r *n1qlResults) Next(valuePtr interface{}) bool {
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

// Don't unmarshal return only []byte
func (r *n1qlResults) GetBytes(valuePtr *[]byte) bool {
	if r.err != nil {
		return false
	}

	row := r.NextBytes()
	if row == nil {
		return false
	}

	*valuePtr = row
	return true
}

func (r *n1qlResults) NextBytes() []byte {
	if r.err != nil {
		return nil
	}

	if r.index+1 >= len(r.rows) {
		r.closed = true
		return nil
	}
	r.index++

	return r.rows[r.index]
}

func (r *n1qlResults) Close() error {
	r.closed = true
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

	// Ignore any errors occurring after we already have our result
	err := r.Close()
	if err != nil {
		// Return no error as we got the one result already.
		return nil
	}

	return nil
}

func (r *n1qlResults) RequestId() string {
	if !r.closed {
		panic("Result must be closed before accessing meta-data")
	}

	return r.requestId
}

func (r *n1qlResults) ClientContextId() string {
	if !r.closed {
		panic("Result must be closed before accessing meta-data")
	}

	return r.clientContextId
}

func (r *n1qlResults) Metrics() QueryResultMetrics {
	if !r.closed {
		panic("Result must be closed before accessing meta-data")
	}

	return r.metrics
}

// Executes the N1QL query (in opts) on the server n1qlEp.
// This function assumes that `opts` already contains all the required
// settings. This function will inject any additional connection or request-level
// settings into the `opts` map (currently this is only the timeout).
func (c *Cluster) executeN1qlQuery(n1qlEp string, opts map[string]interface{}, creds []userPassPair, timeout time.Duration, client *http.Client) (QueryResults, error) {
	reqUri := fmt.Sprintf("%s/query/service", n1qlEp)

	tmostr, castok := opts["timeout"].(string)
	if castok {
		var err error
		timeout, err = time.ParseDuration(tmostr)
		if err != nil {
			return nil, err
		}
	} else {
		// Set the timeout string to its default variant
		opts["timeout"] = timeout.String()
	}

	if len(creds) > 1 {
		opts["creds"] = creds
	}

	reqJson, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", reqUri, bytes.NewBuffer(reqJson))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	if len(creds) == 1 {
		req.SetBasicAuth(creds[0].Username, creds[0].Password)
	}

	resp, err := doHttpWithTimeout(client, req, timeout)
	if err != nil {
		return nil, err
	}

	n1qlResp := n1qlResponse{}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&n1qlResp)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	if len(n1qlResp.Errors) > 0 {
		return nil, (*n1qlMultiError)(&n1qlResp.Errors)
	}

	if resp.StatusCode != 200 {
		return nil, &viewError{
			Message: "HTTP Error",
			Reason:  fmt.Sprintf("Status code was %d.", resp.StatusCode),
		}
	}

	elapsedTime, err := time.ParseDuration(n1qlResp.Metrics.ElapsedTime)
	if err != nil {
		logDebugf("Failed to parse elapsed time duration (%s)", err)
	}

	executionTime, err := time.ParseDuration(n1qlResp.Metrics.ExecutionTime)
	if err != nil {
		logDebugf("Failed to parse execution time duration (%s)", err)
	}

	return &n1qlResults{
		requestId:       n1qlResp.RequestId,
		clientContextId: n1qlResp.ClientContextId,
		index:           -1,
		rows:            n1qlResp.Results,
		metrics: QueryResultMetrics{
			ElapsedTime:   elapsedTime,
			ExecutionTime: executionTime,
			ResultCount:   n1qlResp.Metrics.ResultCount,
			ResultSize:    n1qlResp.Metrics.ResultSize,
			MutationCount: n1qlResp.Metrics.MutationCount,
			SortCount:     n1qlResp.Metrics.SortCount,
			ErrorCount:    n1qlResp.Metrics.ErrorCount,
			WarningCount:  n1qlResp.Metrics.WarningCount,
		},
	}, nil
}

func (c *Cluster) prepareN1qlQuery(n1qlEp string, opts map[string]interface{}, creds []userPassPair, timeout time.Duration, client *http.Client) (*n1qlCache, error) {
	prepOpts := make(map[string]interface{})
	for k, v := range opts {
		prepOpts[k] = v
	}
	prepOpts["statement"] = "PREPARE " + opts["statement"].(string)

	prepRes, err := c.executeN1qlQuery(n1qlEp, prepOpts, creds, timeout, client)
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
func (c *Cluster) doN1qlQuery(b *Bucket, q *N1qlQuery, params interface{}) (QueryResults, error) {
	var err error
	var n1qlEp string
	var timeout time.Duration
	var client *http.Client
	var creds []userPassPair

	if b != nil {
		n1qlEp, err = b.getN1qlEp()
		if err != nil {
			return nil, err
		}

		if b.n1qlTimeout < c.n1qlTimeout {
			timeout = b.n1qlTimeout
		} else {
			timeout = c.n1qlTimeout
		}
		client = b.client.HttpClient()
		if c.auth != nil {
			creds = c.auth.bucketN1ql(b.name)
		} else {
			creds = []userPassPair{
				{
					Username: b.name,
					Password: b.password,
				},
			}
		}
	} else {
		if c.auth == nil {
			panic("Cannot perform cluster level queries without Cluster Authenticator.")
		}

		tmpB, err := c.randomBucket()
		if err != nil {
			return nil, err
		}

		n1qlEp, err = tmpB.getN1qlEp()
		if err != nil {
			return nil, err
		}

		timeout = c.n1qlTimeout
		client = tmpB.client.HttpClient()
		creds = c.auth.clusterN1ql()
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
		return c.executeN1qlQuery(n1qlEp, execOpts, creds, timeout, client)
	}

	// Do Prepared Statement Logic
	var cachedStmt *n1qlCache

	stmtStr, isStr := q.options["statement"].(string)
	if !isStr {
		return nil, ErrCliInternalError
	}

	c.clusterLock.RLock()
	cachedStmt = c.queryCache[stmtStr]
	c.clusterLock.RUnlock()

	if cachedStmt != nil {
		// Attempt to execute our cached query plan
		delete(execOpts, "statement")
		execOpts["prepared"] = cachedStmt.name
		execOpts["encoded_plan"] = cachedStmt.encodedPlan

		results, err := c.executeN1qlQuery(n1qlEp, execOpts, creds, timeout, client)
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
	cachedStmt, err = c.prepareN1qlQuery(n1qlEp, q.options, creds, timeout, client)
	if err != nil {
		return nil, err
	}

	// Save new cached statement
	c.clusterLock.Lock()
	c.queryCache[stmtStr] = cachedStmt
	c.clusterLock.Unlock()

	// Update with new prepared data
	delete(execOpts, "statement")
	execOpts["prepared"] = cachedStmt.name
	execOpts["encoded_plan"] = cachedStmt.encodedPlan

	return c.executeN1qlQuery(n1qlEp, execOpts, creds, timeout, client)
}

// ExecuteN1qlQuery performs a n1ql query and returns a list of rows or an error.
func (c *Cluster) ExecuteN1qlQuery(q *N1qlQuery, params interface{}) (QueryResults, error) {
	return c.doN1qlQuery(nil, q, params)
}
