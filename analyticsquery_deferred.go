package gocb

import (
	"encoding/json"
	"time"

	"gopkg.in/couchbase/gocbcore.v8"
)

// AnalyticsDeferredResultHandle allows access to the handle of a deferred Analytics query.
//
// Experimental: This API is subject to change at any time.
type AnalyticsDeferredResultHandle interface {
	One(valuePtr interface{}) error
	Next(valuePtr interface{}) bool
	NextBytes() []byte
	Close() error

	Status() (string, error)
}

type analyticsDeferredResultHandle struct {
	handleUri string
	status    string
	rows      *analyticsRows
	err       error
	provider  httpProvider
	hasResult bool
	timeout   time.Duration
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *analyticsDeferredResultHandle) Next(valuePtr interface{}) bool {
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
// TODO: how to deadline/timeout this?
func (r *analyticsDeferredResultHandle) NextBytes() []byte {
	if r.err != nil {
		return nil
	}

	if r.status == "success" && !r.hasResult {
		req := &gocbcore.HttpRequest{
			Service: gocbcore.CbasService,
			Path:    r.handleUri,
			Method:  "GET",
		}

		err := r.executeHandle(req, &r.rows.rows)
		if err != nil {
			r.err = err
			return nil
		}
		r.hasResult = true
	} else if r.status != "success" {
		return nil
	}

	return r.rows.NextBytes()
}

// Close marks the results as closed, returning any errors that occurred during reading the results.
func (r *analyticsDeferredResultHandle) Close() error {
	r.rows.Close()
	return r.err
}

// One assigns the first value from the results into the value pointer.
func (r *analyticsDeferredResultHandle) One(valuePtr interface{}) error {
	if !r.Next(valuePtr) {
		err := r.Close()
		if err != nil {
			return err
		}
		// return ErrNoResults
	}

	// Ignore any errors occurring after we already have our result
	err := r.Close()
	if err != nil {
		// Return no error as we got the one result already.
		return nil
	}

	return nil
}

// Status triggers a network call to the handle URI, returning the current status of the long running query.
// TODO: how to deadline/timeout this?
func (r *analyticsDeferredResultHandle) Status() (string, error) {
	req := &gocbcore.HttpRequest{
		Service: gocbcore.CbasService,
		Path:    r.handleUri,
		Method:  "GET",
	}

	var resp *analyticsResponseHandle
	err := r.executeHandle(req, &resp)
	if err != nil {
		r.err = err
		return "", err
	}

	r.status = resp.Status
	r.handleUri = resp.Handle
	return r.status, nil
}

// TODO: how to deadline/timeout this?
func (r *analyticsDeferredResultHandle) executeHandle(req *gocbcore.HttpRequest, valuePtr interface{}) error {
	resp, err := r.provider.DoHttpRequest(req)
	if err != nil {
		return err
	}

	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(valuePtr)
	if err != nil {
		return err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}
