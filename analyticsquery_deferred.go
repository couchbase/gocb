package gocb

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/couchbase/gocbcore/v8"
	"github.com/pkg/errors"
)

// AnalyticsDeferredResultHandle allows access to the handle of a deferred Analytics query.
//
// Experimental: This API is subject to change at any time.
type AnalyticsDeferredResultHandle struct {
	handleUri string
	status    string
	err       error
	provider  httpProvider
	decoder   *json.Decoder
	timeout   time.Duration
	stream    io.ReadCloser
}

type analyticsResponseHandle struct {
	Status string `json:"status,omitempty"`
	Handle string `json:"handle,omitempty"`
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *AnalyticsDeferredResultHandle) Next(valuePtr interface{}) bool {
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
func (r *AnalyticsDeferredResultHandle) NextBytes() []byte {
	if r.err != nil {
		return nil
	}

	if r.status == "success" && r.decoder == nil {
		req := &gocbcore.HttpRequest{
			Service:  gocbcore.CbasService,
			Endpoint: r.handleUri,
			Method:   "GET",
		}

		resp, err := r.provider.DoHttpRequest(req)
		if err != nil {
			r.err = err
			return nil
		}
		if resp.StatusCode != 200 {
			r.err = fmt.Errorf("handle request failed, received http status %d", resp.StatusCode)
			return nil
		}

		r.decoder = json.NewDecoder(resp.Body)
		t, err := r.decoder.Token()
		if err != nil {
			bodyErr := resp.Body.Close()
			if bodyErr != nil {
				logDebugf("Failed to close response body, %s", bodyErr.Error())
			}
			r.err = err
			return nil
		}
		if delim, ok := t.(json.Delim); !ok || delim != '[' {
			bodyErr := resp.Body.Close()
			if bodyErr != nil {
				logDebugf("Failed to close response body, %s", bodyErr.Error())
			}
			r.err = errors.New("expected response opening token to be [ but was " + t.(string))
			return nil
		}

		r.stream = resp.Body
	} else if r.status != "success" {
		return nil
	}

	return r.nextBytes()
}

func (r *AnalyticsDeferredResultHandle) nextBytes() []byte {
	if r.decoder.More() {
		var raw json.RawMessage
		err := r.decoder.Decode(&raw)
		if err != nil {
			r.err = err
			return nil
		}

		return raw
	}

	return nil
}

// Close marks the results as closed, returning any errors that occurred during reading the results.
func (r *AnalyticsDeferredResultHandle) Close() error {
	if r.stream == nil {
		return r.err
	}

	err := r.stream.Close()
	if r.err != nil {
		return r.err
	}
	return err
}

// One assigns the first value from the results into the value pointer.
func (r *AnalyticsDeferredResultHandle) One(valuePtr interface{}) error {
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
func (r *AnalyticsDeferredResultHandle) Status() (string, error) {
	req := &gocbcore.HttpRequest{
		Service:  gocbcore.CbasService,
		Endpoint: r.handleUri,
		Method:   "GET",
	}

	var resp *analyticsResponseHandle
	err := r.executeHandle(req, &resp)
	if err != nil {
		return "", err
	}

	r.status = resp.Status
	if r.status == "success" {
		r.handleUri = resp.Handle
	}
	return r.status, nil
}

// TODO: how to deadline/timeout this?
func (r *AnalyticsDeferredResultHandle) executeHandle(req *gocbcore.HttpRequest, valuePtr interface{}) error {
	resp, err := r.provider.DoHttpRequest(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("handle request failed, received http status %d", resp.StatusCode)
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
