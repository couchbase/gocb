package gocb

import (
	"io"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

type mgmtRequest struct {
	Service      ServiceType
	Method       string
	Path         string
	Body         []byte
	Headers      map[string]string
	ContentType  string
	IsIdempotent bool
	UniqueID     string

	Timeout       time.Duration
	RetryStrategy RetryStrategy

	parentSpan requestSpanContext
}

type mgmtResponse struct {
	Endpoint   string
	StatusCode int
	Body       io.ReadCloser
}

func (c *Cluster) executeMgmtRequest(req mgmtRequest) (*mgmtResponse, error) {
	provider, err := c.getHTTPProvider()
	if err != nil {
		return nil, err
	}

	timeout := c.sb.ManagementTimeout
	if req.Timeout > 0 && req.Timeout < timeout {
		timeout = req.Timeout
	}

	retryStrategy := c.sb.RetryStrategyWrapper
	if req.RetryStrategy != nil {
		retryStrategy = newRetryStrategyWrapper(req.RetryStrategy)
	}

	corereq := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(req.Service),
		Method:        req.Method,
		Path:          req.Path,
		Body:          req.Body,
		Headers:       req.Headers,
		ContentType:   req.ContentType,
		IsIdempotent:  req.IsIdempotent,
		UniqueID:      req.UniqueID,
		Timeout:       timeout,
		RetryStrategy: retryStrategy,
	}

	coreresp, err := provider.DoHTTPRequest(corereq)
	if err != nil {
		return nil, makeGenericHTTPError(err, corereq, coreresp)
	}

	resp := &mgmtResponse{
		Endpoint:   coreresp.Endpoint,
		StatusCode: coreresp.StatusCode,
		Body:       coreresp.Body,
	}
	return resp, nil
}

func (b *Bucket) executeMgmtRequest(req mgmtRequest) (*mgmtResponse, error) {
	provider, err := b.sb.getCachedClient().getHTTPProvider()
	if err != nil {
		return nil, err
	}

	timeout := b.sb.ManagementTimeout
	if req.Timeout > 0 && req.Timeout < timeout {
		timeout = req.Timeout
	}

	retryStrategy := b.sb.RetryStrategyWrapper
	if req.RetryStrategy != nil {
		retryStrategy = newRetryStrategyWrapper(req.RetryStrategy)
	}

	corereq := &gocbcore.HTTPRequest{
		Service:       gocbcore.ServiceType(req.Service),
		Method:        req.Method,
		Path:          req.Path,
		Body:          req.Body,
		Headers:       req.Headers,
		ContentType:   req.ContentType,
		IsIdempotent:  req.IsIdempotent,
		UniqueID:      req.UniqueID,
		Timeout:       timeout,
		RetryStrategy: retryStrategy,
	}

	coreresp, err := provider.DoHTTPRequest(corereq)
	if err != nil {
		return nil, makeGenericHTTPError(err, corereq, coreresp)
	}

	resp := &mgmtResponse{
		Endpoint:   coreresp.Endpoint,
		StatusCode: coreresp.StatusCode,
		Body:       coreresp.Body,
	}
	return resp, nil
}
