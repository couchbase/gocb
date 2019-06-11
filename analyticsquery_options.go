package gocb

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// AnalyticsQueryOptions is the set of options available to an Analytics query.
type AnalyticsQueryOptions struct {
	ServerSideTimeout time.Duration
	// If Context is used then cancellation will only be applicable during initial http connect.
	// If a timeout value is supplied with the context then that value will be propagated to the server
	// and used to timeout the results stream.
	Context              context.Context
	ParentSpanContext    opentracing.SpanContext
	ClientContextID      string
	RawParam             map[string]interface{}
	Priority             bool
	PositionalParameters []interface{}
	NamedParameters      map[string]interface{}

	// Experimental: This API is subject to change at any time.
	Deferred bool

	// Serializer is used to deserialize each row in the result. This should be a JSON deserializer as results are JSON.
	Serializer Serializer
}

func (opts *AnalyticsQueryOptions) toMap(statement string) (map[string]interface{}, error) {
	execOpts := make(map[string]interface{})
	execOpts["statement"] = statement

	if opts.ServerSideTimeout != 0 {
		execOpts["timeout"] = opts.ServerSideTimeout.String()
	}

	if opts.ClientContextID == "" {
		execOpts["client_context_id"] = uuid.New()
	} else {
		execOpts["client_context_id"] = opts.ClientContextID
	}

	if opts.Priority {
		execOpts["priority"] = -1
	}

	if opts.Deferred {
		execOpts["mode"] = "async"
	}

	if opts.PositionalParameters != nil && opts.NamedParameters != nil {
		return nil, errors.New("Positional and named parameters must be used exclusively")
	}

	if opts.PositionalParameters != nil {
		execOpts["args"] = opts.PositionalParameters
	}

	if opts.NamedParameters != nil {
		for key, value := range opts.NamedParameters {
			if !strings.HasPrefix(key, "$") {
				key = "$" + key
			}
			execOpts[key] = value
		}
	}

	if opts.RawParam != nil {
		for k, v := range opts.RawParam {
			execOpts[k] = v
		}
	}

	return execOpts, nil
}
