package gocb

import (
	"context"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// AnalyticsQueryOptions is the set of options available to an Analytics query.
type AnalyticsQueryOptions struct {
	ServerSideTimeout    time.Duration
	Context              context.Context
	ParentSpanContext    opentracing.SpanContext
	Pretty               bool
	ContextID            string
	RawParam             map[string]interface{}
	Priority             bool
	PositionalParameters []interface{}
	NamedParameters      map[string]interface{}

	// Experimental: This API is subject to change at any time.
	Deferred bool
}

func (opts *AnalyticsQueryOptions) toMap(statement string) (map[string]interface{}, error) {
	execOpts := make(map[string]interface{})
	execOpts["statement"] = statement

	if opts.ServerSideTimeout != 0 {
		execOpts["timeout"] = opts.ServerSideTimeout.String()
	}

	if opts.Pretty {
		execOpts["pretty"] = opts.Pretty
	}

	if opts.ContextID != "" {
		execOpts["client_context_id"] = opts.ContextID
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
