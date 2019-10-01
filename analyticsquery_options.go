package gocb

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
)

// AnalyticsScanConsistency indicates the level of data consistency desired for an analytics query.
type AnalyticsScanConsistency int

const (
	// AnalyticsScanConsistencyNotBounded indicates no data consistency is required.
	AnalyticsScanConsistencyNotBounded = AnalyticsScanConsistency(1)
	// AnalyticsScanConsistencyRequestPlus indicates that request-level data consistency is required.
	AnalyticsScanConsistencyRequestPlus = AnalyticsScanConsistency(2)
)

// AnalyticsOptions is the set of options available to an Analytics query.
type AnalyticsOptions struct {
	// Timeout and context are used to control cancellation of the data stream. Any timeout or deadline will also be
	// propagated to the server.
	ServerSideTimeout    time.Duration
	Context              context.Context
	ClientContextID      string
	Raw                  map[string]interface{}
	Priority             bool
	PositionalParameters []interface{}
	NamedParameters      map[string]interface{}
	ReadOnly             bool
	ScanConsistency      AnalyticsScanConsistency

	// JSONSerializer is used to deserialize each row in the result. This should be a JSON deserializer as results are JSON.
	// NOTE: if not set then query will always default to DefaultJSONSerializer.
	Serializer JSONSerializer
}

func (opts *AnalyticsOptions) toMap(statement string) (map[string]interface{}, error) {
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

	if opts.ScanConsistency != 0 {
		if opts.ScanConsistency == AnalyticsScanConsistencyNotBounded {
			execOpts["scan_consistency"] = "not_bounded"
		} else if opts.ScanConsistency == AnalyticsScanConsistencyRequestPlus {
			execOpts["scan_consistency"] = "request_plus"
		} else {
			return nil, configurationError{message: "unexpected consistency option"}
		}
	}

	if opts.Priority {
		execOpts["priority"] = -1
	}

	if opts.PositionalParameters != nil && opts.NamedParameters != nil {
		return nil, configurationError{message: "positional and named parameters must be used exclusively"}
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

	if opts.Raw != nil {
		for k, v := range opts.Raw {
			execOpts[k] = v
		}
	}

	if opts.ReadOnly {
		execOpts["readonly"] = true
	}

	return execOpts, nil
}
