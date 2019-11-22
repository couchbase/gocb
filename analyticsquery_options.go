package gocb

import (
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
	ClientContextID      string
	Priority             bool
	PositionalParameters []interface{}
	NamedParameters      map[string]interface{}
	ReadOnly             bool
	ScanConsistency      AnalyticsScanConsistency
	Raw                  map[string]interface{}

	Timeout       time.Duration
	RetryStrategy RetryStrategy

	parentSpan requestSpanContext
}

func (opts *AnalyticsOptions) toMap() (map[string]interface{}, error) {
	execOpts := make(map[string]interface{})

	if opts.ClientContextID == "" {
		execOpts["client_context_id"] = uuid.New().String()
	} else {
		execOpts["client_context_id"] = opts.ClientContextID
	}

	if opts.ScanConsistency != 0 {
		if opts.ScanConsistency == AnalyticsScanConsistencyNotBounded {
			execOpts["scan_consistency"] = "not_bounded"
		} else if opts.ScanConsistency == AnalyticsScanConsistencyRequestPlus {
			execOpts["scan_consistency"] = "request_plus"
		} else {
			return nil, makeInvalidArgumentsError("unexpected consistency option")
		}
	}

	if opts.PositionalParameters != nil && opts.NamedParameters != nil {
		return nil, makeInvalidArgumentsError("positional and named parameters must be used exclusively")
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

	if opts.ReadOnly {
		execOpts["readonly"] = true
	}

	if opts.Raw != nil {
		for k, v := range opts.Raw {
			execOpts[k] = v
		}
	}

	return execOpts, nil
}
