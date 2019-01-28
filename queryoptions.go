package gocb

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// ConsistencyMode indicates the level of data consistency desired for a query.
type ConsistencyMode int

const (
	// NotBounded indicates no data consistency is required.
	NotBounded = ConsistencyMode(1)
	// RequestPlus indicates that request-level data consistency is required.
	RequestPlus = ConsistencyMode(2)
	// StatementPlus indicates that statement-level data consistency is required.
	StatementPlus = ConsistencyMode(3)
)

// QueryOptions represents the options available when executing a N1QL query.
type QueryOptions struct {
	Consistency    ConsistencyMode
	ConsistentWith *MutationState
	Prepared       bool
	Profile        QueryProfileType
	// ScanCap specifies the maximum buffered channel size between the indexer
	// client and the query service for index scans. This parameter controls
	// when to use scan backfill. Use a negative number to disable.
	ScanCap int
	// PipelineBatch controls the number of items execution operators can
	// batch for fetch from the KV node.
	PipelineBatch int
	// PipelineCap controls the maximum number of items each execution operator
	// can buffer between various operators.
	PipelineCap int
	// ReadOnly controls whether a query can change a resulting recordset.  If
	// readonly is true, then only SELECT statements are permitted.
	ReadOnly             bool
	Timeout              time.Duration
	PositionalParameters []interface{}
	NamedParameters      map[string]interface{}
	Context              context.Context
	ParentSpanContext    opentracing.SpanContext
	// Custom allows specifying custom query options.
	Custom map[string]interface{}
}

func (opts *QueryOptions) toMap(statement string) (map[string]interface{}, error) {
	execOpts := make(map[string]interface{})
	execOpts["statement"] = statement
	if opts.Timeout != 0 {
		execOpts["timeout"] = opts.Timeout.String()
	}

	if opts.Consistency != 0 && opts.ConsistentWith != nil {
		return nil, errors.New("Consistent and ConsistentWith must be used exclusively")
	}

	if opts.Consistency != 0 {
		if opts.Consistency == NotBounded {
			execOpts["scan_consistency"] = "not_bounded"
		} else if opts.Consistency == RequestPlus {
			execOpts["scan_consistency"] = "request_plus"
		} else if opts.Consistency == StatementPlus {
			execOpts["scan_consistency"] = "statement_plus"
		} else {
			return nil, errors.New("Unexpected consistency option")
		}
	}

	if opts.ConsistentWith != nil {
		execOpts["scan_consistency"] = "at_plus"
		execOpts["scan_vectors"] = opts.ConsistentWith
	}

	if opts.Profile != "" {
		execOpts["profile"] = opts.Profile
	}

	if opts.ReadOnly {
		execOpts["readonly"] = opts.ReadOnly
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

	if opts.ScanCap != 0 {
		execOpts["scan_cap"] = strconv.Itoa(opts.ScanCap)
	}

	if opts.PipelineBatch != 0 {
		execOpts["pipeline_batch"] = strconv.Itoa(opts.PipelineBatch)
	}

	if opts.PipelineCap != 0 {
		execOpts["pipeline_cap"] = strconv.Itoa(opts.PipelineCap)
	}

	if opts.Custom != nil {
		for k, v := range opts.Custom {
			execOpts[k] = v
		}
	}

	return execOpts, nil
}
