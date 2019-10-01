package gocb

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// QueryScanConsistency indicates the level of data consistency desired for a query.
type QueryScanConsistency int

const (
	// QueryScanConsistencyNotBounded indicates no data consistency is required.
	QueryScanConsistencyNotBounded = QueryScanConsistency(1)
	// QueryScanConsistencyRequestPlus indicates that request-level data consistency is required.
	QueryScanConsistencyRequestPlus = QueryScanConsistency(2)
)

// QueryOptions represents the options available when executing a N1QL query.
type QueryOptions struct {
	ScanConsistency QueryScanConsistency
	ConsistentWith  *MutationState
	AdHoc           bool
	Profile         QueryProfileType
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
	// ScanWait specifies the maximum time wait for a scan.
	ScanWait time.Duration
	// ReadOnly controls whether a query can change a resulting recordset.  If
	// readonly is true, then only SELECT statements are permitted.
	ReadOnly bool
	// MaxParallelism controls the number of logical cores to use in parallel for this query.
	MaxParallelism  int
	ClientContextID string
	// Timeout and context are used to control cancellation of the data stream. Any timeout or deadline will also be
	// propagated to the server.
	Timeout              time.Duration
	Context              context.Context
	PositionalParameters []interface{}
	NamedParameters      map[string]interface{}
	// Metrics specifies whether or not to fetch metrics when executing the query.
	Metrics bool
	// Raw allows specifying custom query options.
	Raw map[string]interface{}

	// JSONSerializer is used to deserialize each row in the result. This should be a JSON deserializer as results are JSON.
	// NOTE: if not set then query will always default to DefaultJSONSerializer.
	Serializer JSONSerializer
}

func (opts *QueryOptions) toMap(statement string) (map[string]interface{}, error) {
	execOpts := make(map[string]interface{})
	execOpts["statement"] = statement
	if opts.Timeout != 0 {
		execOpts["timeout"] = opts.Timeout.String()
	}

	if opts.ScanConsistency != 0 && opts.ConsistentWith != nil {
		return nil, configurationError{message: "ScanConsistency and ConsistentWith must be used exclusively"}
	}

	if opts.ScanConsistency != 0 {
		if opts.ScanConsistency == QueryScanConsistencyNotBounded {
			execOpts["scan_consistency"] = "not_bounded"
		} else if opts.ScanConsistency == QueryScanConsistencyRequestPlus {
			execOpts["scan_consistency"] = "request_plus"
		} else {
			return nil, configurationError{message: "Unexpected consistency option"}
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
		return nil, configurationError{message: "Positional and named parameters must be used exclusively"}
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

	if opts.ScanWait > 0 {
		execOpts["scan_wait"] = opts.ScanWait.String()
	}

	if opts.Raw != nil {
		for k, v := range opts.Raw {
			execOpts[k] = v
		}
	}

	if opts.MaxParallelism > 0 {
		execOpts["max_parallelism"] = strconv.Itoa(opts.MaxParallelism)
	}

	if !opts.Metrics {
		execOpts["metrics"] = false
	}

	if opts.ClientContextID == "" {
		execOpts["client_context_id"] = uuid.New()
	} else {
		execOpts["client_context_id"] = opts.ClientContextID
	}

	return execOpts, nil
}
