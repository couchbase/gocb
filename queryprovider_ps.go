package gocb

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc/status"

	"github.com/couchbase/goprotostellar/genproto/kv_v1"
	"github.com/couchbase/goprotostellar/genproto/query_v1"

	"google.golang.org/protobuf/types/known/durationpb"
)

type queryProviderPs struct {
	provider query_v1.QueryServiceClient

	timeouts TimeoutsConfig
	tracer   RequestTracer
	meter    *meterWrapper
}

func (qpc *queryProviderPs) Query(statement string, s *Scope, opts *QueryOptions) (*QueryResult, error) {
	start := time.Now()
	defer qpc.meter.ValueRecord(meterValueServiceQuery, "query", start)

	span := createSpan(qpc.tracer, opts.ParentSpan, "query", "query")
	span.SetAttribute("db.statement", statement)
	if s != nil {
		span.SetAttribute("db.name", s.BucketName())
		span.SetAttribute("db.couchbase.scope", s.Name())
	}
	defer span.End()

	prepared := !opts.Adhoc
	req := &query_v1.QueryRequest{
		Statement: statement,
		Prepared:  &prepared,
	}
	if s != nil {
		req.BucketName = &s.bucket.bucketName
		req.ScopeName = &s.scopeName
	}
	if opts.Readonly {
		req.ReadOnly = &opts.Readonly
	}
	req.TuningOptions = &query_v1.QueryRequest_TuningOptions{}
	if opts.MaxParallelism > 0 {
		req.TuningOptions.MaxParallelism = &opts.MaxParallelism
	}
	if opts.PipelineBatch > 0 {
		req.TuningOptions.PipelineBatch = &opts.PipelineBatch
	}
	if opts.PipelineCap > 0 {
		req.TuningOptions.PipelineCap = &opts.PipelineCap
	}
	if opts.ScanWait > 0 {
		req.TuningOptions.ScanWait = durationpb.New(opts.ScanWait)
	}
	if opts.ScanCap > 0 {
		req.TuningOptions.ScanCap = &opts.ScanCap
	}
	disableMetrics := !opts.Metrics
	if disableMetrics {
		req.TuningOptions.DisableMetrics = &disableMetrics
	}
	if opts.ClientContextID != "" {
		req.ClientContextId = &opts.ClientContextID
	}
	if opts.ScanConsistency != 0 {
		var consistency query_v1.QueryRequest_ScanConsistency
		if opts.ScanConsistency == QueryScanConsistencyNotBounded {
			consistency = query_v1.QueryRequest_SCAN_CONSISTENCY_NOT_BOUNDED
		} else if opts.ScanConsistency == QueryScanConsistencyRequestPlus {
			consistency = query_v1.QueryRequest_SCAN_CONSISTENCY_REQUEST_PLUS
		} else {
			return nil, makeInvalidArgumentsError("unexpected consistency option")
		}
		req.ScanConsistency = &consistency
	}

	if len(opts.PositionalParameters) > 0 {
		params := make([][]byte, len(opts.PositionalParameters))
		for i, param := range opts.PositionalParameters {
			b, err := json.Marshal(param)
			if err != nil {
				return nil, err
			}

			params[i] = b
		}

		req.PositionalParameters = params
	}
	if len(opts.NamedParameters) > 0 {
		params := make(map[string][]byte, len(opts.NamedParameters))
		for k, param := range opts.NamedParameters {
			b, err := json.Marshal(param)
			if err != nil {
				return nil, err
			}

			params[k] = b
		}

		req.NamedParameters = params
	}
	if opts.FlexIndex {
		req.FlexIndex = &opts.FlexIndex
	}
	if opts.PreserveExpiry {
		req.PreserveExpiry = &opts.PreserveExpiry
	}

	if opts.ConsistentWith != nil {
		tokens := make([]*kv_v1.MutationToken, len(opts.ConsistentWith.tokens))
		for i, tok := range opts.ConsistentWith.tokens {
			tokens[i] = &kv_v1.MutationToken{
				BucketName:  tok.BucketName(),
				VbucketId:   uint32(tok.PartitionID()),
				VbucketUuid: tok.PartitionUUID(),
				SeqNo:       tok.SequenceNumber(),
			}
		}
		req.ConsistentWith = tokens
	}

	if opts.Profile != "" {
		var profileMode query_v1.QueryRequest_ProfileMode
		switch opts.Profile {
		case QueryProfileModeNone:
			profileMode = query_v1.QueryRequest_PROFILE_MODE_OFF
		case QueryProfileModePhases:
			profileMode = query_v1.QueryRequest_PROFILE_MODE_PHASES
		case QueryProfileModeTimings:
			profileMode = query_v1.QueryRequest_PROFILE_MODE_TIMINGS
		default:
			return nil, makeInvalidArgumentsError("unexpected profile mode option")
		}
		req.ProfileMode = &profileMode
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = qpc.timeouts.QueryTimeout
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	res, err := qpc.provider.Query(ctx, req)
	if err != nil {
		cancel()
		st, ok := status.FromError(err)
		if !ok {
			return nil, &QueryError{
				InnerError: err,
				Statement:  statement,
			}
		}
		gocbErr := tryMapPsErrorStatusToGocbError(st, opts.Readonly)
		if gocbErr == nil {
			gocbErr = err
		}

		if errors.Is(gocbErr, ErrTimeout) {
			return nil, &TimeoutError{
				InnerError:    gocbErr,
				TimeObserved:  time.Since(start),
				RetryReasons:  nil,
				RetryAttempts: 0,
			}
		}

		return nil, &QueryError{
			InnerError: gocbErr,
			Statement:  statement,
			Errors: []QueryErrorDesc{
				{
					Code:    uint32(st.Code()),
					Message: st.Message(),
				},
			},
		}
	}

	reader := &queryProviderPsRowReader{
		cli:        res,
		cancelFunc: cancel,

		statement: statement,
		readOnly:  opts.Readonly,
	}
	return newQueryResult(reader), nil
}

type queryProviderPsRowReader struct {
	cli        query_v1.QueryService_QueryClient
	cancelFunc context.CancelFunc

	statement string
	readOnly  bool

	nextRowsIndex int
	nextRows      [][]byte
	err           error
	meta          *query_v1.QueryResponse_MetaData
}

func (q *queryProviderPsRowReader) NextRow() []byte {
	if q.nextRowsIndex < len(q.nextRows) {
		row := q.nextRows[q.nextRowsIndex]
		q.nextRowsIndex++
		return row
	}

	res, err := q.cli.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			q.finishWithoutError()
			return nil
		}
		q.finishWithError(err)
		return nil
	}

	q.nextRows = res.Rows
	q.nextRowsIndex = 1
	q.meta = res.MetaData

	if len(res.Rows) > 0 {
		return res.Rows[0]
	}

	return nil
}

func (q *queryProviderPsRowReader) Err() error {
	if q.err == nil {
		return nil
	}
	st, ok := status.FromError(q.err)
	if !ok {
		return &QueryError{
			InnerError: q.err,
			Statement:  q.statement,
		}
	}
	gocbErr := tryMapPsErrorStatusToGocbError(st, q.readOnly)
	if gocbErr == nil {
		gocbErr = q.err
	}
	return &QueryError{
		InnerError: gocbErr,
		Statement:  q.statement,
		Errors: []QueryErrorDesc{
			{
				Code:    uint32(st.Code()),
				Message: st.Message(),
			},
		},
	}
}

func (q *queryProviderPsRowReader) MetaData() ([]byte, error) {
	if err := q.Err(); err != nil {
		return nil, err
	}
	if q.cli != nil {
		return nil, errors.New("the result must be fully read before accessing the meta-data")
	}
	if q.meta == nil {
		return nil, errors.New("an error occurred during querying which has made the meta-data unavailable")
	}

	meta := jsonQueryResponse{
		RequestID:       q.meta.RequestId,
		ClientContextID: q.meta.ClientContextId,
		Profile:         q.meta.Profile,
		Signature:       q.meta.Signature,
	}
	switch q.meta.Status {
	case query_v1.QueryResponse_MetaData_STATUS_RUNNING:
		meta.Status = QueryStatusRunning
	case query_v1.QueryResponse_MetaData_STATUS_SUCCESS:
		meta.Status = QueryStatusSuccess
	case query_v1.QueryResponse_MetaData_STATUS_ERRORS:
		meta.Status = QueryStatusErrors
	case query_v1.QueryResponse_MetaData_STATUS_COMPLETED:
		meta.Status = QueryStatusCompleted
	case query_v1.QueryResponse_MetaData_STATUS_STOPPED:
		meta.Status = QueryStatusStopped
	case query_v1.QueryResponse_MetaData_STATUS_TIMEOUT:
		meta.Status = QueryStatusTimeout
	case query_v1.QueryResponse_MetaData_STATUS_CLOSED:
		meta.Status = QueryStatusClosed
	case query_v1.QueryResponse_MetaData_STATUS_FATAL:
		meta.Status = QueryStatusFatal
	case query_v1.QueryResponse_MetaData_STATUS_ABORTED:
		meta.Status = QueryStatusAborted
	case query_v1.QueryResponse_MetaData_STATUS_UNKNOWN:
		meta.Status = QueryStatusUnknown
	default:
		meta.Status = QueryStatusUnknown
	}

	if len(q.meta.Warnings) > 0 {
		meta.Warnings = make([]jsonQueryWarning, len(q.meta.Warnings))
		for i, warning := range q.meta.Warnings {
			meta.Warnings[i] = jsonQueryWarning{
				Code:    warning.Code,
				Message: warning.Message,
			}
		}
	}

	if q.meta.Metrics != nil {
		meta.Metrics = &jsonQueryMetrics{
			ElapsedTime:   q.meta.Metrics.ElapsedTime.AsDuration().String(),
			ExecutionTime: q.meta.Metrics.ExecutionTime.AsDuration().String(),
			ResultCount:   q.meta.Metrics.ResultCount,
			ResultSize:    q.meta.Metrics.ResultSize,
			MutationCount: q.meta.Metrics.MutationCount,
			SortCount:     q.meta.Metrics.SortCount,
			ErrorCount:    q.meta.Metrics.ErrorCount,
			WarningCount:  q.meta.Metrics.WarningCount,
		}
	}

	return json.Marshal(meta)
}

func (q *queryProviderPsRowReader) Close() error {
	if q.err != nil {
		return q.err
	}
	// if the client is nil then we must be closed already.
	if q.cli == nil {
		return nil
	}
	err := q.cli.CloseSend()
	q.cli = nil
	return err
}

func (q *queryProviderPsRowReader) PreparedName() (string, error) {
	return "", nil
}

func (q *queryProviderPsRowReader) Endpoint() string {
	return ""
}

func (r *queryProviderPsRowReader) finishWithoutError() {
	r.cancelFunc()
	// Close the stream now that we are done with it
	err := r.cli.CloseSend()
	if err != nil {
		logWarnf("query stream close failed after meta-data: %s", err)
	}

	r.cli = nil
}

func (r *queryProviderPsRowReader) finishWithError(err error) {
	// Lets record the error that happened
	r.err = err
	r.cancelFunc()

	// Lets Close the underlying stream
	closeErr := r.cli.CloseSend()
	if closeErr != nil {
		// We log this at debug level, but its almost always going to be an
		// error since thats the most likely reason we are in finishWithError
		logDebugf("query stream close failed after error: %s", closeErr)
	}

	// Our client is invalidated as soon as an error occurs
	r.cli = nil
}
