package executor

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/couchbase/gocb/v2"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/query"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/query/index/manager"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
)

func (e *Executor) createQueryOptions(opts *query.QueryOptions) (*gocb.QueryOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.QueryOptions{
		ScanConsistency: gocb.QueryScanConsistency(opts.GetScanConsistency()),
		Profile:         gocb.QueryProfileMode(opts.GetProfile()),
		ScanCap:         uint32(opts.GetScanCap()),
		PipelineBatch:   uint32(opts.GetPipelineBatch()),
		PipelineCap:     uint32(opts.GetPipelineCap()),
		ScanWait:        time.Duration(opts.GetScanWaitMillis()) * time.Millisecond,
		Readonly:        opts.GetReadonly(),
		MaxParallelism:  uint32(opts.GetMaxParallelism()),
		Metrics:         opts.GetMetrics(),
		Timeout:         time.Duration(opts.GetTimeoutMillis()) * time.Millisecond,
		Adhoc:           opts.GetAdhoc(),
		ClientContextID: opts.GetClientContextId(),
	}

	if opts.ScanConsistency != nil {
		switch opts.GetScanConsistency() {
		case shared.ScanConsistency_NOT_BOUNDED:
			gocbOpts.ScanConsistency = gocb.QueryScanConsistencyNotBounded
		case shared.ScanConsistency_REQUEST_PLUS:
			gocbOpts.ScanConsistency = gocb.QueryScanConsistencyRequestPlus
		default:
			return nil, errors.New("unknown scan consistency")
		}
	}

	if opts.ParametersPositional != nil {
		var positional []interface{}
		for _, param := range opts.GetParametersPositional() {
			positional = append(positional, param)
		}
		gocbOpts.PositionalParameters = positional
	}

	if opts.ParametersNamed != nil {
		named := make(map[string]interface{}, len(opts.GetParametersNamed()))
		for name, param := range opts.GetParametersNamed() {
			named[name] = param
		}
		gocbOpts.NamedParameters = named
	}

	if opts.Raw != nil {
		raw := make(map[string]interface{}, len(opts.GetRaw()))
		for name, param := range opts.GetRaw() {
			raw[name] = param
		}
		gocbOpts.Raw = raw
	}

	if opts.ConsistentWith != nil {
		consistentWith, err := helpers.ProtoMutationStateToGocb(opts.ConsistentWith)
		if err != nil {
			return nil, err
		}
		gocbOpts.ConsistentWith = consistentWith
	}

	gocbOpts.FlexIndex = opts.GetFlexIndex()

	if opts.SingleQueryTransactionOptions != nil {
		gocbOpts.AsTransaction = &gocb.SingleQueryTransactionOptions{
			DurabilityLevel: helpers.ProtocolDuraToSDK(opts.SingleQueryTransactionOptions.GetDurability()),
		}
	}

	gocbOpts.PreserveExpiry = opts.GetPreserveExpiry()

	if opts.UseReplica != nil {
		if opts.GetUseReplica() {
			gocbOpts.UseReplica = gocb.QueryUseReplicaLevelOn
		} else {
			gocbOpts.UseReplica = gocb.QueryUseReplicaLevelOff
		}
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func parseQueryResult(contentAs *shared.ContentAs, res *gocb.QueryResult) (*query.QueryResult, error) {
	var allContent []*shared.ContentTypes
	for res.Next() {
		content, err := helpers.ParseContentAs(contentAs, func(content interface{}) error {
			return res.Row(&content)
		})
		if err != nil {
			return nil, err
		}

		allContent = append(allContent, content)
	}

	err := res.Err()
	if err != nil {
		return nil, err
	}

	meta, err := res.MetaData()
	if err != nil {
		return nil, err
	}

	var metrics *query.QueryMetrics
	if meta.Metrics.ElapsedTime > 0 {
		metrics = &query.QueryMetrics{
			ElapsedTime:   durationpb.New(meta.Metrics.ElapsedTime),
			ExecutionTime: durationpb.New(meta.Metrics.ExecutionTime),
			SortCount:     meta.Metrics.SortCount,
			ResultCount:   meta.Metrics.ResultCount,
			ResultSize:    meta.Metrics.ResultSize,
			MutationCount: meta.Metrics.MutationCount,
			ErrorCount:    meta.Metrics.ErrorCount,
			WarningCount:  meta.Metrics.WarningCount,
		}
	}

	warnings := make([]*query.QueryWarning, len(meta.Warnings))
	for i, warning := range meta.Warnings {
		warnings[i] = &query.QueryWarning{
			Code:    int32(warning.Code),
			Message: warning.Message,
		}
	}

	var queryStatus query.QueryStatus
	switch meta.Status {
	case gocb.QueryStatusRunning:
		queryStatus = query.QueryStatus_RUNNING
	case gocb.QueryStatusSuccess:
		queryStatus = query.QueryStatus_SUCCESS
	case gocb.QueryStatusErrors:
		queryStatus = query.QueryStatus_ERRORS
	case gocb.QueryStatusCompleted:
		queryStatus = query.QueryStatus_COMPLETED
	case gocb.QueryStatusStopped:
		queryStatus = query.QueryStatus_STOPPED
	case gocb.QueryStatusTimeout:
		queryStatus = query.QueryStatus_TIMEOUT
	case gocb.QueryStatusClosed:
		queryStatus = query.QueryStatus_CLOSED
	case gocb.QueryStatusFatal:
		queryStatus = query.QueryStatus_FATAL
	case gocb.QueryStatusAborted:
		queryStatus = query.QueryStatus_ABORTED
	case gocb.QueryStatusUnknown:
		queryStatus = query.QueryStatus_UNKNOWN
	}

	var profile []byte
	if meta.Profile != nil {
		profile, err = json.Marshal(meta.Profile)
		if err != nil {
			return nil, err
		}
	}

	var sig []byte
	if meta.Signature != nil {
		sig, err = json.Marshal(meta.Signature)
		if err != nil {
			return nil, err
		}
	}

	protoMeta := &query.QueryMetaData{
		RequestId:       meta.RequestID,
		ClientContextId: meta.ClientContextID,
		Status:          queryStatus,
		Signature:       sig,
		Warnings:        warnings,
		Metrics:         metrics,
		Profile:         profile,
	}

	return &query.QueryResult{
		Content:  allContent,
		MetaData: protoMeta,
	}, nil
}

func queryIndexTypeToProto(typ gocb.QueryIndexType) manager.QueryIndexType {
	switch typ {
	case gocb.QueryIndexTypeView:
		return manager.QueryIndexType_VIEW
	case gocb.QueryIndexTypeGsi:
		return manager.QueryIndexType_GSI
	default:
		return -1
	}
}

func queryIndexToProto(index gocb.QueryIndex) *manager.QueryIndex {
	var condition *string
	if index.Condition != "" {
		condition = &index.Condition
	}
	var partition *string
	if index.Partition != "" {
		partition = &index.Partition
	}

	managerIdx := &manager.QueryIndex{
		Name:      index.Name,
		IsPrimary: index.IsPrimary,
		Type:      queryIndexTypeToProto(index.Type),
		State:     index.State,
		Keyspace:  index.Keyspace,
		IndexKey:  index.IndexKey,
		Condition: condition,
		Partition: partition,
	}

	if index.ScopeName != "" {
		managerIdx.ScopeName = &index.ScopeName
	}
	if index.CollectionName != "" {
		managerIdx.CollectionName = &index.CollectionName
	}
	managerIdx.BucketName = index.BucketName

	return managerIdx
}
