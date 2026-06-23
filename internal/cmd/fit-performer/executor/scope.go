package executor

import (
	"encoding/json"

	"time"

	"github.com/couchbase/gocb/v2"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk"

	searchmgrpb "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/search/index-manager"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/sender"
)

func (e *Executor) handleScopeLevelCommand(command *sdk.ScopeLevelCommand, sender sender.ResultSender, returnResult bool) (bool, error) {
	scope := e.conn.Cluster().
		Bucket(command.GetScope().GetBucketName()).
		Scope(command.GetScope().GetScopeName())

	switch op := command.Command.(type) {

	case *sdk.ScopeLevelCommand_Query:
		result := &run.Result{
			Initiated: timestamppb.Now(),
		}

		opts, err := e.createQueryOptions(op.Query.Options)
		if err != nil {
			return false, err
		}

		start := time.Now()
		res, err := scope.Query(op.Query.Statement, opts)
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if returnResult {
			queryRes, err := parseQueryResult(op.Query.ContentAs, res)
			if err != nil {
				e.sendSDKError(err, sender)
				return false, nil
			}

			result.Result = &run.Result_Sdk{
				Sdk: &sdk.Result{
					Result: &sdk.Result_QueryResult{
						QueryResult: queryRes,
					},
				},
			}
		} else {
			for res.Next() {
			}
			if err := res.Close(); err != nil {
				e.sendSDKError(err, sender)
				return false, nil
			}
			result.Result = e.makeSuccessResult()
		}

		sender.Send(result)
		return true, nil

	case *sdk.ScopeLevelCommand_SearchV2:
		return e.handleScopeSearchV2(scope, op, sender, returnResult)
	case *sdk.ScopeLevelCommand_SearchIndexManager:
		return e.handleScopeSearchIndexManager(scope, op, sender, returnResult)

	default:
		return false, status.Error(codes.Unimplemented, "unknown command type")
	}
}

func (e *Executor) handleScopeSearchV2(scope *gocb.Scope, op *sdk.ScopeLevelCommand_SearchV2, sender sender.ResultSender, returnResult bool) (bool, error) {
	initiated := timestamppb.Now()

	searchRequest, err := e.parseSearchRequest(op.SearchV2.Search.Request)
	if err != nil {
		return false, err
	}
	opts, err := e.parseSearchOptions(op.SearchV2.Search.Options)
	if err != nil {
		return false, err
	}

	start := time.Now()
	res, err := scope.Search(op.SearchV2.Search.IndexName, *searchRequest, opts)
	if err != nil {
		e.sendSDKError(err, sender)
		return false, nil
	}

	result := &run.Result{
		Initiated:    initiated,
		ElapsedNanos: time.Since(start).Nanoseconds(),
	}

	return e.handleSearchResult(res, result, op.SearchV2.StreamConfig, op.SearchV2.FieldsAs, sender, returnResult)
}

func (e *Executor) handleScopeSearchIndexManager(scope *gocb.Scope, clusterOp *sdk.ScopeLevelCommand_SearchIndexManager, sender sender.ResultSender, returnResult bool) (bool, error) {
	mgr := scope.SearchIndexes()
	result := &run.Result{
		Initiated: timestamppb.Now(),
	}

	switch op := clusterOp.SearchIndexManager.GetShared().Command.(type) {
	case *searchmgrpb.Command_GetIndex:
		cmd := op.GetIndex
		opts, err := e.parseGetSearchIndexOptions(cmd)
		if err != nil {
			return false, err
		}

		start := time.Now()
		index, err := mgr.GetIndex(cmd.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		if returnResult {
			protoRes, err := e.makeGetSearchIndexResult(index)
			if err != nil {
				return false, err
			}
			result.Result = protoRes
		} else {
			result.Result = e.makeSuccessResult()
		}
		sender.Send(result)
		return true, nil

	case *searchmgrpb.Command_GetAllIndexes:
		opts, err := e.parseGetAllSearchIndexOptions(op.GetAllIndexes)
		if err != nil {
			return false, err
		}

		start := time.Now()
		indexes, err := mgr.GetAllIndexes(opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}

		if returnResult {
			protoRes, err := e.makeGetAllSearchIndexResult(indexes)
			if err != nil {
				return false, err
			}
			result.Result = protoRes
		} else {
			result.Result = e.makeSuccessResult()
		}
		sender.Send(result)
		return true, nil

	case *searchmgrpb.Command_UpsertIndex:
		cmd := op.UpsertIndex
		opts, err := e.parseUpsertSearchIndexOptions(cmd)
		if err != nil {
			return false, err
		}

		index, err := toGocbSearchIndex(cmd.GetIndexDefinition())
		if err != nil {
			return false, err
		}

		start := time.Now()
		err = mgr.UpsertIndex(*index, opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchmgrpb.Command_DropIndex:
		cmd := op.DropIndex
		opts, err := e.parseDropSearchIndexOptions(cmd)
		if err != nil {
			return false, err
		}

		start := time.Now()
		err = mgr.DropIndex(cmd.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchmgrpb.Command_GetIndexedDocumentsCount:
		cmd := op.GetIndexedDocumentsCount
		opts, err := e.parseGetIndexedDocumentsCountOptions(cmd)
		if err != nil {
			return false, err
		}

		start := time.Now()
		count, err := mgr.GetIndexedDocumentsCount(op.GetIndexedDocumentsCount.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		if returnResult {
			protoRes, err := e.makeGetIndexedDocumentCountResult(count)
			if err != nil {
				return false, err
			}
			result.Result = protoRes
		} else {
			result.Result = e.makeSuccessResult()
		}
		sender.Send(result)
		return true, nil

	case *searchmgrpb.Command_PauseIngest:
		opts, err := e.parsePauseIngestSearchIndexOptions(op.PauseIngest)
		if err != nil {
			return false, err
		}

		start := time.Now()
		err = mgr.PauseIngest(op.PauseIngest.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchmgrpb.Command_ResumeIngest:
		cmd := op.ResumeIngest
		opts, err := e.parseResumeIngestSearchIndexOptions(cmd)
		if err != nil {
			return false, err
		}

		start := time.Now()
		err = mgr.ResumeIngest(cmd.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchmgrpb.Command_AllowQuerying:
		cmd := op.AllowQuerying
		opts, err := e.parseAllowQueryingSearchIndexOptions(cmd)
		if err != nil {
			return false, err
		}

		start := time.Now()
		err = mgr.AllowQuerying(cmd.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchmgrpb.Command_DisallowQuerying:
		cmd := op.DisallowQuerying
		opts, err := e.parseDisallowQueryingSearchIndexOptions(cmd)
		if err != nil {
			return false, err
		}

		start := time.Now()
		err = mgr.DisallowQuerying(cmd.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchmgrpb.Command_FreezePlan:
		cmd := op.FreezePlan
		opts, err := e.parseFreezePlanSearchIndexOptions(cmd)
		if err != nil {
			return false, err
		}

		start := time.Now()
		err = mgr.FreezePlan(cmd.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchmgrpb.Command_UnfreezePlan:
		cmd := op.UnfreezePlan
		opts, err := e.parseUnfreezePlanSearchIndexOptions(cmd)
		if err != nil {
			return false, err
		}

		start := time.Now()
		err = mgr.UnfreezePlan(op.UnfreezePlan.GetIndexName(), opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		result.Result = e.makeSuccessResult()
		sender.Send(result)
		return true, nil

	case *searchmgrpb.Command_AnalyzeDocument:
		cmd := op.AnalyzeDocument
		opts, err := e.parseAnalyzeDocumentOptions(cmd)
		if err != nil {
			return false, err
		}

		var doc interface{}
		err = json.Unmarshal(op.AnalyzeDocument.GetDocument(), &doc)
		if err != nil {
			return false, err
		}
		start := time.Now()
		analyzeResults, err := mgr.AnalyzeDocument(cmd.GetIndexName(), doc, opts)
		result.ElapsedNanos = time.Since(start).Nanoseconds()
		if err != nil {
			e.sendSDKError(err, sender)
			return false, nil
		}
		if returnResult {
			protoRes, err := e.makeAnalyzeDocumentResult(analyzeResults)
			if err != nil {
				return false, err
			}
			result.Result = protoRes
		} else {
			result.Result = e.makeSuccessResult()
		}
		sender.Send(result)
		return true, nil
	default:
		return false, status.Error(codes.Unimplemented, "unknown command type")
	}
}
