package executor

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/run"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk"

	searchmgrpb "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/search/index-manager"
)

func (e *Executor) makeGetAllSearchIndexResult(indexes []gocb.SearchIndex) (*run.Result_Sdk, error) {
	protoIndexes := &searchmgrpb.SearchIndexes{}
	for _, index := range indexes {
		protoIndex, err := fromGocbSearchIndex(&index)
		if err != nil {
			return nil, err
		}
		protoIndexes.Indexes = append(protoIndexes.Indexes, protoIndex)
	}
	return &run.Result_Sdk{
		Sdk: &sdk.Result{
			Result: &sdk.Result_SearchIndexManagerResult{
				SearchIndexManagerResult: &searchmgrpb.Result{
					Result: &searchmgrpb.Result_Indexes{Indexes: protoIndexes},
				},
			},
		},
	}, nil
}

func (e *Executor) makeGetSearchIndexResult(index *gocb.SearchIndex) (*run.Result_Sdk, error) {
	protoIndex, err := fromGocbSearchIndex(index)
	if err != nil {
		return nil, err
	}
	return &run.Result_Sdk{
		Sdk: &sdk.Result{
			Result: &sdk.Result_SearchIndexManagerResult{
				SearchIndexManagerResult: &searchmgrpb.Result{
					Result: &searchmgrpb.Result_Index{Index: protoIndex},
				},
			},
		},
	}, nil
}

func (e *Executor) makeGetIndexedDocumentCountResult(count uint64) (*run.Result_Sdk, error) {
	return &run.Result_Sdk{
		Sdk: &sdk.Result{
			Result: &sdk.Result_SearchIndexManagerResult{
				SearchIndexManagerResult: &searchmgrpb.Result{
					Result: &searchmgrpb.Result_IndexedDocumentCounts{IndexedDocumentCounts: int32(count)},
				},
			},
		},
	}, nil
}

func (e *Executor) makeAnalyzeDocumentResult(analyzeResults []interface{}) (*run.Result_Sdk, error) {
	protoAnalyzeResult := &searchmgrpb.AnalyzeDocumentResult{}
	for _, r := range analyzeResults {
		bytesRes, err := json.Marshal(r)
		if err != nil {
			return nil, err
		}
		protoAnalyzeResult.Results = append(protoAnalyzeResult.Results, bytesRes)
	}
	return &run.Result_Sdk{
		Sdk: &sdk.Result{
			Result: &sdk.Result_SearchIndexManagerResult{
				SearchIndexManagerResult: &searchmgrpb.Result{
					Result: &searchmgrpb.Result_AnalyzeDocument{AnalyzeDocument: protoAnalyzeResult},
				},
			},
		},
	}, nil
}

func (e *Executor) parseGetSearchIndexOptions(cmd *searchmgrpb.GetIndex) (*gocb.GetSearchIndexOptions, error) {
	if cmd.GetOptions() == nil {
		return nil, nil
	}
	opts := &gocb.GetSearchIndexOptions{
		Timeout: time.Duration(cmd.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
	}

	if cmd.GetOptions().GetParentSpanId() != "" {
		parent, ok := e.spanOwner.GetSpan(cmd.GetOptions().GetParentSpanId())
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *cmd.Options.ParentSpanId)
		}
		opts.ParentSpan = parent
	}

	return opts, nil
}

func (e *Executor) parseGetAllSearchIndexOptions(cmd *searchmgrpb.GetAllIndexes) (*gocb.GetAllSearchIndexOptions, error) {
	if cmd.GetOptions() == nil {
		return nil, nil
	}
	opts := &gocb.GetAllSearchIndexOptions{
		Timeout: time.Duration(cmd.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
	}
	if cmd.GetOptions().GetParentSpanId() != "" {
		parent, ok := e.spanOwner.GetSpan(cmd.GetOptions().GetParentSpanId())
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *cmd.Options.ParentSpanId)
		}
		opts.ParentSpan = parent
	}

	return opts, nil
}

func (e *Executor) parseUpsertSearchIndexOptions(cmd *searchmgrpb.UpsertIndex) (*gocb.UpsertSearchIndexOptions, error) {
	if cmd.GetOptions() == nil {
		return nil, nil
	}
	opts := &gocb.UpsertSearchIndexOptions{
		Timeout: time.Duration(cmd.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
	}
	if cmd.GetOptions().GetParentSpanId() != "" {
		parent, ok := e.spanOwner.GetSpan(cmd.GetOptions().GetParentSpanId())
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *cmd.Options.ParentSpanId)
		}
		opts.ParentSpan = parent
	}

	return opts, nil
}

func (e *Executor) parseDropSearchIndexOptions(cmd *searchmgrpb.DropIndex) (*gocb.DropSearchIndexOptions, error) {
	if cmd.GetOptions() == nil {
		return nil, nil
	}
	opts := &gocb.DropSearchIndexOptions{
		Timeout: time.Duration(cmd.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
	}
	if cmd.GetOptions().GetParentSpanId() != "" {
		parent, ok := e.spanOwner.GetSpan(cmd.GetOptions().GetParentSpanId())
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *cmd.Options.ParentSpanId)
		}
		opts.ParentSpan = parent
	}

	return opts, nil
}

func (e *Executor) parseGetIndexedDocumentsCountOptions(cmd *searchmgrpb.GetIndexedDocumentsCount) (*gocb.GetIndexedDocumentsCountOptions, error) {
	if cmd.GetOptions() == nil {
		return nil, nil
	}
	opts := &gocb.GetIndexedDocumentsCountOptions{
		Timeout: time.Duration(cmd.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
	}
	if cmd.GetOptions().GetParentSpanId() != "" {
		parent, ok := e.spanOwner.GetSpan(cmd.GetOptions().GetParentSpanId())
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *cmd.Options.ParentSpanId)
		}
		opts.ParentSpan = parent
	}

	return opts, nil
}

func (e *Executor) parsePauseIngestSearchIndexOptions(cmd *searchmgrpb.PauseIngest) (*gocb.PauseIngestSearchIndexOptions, error) {
	if cmd.GetOptions() == nil {
		return nil, nil
	}
	opts := &gocb.PauseIngestSearchIndexOptions{
		Timeout: time.Duration(cmd.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
	}
	if cmd.GetOptions().GetParentSpanId() != "" {
		parent, ok := e.spanOwner.GetSpan(cmd.GetOptions().GetParentSpanId())
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *cmd.Options.ParentSpanId)
		}
		opts.ParentSpan = parent
	}
	return opts, nil
}

func (e *Executor) parseResumeIngestSearchIndexOptions(cmd *searchmgrpb.ResumeIngest) (*gocb.ResumeIngestSearchIndexOptions, error) {
	if cmd.GetOptions() == nil {
		return nil, nil
	}
	opts := &gocb.ResumeIngestSearchIndexOptions{
		Timeout: time.Duration(cmd.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
	}
	if cmd.GetOptions().GetParentSpanId() != "" {
		parent, ok := e.spanOwner.GetSpan(cmd.GetOptions().GetParentSpanId())
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *cmd.Options.ParentSpanId)
		}
		opts.ParentSpan = parent
	}

	return opts, nil
}

func (e *Executor) parseAllowQueryingSearchIndexOptions(cmd *searchmgrpb.AllowQuerying) (*gocb.AllowQueryingSearchIndexOptions, error) {
	if cmd.GetOptions() == nil {
		return nil, nil
	}
	opts := &gocb.AllowQueryingSearchIndexOptions{
		Timeout: time.Duration(cmd.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
	}
	if cmd.GetOptions().GetParentSpanId() != "" {
		parent, ok := e.spanOwner.GetSpan(cmd.GetOptions().GetParentSpanId())
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *cmd.Options.ParentSpanId)
		}
		opts.ParentSpan = parent
	}

	return opts, nil
}

func (e *Executor) parseDisallowQueryingSearchIndexOptions(cmd *searchmgrpb.DisallowQuerying) (*gocb.DisallowQueryingSearchIndexOptions, error) {
	if cmd.GetOptions() == nil {
		return nil, nil
	}
	opts := &gocb.DisallowQueryingSearchIndexOptions{
		Timeout: time.Duration(cmd.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
	}
	if cmd.GetOptions().GetParentSpanId() != "" {
		parent, ok := e.spanOwner.GetSpan(cmd.GetOptions().GetParentSpanId())
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *cmd.Options.ParentSpanId)
		}
		opts.ParentSpan = parent
	}

	return opts, nil
}

func (e *Executor) parseFreezePlanSearchIndexOptions(cmd *searchmgrpb.FreezePlan) (*gocb.FreezePlanSearchIndexOptions, error) {
	if cmd.GetOptions() == nil {
		return nil, nil
	}
	opts := &gocb.FreezePlanSearchIndexOptions{
		Timeout: time.Duration(cmd.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
	}
	if cmd.GetOptions().GetParentSpanId() != "" {
		parent, ok := e.spanOwner.GetSpan(cmd.GetOptions().GetParentSpanId())
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *cmd.Options.ParentSpanId)
		}
		opts.ParentSpan = parent
	}

	return opts, nil
}

func (e *Executor) parseUnfreezePlanSearchIndexOptions(cmd *searchmgrpb.UnfreezePlan) (*gocb.UnfreezePlanSearchIndexOptions, error) {
	if cmd.GetOptions() == nil {
		return nil, nil
	}
	opts := &gocb.UnfreezePlanSearchIndexOptions{
		Timeout: time.Duration(cmd.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
	}
	if cmd.GetOptions().GetParentSpanId() != "" {
		parent, ok := e.spanOwner.GetSpan(cmd.GetOptions().GetParentSpanId())
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *cmd.Options.ParentSpanId)
		}
		opts.ParentSpan = parent
	}

	return opts, nil
}

func (e *Executor) parseAnalyzeDocumentOptions(cmd *searchmgrpb.AnalyzeDocument) (*gocb.AnalyzeDocumentOptions, error) {
	if cmd.GetOptions() == nil {
		return nil, nil
	}
	opts := &gocb.AnalyzeDocumentOptions{
		Timeout: time.Duration(cmd.GetOptions().GetTimeoutMsecs()) * time.Millisecond,
	}
	if cmd.GetOptions().GetParentSpanId() != "" {
		parent, ok := e.spanOwner.GetSpan(cmd.GetOptions().GetParentSpanId())
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *cmd.Options.ParentSpanId)
		}
		opts.ParentSpan = parent
	}

	return opts, nil
}

func fromGocbSearchIndex(index *gocb.SearchIndex) (*searchmgrpb.SearchIndex, error) {
	params, err := json.Marshal(index.Params)
	if err != nil {
		return nil, err
	}
	sourceParams, err := json.Marshal(index.SourceParams)
	if err != nil {
		return nil, err
	}
	planParams, err := json.Marshal(index.PlanParams)
	if err != nil {
		return nil, err
	}

	return &searchmgrpb.SearchIndex{
		Uuid:         index.UUID,
		Name:         index.Name,
		Type:         index.Type,
		SourceUuid:   index.SourceUUID,
		SourceType:   index.SourceType,
		Params:       params,
		SourceParams: sourceParams,
		PlanParams:   planParams,
	}, nil
}

func toGocbSearchIndex(encodedIndex []byte) (*gocb.SearchIndex, error) {
	var index struct {
		UUID         string                 `json:"uuid,omitempty"`
		Name         string                 `json:"name,omitempty"`
		Type         string                 `json:"type,omitempty"`
		SourceUUID   string                 `json:"sourceUUID,omitempty"`
		SourceType   string                 `json:"sourceType,omitempty"`
		SourceName   string                 `json:"sourceName,omitempty"`
		Params       map[string]interface{} `json:"params,omitempty"`
		SourceParams map[string]interface{} `json:"sourceParams,omitempty"`
		PlanParams   map[string]interface{} `json:"planParams,omitempty"`
	}
	err := json.Unmarshal(encodedIndex, &index)
	if err != nil {
		return nil, err
	}
	return &gocb.SearchIndex{
		UUID:       index.UUID,
		Name:       index.Name,
		Type:       index.Type,
		SourceUUID: index.SourceUUID,
		SourceType: index.SourceType,
		SourceName: index.SourceName,
		Params:     index.Params,
		PlanParams: index.PlanParams,
	}, nil
}
