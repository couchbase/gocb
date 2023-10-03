package gocb

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/couchbase/goprotostellar/genproto/admin_search_v1"
)

type searchIndexProviderPs struct {
	provider admin_search_v1.SearchAdminServiceClient

	managerProvider *psOpManagerProvider
}

var _ searchIndexProvider = (*searchIndexProviderPs)(nil)

func (sip *searchIndexProviderPs) newOpManager(parentSpan RequestSpan, opName string, attribs map[string]interface{}) *psOpManager {
	return sip.managerProvider.NewManager(parentSpan, opName, attribs)
}

func (sip *searchIndexProviderPs) GetAllIndexes(opts *GetAllSearchIndexOptions) ([]SearchIndex, error) {
	manager := sip.newOpManager(opts.ParentSpan, "manager_search_get_all_indexes", map[string]interface{}{
		"db.operation": "ListIndexes",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(true)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return nil, err
	}

	req := &admin_search_v1.ListIndexesRequest{}

	src, err := manager.Wrap(func(ctx context.Context) (interface{}, error) {
		return sip.provider.ListIndexes(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	resp, ok := src.(*admin_search_v1.ListIndexesResponse)
	if !ok {
		return nil, errors.New("response was not expected type, please file a bug")
	}

	indexes := make([]SearchIndex, len(resp.Indexes))
	for i, idx := range resp.Indexes {
		params, err := deserializeBytesMap(idx.Params)
		if err != nil {
			return nil, makeGenericError(err, nil)
		}
		sourceParams, err := deserializeBytesMap(idx.Params)
		if err != nil {
			return nil, makeGenericError(err, nil)
		}
		planParams, err := deserializeBytesMap(idx.Params)
		if err != nil {
			return nil, makeGenericError(err, nil)
		}

		index := SearchIndex{
			UUID:         idx.Uuid,
			Name:         idx.Name,
			SourceName:   idx.GetSourceName(),
			Type:         idx.Type,
			Params:       params,
			SourceUUID:   idx.GetSourceUuid(),
			SourceParams: sourceParams,
			SourceType:   idx.GetSourceType(),
			PlanParams:   planParams,
		}

		indexes[i] = index
	}

	return indexes, nil
}

func (sip *searchIndexProviderPs) GetIndex(indexName string, opts *GetSearchIndexOptions) (*SearchIndex, error) {
	manager := sip.newOpManager(opts.ParentSpan, "manager_search_get_index", map[string]interface{}{
		"db.operation": "GetIndex",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(true)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return nil, err
	}

	req := &admin_search_v1.GetIndexRequest{
		Name: indexName,
	}

	src, err := manager.Wrap(func(ctx context.Context) (interface{}, error) {
		return sip.provider.GetIndex(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	resp, ok := src.(*admin_search_v1.GetIndexResponse)
	if !ok {
		return nil, errors.New("response was not expected type, please file a bug")
	}

	idx := resp.Index

	params, err := deserializeBytesMap(idx.Params)
	if err != nil {
		return nil, makeGenericError(err, nil)
	}
	sourceParams, err := deserializeBytesMap(idx.Params)
	if err != nil {
		return nil, makeGenericError(err, nil)
	}
	planParams, err := deserializeBytesMap(idx.Params)
	if err != nil {
		return nil, makeGenericError(err, nil)
	}

	return &SearchIndex{
		UUID:         idx.Uuid,
		Name:         idx.Name,
		SourceName:   idx.GetSourceName(),
		Type:         idx.Type,
		Params:       params,
		SourceUUID:   idx.GetSourceUuid(),
		SourceParams: sourceParams,
		SourceType:   idx.GetSourceType(),
		PlanParams:   planParams,
	}, nil
}

func (sip *searchIndexProviderPs) UpsertIndex(indexDefinition SearchIndex, opts *UpsertSearchIndexOptions) error {
	if indexDefinition.UUID == "" {
		return sip.createIndex(indexDefinition, opts)
	}

	return sip.updateIndex(indexDefinition, opts)
}

func (sip *searchIndexProviderPs) updateIndex(index SearchIndex, opts *UpsertSearchIndexOptions) error {
	manager := sip.newOpManager(opts.ParentSpan, "manager_search_upsert_index", map[string]interface{}{
		"db.operation": "UpdateIndex",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return err
	}

	req := &admin_search_v1.UpdateIndexRequest{}
	var err error
	req.Index, err = sip.makeIndex(index)
	if err != nil {
		return err
	}

	_, err = manager.Wrap(func(ctx context.Context) (interface{}, error) {
		return sip.provider.UpdateIndex(ctx, req)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sip *searchIndexProviderPs) createIndex(index SearchIndex, opts *UpsertSearchIndexOptions) error {
	manager := sip.newOpManager(opts.ParentSpan, "manager_search_upsert_index", map[string]interface{}{
		"db.operation": "CreateIndex",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return err
	}

	req := &admin_search_v1.CreateIndexRequest{
		Name: index.Name,
		Type: index.Type,
	}

	if index.SourceName != "" {
		req.SourceName = &index.SourceName
	}

	if index.SourceType != "" {
		req.SourceType = &index.SourceType
	}

	if index.SourceUUID != "" {
		req.SourceName = &index.SourceUUID
	}

	var err error
	req.Params, err = serializeBytesMap(index.Params)
	if err != nil {
		return err
	}

	req.PlanParams, err = serializeBytesMap(index.PlanParams)
	if err != nil {
		return err
	}

	req.SourceParams, err = serializeBytesMap(index.SourceParams)
	if err != nil {
		return err
	}

	_, err = manager.Wrap(func(ctx context.Context) (interface{}, error) {
		return sip.provider.CreateIndex(ctx, req)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sip *searchIndexProviderPs) DropIndex(indexName string, opts *DropSearchIndexOptions) error {
	manager := sip.newOpManager(opts.ParentSpan, "manager_search_drop_index", map[string]interface{}{
		"db.operation": "DeleteIndex",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return err
	}

	req := &admin_search_v1.DeleteIndexRequest{
		Name: indexName,
	}

	_, err := manager.Wrap(func(ctx context.Context) (interface{}, error) {
		return sip.provider.DeleteIndex(ctx, req)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sip *searchIndexProviderPs) AnalyzeDocument(indexName string, doc interface{}, opts *AnalyzeDocumentOptions) ([]interface{}, error) {
	manager := sip.newOpManager(opts.ParentSpan, "manager_search_analyze_document", map[string]interface{}{
		"db.operation": "AnalyzeDocument",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(true)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	if err := manager.CheckReadyForOp(); err != nil {
		return nil, err
	}

	b, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	req := &admin_search_v1.AnalyzeDocumentRequest{
		Name: indexName,
		Doc:  b,
	}
	src, err := manager.Wrap(func(ctx context.Context) (interface{}, error) {
		return sip.provider.AnalyzeDocument(ctx, req)
	})
	if err != nil {
		return nil, err
	}

	resp, ok := src.(*admin_search_v1.AnalyzeDocumentResponse)
	if !ok {
		return nil, errors.New("response was not expected type, please file a bug")
	}

	var analyzed []interface{}
	err = json.Unmarshal(resp.Analyzed, &analyzed)
	if err != nil {
		return nil, err
	}

	return analyzed, nil
}

func (sip *searchIndexProviderPs) GetIndexedDocumentsCount(indexName string, opts *GetIndexedDocumentsCountOptions) (uint64, error) {
	manager := sip.newOpManager(opts.ParentSpan, "manager_search_get_indexed_documents_count", map[string]interface{}{
		"db.operation": "GetIndexedDocumentsCount",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(true)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	req := &admin_search_v1.GetIndexedDocumentsCountRequest{
		Name: indexName,
	}

	src, err := manager.Wrap(func(ctx context.Context) (interface{}, error) {
		return sip.provider.GetIndexedDocumentsCount(ctx, req)
	})
	if err != nil {
		return 0, err
	}

	resp, ok := src.(*admin_search_v1.GetIndexedDocumentsCountResponse)
	if !ok {
		return 0, errors.New("response was not expected type, please file a bug")
	}

	return resp.Count, nil
}

func (sip *searchIndexProviderPs) PauseIngest(indexName string, opts *PauseIngestSearchIndexOptions) error {
	manager := sip.newOpManager(opts.ParentSpan, "manager_search_pause_ingest", map[string]interface{}{
		"db.operation": "PauseIndexIngest",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	req := &admin_search_v1.PauseIndexIngestRequest{
		Name: indexName,
	}

	_, err := manager.Wrap(func(ctx context.Context) (interface{}, error) {
		return sip.provider.PauseIndexIngest(ctx, req)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sip *searchIndexProviderPs) ResumeIngest(indexName string, opts *ResumeIngestSearchIndexOptions) error {
	manager := sip.newOpManager(opts.ParentSpan, "manager_search_resume_ingest", map[string]interface{}{
		"db.operation": "ResumeIndexIngest",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	req := &admin_search_v1.ResumeIndexIngestRequest{
		Name: indexName,
	}

	_, err := manager.Wrap(func(ctx context.Context) (interface{}, error) {
		return sip.provider.ResumeIndexIngest(ctx, req)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sip *searchIndexProviderPs) AllowQuerying(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	manager := sip.newOpManager(opts.ParentSpan, "manager_search_allow_querying", map[string]interface{}{
		"db.operation": "AllowIndexQuerying",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	req := &admin_search_v1.AllowIndexQueryingRequest{
		Name: indexName,
	}

	_, err := manager.Wrap(func(ctx context.Context) (interface{}, error) {
		return sip.provider.AllowIndexQuerying(ctx, req)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sip *searchIndexProviderPs) DisallowQuerying(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	manager := sip.newOpManager(opts.ParentSpan, "manager_search_disallow_querying", map[string]interface{}{
		"db.operation": "DisallowIndexQuerying",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	req := &admin_search_v1.DisallowIndexQueryingRequest{
		Name: indexName,
	}

	_, err := manager.Wrap(func(ctx context.Context) (interface{}, error) {
		return sip.provider.DisallowIndexQuerying(ctx, req)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sip *searchIndexProviderPs) FreezePlan(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	manager := sip.newOpManager(opts.ParentSpan, "manager_search_freeze_plan", map[string]interface{}{
		"db.operation": "FreezeIndexPlan",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	req := &admin_search_v1.FreezeIndexPlanRequest{
		Name: indexName,
	}

	_, err := manager.Wrap(func(ctx context.Context) (interface{}, error) {
		return sip.provider.FreezeIndexPlan(ctx, req)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sip *searchIndexProviderPs) UnfreezePlan(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	manager := sip.newOpManager(opts.ParentSpan, "manager_search_unfreeze_plan", map[string]interface{}{
		"db.operation": "UnfreezeIndexPlan",
	})
	defer manager.Finish(false)

	manager.SetContext(opts.Context)
	manager.SetIsIdempotent(false)
	manager.SetRetryStrategy(opts.RetryStrategy)
	manager.SetTimeout(opts.Timeout)

	req := &admin_search_v1.UnfreezeIndexPlanRequest{
		Name: indexName,
	}

	_, err := manager.Wrap(func(ctx context.Context) (interface{}, error) {
		return sip.provider.UnfreezeIndexPlan(ctx, req)
	})
	if err != nil {
		return err
	}

	return nil
}

func (sip *searchIndexProviderPs) makeIndex(idx SearchIndex) (*admin_search_v1.Index, error) {
	newIdx := &admin_search_v1.Index{
		Name: idx.Name,
		Type: idx.Type,
		Uuid: idx.UUID,
	}

	if idx.SourceName != "" {
		newIdx.SourceName = &idx.SourceName
	}

	if idx.SourceType != "" {
		newIdx.SourceType = &idx.SourceType
	}

	if idx.SourceUUID != "" {
		newIdx.SourceUuid = &idx.SourceUUID
	}

	var err error
	newIdx.Params, err = serializeBytesMap(idx.Params)
	if err != nil {
		return nil, err
	}

	newIdx.PlanParams, err = serializeBytesMap(idx.PlanParams)
	if err != nil {
		return nil, err
	}

	newIdx.SourceParams, err = serializeBytesMap(idx.SourceParams)
	if err != nil {
		return nil, err
	}

	return newIdx, nil
}

func serializeBytesMap(m map[string]interface{}) (map[string][]byte, error) {
	deserialized := make(map[string][]byte, len(m))
	for k, v := range m {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}

		deserialized[k] = b
	}

	return deserialized, nil
}

func deserializeBytesMap(m map[string][]byte) (map[string]interface{}, error) {
	deserialized := make(map[string]interface{}, len(m))
	for k, v := range m {
		var d interface{}
		err := json.Unmarshal(v, &d)
		if err != nil {
			return nil, err
		}

		deserialized[k] = d
	}

	return deserialized, nil
}
