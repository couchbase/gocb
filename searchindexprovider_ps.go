package gocb

import (
	"context"
	"encoding/json"
	"time"

	"github.com/couchbase/goprotostellar/genproto/admin_search_v1"
)

type searchIndexProviderPs struct {
	provider admin_search_v1.SearchAdminServiceClient

	defaultTimeout time.Duration
	tracer         RequestTracer
	meter          *meterWrapper
}

var _ searchIndexProvider = (*searchIndexProviderPs)(nil)

func (sip *searchIndexProviderPs) GetAllIndexes(opts *GetAllSearchIndexOptions) ([]SearchIndex, error) {
	start := time.Now()
	defer sip.meter.ValueRecord(meterValueServiceManagement, "manager_search_get_all_indexes", start)

	span := createSpan(sip.tracer, opts.ParentSpan, "manager_search_get_all_indexes", "management")
	span.SetAttribute("db.operation", "GetAllIndexes")
	defer span.End()

	req := &admin_search_v1.ListIndexesRequest{}

	ctx, cancel := sip.makeReqContext(opts.Context, opts.Timeout)
	defer cancel()

	resp, err := sip.provider.ListIndexes(ctx, req)
	if err != nil {
		return nil, mapPsErrorToGocbError(err, true)
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
	start := time.Now()
	defer sip.meter.ValueRecord(meterValueServiceManagement, "manager_search_get_index", start)

	span := createSpan(sip.tracer, opts.ParentSpan, "manager_search_get_index", "management")
	span.SetAttribute("db.operation", "GetIndex")
	defer span.End()

	req := &admin_search_v1.GetIndexRequest{
		Name: indexName,
	}

	ctx, cancel := sip.makeReqContext(opts.Context, opts.Timeout)
	defer cancel()

	resp, err := sip.provider.GetIndex(ctx, req)
	if err != nil {
		return nil, sip.makeRespError(err, true)
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
	start := time.Now()
	defer sip.meter.ValueRecord(meterValueServiceManagement, "manager_search_upsert_index", start)

	span := createSpan(sip.tracer, opts.ParentSpan, "manager_search_upsert_index", "management")
	defer span.End()

	ctx, cancel := sip.makeReqContext(opts.Context, opts.Timeout)
	defer cancel()

	if indexDefinition.UUID == "" {
		return sip.createIndex(ctx, span, indexDefinition)
	}

	return sip.updateIndex(ctx, span, indexDefinition)
}

func (sip *searchIndexProviderPs) updateIndex(ctx context.Context, parentSpan RequestSpan, index SearchIndex) error {
	span := createSpan(sip.tracer, parentSpan, "manager_search_update_index", "management")
	span.SetAttribute("db.operation", "UpdateIndex")
	defer span.End()

	req := &admin_search_v1.UpdateIndexRequest{}
	var err error
	req.Index, err = sip.makeIndex(index)
	if err != nil {
		return err
	}

	_, err = sip.provider.UpdateIndex(ctx, req)
	if err != nil {
		return sip.makeRespError(err, false)
	}

	return nil
}

func (sip *searchIndexProviderPs) createIndex(ctx context.Context, parentSpan RequestSpan, index SearchIndex) error {
	span := createSpan(sip.tracer, parentSpan, "manager_search_create_index", "management")
	span.SetAttribute("db.operation", "CreateIndex")
	defer span.End()

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

	_, err = sip.provider.CreateIndex(ctx, req)
	if err != nil {
		return sip.makeRespError(err, false)
	}

	return nil
}

func (sip *searchIndexProviderPs) DropIndex(indexName string, opts *DropSearchIndexOptions) error {
	start := time.Now()
	defer sip.meter.ValueRecord(meterValueServiceManagement, "manager_search_drop_index", start)

	span := createSpan(sip.tracer, opts.ParentSpan, "manager_search_drop_index", "management")
	span.SetAttribute("db.operation", "DeleteIndex")
	defer span.End()

	req := &admin_search_v1.DeleteIndexRequest{
		Name: indexName,
	}

	ctx, cancel := sip.makeReqContext(opts.Context, opts.Timeout)
	defer cancel()

	_, err := sip.provider.DeleteIndex(ctx, req)
	if err != nil {
		return sip.makeRespError(err, false)
	}
	return nil
}

func (sip *searchIndexProviderPs) AnalyzeDocument(indexName string, doc interface{}, opts *AnalyzeDocumentOptions) ([]interface{}, error) {
	start := time.Now()
	defer sip.meter.ValueRecord(meterValueServiceManagement, "manager_search_analyze_document", start)

	span := createSpan(sip.tracer, opts.ParentSpan, "manager_search_analyze_document", "management")
	span.SetAttribute("db.operation", "AnalyzeDocument")
	defer span.End()

	b, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	req := &admin_search_v1.AnalyzeDocumentRequest{
		Name: indexName,
		Doc:  b,
	}

	ctx, cancel := sip.makeReqContext(opts.Context, opts.Timeout)
	defer cancel()

	resp, err := sip.provider.AnalyzeDocument(ctx, req)
	if err != nil {
		return nil, sip.makeRespError(err, true)
	}

	var analyzed []interface{}
	err = json.Unmarshal(resp.Analyzed, &analyzed)
	if err != nil {
		return nil, sip.makeRespError(err, true)
	}

	return analyzed, nil
}

func (sip *searchIndexProviderPs) GetIndexedDocumentsCount(indexName string, opts *GetIndexedDocumentsCountOptions) (uint64, error) {
	start := time.Now()
	defer sip.meter.ValueRecord(meterValueServiceManagement, "manager_search_get_indexed_documents_count", start)

	span := createSpan(sip.tracer, opts.ParentSpan, "manager_search_get_indexed_documents_count", "management")
	span.SetAttribute("db.operation", "GetIndexedDocumentsCount")
	defer span.End()

	req := &admin_search_v1.GetIndexedDocumentsCountRequest{
		Name: indexName,
	}

	ctx, cancel := sip.makeReqContext(opts.Context, opts.Timeout)
	defer cancel()

	resp, err := sip.provider.GetIndexedDocumentsCount(ctx, req)
	if err != nil {
		return 0, sip.makeRespError(err, true)
	}

	return resp.Count, nil
}

func (sip *searchIndexProviderPs) PauseIngest(indexName string, opts *PauseIngestSearchIndexOptions) error {
	start := time.Now()
	defer sip.meter.ValueRecord(meterValueServiceManagement, "manager_search_pause_ingest", start)

	span := createSpan(sip.tracer, opts.ParentSpan, "manager_search_pause_ingest", "management")
	span.SetAttribute("db.operation", "PauseIndexIngest")
	defer span.End()

	req := &admin_search_v1.PauseIndexIngestRequest{
		Name: indexName,
	}

	ctx, cancel := sip.makeReqContext(opts.Context, opts.Timeout)
	defer cancel()

	_, err := sip.provider.PauseIndexIngest(ctx, req)
	if err != nil {
		return sip.makeRespError(err, false)
	}
	return nil
}

func (sip *searchIndexProviderPs) ResumeIngest(indexName string, opts *ResumeIngestSearchIndexOptions) error {
	start := time.Now()
	defer sip.meter.ValueRecord(meterValueServiceManagement, "manager_search_resume_ingest", start)

	span := createSpan(sip.tracer, opts.ParentSpan, "manager_search_resume_ingest", "management")
	span.SetAttribute("db.operation", "ResumeIndexIngest")
	defer span.End()

	req := &admin_search_v1.ResumeIndexIngestRequest{
		Name: indexName,
	}

	ctx, cancel := sip.makeReqContext(opts.Context, opts.Timeout)
	defer cancel()

	_, err := sip.provider.ResumeIndexIngest(ctx, req)
	if err != nil {
		return sip.makeRespError(err, false)
	}
	return nil
}

func (sip *searchIndexProviderPs) AllowQuerying(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	start := time.Now()
	defer sip.meter.ValueRecord(meterValueServiceManagement, "manager_search_allow_querying", start)

	span := createSpan(sip.tracer, opts.ParentSpan, "manager_search_allow_querying", "management")
	span.SetAttribute("db.operation", "AllowIndexQuerying")
	defer span.End()

	req := &admin_search_v1.AllowIndexQueryingRequest{
		Name: indexName,
	}

	ctx, cancel := sip.makeReqContext(opts.Context, opts.Timeout)
	defer cancel()

	_, err := sip.provider.AllowIndexQuerying(ctx, req)
	if err != nil {
		return sip.makeRespError(err, false)
	}
	return nil
}

func (sip *searchIndexProviderPs) DisallowQuerying(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	start := time.Now()
	defer sip.meter.ValueRecord(meterValueServiceManagement, "manager_search_disallow_querying", start)

	span := createSpan(sip.tracer, opts.ParentSpan, "manager_search_disallow_querying", "management")
	span.SetAttribute("db.operation", "DisallowIndexQuerying")
	defer span.End()

	req := &admin_search_v1.DisallowIndexQueryingRequest{
		Name: indexName,
	}

	ctx, cancel := sip.makeReqContext(opts.Context, opts.Timeout)
	defer cancel()

	_, err := sip.provider.DisallowIndexQuerying(ctx, req)
	if err != nil {
		return sip.makeRespError(err, false)
	}
	return nil
}

func (sip *searchIndexProviderPs) FreezePlan(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	start := time.Now()
	defer sip.meter.ValueRecord(meterValueServiceManagement, "manager_search_freeze_plan", start)

	span := createSpan(sip.tracer, opts.ParentSpan, "manager_search_freeze_plan", "management")
	span.SetAttribute("db.operation", "DisallowQuerying")
	defer span.End()

	req := &admin_search_v1.FreezeIndexPlanRequest{
		Name: indexName,
	}

	ctx, cancel := sip.makeReqContext(opts.Context, opts.Timeout)
	defer cancel()

	_, err := sip.provider.FreezeIndexPlan(ctx, req)
	if err != nil {
		return sip.makeRespError(err, false)
	}
	return nil
}

func (sip *searchIndexProviderPs) UnfreezePlan(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	start := time.Now()
	defer sip.meter.ValueRecord(meterValueServiceManagement, "manager_search_unfreeze_plan", start)

	span := createSpan(sip.tracer, opts.ParentSpan, "manager_search_unfreeze_plan", "management")
	span.SetAttribute("db.operation", "UnfreezeIndexPlan")
	defer span.End()

	req := &admin_search_v1.UnfreezeIndexPlanRequest{
		Name: indexName,
	}

	ctx, cancel := sip.makeReqContext(opts.Context, opts.Timeout)
	defer cancel()

	_, err := sip.provider.UnfreezeIndexPlan(ctx, req)
	if err != nil {
		return sip.makeRespError(err, false)
	}
	return nil
}

func (sip *searchIndexProviderPs) makeReqContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 {
		timeout = sip.defaultTimeout
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, timeout)
}

func (sip *searchIndexProviderPs) makeRespError(err error, readOnly bool) error {
	return mapPsErrorToGocbError(err, readOnly)
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
