package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// SearchIndexManager provides methods for performing Couchbase FTS index management.
// Experimental: This API is subject to change at any time.
type SearchIndexManager struct {
	cluster *Cluster

	tracer requestTracer
}

func (sm *SearchIndexManager) doMgmtRequest(req mgmtRequest) (*mgmtResponse, error) {
	resp, err := sm.cluster.executeMgmtRequest(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

type searchIndexDefs struct {
	IndexDefs   map[string]SearchIndex `json:"indexDefs,omitempty"`
	ImplVersion string                 `json:"implVersion,omitempty"`
}

type searchIndexResp struct {
	Status   string       `json:"status,omitempty"`
	IndexDef *SearchIndex `json:"indexDef,omitempty"`
}

type searchIndexesResp struct {
	Status    string          `json:"status,omitempty"`
	IndexDefs searchIndexDefs `json:"indexDefs,omitempty"`
}

// GetAllSearchIndexOptions is the set of options available to the search indexes GetAllIndexes operation.
type GetAllSearchIndexOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// GetAllIndexes retrieves all of the search indexes for the cluster.
func (sm *SearchIndexManager) GetAllIndexes(opts *GetAllSearchIndexOptions) ([]SearchIndex, error) {
	if opts == nil {
		opts = &GetAllSearchIndexOptions{}
	}

	span := sm.tracer.StartSpan("GetAllIndexes", nil).
		SetTag("couchbase.service", "fts")
	defer span.Finish()

	req := mgmtRequest{
		Service:       SearchService,
		Method:        "GET",
		Path:          "/api/index",
		IsIdempotent:  true,
		RetryStrategy: opts.RetryStrategy,
		Timeout:       opts.Timeout,
	}
	resp, err := sm.doMgmtRequest(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, makeMgmtBadStatusError("failed to get all indexes", &req, resp)
	}

	var indexesResp searchIndexesResp
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&indexesResp)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	indexDefs := indexesResp.IndexDefs.IndexDefs
	var indexes []SearchIndex
	for _, index := range indexDefs {
		indexes = append(indexes, index)
	}

	return indexes, nil
}

// GetSearchIndexOptions is the set of options available to the search indexes GetIndex operation.
type GetSearchIndexOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// GetIndex retrieves a specific search index by name.
func (sm *SearchIndexManager) GetIndex(indexName string, opts *GetSearchIndexOptions) (*SearchIndex, error) {
	if opts == nil {
		opts = &GetSearchIndexOptions{}
	}

	span := sm.tracer.StartSpan("GetIndex", nil).
		SetTag("couchbase.service", "fts")
	defer span.Finish()

	req := mgmtRequest{
		Service:       SearchService,
		Method:        "GET",
		Path:          fmt.Sprintf("/api/index/%s", indexName),
		IsIdempotent:  true,
		RetryStrategy: opts.RetryStrategy,
		Timeout:       opts.Timeout,
	}
	resp, err := sm.doMgmtRequest(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, makeMgmtBadStatusError("failed to get the index", &req, resp)
	}

	var indexResp searchIndexResp
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&indexResp)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return indexResp.IndexDef, nil
}

// UpsertSearchIndexOptions is the set of options available to the search index manager UpsertIndex operation.
type UpsertSearchIndexOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// SearchIndex is used to define a search index.
type SearchIndex struct {
	// UUID is required for updates. It provides a means of ensuring consistency, the UUID must match the UUID value
	// for the index on the server.
	UUID string `json:"uuid"`
	Name string `json:"name"`
	// SourceName is the name of the source of the data for the index e.g. bucket name.
	SourceName string `json:"sourceName,omitempty"`
	// Type is the type of index, e.g. fulltext-index or fulltext-alias.
	Type string `json:"type"`
	// IndexParams are index properties such as store type and mappings.
	Params map[string]interface{} `json:"params"`
	// SourceUUID is the UUID of the data source, this can be used to more tightly tie the index to a source.
	SourceUUID string `json:"sourceUUID,omitempty"`
	// SourceParams are extra parameters to be defined. These are usually things like advanced connection and tuning
	// parameters.
	SourceParams map[string]interface{} `json:"sourceParams,omitempty"`
	// SourceType is the type of the data source, e.g. couchbase or nil depending on the Type field.
	SourceType string `json:"sourceType"`
	// PlanParams are plan properties such as number of replicas and number of partitions.
	PlanParams map[string]interface{} `json:"planParams,omitempty"`
}

// UpsertIndex creates or updates a search index.
func (sm *SearchIndexManager) UpsertIndex(indexDefinition SearchIndex, opts *UpsertSearchIndexOptions) error {
	if opts == nil {
		opts = &UpsertSearchIndexOptions{}
	}

	if indexDefinition.Name == "" {
		return invalidArgumentsError{"index name cannot be empty"}
	}
	if indexDefinition.Type == "" {
		return invalidArgumentsError{"index type cannot be empty"}
	}

	span := sm.tracer.StartSpan("UpsertIndex", nil).
		SetTag("couchbase.service", "fts")
	defer span.Finish()

	b, err := json.Marshal(indexDefinition)
	if err != nil {
		return err
	}

	req := mgmtRequest{
		Service: SearchService,
		Method:  "PUT",
		Path:    fmt.Sprintf("/api/index/%s", indexDefinition.Name),
		Headers: map[string]string{
			"cache-control": "no-cache",
		},
		Body:          b,
		RetryStrategy: opts.RetryStrategy,
		Timeout:       opts.Timeout,
	}
	resp, err := sm.doMgmtRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return makeMgmtBadStatusError("failed to upsert the index", &req, resp)
	}

	return nil
}

// DropSearchIndexOptions is the set of options available to the search index DropIndex operation.
type DropSearchIndexOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// DropIndex removes the search index with the specific name.
func (sm *SearchIndexManager) DropIndex(indexName string, opts *DropSearchIndexOptions) error {
	if opts == nil {
		opts = &DropSearchIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	span := sm.tracer.StartSpan("DropIndex", nil).
		SetTag("couchbase.service", "fts")
	defer span.Finish()

	req := mgmtRequest{
		Service:       SearchService,
		Method:        "DELETE",
		Path:          fmt.Sprintf("/api/index/%s", indexName),
		RetryStrategy: opts.RetryStrategy,
		Timeout:       opts.Timeout,
	}
	resp, err := sm.doMgmtRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return makeMgmtBadStatusError("failed to drop the index", &req, resp)
	}

	return nil
}

// AnalyzeDocumentOptions is the set of options available to the search index AnalyzeDocument operation.
type AnalyzeDocumentOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// AnalyzeDocument returns how a doc is analyzed against a specific index.
func (sm *SearchIndexManager) AnalyzeDocument(indexName string, doc interface{}, opts *AnalyzeDocumentOptions) ([]interface{}, error) {
	if opts == nil {
		opts = &AnalyzeDocumentOptions{}
	}

	if indexName == "" {
		return nil, invalidArgumentsError{"indexName cannot be empty"}
	}

	span := sm.tracer.StartSpan("AnalyzeDocument", nil).
		SetTag("couchbase.service", "fts")
	defer span.Finish()

	b, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	req := mgmtRequest{
		Service:       SearchService,
		Method:        "POST",
		Path:          fmt.Sprintf("/api/index/%s/analyzeDoc", indexName),
		Body:          b,
		IsIdempotent:  true,
		RetryStrategy: opts.RetryStrategy,
		Timeout:       opts.Timeout,
	}
	resp, err := sm.doMgmtRequest(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, makeMgmtBadStatusError("failed to analyze the document", &req, resp)
	}

	var analysis struct {
		Status   string        `json:"status"`
		Analyzed []interface{} `json:"analyzed"`
	}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&analysis)
	if err != nil {
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return analysis.Analyzed, nil
}

// GetIndexedDocumentsCountOptions is the set of options available to the search index GetIndexedDocumentsCount operation.
type GetIndexedDocumentsCountOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// GetIndexedDocumentsCount retrieves the document count for a search index.
func (sm *SearchIndexManager) GetIndexedDocumentsCount(indexName string, opts *GetIndexedDocumentsCountOptions) (int, error) {
	if opts == nil {
		opts = &GetIndexedDocumentsCountOptions{}
	}

	if indexName == "" {
		return 0, invalidArgumentsError{"indexName cannot be empty"}
	}

	span := sm.tracer.StartSpan("GetIndexedDocumentsCount", nil).
		SetTag("couchbase.service", "fts")
	defer span.Finish()

	req := mgmtRequest{
		Service:       SearchService,
		Method:        "GET",
		Path:          fmt.Sprintf("/api/index/%s/count", indexName),
		IsIdempotent:  true,
		RetryStrategy: opts.RetryStrategy,
		Timeout:       opts.Timeout,
	}
	resp, err := sm.doMgmtRequest(req)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode != 200 {
		return 0, makeMgmtBadStatusError("failed to get the indexed documents count", &req, resp)
	}

	var count struct {
		Count int `json:"count"`
	}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&count)
	if err != nil {
		return 0, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return count.Count, nil
}

func (sm *SearchIndexManager) performControlRequest(
	tracectx requestSpanContext,
	method, uri string,
	timeout time.Duration,
	retryStrategy RetryStrategy,
) error {
	req := mgmtRequest{
		Service:       SearchService,
		Method:        method,
		Path:          uri,
		IsIdempotent:  true,
		Timeout:       timeout,
		RetryStrategy: retryStrategy,
	}
	resp, err := sm.doMgmtRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return makeMgmtBadStatusError("failed to perform the control request", &req, resp)
	}

	return nil
}

// PauseIngestSearchIndexOptions is the set of options available to the search index PauseIngest operation.
type PauseIngestSearchIndexOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// PauseIngest pauses updates and maintenance for an index.
func (sm *SearchIndexManager) PauseIngest(indexName string, opts *PauseIngestSearchIndexOptions) error {
	if opts == nil {
		opts = &PauseIngestSearchIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	span := sm.tracer.StartSpan("PauseIngest", nil).
		SetTag("couchbase.service", "fts")
	defer span.Finish()

	return sm.performControlRequest(
		span.Context(),
		"POST",
		fmt.Sprintf("/api/index/%s/ingestControl/pause", indexName),
		opts.Timeout,
		opts.RetryStrategy)
}

// ResumeIngestSearchIndexOptions is the set of options available to the search index ResumeIngest operation.
type ResumeIngestSearchIndexOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// ResumeIngest resumes updates and maintenance for an index.
func (sm *SearchIndexManager) ResumeIngest(indexName string, opts *ResumeIngestSearchIndexOptions) error {
	if opts == nil {
		opts = &ResumeIngestSearchIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	span := sm.tracer.StartSpan("ResumeIngest", nil).
		SetTag("couchbase.service", "fts")
	defer span.Finish()

	return sm.performControlRequest(
		span.Context(),
		"POST",
		fmt.Sprintf("/api/index/%s/ingestControl/resume", indexName),
		opts.Timeout,
		opts.RetryStrategy)
}

// AllowQueryingSearchIndexOptions is the set of options available to the search index AllowQuerying operation.
type AllowQueryingSearchIndexOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// AllowQuerying allows querying against an index.
func (sm *SearchIndexManager) AllowQuerying(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	if opts == nil {
		opts = &AllowQueryingSearchIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	span := sm.tracer.StartSpan("AllowQuerying", nil).
		SetTag("couchbase.service", "fts")
	defer span.Finish()

	return sm.performControlRequest(
		span.Context(),
		"POST",
		fmt.Sprintf("/api/index/%s/queryControl/allow", indexName),
		opts.Timeout,
		opts.RetryStrategy)
}

// DisallowQueryingSearchIndexOptions is the set of options available to the search index DisallowQuerying operation.
type DisallowQueryingSearchIndexOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// DisallowQuerying disallows querying against an index.
func (sm *SearchIndexManager) DisallowQuerying(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	if opts == nil {
		opts = &AllowQueryingSearchIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	span := sm.tracer.StartSpan("DisallowQuerying", nil).
		SetTag("couchbase.service", "fts")
	defer span.Finish()

	return sm.performControlRequest(
		span.Context(),
		"POST",
		fmt.Sprintf("/api/index/%s/queryControl/disallow", indexName),
		opts.Timeout,
		opts.RetryStrategy)
}

// FreezePlanSearchIndexOptions is the set of options available to the search index FreezePlan operation.
type FreezePlanSearchIndexOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// FreezePlan freezes the assignment of index partitions to nodes.
func (sm *SearchIndexManager) FreezePlan(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	if opts == nil {
		opts = &AllowQueryingSearchIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	span := sm.tracer.StartSpan("FreezePlan", nil).
		SetTag("couchbase.service", "fts")
	defer span.Finish()

	return sm.performControlRequest(
		span.Context(),
		"POTS",
		fmt.Sprintf("/api/index/%s/planFreezeControl/freeze", indexName),
		opts.Timeout,
		opts.RetryStrategy)
}

// UnfreezePlanSearchIndexOptions is the set of options available to the search index UnfreezePlan operation.
type UnfreezePlanSearchIndexOptions struct {
	Timeout       time.Duration
	Context       context.Context
	RetryStrategy RetryStrategy
}

// UnfreezePlan unfreezes the assignment of index partitions to nodes.
func (sm *SearchIndexManager) UnfreezePlan(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	if opts == nil {
		opts = &AllowQueryingSearchIndexOptions{}
	}

	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	span := sm.tracer.StartSpan("UnfreezePlan", nil).
		SetTag("couchbase.service", "fts")
	defer span.Finish()

	return sm.performControlRequest(
		span.Context(),
		"POST",
		fmt.Sprintf("/api/index/%s/planFreezeControl/unfreeze", indexName),
		opts.Timeout,
		opts.RetryStrategy)
}
