package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/couchbase/gocbcore/v8"
)

// SearchIndexManager provides methods for performing Couchbase FTS index management.
// Experimental: This API is subject to change at any time.
type SearchIndexManager struct {
	httpClient httpProvider
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
	Timeout time.Duration
	Context context.Context
}

// GetAllIndexes retrieves all of the search indexes for the cluster.
func (sim *SearchIndexManager) GetAllIndexes(opts *GetAllSearchIndexOptions) ([]SearchIndex, error) {
	if opts == nil {
		opts = &GetAllSearchIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(SearchService),
		Method:  "GET",
		Path:    "/api/index",
		Context: ctx,
	}

	res, err := sim.httpClient.DoHttpRequest(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		data, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}
		err = res.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return nil, searchIndexError{message: string(data), statusCode: res.StatusCode}
	}

	var indexesResp searchIndexesResp
	jsonDec := json.NewDecoder(res.Body)
	err = jsonDec.Decode(&indexesResp)
	if err != nil {
		return nil, err
	}

	err = res.Body.Close()
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
	Timeout time.Duration
	Context context.Context
}

// GetIndex retrieves a specific search index by name.
func (sim *SearchIndexManager) GetIndex(indexName string, opts *GetSearchIndexOptions) (*SearchIndex, error) {
	if opts == nil {
		opts = &GetSearchIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(SearchService),
		Method:  "GET",
		Path:    fmt.Sprintf("/api/index/%s", indexName),
		Context: ctx,
	}
	resp, err := sim.httpClient.DoHttpRequest(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return nil, searchIndexError{message: string(data), statusCode: resp.StatusCode}
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
	Timeout time.Duration
	Context context.Context
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
func (sim *SearchIndexManager) UpsertIndex(indexDefinition SearchIndex, opts *UpsertSearchIndexOptions) error {
	if indexDefinition.Name == "" {
		return invalidArgumentsError{"index name cannot be empty"}
	}
	if indexDefinition.Type == "" {
		return invalidArgumentsError{"index type cannot be empty"}
	}

	if opts == nil {
		opts = &UpsertSearchIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	b, err := json.Marshal(indexDefinition)
	if err != nil {
		return err
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(SearchService),
		Method:  "PUT",
		Path:    fmt.Sprintf("/api/index/%s", indexDefinition.Name),
		Headers: make(map[string]string),
		Context: ctx,
		Body:    b,
	}
	req.Headers["cache-control"] = "no-cache"

	res, err := sim.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		data, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		err = res.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return searchIndexError{message: string(data), statusCode: res.StatusCode}
	}

	err = res.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

// DropSearchIndexOptions is the set of options available to the search index DropIndex operation.
type DropSearchIndexOptions struct {
	Timeout time.Duration
	Context context.Context
}

// DropIndex removes the search index with the specific name.
func (sim *SearchIndexManager) DropIndex(indexName string, opts *DropSearchIndexOptions) error {
	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	if opts == nil {
		opts = &DropSearchIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(SearchService),
		Method:  "DELETE",
		Path:    fmt.Sprintf("/api/index/%s", indexName),
		Context: ctx,
	}
	res, err := sim.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		data, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		err = res.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return searchIndexError{message: string(data), statusCode: res.StatusCode}
	}

	err = res.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

// AnalyzeDocOptions is the set of options available to the search index AnalyzeDoc operation.
type AnalyzeDocOptions struct {
	Timeout time.Duration
	Context context.Context
}

// AnalyzeDoc returns how a doc is analyzed against a specific index.
func (sim *SearchIndexManager) AnalyzeDoc(indexName string, doc interface{}, opts *AnalyzeDocOptions) (interface{}, error) {
	if indexName == "" {
		return nil, invalidArgumentsError{"indexName cannot be empty"}
	}

	if opts == nil {
		opts = &AnalyzeDocOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	b, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(SearchService),
		Method:  "POST",
		Path:    fmt.Sprintf("/api/index/%s/analyzeDoc", indexName),
		Context: ctx,
		Body:    b,
	}
	res, err := sim.httpClient.DoHttpRequest(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		data, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}
		err = res.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return nil, searchIndexError{message: string(data), statusCode: res.StatusCode}
	}

	var analysis interface{}
	jsonDec := json.NewDecoder(res.Body)
	err = jsonDec.Decode(&analysis)
	if err != nil {
		return nil, err
	}

	err = res.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return analysis, nil
}

// GetIndexedDocumentsCountOptions is the set of options available to the search index GetIndexedDocumentsCount operation.
type GetIndexedDocumentsCountOptions struct {
	Timeout time.Duration
	Context context.Context
}

// GetIndexedDocumentsCount retrieves the document count for a search index.
func (sim *SearchIndexManager) GetIndexedDocumentsCount(indexName string, opts *GetIndexedDocumentsCountOptions) (int, error) {
	if indexName == "" {
		return 0, invalidArgumentsError{"indexName cannot be empty"}
	}

	if opts == nil {
		opts = &GetIndexedDocumentsCountOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(SearchService),
		Method:  "GET",
		Path:    fmt.Sprintf("/api/index/%s/count", indexName),
		Context: ctx,
	}
	res, err := sim.httpClient.DoHttpRequest(req)
	if err != nil {
		return 0, err
	}

	if res.StatusCode != 200 {
		data, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return 0, err
		}
		err = res.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return 0, searchIndexError{message: string(data), statusCode: res.StatusCode}
	}

	var count struct {
		Count int `json:"count"`
	}
	jsonDec := json.NewDecoder(res.Body)
	err = jsonDec.Decode(&count)
	if err != nil {
		return 0, err
	}

	err = res.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return count.Count, nil
}

func (sim *SearchIndexManager) performControlRequest(ctx context.Context, uri, method string) error {
	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(SearchService),
		Method:  method,
		Path:    uri,
		Context: ctx,
	}
	res, err := sim.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		data, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		err = res.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return searchIndexError{message: string(data), statusCode: res.StatusCode}
	}

	err = res.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil

}

// PauseIngestSearchIndexOptions is the set of options available to the search index PauseIngest operation.
type PauseIngestSearchIndexOptions struct {
	Timeout time.Duration
	Context context.Context
}

// PauseIngest pauses updates and maintenance for an index.
func (sim *SearchIndexManager) PauseIngest(indexName string, opts *PauseIngestSearchIndexOptions) error {
	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	if opts == nil {
		opts = &PauseIngestSearchIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	return sim.performControlRequest(ctx, fmt.Sprintf("/api/index/%s/ingestControl/pause", indexName), "POST")
}

// ResumeIngestSearchIndexOptions is the set of options available to the search index ResumeIngest operation.
type ResumeIngestSearchIndexOptions struct {
	Timeout time.Duration
	Context context.Context
}

// ResumeIngest resumes updates and maintenance for an index.
func (sim *SearchIndexManager) ResumeIngest(indexName string, opts *ResumeIngestSearchIndexOptions) error {
	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	if opts == nil {
		opts = &ResumeIngestSearchIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	return sim.performControlRequest(ctx, fmt.Sprintf("/api/index/%s/ingestControl/resume", indexName), "POST")
}

// AllowQueryingSearchIndexOptions is the set of options available to the search index AllowQuerying operation.
type AllowQueryingSearchIndexOptions struct {
	Timeout time.Duration
	Context context.Context
}

// AllowQuerying allows querying against an index.
func (sim *SearchIndexManager) AllowQuerying(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	if opts == nil {
		opts = &AllowQueryingSearchIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	return sim.performControlRequest(ctx, fmt.Sprintf("/api/index/%s/queryControl/allow", indexName), "POST")
}

// DisallowQueryingSearchIndexOptions is the set of options available to the search index DisallowQuerying operation.
type DisallowQueryingSearchIndexOptions struct {
	Timeout time.Duration
	Context context.Context
}

// DisallowQuerying disallows querying against an index.
func (sim *SearchIndexManager) DisallowQuerying(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	if opts == nil {
		opts = &AllowQueryingSearchIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	return sim.performControlRequest(ctx, fmt.Sprintf("/api/index/%s/queryControl/disallow", indexName), "POST")
}

// FreezePlanSearchIndexOptions is the set of options available to the search index FreezePlan operation.
type FreezePlanSearchIndexOptions struct {
	Timeout time.Duration
	Context context.Context
}

// FreezePlan freezes the assignment of index partitions to nodes.
func (sim *SearchIndexManager) FreezePlan(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	if opts == nil {
		opts = &AllowQueryingSearchIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	return sim.performControlRequest(ctx, fmt.Sprintf("/api/index/%s/planFreezeControl/freeze", indexName), "POST")
}

// UnfreezePlanSearchIndexOptions is the set of options available to the search index UnfreezePlan operation.
type UnfreezePlanSearchIndexOptions struct {
	Timeout time.Duration
	Context context.Context
}

// UnfreezePlan unfreezes the assignment of index partitions to nodes.
func (sim *SearchIndexManager) UnfreezePlan(indexName string, opts *AllowQueryingSearchIndexOptions) error {
	if indexName == "" {
		return invalidArgumentsError{"indexName cannot be empty"}
	}

	if opts == nil {
		opts = &AllowQueryingSearchIndexOptions{}
	}

	ctx, cancel := contextFromMaybeTimeout(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	return sim.performControlRequest(ctx, fmt.Sprintf("/api/index/%s/planFreezeControl/unfreeze", indexName), "POST")
}
