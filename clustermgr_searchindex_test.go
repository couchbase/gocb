package gocb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

type mockSearchIndexCluster struct {
	ftsEp string
}

func (m mockSearchIndexCluster) getFtsEp() (string, error) {
	return "http://fts/ep/", nil
}

func createSimpleIndex(t *testing.T, name string, searchManager *SearchIndexManager) {
	index := make(map[string]interface{})
	index["name"] = name
	index["sourceType"] = "couchbase"
	index["sourceName"] = "default"
	index["type"] = "fulltext-index"

	b := new(bytes.Buffer)
	err := json.NewEncoder(b).Encode(index)
	if err != nil {
		t.Fatalf("Could not encode index %v", err)
	}

	uri := fmt.Sprintf("/api/index/%s", name)
	res, err := searchManager.doSearchIndexRequest("PUT", uri, "application/json", "no-cache", b)
	if err != nil {
		t.Fatalf("Could not create index %v", err)
	}

	defer res.Body.Close()

	err = searchManager.checkRespBodyForError(res)
	if err != nil {
		t.Fatalf("Could not create index %v", res.Status)
	}

	_, err = searchManager.checkRespBodyStatusOK(res)
	if err != nil {
		t.Fatalf("Could not create index %v", res.Status)
	}
}

func deleteIndex(t *testing.T, name string, searchManager *SearchIndexManager) {
	deleteRes, err := searchManager.DeleteIndex(name)
	if err != nil {
		t.Fatalf("Failed to delete index %v", err)
	}

	if !deleteRes {
		t.Fatalf("Could not delete index %s", name)
	}
}

func TestCreateIndex(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Search indexes not supported")
	}

	manager := globalCluster.Manager("", "")
	searchManager := manager.SearchIndexManager()

	builder := SearchIndexDefinitionBuilder{}
	builder.AddField("name", "test_search_index_1").
		AddField("type", "fulltext-index").
		AddField("sourceName", globalBucket.Name()).
		AddField("sourceType", "couchbase")

	defer deleteIndex(t, "test_search_index_1", searchManager)

	err := searchManager.CreateIndex(builder)
	if err != nil {
		t.Fatalf("Failed to CreateIndex %v", err)
	}

	_, err = searchManager.GetIndexDefinition("test_search_index_1")
	if err != nil {
		t.Fatalf("Failed to create index %v", err)
	}

	indexes, err := searchManager.GetAllIndexDefinitions()
	if err != nil {
		t.Fatalf("Failed to GetAllIndexDefinitions %v", err)
	}

	if len(indexes) != 1 {
		t.Fatalf("GetAllIndexDefinitions returned incorrect number of indexes, expected 1 but was %d", len(indexes))
	}

	rawIndex, err := searchManager.GetIndexDefinition("test_search_index_1")
	if err != nil {
		t.Fatalf("Failed to GetIndexDefinition %v", err)
	}

	index := rawIndex.(map[string]interface{})
	if index["name"] != "test_search_index_1" {
		t.Fatalf("GetIndexDefinition returned incorrect index, expected %s, was %v", "test_search_index_1", index)
	}
}

func TestCreateIndexNoName(t *testing.T) {
	manager := globalCluster.Manager("", "")
	searchManager := manager.SearchIndexManager()

	builder := SearchIndexDefinitionBuilder{}
	builder.AddField("type", "fulltext-index").
		AddField("sourceName", globalBucket.Name()).
		AddField("sourceType", "couchbase")

	err := searchManager.CreateIndex(builder)
	if err == nil {
		t.Fatal("CreateIndexNoName should have errored, but didn't")
	}
}

func TestCreateIndexNoType(t *testing.T) {
	manager := globalCluster.Manager("", "")
	searchManager := manager.SearchIndexManager()

	builder := SearchIndexDefinitionBuilder{}
	builder.AddField("name", "test_search_index_1").
		AddField("sourceName", globalBucket.Name()).
		AddField("sourceType", "couchbase")

	err := searchManager.CreateIndex(builder)
	if err == nil {
		t.Fatal("CreateIndexNoType should have errored, but didn't")
	}
}

func TestCreateIndexNoSourceName(t *testing.T) {
	manager := globalCluster.Manager("", "")
	searchManager := manager.SearchIndexManager()

	builder := SearchIndexDefinitionBuilder{}
	builder.AddField("name", "test_search_index_1").
		AddField("type", "fulltext-index").
		AddField("sourceType", "couchbase")

	err := searchManager.CreateIndex(builder)
	if err == nil {
		t.Fatal("CreateIndexNoSourceName should have errored, but didn't")
	}
}

func TestCreateIndexNoSourceType(t *testing.T) {
	manager := globalCluster.Manager("", "")
	searchManager := manager.SearchIndexManager()

	builder := SearchIndexDefinitionBuilder{}
	builder.AddField("name", "test_search_index_1").
		AddField("type", "fulltext-index").
		AddField("sourceName", globalBucket.Name())

	err := searchManager.CreateIndex(builder)
	if err == nil {
		t.Fatal("TestCreateIndexNoSourceType should have errored, but didn't")
	}
}

func TestGetIndexedDocumentCount(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Search indexes not supported")
	}

	indexName := "test_search_index_1"
	expectedCount := 5
	response := make(map[string]interface{})
	response["status"] = "ok"
	response["count"] = expectedCount
	respBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Couldnt not marshal response: %v", err)
	}

	client := newMockHTTPClient(t, fmt.Sprintf("/api/index/%s/count", indexName), "GET", respBytes)

	searchManager := &SearchIndexManager{
		authenticator: &mockAuthenticator{},
		httpCli:       client,
		cluster:       &mockSearchIndexCluster{},
	}

	count, err := searchManager.GetIndexedDocumentCount(indexName)
	if err != nil {
		t.Fatalf("Failed to GetIndexedDocumentCount %v", err)
	}

	if count != expectedCount {
		t.Fatalf("GetIndexedDocumentCount returned incorrect count, expected 0 was %d", count)
	}
}

func TestSetIndexIngestionInvalid(t *testing.T) {
	manager := globalCluster.Manager("", "")
	searchManager := manager.SearchIndexManager()

	_, err := searchManager.SetIndexIngestion("test", "invalid")
	if err == nil {
		t.Fatal("Failed to SetIndexIngestionInvalid expected error")
	}
}

func TestSetIndexIngestionPause(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Search indexes not supported")
	}

	indexName := "test_search_index_1"
	response := make(map[string]interface{})
	response["status"] = "ok"
	respBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Couldnt not marshal response: %v", err)
	}

	client := newMockHTTPClient(t, fmt.Sprintf("/api/index/%s/ingestControl/pause", indexName), "POST", respBytes)

	searchManager := &SearchIndexManager{
		authenticator: &mockAuthenticator{},
		httpCli:       client,
		cluster:       &mockSearchIndexCluster{},
	}

	success, err := searchManager.SetIndexIngestion(indexName, "pause")
	if err != nil {
		t.Fatalf("Failed to SetIndexIngestionPause %s", err)
	}

	if success != true {
		t.Fatalf("Expected SetIndexIngestionPause to return success as true but was false")
	}
}

func TestSetIndexIngestionResume(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Search indexes not supported")
	}

	indexName := "test_search_index_1"
	response := make(map[string]interface{})
	response["status"] = "ok"
	respBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Couldnt not marshal response: %v", err)
	}

	client := newMockHTTPClient(t, fmt.Sprintf("/api/index/%s/ingestControl/resume", indexName), "POST", respBytes)

	searchManager := &SearchIndexManager{
		authenticator: &mockAuthenticator{},
		httpCli:       client,
		cluster:       &mockSearchIndexCluster{},
	}

	success, err := searchManager.SetIndexIngestion(indexName, "resume")
	if err != nil {
		t.Fatalf("Failed to SetIndexIngestionPause %s", err)
	}

	if success != true {
		t.Fatalf("Expected SetIndexIngestionPause to return success as true but was false")
	}
}

func TestSetIndexQueryingInvalid(t *testing.T) {
	manager := globalCluster.Manager("", "")
	searchManager := manager.SearchIndexManager()

	_, err := searchManager.SetIndexQuerying("test", "invalid")
	if err == nil {
		t.Fatal("Failed to SetIndexQueryingInvalid expected error")
	}
}

func TestSetIndexQueryingDisallow(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Search indexes not supported")
	}

	indexName := "test_search_index_1"
	response := make(map[string]interface{})
	response["status"] = "ok"
	respBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Couldnt not marshal response: %v", err)
	}

	client := newMockHTTPClient(t, fmt.Sprintf("/api/index/%s/queryControl/disallow", indexName), "POST", respBytes)

	searchManager := &SearchIndexManager{
		authenticator: &mockAuthenticator{},
		httpCli:       client,
		cluster:       &mockSearchIndexCluster{},
	}

	success, err := searchManager.SetIndexQuerying(indexName, "disallow")
	if err != nil {
		t.Fatalf("Failed to SetIndexIngestionPause %s", err)
	}

	if success != true {
		t.Fatalf("Expected SetIndexIngestionPause to return success as true but was false")
	}
}

func TestSetIndexQueryingAllow(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Search indexes not supported")
	}

	indexName := "test_search_index_1"
	response := make(map[string]interface{})
	response["status"] = "ok"
	respBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Couldnt not marshal response: %v", err)
	}

	client := newMockHTTPClient(t, fmt.Sprintf("/api/index/%s/queryControl/allow", indexName), "POST", respBytes)

	searchManager := &SearchIndexManager{
		authenticator: &mockAuthenticator{},
		httpCli:       client,
		cluster:       &mockSearchIndexCluster{},
	}

	success, err := searchManager.SetIndexQuerying(indexName, "allow")
	if err != nil {
		t.Fatalf("Failed to SetIndexIngestionPause %s", err)
	}

	if success != true {
		t.Fatalf("Expected SetIndexIngestionPause to return success as true but was false")
	}
}

func TestSetIndexPlanFreezeInvalid(t *testing.T) {
	manager := globalCluster.Manager("", "")
	searchManager := manager.SearchIndexManager()

	_, err := searchManager.SetIndexPlanFreeze("test", "invalid")
	if err == nil {
		t.Fatal("Failed to SetIndexPlanFreezeInvalid expected error")
	}
}

func TestSetIndexPlanFreezeFreeze(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Search indexes not supported")
	}

	indexName := "test_search_index_1"
	response := make(map[string]interface{})
	response["status"] = "ok"
	respBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Couldnt not marshal response: %v", err)
	}

	client := newMockHTTPClient(t, fmt.Sprintf("/api/index/%s/planFreezeControl/freeze", indexName), "POST", respBytes)

	searchManager := &SearchIndexManager{
		authenticator: &mockAuthenticator{},
		httpCli:       client,
		cluster:       &mockSearchIndexCluster{},
	}

	success, err := searchManager.SetIndexPlanFreeze(indexName, "freeze")
	if err != nil {
		t.Fatalf("Failed to SetIndexIngestionPause %s", err)
	}

	if success != true {
		t.Fatalf("Expected SetIndexIngestionPause to return success as true but was false")
	}
}

func TestSetIndexPlanFreezeUnfreeze(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Search indexes not supported")
	}

	indexName := "test_search_index_1"
	response := make(map[string]interface{})
	response["status"] = "ok"
	respBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Couldnt not marshal response: %v", err)
	}

	client := newMockHTTPClient(t, fmt.Sprintf("/api/index/%s/planFreezeControl/unfreeze", indexName), "POST", respBytes)

	searchManager := &SearchIndexManager{
		authenticator: &mockAuthenticator{},
		httpCli:       client,
		cluster:       &mockSearchIndexCluster{},
	}

	success, err := searchManager.SetIndexPlanFreeze(indexName, "unfreeze")
	if err != nil {
		t.Fatalf("Failed to SetIndexIngestionPause %s", err)
	}

	if success != true {
		t.Fatalf("Expected SetIndexIngestionPause to return success as true but was false")
	}
}

func TestGetAllIndexStats(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Search indexes not supported")
	}

	response := make(map[string]interface{})
	response["feeds"] = make(map[string]interface{})
	response["pIndexes"] = make(map[string]interface{})
	response["manager"] = make(map[string]interface{})
	respBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Couldnt not marshal response: %v", err)
	}

	client := newMockHTTPClient(t, "/api/stats", "GET", respBytes)

	searchManager := &SearchIndexManager{
		authenticator: &mockAuthenticator{},
		httpCli:       client,
		cluster:       &mockSearchIndexCluster{},
	}

	_, err = searchManager.GetAllIndexStats()
	if err != nil {
		t.Fatalf("Failed to GetAllIndexStats %s", err)
	}
}

func TestGetIndexStats(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Search indexes not supported")
	}

	indexName := "test_search_index_1"
	response := make(map[string]interface{})
	response["feeds"] = make(map[string]interface{})
	respBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Couldnt not marshal response: %v", err)
	}

	client := newMockHTTPClient(t, fmt.Sprintf("/api/stats/index/%s", indexName), "GET", respBytes)

	searchManager := &SearchIndexManager{
		authenticator: &mockAuthenticator{},
		httpCli:       client,
		cluster:       &mockSearchIndexCluster{},
	}

	_, err = searchManager.GetIndexStats(indexName)
	if err != nil {
		t.Fatalf("Failed to GetAllIndexStats %s", err)
	}
}

func TestGetAllIndexPartitionInfo(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Search indexes not supported")
	}

	response := make(map[string]interface{})
	response["status"] = "ok"
	response["pindexes"] = make(map[string]interface{})
	respBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Couldnt not marshal response: %v", err)
	}

	client := newMockHTTPClient(t, "/api/pindex", "GET", respBytes)

	searchManager := &SearchIndexManager{
		authenticator: &mockAuthenticator{},
		httpCli:       client,
		cluster:       &mockSearchIndexCluster{},
	}

	_, err = searchManager.GetAllIndexPartitionInfo()
	if err != nil {
		t.Fatalf("Failed to GetAllIndexPartitionInfo %s", err)
	}
}

func TestGetIndexPartitionInfo(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Search indexes not supported")
	}

	indexName := "test_search_index"
	response := make(map[string]interface{})
	response["status"] = "ok"
	response["pindex"] = make(map[string]interface{})
	respBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Couldnt not marshal response: %v", err)
	}

	client := newMockHTTPClient(t, fmt.Sprintf("/api/pindex/%s", indexName), "GET", respBytes)

	searchManager := &SearchIndexManager{
		authenticator: &mockAuthenticator{},
		httpCli:       client,
		cluster:       &mockSearchIndexCluster{},
	}

	_, err = searchManager.GetIndexPartitionInfo(indexName)
	if err != nil {
		t.Fatalf("Failed to GetIndexPartitionInfo %s", err)
	}
}

func TestGetIndexPartitionIndexedDocumentCount(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Search indexes not supported")
	}

	indexName := "test_search_index"
	expectedCount := 3
	response := make(map[string]interface{})
	response["status"] = "ok"
	response["count"] = expectedCount
	respBytes, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Couldnt not marshal response: %v", err)
	}

	client := newMockHTTPClient(t, fmt.Sprintf("/api/pindex/%s/count", indexName), "GET", respBytes)

	searchManager := &SearchIndexManager{
		authenticator: &mockAuthenticator{},
		httpCli:       client,
		cluster:       &mockSearchIndexCluster{},
	}

	count, err := searchManager.GetIndexPartitionIndexedDocumentCount(indexName)
	if err != nil {
		t.Fatalf("Failed to GetAllIndexPartitionInfo %s", err)
	}

	if count != expectedCount {
		t.Fatalf("GetIndexPartitionIndexedDocumentCount should return %d for GetIndexPartitionIndexedDocumentCount but was %d", expectedCount, count)
	}
}
