package gocb

import (
	"testing"
)

// Most of this file is commented out due to the testing framework currently being reliant on Couchbase Mock, which does
// as yet support FTS index management. See: https://issues.couchbase.com/browse/GOCBC-230

//func createSimpleIndex(t *testing.T, name string, searchManager *SearchIndexManager) {
//	index := make(map[string]interface{})
//	index["name"] = name
//	index["sourceType"] = "couchbase"
//	index["sourceName"] = "default"
//	index["type"] = "fulltext-index"
//
//	b := new(bytes.Buffer)
//	err := json.NewEncoder(b).Encode(index)
//	if err != nil {
//		t.Fatalf("Could not encode index %v", err)
//	}
//
//	uri := fmt.Sprintf("/api/index/%s", name)
//	res, err := searchManager.doSearchIndexRequest("PUT", uri, "application/json", "no-cache", b)
//	if err != nil {
//		t.Fatalf("Could not create index %v", err)
//	}
//
//	defer res.Body.Close()
//
//	err = searchManager.checkRespBodyForError(res)
//	if err != nil {
//		t.Fatalf("Could not create index %v", res.Status)
//	}
//
//	_, err = searchManager.checkRespBodyStatusOK(res)
//	if err != nil {
//		t.Fatalf("Could not create index %v", res.Status)
//	}
//}
//
//func deleteIndex(t *testing.T, name string, searchManager *SearchIndexManager) {
//	deleteRes, err := searchManager.doSearchIndexRequest("DELETE", fmt.Sprintf("/api/index/%s", name), "", "", nil)
//	if err != nil {
//		t.Fatalf("Failed to delete index %v", err)
//	}
//	deleteRes.Body.Close()
//	if deleteRes.StatusCode != 200 {
//		t.Fatalf("Could not delete index %v", deleteRes.Status)
//	}
//}
//
//func TestCreateIndex(t *testing.T) {
//	manager := globalCluster.Manager("", "")
//	searchManager := manager.SearchIndexManager()
//
//	builder := SearchIndexDefinitionBuilder{}
//	builder.AddField("name", "test_search_index_1").
//		AddField("type", "fulltext-index").
//		AddField("sourceName", globalBucket.Name()).
//		AddField("sourceType", "couchbase")
//
//	defer deleteIndex(t, "test_search_index_1", searchManager)
//
//	err := searchManager.CreateIndex(builder)
//	if err != nil {
//		t.Fatalf("Failed to CreateIndex %v", err)
//	}
//
//	getRes, err := searchManager.doSearchIndexRequest("GET","/api/index", "", "", nil)
//	if err != nil {
//		t.Fatalf("Failed to get indexes %v", err)
//	}
//	defer getRes.Body.Close()
//
//	var indexesResp searchIndexesResp
//	jsonDec := json.NewDecoder(getRes.Body)
//	err = jsonDec.Decode(&indexesResp)
//	if err != nil {
//		t.Fatalf("Failed to parse index response %v", err)
//	}
//
//	if len(indexesResp.IndexDefs.IndexDefs) != 1 {
//		t.Fatalf("CreateIndexes created incorrect number of indexes, expected 1 but was %d", len(indexesResp.IndexDefs.IndexDefs))
//	}
//}

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

//func TestGetIndexes(t *testing.T) {
//	manager := globalCluster.Manager("", "")
//	searchManager := manager.SearchIndexManager()
//
//	defer deleteIndex(t, "test_search_index_1", searchManager)
//	defer deleteIndex(t, "test_search_index_2", searchManager)
//
//	createSimpleIndex(t, "test_search_index_1", searchManager)
//	createSimpleIndex(t, "test_search_index_2", searchManager)
//
//	indexes, err := searchManager.GetIndexes()
//	if err != nil {
//		t.Fatalf("Failed to GetIndexes %v", err)
//	}
//
//	if len(indexes) != 2 {
//		t.Fatalf("GetIndexes returned incorrect number of indexes, expected 2 but was %d", len(indexes))
//	}
//}
//
//func TestGetIndex(t *testing.T) {
//	manager := globalCluster.Manager("", "")
//	searchManager := manager.SearchIndexManager()
//
//	expectedIdxName := "test_search_index_1"
//	defer deleteIndex(t, expectedIdxName, searchManager)
//	createSimpleIndex(t, expectedIdxName, searchManager)
//
//	rawIndex, err := searchManager.GetIndex(expectedIdxName)
//	if err != nil {
//		t.Fatalf("Failed to GetIndex %v", err)
//	}
//
//	index := rawIndex.(map[string]interface{})
//	if index["name"] != expectedIdxName {
//		t.Fatalf("GetIndex returned incorrect index, expected %s, was %v", expectedIdxName, index)
//	}
//}
//
//func TestDeleteIndex(t *testing.T) {
//	manager := globalCluster.Manager("", "")
//	searchManager := manager.SearchIndexManager()
//
//	expectedIdxName := "test_search_index_1"
//	createSimpleIndex(t, expectedIdxName, searchManager)
//
//	success, err := searchManager.DeleteIndex(expectedIdxName)
//	if err != nil {
//		t.Fatalf("Failed to DeleteIndex %v", err)
//	}
//
//	if success != true {
//		t.Fatalf("Expected DeleteIndex to return success as true but was false")
//	}
//}
//
//func TestGetIndexedDocumentCount(t *testing.T) {
//	manager := globalCluster.Manager("", "")
//	searchManager := manager.SearchIndexManager()
//
//	creds, err := globalCluster.auth.Credentials(AuthCredsRequest{
//		Service:  CapiService,
//	})
//	if err != nil {
//		t.Fatalf("Failed to create credentials %v", err)
//	}
//	err = globalBucket.Manager(creds[0].Username, creds[0].Password).Flush()
//	if err != nil {
//		t.Fatalf("Failed to flush bucket %v", err)
//	}
//
//	testDoc := struct {
//		field1 string
//		field2 string
//	} {
//		"field1",
//		"field2",
//	}
//	_, err = globalBucket.Insert("test1", testDoc, 0)
//	if err != nil {
//		t.Fatalf("Failed to insert document %v", err)
//	}
//	_, err = globalBucket.Insert("test2", testDoc, 0)
//	if err != nil {
//		t.Fatalf("Failed to insert document %v", err)
//	}
//	_, err = globalBucket.Insert("test3", testDoc, 0)
//	if err != nil {
//		t.Fatalf("Failed to insert document %v", err)
//	}
//
//	indexName := "test_search_index_1"
//	defer deleteIndex(t, indexName, searchManager)
//	createSimpleIndex(t, indexName, searchManager)
//
//	time.Sleep(100 * time.Millisecond) // This doesn't feel good
//
//	count, err := searchManager.GetIndexedDocumentCount(indexName)
//	if err != nil {
//		t.Fatalf("Failed to GetIndexedDocumentCount %v", err)
//	}
//
//	if count != 3 {
//		t.Fatalf("GetIndexedDocumentCount returned incorrect count, expected 3 was %d", count)
//	}
//
//	err = globalBucket.Manager(creds[0].Username, creds[0].Password).Flush()
//	if err != nil {
//		t.Fatalf("Failed to flush bucket %v", err)
//	}
//}
//
func TestSetIndexIngestionInvalid(t *testing.T) {
	manager := globalCluster.Manager("", "")
	searchManager := manager.SearchIndexManager()

	_, err := searchManager.SetIndexIngestion("test", "invalid")
	if err == nil {
		t.Fatal("Failed to SetIndexIngestionInvalid expected error")
	}
}

//
//func TestSetIndexIngestionPause(t *testing.T) {
//	manager := globalCluster.Manager("", "")
//	searchManager := manager.SearchIndexManager()
//
//	expectedIdxName := "test_search_index_1"
//	defer deleteIndex(t, expectedIdxName, searchManager)
//	createSimpleIndex(t, expectedIdxName, searchManager)
//
//	success, err := searchManager.SetIndexIngestion(expectedIdxName, "pause")
//	if err != nil {
//		t.Fatalf("Failed to SetIndexIngestionPause %s", err)
//	}
//
//	if success != true {
//		t.Fatalf("Expected SetIndexIngestionPause to return success as true but was false")
//	}
//
//	time.Sleep(100 * time.Millisecond) // This doesn't feel good
//
//	getRes, err := searchManager.doSearchIndexRequest("GET",fmt.Sprintf("/api/index/%s", expectedIdxName), "", "", nil)
//	if err != nil {
//		t.Fatalf("Failed to get index %v", err)
//	}
//	defer getRes.Body.Close()
//
//	var indexResp searchIndexResp
//	jsonDec := json.NewDecoder(getRes.Body)
//	err = jsonDec.Decode(&indexResp)
//	if err != nil {
//		t.Fatalf("Failed to parse index response %v", err)
//	}
//
//	index := indexResp.IndexDef.(map[string]interface{})
//	planParams := index["planParams"].(map[string]interface{})
//	nodePlanParams := planParams["nodePlanParams"].(map[string]interface{})
//	props := nodePlanParams[""].(map[string]interface{})
//	nestProps := props[""].(map[string]interface{})
//	canRead := nestProps["canRead"].(bool)
//	canWrite := nestProps["canWrite"].(bool)
//
//	if canRead != true {
//		t.Fatal("TestSetIndexIngestionPause should set canRead to true but was false")
//	}
//
//	if canWrite != false {
//		t.Fatal("TestSetIndexIngestionPause should set canWrite to false but was true")
//	}
//}
//
//func TestSetIndexIngestionResume(t *testing.T) {
//	manager := globalCluster.Manager("", "")
//	searchManager := manager.SearchIndexManager()
//
//	expectedIdxName := "test_search_index_1"
//	defer deleteIndex(t, expectedIdxName, searchManager)
//	createSimpleIndex(t, expectedIdxName, searchManager)
//
//	success, err := searchManager.SetIndexIngestion(expectedIdxName, "resume")
//	if err != nil {
//		t.Fatalf("Failed to SetIndexIngestionResume %s", err)
//	}
//
//	if success != true {
//		t.Fatalf("Expected SetIndexIngestionResume to return success as true but was false")
//	}
//
//	time.Sleep(100 * time.Millisecond) // This doesn't feel good
//
//	getRes, err := searchManager.doSearchIndexRequest("GET",fmt.Sprintf("/api/index/%s", expectedIdxName), "", "", nil)
//	if err != nil {
//		t.Fatalf("Failed to get index %v", err)
//	}
//	defer getRes.Body.Close()
//
//	var indexResp searchIndexResp
//	jsonDec := json.NewDecoder(getRes.Body)
//	err = jsonDec.Decode(&indexResp)
//	if err != nil {
//		t.Fatalf("Failed to parse index response %v", err)
//	}
//
//	index := indexResp.IndexDef.(map[string]interface{})
//	planParams := index["planParams"].(map[string]interface{})
//	nodePlanParams := planParams["nodePlanParams"].(map[string]interface{})
//	props := nodePlanParams[""].(map[string]interface{})
//	nestProps, ok := props[""].(map[string]interface{})
//
//	if ok {
//		t.Fatalf("SetIndexIngestionResume should have left the nodePlanParams nested object empty but was %v", nestProps)
//	}
//}

func TestSetIndexQueryingInvalid(t *testing.T) {
	manager := globalCluster.Manager("", "")
	searchManager := manager.SearchIndexManager()

	_, err := searchManager.SetIndexQuerying("test", "invalid")
	if err == nil {
		t.Fatal("Failed to SetIndexQueryingInvalid expected error")
	}
}

//func TestSetIndexQueryingDisallow(t *testing.T) {
//	manager := globalCluster.Manager("", "")
//	searchManager := manager.SearchIndexManager()
//
//	expectedIdxName := "test_search_index_1"
//	defer deleteIndex(t, expectedIdxName, searchManager)
//	createSimpleIndex(t, expectedIdxName, searchManager)
//
//	success, err := searchManager.SetIndexQuerying(expectedIdxName, "disallow")
//	if err != nil {
//		t.Fatalf("Failed to SetIndexQueryingDisallow %s", err)
//	}
//
//	if success != true {
//		t.Fatalf("Expected SetIndexQueryingDisallow to return success as true but was false")
//	}
//
//	time.Sleep(100 * time.Millisecond) // This doesn't feel good
//
//	getRes, err := searchManager.doSearchIndexRequest("GET",fmt.Sprintf("/api/index/%s", expectedIdxName), "", "", nil)
//	if err != nil {
//		t.Fatalf("Failed to get index %v", err)
//	}
//	defer getRes.Body.Close()
//
//	var indexResp searchIndexResp
//	jsonDec := json.NewDecoder(getRes.Body)
//	err = jsonDec.Decode(&indexResp)
//	if err != nil {
//		t.Fatalf("Failed to parse index response %v", err)
//	}
//
//	index := indexResp.IndexDef.(map[string]interface{})
//	planParams := index["planParams"].(map[string]interface{})
//	nodePlanParams := planParams["nodePlanParams"].(map[string]interface{})
//	props := nodePlanParams[""].(map[string]interface{})
//	nestProps := props[""].(map[string]interface{})
//	canRead := nestProps["canRead"].(bool)
//	canWrite := nestProps["canWrite"].(bool)
//
//	if canRead != false {
//		t.Fatal("SetIndexQueryingDisallow should set canRead to false but was true")
//	}
//
//	if canWrite != true {
//		t.Fatal("SetIndexQueryingDisallow should set canWrite to true but was false")
//	}
//}
//
//func TestSetIndexQueryingAllow(t *testing.T) {
//	manager := globalCluster.Manager("", "")
//	searchManager := manager.SearchIndexManager()
//
//	expectedIdxName := "test_search_index_1"
//	defer deleteIndex(t, expectedIdxName, searchManager)
//	createSimpleIndex(t, expectedIdxName, searchManager)
//
//	success, err := searchManager.SetIndexQuerying(expectedIdxName, "allow")
//	if err != nil {
//		t.Fatalf("Failed to SetIndexQueryingAllow %s", err)
//	}
//
//	if success != true {
//		t.Fatalf("Expected SetIndexQueryingAllow to return success as true but was false")
//	}
//
//	time.Sleep(100 * time.Millisecond) // This doesn't feel good
//
//	getRes, err := searchManager.doSearchIndexRequest("GET",fmt.Sprintf("/api/index/%s", expectedIdxName), "", "", nil)
//	if err != nil {
//		t.Fatalf("Failed to get index %v", err)
//	}
//	defer getRes.Body.Close()
//
//	var indexResp searchIndexResp
//	jsonDec := json.NewDecoder(getRes.Body)
//	err = jsonDec.Decode(&indexResp)
//	if err != nil {
//		t.Fatalf("Failed to parse index response %v", err)
//	}
//
//	index := indexResp.IndexDef.(map[string]interface{})
//	planParams := index["planParams"].(map[string]interface{})
//	nodePlanParams := planParams["nodePlanParams"].(map[string]interface{})
//	props := nodePlanParams[""].(map[string]interface{})
//	nestProps, ok := props[""].(map[string]interface{})
//
//	if ok {
//		t.Fatalf("SetIndexQueryingAllow should have left the nodePlanParams nested object empty but was %v", nestProps)
//	}
//}

func TestSetIndexPlanFreezeInvalid(t *testing.T) {
	manager := globalCluster.Manager("", "")
	searchManager := manager.SearchIndexManager()

	_, err := searchManager.SetIndexPlanFreeze("test", "invalid")
	if err == nil {
		t.Fatal("Failed to SetIndexPlanFreezeInvalid expected error")
	}
}

//func TestSetIndexPlanFreezeFreeze(t *testing.T) {
//	manager := globalCluster.Manager("", "")
//	searchManager := manager.SearchIndexManager()
//
//	expectedIdxName := "test_search_index_1"
//	defer deleteIndex(t, expectedIdxName, searchManager)
//	createSimpleIndex(t, expectedIdxName, searchManager)
//
//	success, err := searchManager.SetIndexPlanFreeze(expectedIdxName, "freeze")
//	if err != nil {
//		t.Fatalf("Failed to SetIndexPlanFreezeFreeze %s", err)
//	}
//
//	if success != true {
//		t.Fatalf("Expected SetIndexPlanFreezeFreeze to return success as true but was false")
//	}
//
//	time.Sleep(100 * time.Millisecond) // This doesn't feel good
//
//	getRes, err := searchManager.doSearchIndexRequest("GET",fmt.Sprintf("/api/index/%s", expectedIdxName), "", "", nil)
//	if err != nil {
//		t.Fatalf("Failed to get index %v", err)
//	}
//	defer getRes.Body.Close()
//
//	var indexResp searchIndexResp
//	jsonDec := json.NewDecoder(getRes.Body)
//	err = jsonDec.Decode(&indexResp)
//	if err != nil {
//		t.Fatalf("Failed to parse index response %v", err)
//	}
//
//	index := indexResp.IndexDef.(map[string]interface{})
//	planParams := index["planParams"].(map[string]interface{})
//	planFrozen := planParams["planFrozen"].(bool)
//
//	if planFrozen != true {
//		t.Fatal("SetIndexPlanFreezeFreeze should set planFrozen to true but was false")
//	}
//}
//
//func TestSetIndexPlanFreezeUnfreeze(t *testing.T) {
//	manager := globalCluster.Manager("", "")
//	searchManager := manager.SearchIndexManager()
//
//	expectedIdxName := "test_search_index_1"
//	defer deleteIndex(t, expectedIdxName, searchManager)
//	createSimpleIndex(t, expectedIdxName, searchManager)
//
//	success, err := searchManager.SetIndexPlanFreeze(expectedIdxName, "unfreeze")
//	if err != nil {
//		t.Fatalf("Failed to SetIndexPlanFreezeUnfreeze %s", err)
//	}
//
//	if success != true {
//		t.Fatalf("Expected SetIndexPlanFreezeUnfreeze to return success as true but was false")
//	}
//
//	time.Sleep(100 * time.Millisecond) // This doesn't feel good
//
//	getRes, err := searchManager.doSearchIndexRequest("GET",fmt.Sprintf("/api/index/%s", expectedIdxName), "", "", nil)
//	if err != nil {
//		t.Fatalf("Failed to get index %v", err)
//	}
//	defer getRes.Body.Close()
//
//	var indexResp searchIndexResp
//	jsonDec := json.NewDecoder(getRes.Body)
//	err = jsonDec.Decode(&indexResp)
//	if err != nil {
//		t.Fatalf("Failed to parse index response %v", err)
//	}
//
//	index := indexResp.IndexDef.(map[string]interface{})
//	planParams := index["planParams"].(map[string]interface{})
//	_, ok := planParams["planFrozen"].(bool)
//
//	if ok {
//		t.Fatal("SetIndexPlanFreezeUnfreeze should remove planFrozen")
//	}
//}
//
//func TestGetAllIndexStats(t *testing.T) {
//	manager := globalCluster.Manager("", "")
//	searchManager := manager.SearchIndexManager()
//
//	indexName := "test_search_index_1"
//	defer deleteIndex(t, indexName, searchManager)
//	createSimpleIndex(t, indexName, searchManager)
//
//	rawStats, err := searchManager.GetAllIndexStats()
//	if err != nil {
//		t.Fatalf("Failed to GetAllIndexStats %s", err)
//	}
//
//	stats := rawStats.(map[string]interface{})
//	_, ok := stats["feeds"]
//	if !ok {
//		t.Fatal("GetAllIndexStats should have a feeds object but doesn't")
//	}
//
//	_, ok = stats["pindexes"]
//	if !ok {
//		t.Fatal("GetAllIndexStats should have a pindexes object but doesn't")
//	}
//
//	_, ok = stats["manager"]
//	if !ok {
//		t.Fatal("GetAllIndexStats should have a manager object but doesn't")
//	}
//}
//
//func TestGetIndexStats(t *testing.T) {
//	manager := globalCluster.Manager("", "")
//	searchManager := manager.SearchIndexManager()
//
//	indexName := "test_search_index_1"
//	defer deleteIndex(t, indexName, searchManager)
//	createSimpleIndex(t, indexName, searchManager)
//
//	rawStats, err := searchManager.GetIndexStats(indexName)
//	if err != nil {
//		t.Fatalf("Failed to GetIndexStats %s", err)
//	}
//
//	stats := rawStats.(map[string]interface{})
//	_, ok := stats["feeds"]
//	if !ok {
//		t.Fatal("GetIndexStats should have a feeds object but doesn't")
//	}
//
//	_, ok = stats["pindexes"]
//	if !ok {
//		t.Fatal("GetIndexStats should have a pindexes object but doesn't")
//	}
//}
//
//// This tests both All and One due to needing to get an index name from All
//func TestGetAllIndexPartitionInfo(t *testing.T) {
//	manager := globalCluster.Manager("", "")
//	searchManager := manager.SearchIndexManager()
//
//	indexName := "test_search_index_1"
//	defer deleteIndex(t, indexName, searchManager)
//	createSimpleIndex(t, indexName, searchManager)
//
//	time.Sleep(100 * time.Millisecond) // This doesn't feel good
//
//	allRawInfo, err := searchManager.GetAllIndexPartitionInfo()
//	if err != nil {
//		t.Fatalf("Failed to GetAllIndexPartitionInfo %s", err)
//	}
//
//	allInfo := allRawInfo.(map[string]interface{})
//	if len(allInfo) !=1 {
//		t.Fatalf("GetAllIndexPartitionInfo should contain 1 pindex but contains %d", len(allInfo))
//	}
//
//	var name string
//	for key := range allInfo {
//		name = key
//	}
//
//	oneRawInfo, err := searchManager.GetIndexPartitionInfo(name)
//	if err != nil {
//		t.Fatalf("Failed to GetAllIndexPartitionInfo %s", err)
//	}
//
//	oneInfo := oneRawInfo.(map[string]interface{})
//	actualName := oneInfo["name"].(string)
//	if actualName != name {
//		t.Fatalf("GetAllIndexPartitionInfo should return correct name for getting one" +
//			"index partition info excepted %s was %s", name, actualName)
//	}
//
//
//	count, err := searchManager.GetIndexPartitionIndexedDocumentCount(name)
//	if err != nil {
//		t.Fatalf("Failed to GetAllIndexPartitionInfo %s", err)
//	}
//
//	if count != 0 {
//		t.Fatalf("GetAllIndexPartitionInfo should return 0 for GetIndexPartitionIndexedDocumentCount but was %d", count)
//	}
//}
