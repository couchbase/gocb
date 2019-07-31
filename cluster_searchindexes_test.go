package gocb

import (
	"testing"
)

func TestSearchIndexesCrud(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Skipping test as search indexes not supported")
	}

	mgr, err := globalCluster.SearchIndexes()
	if err != nil {
		t.Fatalf("Expected err to be nil but was %v", err)
	}

	err = mgr.Upsert(SearchIndex{
		Name:       "test",
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
	}, nil)
	if err != nil {
		t.Fatalf("Expected Upsert err to be nil but was %v", err)
	}

	err = mgr.Upsert(SearchIndex{
		Name:       "test2",
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
		PlanParams: map[string]interface{}{
			"indexPartitions": 3,
		},
		Params: map[string]interface{}{
			"store": map[string]string{
				"indexType":   "upside_down",
				"kvStoreName": "moss",
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Expected Upsert err to be nil but was %v", err)
	}

	err = mgr.Upsert(SearchIndex{
		Name:       "testAlias",
		Type:       "fulltext-alias",
		SourceType: "nil",
		Params: map[string]interface{}{
			"targets": map[string]interface{}{
				"test":  map[string]interface{}{},
				"test2": map[string]interface{}{},
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Expected UpsertAlias err to be nil but was %v", err)
	}

	index, err := mgr.Get("test", nil)
	if err != nil {
		t.Fatalf("Expected Get err to be nil but was %v", err)
	}

	if index.Name != "test" {
		t.Fatalf("Index name was not equal, expected test but was %v", index.Name)
	}

	if index.Type != "fulltext-index" {
		t.Fatalf("Index type was not equal, expected fulltext-index but was %v", index.Type)
	}

	err = mgr.Upsert(*index, &UpsertSearchIndexOptions{})
	if err != nil {
		t.Fatalf("Expected Upsert err to be nil but was %v", err)
	}

	indexes, err := mgr.GetAll(nil)
	if err != nil {
		t.Fatalf("Expected GetAll err to be nil but was %v", err)
	}

	if len(indexes) == 0 {
		t.Fatalf("Expected GetAll to return more than 0 indexes")
	}

	err = mgr.Drop("test", nil)
	if err != nil {
		t.Fatalf("Expected Drop err to be nil but was %v", err)
	}

	err = mgr.Drop("test2", nil)
	if err != nil {
		t.Fatalf("Expected Drop err to be nil but was %v", err)
	}

	err = mgr.Drop("testAlias", nil)
	if err != nil {
		t.Fatalf("Expected Drop err to be nil but was %v", err)
	}

	_, err = mgr.Get("newTest", nil)
	if err == nil {
		t.Fatalf("Expected Get err to be not nil but was")
	}

	if !IsSearchIndexNotFoundError(err) {
		t.Fatalf("Expected Get to return a not found error but was %v", err)
	}

	_, err = mgr.Get("test2", nil)
	if err == nil {
		t.Fatalf("Expected Get err to be not nil but was")
	}

	if !IsSearchIndexNotFoundError(err) {
		t.Fatalf("Expected Get to return a not found error but was %v", err)
	}
}

func TestSearchIndexesUpsertNoName(t *testing.T) {
	mgr, err := globalCluster.SearchIndexes()
	if err != nil {
		t.Fatalf("Expected err to be nil but was %v", err)
	}

	err = mgr.Upsert(SearchIndex{}, nil)
	if err == nil {
		t.Fatalf("Expected Upsert err to be not nil but was")
	}

	if !IsInvalidArgumentsError(err) {
		t.Fatalf("Expected error to he InvalidArgumentsError but was %v", err)
	}
}

func TestSearchIndexesIngestControl(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Skipping test as search indexes not supported")
	}

	mgr, err := globalCluster.SearchIndexes()
	if err != nil {
		t.Fatalf("Expected err to be nil but was %v", err)
	}

	err = mgr.Upsert(SearchIndex{
		Name:       "test",
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
	}, nil)
	if err != nil {
		t.Fatalf("Expected Upsert err to be nil but was %v", err)
	}

	defer mgr.Drop("test", nil)

	err = mgr.PauseIngest("test", nil)
	if err != nil {
		t.Fatalf("Expected PauseIngest err to be nil but was %v", err)
	}

	err = mgr.ResumeIngest("test", nil)
	if err != nil {
		t.Fatalf("Expected ResumeIngest err to be nil but was %v", err)
	}
}

func TestSearchIndexesQueryControl(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Skipping test as search indexes not supported")
	}

	mgr, err := globalCluster.SearchIndexes()
	if err != nil {
		t.Fatalf("Expected err to be nil but was %v", err)
	}

	err = mgr.Upsert(SearchIndex{
		Name:       "test",
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
	}, nil)
	if err != nil {
		t.Fatalf("Expected Upsert err to be nil but was %v", err)
	}

	defer mgr.Drop("test", nil)

	err = mgr.DisallowQuerying("test", nil)
	if err != nil {
		t.Fatalf("Expected PauseIngest err to be nil but was %v", err)
	}

	err = mgr.AllowQuerying("test", nil)
	if err != nil {
		t.Fatalf("Expected ResumeIngest err to be nil but was %v", err)
	}
}

func TestSearchIndexesPartitionControl(t *testing.T) {
	if !globalCluster.SupportsFeature(FtsIndexFeature) {
		t.Skip("Skipping test as search indexes not supported")
	}

	mgr, err := globalCluster.SearchIndexes()
	if err != nil {
		t.Fatalf("Expected err to be nil but was %v", err)
	}

	err = mgr.Upsert(SearchIndex{
		Name:       "test",
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
	}, nil)
	if err != nil {
		t.Fatalf("Expected Upsert err to be nil but was %v", err)
	}

	defer mgr.Drop("test", nil)

	err = mgr.FreezePlan("test", nil)
	if err != nil {
		t.Fatalf("Expected PauseIngest err to be nil but was %v", err)
	}

	err = mgr.UnfreezePlan("test", nil)
	if err != nil {
		t.Fatalf("Expected ResumeIngest err to be nil but was %v", err)
	}
}
