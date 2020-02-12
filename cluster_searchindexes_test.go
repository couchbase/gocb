package gocb

import (
	"errors"
	"time"
)

func (suite *IntegrationTestSuite) TestSearchIndexesCrud() {
	if !globalCluster.SupportsFeature(SearchIndexFeature) {
		suite.T().Skip("Skipping test as search indexes not supported")
	}

	mgr := globalCluster.SearchIndexes()

	err := mgr.UpsertIndex(SearchIndex{
		Name:       "test",
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected UpsertIndex err to be nil but was %v", err)
	}

	err = mgr.UpsertIndex(SearchIndex{
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
		suite.T().Fatalf("Expected UpsertIndex err to be nil but was %v", err)
	}

	err = mgr.UpsertIndex(SearchIndex{
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
		suite.T().Fatalf("Expected UpsertIndexAlias err to be nil but was %v", err)
	}

	index, err := mgr.GetIndex("test", nil)
	if err != nil {
		suite.T().Fatalf("Expected GetIndex err to be nil but was %v", err)
	}

	if index.Name != "test" {
		suite.T().Fatalf("Index name was not equal, expected test but was %v", index.Name)
	}

	if index.Type != "fulltext-index" {
		suite.T().Fatalf("Index type was not equal, expected fulltext-index but was %v", index.Type)
	}

	err = mgr.UpsertIndex(*index, &UpsertSearchIndexOptions{})
	if err != nil {
		suite.T().Fatalf("Expected UpsertIndex err to be nil but was %v", err)
	}

	indexes, err := mgr.GetAllIndexes(nil)
	if err != nil {
		suite.T().Fatalf("Expected GetAll err to be nil but was %v", err)
	}

	if len(indexes) == 0 {
		suite.T().Fatalf("Expected GetAll to return more than 0 indexes")
	}

	if globalCluster.SupportsFeature(SearchAnalyzeFeature) {
		// Analyze required pindexes to be built which takes time so we need to be a bit resilient here.
		timer := time.NewTimer(2 * time.Second)
		for {
			select {
			case <-timer.C:
				suite.T().Fatalf("Time to wait for analyze to succeed expired")
			default:
			}
			analysis, err := mgr.AnalyzeDocument("test", struct {
				Field1 string
				Field2 string
			}{
				Field1: "test",
				Field2: "imaginative field value",
			}, nil)
			if err != nil {
				globalCluster.TimeTravel(100 * time.Millisecond)
				logErrorf("Expected AnalyzeDocument err to be nil but was %v", err)
				continue
			}

			if analysis == nil || len(analysis) == 0 {
				suite.T().Fatalf("Expected analysis to be not nil")
			}
			break
		}
	} else {
		suite.T().Log("Skipping AnalyzeDocument feature as not supported.")
	}

	err = mgr.DropIndex("test", nil)
	if err != nil {
		suite.T().Fatalf("Expected DropIndex err to be nil but was %v", err)
	}

	err = mgr.DropIndex("test2", nil)
	if err != nil {
		suite.T().Fatalf("Expected DropIndex err to be nil but was %v", err)
	}

	err = mgr.DropIndex("testAlias", nil)
	if err != nil {
		suite.T().Fatalf("Expected DropIndex err to be nil but was %v", err)
	}

	_, err = mgr.GetIndex("newTest", nil)
	if err == nil {
		suite.T().Fatalf("Expected GetIndex err to be not nil but was")
	}

	_, err = mgr.GetIndex("test2", nil)
	if err == nil {
		suite.T().Fatalf("Expected GetIndex err to be not nil but was")
	}
}

func (suite *IntegrationTestSuite) TestSearchIndexesUpsertIndexNoName() {
	if !globalCluster.SupportsFeature(SearchIndexFeature) {
		suite.T().Skip("Skipping test as search indexes not supported")
	}

	mgr := globalCluster.SearchIndexes()

	err := mgr.UpsertIndex(SearchIndex{}, nil)
	if err == nil {
		suite.T().Fatalf("Expected UpsertIndex err to be not nil but was")
	}

	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Expected error to be InvalidArgument but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestSearchIndexesIngestControl() {
	if !globalCluster.SupportsFeature(SearchIndexFeature) {
		suite.T().Skip("Skipping test as search indexes not supported")
	}

	mgr := globalCluster.SearchIndexes()

	err := mgr.UpsertIndex(SearchIndex{
		Name:       "test",
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected UpsertIndex err to be nil but was %v", err)
	}

	defer mgr.DropIndex("test", nil)

	err = mgr.PauseIngest("test", nil)
	if err != nil {
		suite.T().Fatalf("Expected PauseIngest err to be nil but was %v", err)
	}

	err = mgr.ResumeIngest("test", nil)
	if err != nil {
		suite.T().Fatalf("Expected ResumeIngest err to be nil but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestSearchIndexesQueryControl() {
	if !globalCluster.SupportsFeature(SearchIndexFeature) {
		suite.T().Skip("Skipping test as search indexes not supported")
	}

	mgr := globalCluster.SearchIndexes()

	err := mgr.UpsertIndex(SearchIndex{
		Name:       "test",
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected UpsertIndex err to be nil but was %v", err)
	}

	defer mgr.DropIndex("test", nil)

	err = mgr.DisallowQuerying("test", nil)
	if err != nil {
		suite.T().Fatalf("Expected PauseIngest err to be nil but was %v", err)
	}

	err = mgr.AllowQuerying("test", nil)
	if err != nil {
		suite.T().Fatalf("Expected ResumeIngest err to be nil but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestSearchIndexesPartitionControl() {
	if !globalCluster.SupportsFeature(SearchIndexFeature) {
		suite.T().Skip("Skipping test as search indexes not supported")
	}

	mgr := globalCluster.SearchIndexes()

	err := mgr.UpsertIndex(SearchIndex{
		Name:       "test",
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected UpsertIndex err to be nil but was %v", err)
	}

	defer mgr.DropIndex("test", nil)

	err = mgr.FreezePlan("test", nil)
	if err != nil {
		suite.T().Fatalf("Expected PauseIngest err to be nil but was %v", err)
	}

	err = mgr.UnfreezePlan("test", nil)
	if err != nil {
		suite.T().Fatalf("Expected ResumeIngest err to be nil but was %v", err)
	}
}
