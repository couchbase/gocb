package gocb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"

	"github.com/stretchr/testify/mock"
)

func (suite *IntegrationTestSuite) newSearchIndexName() string {
	indexName := "a" + uuid.New().String()
	return indexName
}

func (suite *IntegrationTestSuite) TestSearchIndexesCrud() {
	suite.skipIfUnsupported(SearchIndexFeature)
	suite.skipIfServerVersionEquals(srvVer750)

	mgr := globalCluster.SearchIndexes()

	indexName := suite.newSearchIndexName()

	source := []byte(fmt.Sprintf(`{"name":"%s","type":"fulltext-index","sourceType":"couchbase","sourceName":"%s"}`, indexName, globalBucket.Name()))
	var upsertIndex SearchIndex
	err := json.Unmarshal(source, &upsertIndex)
	suite.Require().NoError(err)

	err = mgr.UpsertIndex(upsertIndex, nil)
	if err != nil {
		suite.T().Fatalf("Expected UpsertIndex err to be nil but was %v", err)
	}

	// Upsert requires a UUID.
	err = mgr.UpsertIndex(SearchIndex{
		Name:       indexName,
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
	}, nil)
	if !errors.Is(err, ErrIndexExists) {
		suite.T().Fatalf("Expected UpsertIndex err to be already exists but was %v", err)
	}

	err = mgr.UpsertIndex(SearchIndex{
		Name:       "test2",
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
		PlanParams: map[string]interface{}{
			"indexPartitions": 1,
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

	index, err := mgr.GetIndex(indexName, nil)
	if err != nil {
		suite.T().Fatalf("Expected GetIndex err to be nil but was %v", err)
	}

	indexBytes, err := json.Marshal(index)
	suite.Require().NoError(err)

	var unmarhsalledIndex SearchIndex
	err = json.Unmarshal(indexBytes, &unmarhsalledIndex)
	suite.Require().NoError(err)

	_, err = mgr.GetIndex("testindexthatdoesnotexist", nil)
	if !errors.Is(err, ErrIndexNotFound) {
		suite.T().Fatalf("Expected GetIndex err to be not exists but was %v", err)
	}

	if index.Name != indexName {
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

	err = mgr.DropIndex(indexName, nil)
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
	if !errors.Is(err, ErrIndexNotFound) {
		suite.T().Fatalf("Expected GetIndex err to be not found but was %s", err)
	}

	_, err = mgr.GetIndex("test2", nil)
	if !errors.Is(err, ErrIndexNotFound) {
		suite.T().Fatalf("Expected GetIndex err to be not found but was %s", err)
	}

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_search_upsert_index"), 5, true)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_search_get_index"), 4, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_search_drop_index"), 3, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_search_get_all_indexes"), 1, false)
}

func (suite *IntegrationTestSuite) TestSearchIndexesUpsertIndexNoName() {
	suite.skipIfUnsupported(SearchIndexFeature)

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
	suite.skipIfUnsupported(SearchIndexFeature)

	mgr := globalCluster.SearchIndexes()

	indexName := suite.newSearchIndexName()

	err := mgr.UpsertIndex(SearchIndex{
		Name:       indexName,
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected UpsertIndex err to be nil but was %v", err)
	}

	defer mgr.DropIndex(indexName, nil)

	err = mgr.PauseIngest(indexName, nil)
	if err != nil {
		suite.T().Fatalf("Expected PauseIngest err to be nil but was %v", err)
	}

	err = mgr.ResumeIngest(indexName, nil)
	if err != nil {
		suite.T().Fatalf("Expected ResumeIngest err to be nil but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestSearchIndexesQueryControl() {
	suite.skipIfUnsupported(SearchIndexFeature)

	mgr := globalCluster.SearchIndexes()

	indexName := suite.newSearchIndexName()
	err := mgr.UpsertIndex(SearchIndex{
		Name:       indexName,
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected UpsertIndex err to be nil but was %v", err)
	}

	defer mgr.DropIndex(indexName, nil)

	err = mgr.DisallowQuerying(indexName, nil)
	if err != nil {
		suite.T().Fatalf("Expected PauseIngest err to be nil but was %v", err)
	}

	err = mgr.AllowQuerying(indexName, nil)
	if err != nil {
		suite.T().Fatalf("Expected ResumeIngest err to be nil but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestSearchIndexesPartitionControl() {
	suite.skipIfUnsupported(SearchIndexFeature)

	mgr := globalCluster.SearchIndexes()

	indexName := suite.newSearchIndexName()
	err := mgr.UpsertIndex(SearchIndex{
		Name:       indexName,
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected UpsertIndex err to be nil but was %v", err)
	}

	defer mgr.DropIndex(indexName, nil)

	err = mgr.FreezePlan(indexName, nil)
	if err != nil {
		suite.T().Fatalf("Expected PauseIngest err to be nil but was %v", err)
	}

	err = mgr.UnfreezePlan(indexName, nil)
	if err != nil {
		suite.T().Fatalf("Expected ResumeIngest err to be nil but was %v", err)
	}
}

func (suite *UnitTestSuite) TestSearchIndexesAnalyzeDocumentCore() {
	analyzeResp, err := loadRawTestDataset("search_analyzedoc")
	suite.Require().Nil(err, err)

	resp := &mgmtResponse{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(analyzeResp)),
	}

	indexName := "searchy"

	mockProvider := new(mockMgmtProvider)
	mockProvider.
		On("executeMgmtRequest", nil, mock.AnythingOfType("mgmtRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(1).(mgmtRequest)

			suite.Assert().Equal(fmt.Sprintf("/api/index/%s/analyzeDoc", indexName), req.Path)
			suite.Assert().Equal(ServiceTypeSearch, req.Service)
			suite.Assert().True(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("POST", req.Method)
			suite.Assert().Nil(req.RetryStrategy)
		}).
		Return(resp, nil)

	mgr := &searchIndexProviderCore{
		mgmtProvider: mockProvider,
		tracer:       &NoopTracer{},
	}

	res, err := mgr.AnalyzeDocument(indexName, struct{}{}, &AnalyzeDocumentOptions{
		Timeout: 1 * time.Second,
	})
	suite.Require().Nil(err, err)

	suite.Require().NotNil(res)
}

func (suite *UnitTestSuite) TestSearchIndexSerialization() {
	source := []byte(`{"name":"test","type":"fulltext-index","sourceType":"couchbase","sourceName":"bucket"}`)
	var index SearchIndex
	err := json.Unmarshal(source, &index)
	suite.Require().NoError(err)

	b, err := json.Marshal(index)
	suite.Require().NoError(err)

	var index2 SearchIndex
	err = json.Unmarshal(b, &index2)
	suite.Require().NoError(err)

	suite.Assert().Equal(index, index2)

}

func (suite *UnitTestSuite) TestSearchIndexCanDeserializeFromUI() {
	data, err := loadRawTestDataset("uisearchindex")
	suite.Require().NoError(err)

	var index SearchIndex
	err = json.Unmarshal(data, &index)
	suite.Require().NoError(err)

	suite.Assert().Equal("test", index.Name)
}
