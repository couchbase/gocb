package gocb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/couchbase/gocbcore/v10"
)

func (suite *IntegrationTestSuite) TestScopeSearchIndexesCrud() {
	suite.skipIfUnsupported(ScopeSearchIndexFeature)

	mgr := globalScope.SearchIndexes()

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

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "search", "manager_search_upsert_index"), 3, true)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "search", "manager_search_get_index"), 2, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "search", "manager_search_drop_index"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "search", "manager_search_get_all_indexes"), 1, false)
}

func (suite *IntegrationTestSuite) TestScopeSearchIndexesUpsertIndexNoName() {
	suite.skipIfUnsupported(ScopeSearchIndexFeature)

	mgr := globalScope.SearchIndexes()

	err := mgr.UpsertIndex(SearchIndex{}, nil)
	if err == nil {
		suite.T().Fatalf("Expected UpsertIndex err to be not nil but was")
	}

	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Expected error to be InvalidArgument but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestScopeSearchIndexesUpsertFullyQualifiedIndexName() {
	suite.skipIfUnsupported(ScopeSearchIndexFeature)

	mgr := globalScope.SearchIndexes()

	err := mgr.UpsertIndex(SearchIndex{
		Name:       fmt.Sprintf("%s.%s.%s", globalScope.BucketName(), globalScope.Name(), "testindex"),
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
	}, nil)
	if err == nil {
		suite.T().Fatalf("Expected UpsertIndex err to be not nil but was")
	}

	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		suite.T().Fatalf("Expected error to be a generic HTTPError but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestScopeSearchIndexesIngestControl() {
	suite.skipIfUnsupported(ScopeSearchIndexFeature)

	mgr := globalScope.SearchIndexes()

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

func (suite *IntegrationTestSuite) TestScopeSearchIndexesQueryControl() {
	suite.skipIfUnsupported(ScopeSearchIndexFeature)

	mgr := globalScope.SearchIndexes()

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

func (suite *IntegrationTestSuite) TestScopeSearchIndexesPartitionControl() {
	suite.skipIfUnsupported(ScopeSearchIndexFeature)

	mgr := globalScope.SearchIndexes()

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

func (suite *IntegrationTestSuite) TestScopeSearchIndexesIndexNotFound() {
	suite.skipIfUnsupported(ScopeSearchIndexFeature)

	mgr := globalScope.SearchIndexes()

	indexName := "does-not-exist"

	suite.Run("TestDropSearchIndexNotFound", func() {
		err := mgr.DropIndex(indexName, nil)
		suite.Require().ErrorIs(err, ErrIndexNotFound)
	})
	suite.Run("TestGetSearchIndexNotFound", func() {
		_, err := mgr.GetIndex(indexName, nil)
		suite.Require().ErrorIs(err, ErrIndexNotFound)
	})
	suite.Run("TestGetIndexedDocumentsCountIndexNotFound", func() {
		_, err := mgr.GetIndexedDocumentsCount(indexName, nil)
		suite.Require().ErrorIs(err, ErrIndexNotFound)
	})
	suite.Run("TestPauseIngestIndexNotFound", func() {
		err := mgr.PauseIngest(indexName, nil)
		suite.Require().ErrorIs(err, ErrIndexNotFound)
	})
	suite.Run("TestResumeIngestIndexNotFound", func() {
		err := mgr.ResumeIngest(indexName, nil)
		suite.Require().ErrorIs(err, ErrIndexNotFound)
	})
	suite.Run("TestAllowQueryingIndexNotFound", func() {
		err := mgr.AllowQuerying(indexName, nil)
		suite.Require().ErrorIs(err, ErrIndexNotFound)
	})
	suite.Run("TestDisallowQueryingIndexNotFound", func() {
		err := mgr.DisallowQuerying(indexName, nil)
		suite.Require().ErrorIs(err, ErrIndexNotFound)
	})
	suite.Run("TestFreezePlanIndexNotFound", func() {
		err := mgr.FreezePlan(indexName, nil)
		suite.Require().ErrorIs(err, ErrIndexNotFound)
	})
	suite.Run("TestUnfreezePlanIndexNotFound", func() {
		err := mgr.UnfreezePlan(indexName, nil)
		suite.Require().ErrorIs(err, ErrIndexNotFound)
	})
}

func (suite *UnitTestSuite) TestScopeSearchIndexesAnalyzeDocumentCore() {
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

			expectedPath := fmt.Sprintf("/api/bucket/%s/scope/%s/index/%s/analyzeDoc", globalScope.bucket.bucketName, globalScope.scopeName, indexName)

			suite.Assert().Equal(expectedPath, req.Path)
			suite.Assert().Equal(ServiceTypeSearch, req.Service)
			suite.Assert().True(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("POST", req.Method)
			suite.Assert().Nil(req.RetryStrategy)
		}).
		Return(resp, nil)

	mockCapVerifier := new(mockSearchCapabilityVerifier)
	mockCapVerifier.
		On("SearchCapabilityStatus", mock.AnythingOfType("gocbcore.SearchCapability")).
		Return(gocbcore.CapabilityStatusSupported)

	mgr := &searchIndexProviderCore{
		mgmtProvider:      mockProvider,
		searchCapVerifier: mockCapVerifier,
		tracer:            newTracerWrapper(&NoopTracer{}),
	}

	res, err := mgr.AnalyzeDocument(globalScope, indexName, struct{}{}, &AnalyzeDocumentOptions{
		Timeout: 1 * time.Second,
	})
	suite.Require().Nil(err, err)

	suite.Require().NotNil(res)
}

func (suite *UnitTestSuite) TestScopeSearchIndexesFeatureNotAvailable() {
	mockCapVerifier := new(mockSearchCapabilityVerifier)
	mockCapVerifier.
		On("SearchCapabilityStatus", gocbcore.SearchCapabilityScopedIndexes).
		Return(gocbcore.CapabilityStatusUnsupported)

	cli := new(mockConnectionManager)
	cli.On("getSearchIndexProvider").Return(&searchIndexProviderCore{
		searchCapVerifier: mockCapVerifier,
	}, nil)
	cli.On("getMeter").Return(nil)
	cli.On("MarkOpBeginning").Return()
	cli.On("MarkOpCompleted").Return()

	b := suite.bucket("mock", cli)
	s := suite.newScope(b, "test")
	mgr := s.SearchIndexes()

	indexName := "test-index"

	suite.Run("DropIndex", func() {
		err := mgr.DropIndex(indexName, nil)
		suite.Require().ErrorIs(err, ErrFeatureNotAvailable)
	})
	suite.Run("GetIndex", func() {
		_, err := mgr.GetIndex(indexName, nil)
		suite.Require().ErrorIs(err, ErrFeatureNotAvailable)
	})
	suite.Run("GetIndexedDocumentsCount", func() {
		_, err := mgr.GetIndexedDocumentsCount(indexName, nil)
		suite.Require().ErrorIs(err, ErrFeatureNotAvailable)
	})
	suite.Run("PauseIngest", func() {
		err := mgr.PauseIngest(indexName, nil)
		suite.Require().ErrorIs(err, ErrFeatureNotAvailable)
	})
	suite.Run("ResumeIngest", func() {
		err := mgr.ResumeIngest(indexName, nil)
		suite.Require().ErrorIs(err, ErrFeatureNotAvailable)
	})
	suite.Run("AllowQuerying", func() {
		err := mgr.AllowQuerying(indexName, nil)
		suite.Require().ErrorIs(err, ErrFeatureNotAvailable)
	})
	suite.Run("DisallowQuerying", func() {
		err := mgr.DisallowQuerying(indexName, nil)
		suite.Require().ErrorIs(err, ErrFeatureNotAvailable)
	})
	suite.Run("FreezePlan", func() {
		err := mgr.FreezePlan(indexName, nil)
		suite.Require().ErrorIs(err, ErrFeatureNotAvailable)
	})
	suite.Run("UnfreezePlan", func() {
		err := mgr.UnfreezePlan(indexName, nil)
		suite.Require().ErrorIs(err, ErrFeatureNotAvailable)
	})
	suite.Run("AnalyzeDocument", func() {
		_, err := mgr.AnalyzeDocument(indexName, "sample-content", nil)
		suite.Require().ErrorIs(err, ErrFeatureNotAvailable)
	})
}
