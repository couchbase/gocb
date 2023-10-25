package gocb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/stretchr/testify/mock"
)

func (suite *IntegrationTestSuite) TestViewIndexManagerCrud() {
	suite.skipIfUnsupported(ViewFeature)
	suite.skipIfUnsupported(ViewIndexUpsertBugFeature)

	mgr := globalBucket.ViewIndexes()

	err := mgr.UpsertDesignDocument(DesignDocument{
		Name: "test",
		Views: map[string]View{
			"testView": {
				Map: `function(doc, meta)
				{
					emit(doc, null);
				}`,
				Reduce: "_count",
			},
		},
	}, DesignDocumentNamespaceDevelopment, &UpsertDesignDocumentOptions{})
	suite.Require().Nil(err, err)

	var designdoc *DesignDocument
	numGetsStart := 0
	success := suite.tryUntil(time.Now().Add(5*time.Second), 500*time.Millisecond, func() bool {
		numGetsStart++
		designdoc, err = mgr.GetDesignDocument("test", DesignDocumentNamespaceDevelopment, &GetDesignDocumentOptions{})
		if err != nil {
			return false
		}

		return true
	})
	if !success {
		suite.T().Fatalf("Wait time for get design document expired.")
	}

	suite.Assert().Equal("test", designdoc.Name)
	suite.Require().Equal(1, len(designdoc.Views))
	suite.Require().Contains(designdoc.Views, "testView")

	view := designdoc.Views["testView"]
	suite.Assert().NotEmpty(view.Map)
	suite.Assert().NotEmpty(view.Reduce)

	designdocs, err := mgr.GetAllDesignDocuments(DesignDocumentNamespaceDevelopment, &GetAllDesignDocumentsOptions{})
	suite.Require().Nil(err, err)

	suite.Require().GreaterOrEqual(len(designdocs), 1)

	err = mgr.PublishDesignDocument("test", &PublishDesignDocumentOptions{})
	suite.Require().Nil(err, err)

	// It can take time for the published doc to come online
	numGetsPublished := 0
	success = suite.tryUntil(time.Now().Add(5*time.Second), 500*time.Millisecond, func() bool {
		numGetsPublished++
		designdoc, err = mgr.GetDesignDocument("test", DesignDocumentNamespaceProduction, &GetDesignDocumentOptions{})
		if err != nil {
			return false
		}

		return true
	})
	if !success {
		suite.T().Fatalf("Wait time for get design document expired.")
	}

	suite.Assert().Equal("test", designdoc.Name)
	suite.Require().Equal(1, len(designdoc.Views))

	err = mgr.DropDesignDocument("test", DesignDocumentNamespaceProduction, &DropDesignDocumentOptions{})
	suite.Require().Nil(err, err)

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(4+numGetsPublished+numGetsStart, len(nilParents))
	suite.AssertHTTPOpSpan(nilParents[0], "manager_views_upsert_design_document",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			operationID:             "PUT /_design/dev_test",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			dispatchOperationID:     "any",
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[1], "manager_views_get_design_document",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			operationID:             "GET /_design/dev_test",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[numGetsStart+1], "manager_views_get_all_design_documents",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			operationID:             "GET " + fmt.Sprintf("/pools/default/buckets/%s/ddocs", globalConfig.Bucket),
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "management",
		})

	publishParents := nilParents[numGetsStart+2]
	suite.AssertHTTPSpan(publishParents, "manager_views_publish_design_document", globalConfig.Bucket, "",
		"", "management", "", "")
	suite.Require().Len(publishParents.Spans, 2)
	suite.Require().Contains(publishParents.Spans, "manager_views_get_design_document")
	suite.Require().Contains(publishParents.Spans, "manager_views_upsert_design_document")
	suite.AssertHTTPOpSpan(publishParents.Spans["manager_views_get_design_document"][0], "manager_views_get_design_document",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			operationID:             "GET /_design/dev_test",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(publishParents.Spans["manager_views_upsert_design_document"][0], "manager_views_upsert_design_document",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			operationID:             "PUT /_design/test",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			dispatchOperationID:     "any",
			service:                 "management",
		})

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_views_upsert_design_document"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_views_get_design_document"), 2, true)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_views_get_all_design_documents"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_views_drop_design_document"), 1, false)
}

func (suite *UnitTestSuite) TestViewIndexManagerGetDoesntExist() {
	ddocName := "ddoc"
	retErr := `{"error": "not_found", "reason": "missing}`
	resp := &mgmtResponse{
		Endpoint:   "http://localhost:8092/default",
		StatusCode: 404,
		Body:       io.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	mockProvider := new(mockMgmtProvider)
	mockProvider.
		On("executeMgmtRequest", nil, mock.AnythingOfType("mgmtRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(1).(mgmtRequest)

			suite.Assert().Equal("/_design/dev_"+ddocName, req.Path)
			suite.Assert().Equal(ServiceTypeViews, req.Service)
			suite.Assert().True(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("GET", req.Method)
			suite.Assert().Nil(req.RetryStrategy)
		}).
		Return(resp, nil)

	viewMgr := ViewIndexManager{
		mgmtProvider: mockProvider,
		bucketName:   "mock",
		tracer:       &NoopTracer{},
		meter:        &meterWrapper{meter: &NoopMeter{}},
	}

	_, err := viewMgr.GetDesignDocument(ddocName, DesignDocumentNamespaceDevelopment, &GetDesignDocumentOptions{
		Timeout: 1 * time.Second,
	})
	if !errors.Is(err, ErrDesignDocumentNotFound) {
		suite.T().Fatalf("Expected design document not found: %s", err)
	}
}

func (suite *UnitTestSuite) TestViewIndexManagerPublishDoesntExist() {
	ddocName := "ddoc"
	retErr := `{"error": "not_found", "reason": "missing}`
	resp := &mgmtResponse{
		Endpoint:   "http://localhost:8092/default",
		StatusCode: 404,
		Body:       io.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	mockProvider := new(mockMgmtProvider)
	mockProvider.
		On("executeMgmtRequest", nil, mock.AnythingOfType("mgmtRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(1).(mgmtRequest)

			suite.Assert().Equal("/_design/dev_"+ddocName, req.Path)
			suite.Assert().Equal(ServiceTypeViews, req.Service)
			suite.Assert().True(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("GET", req.Method)
			suite.Assert().Nil(req.RetryStrategy)
		}).
		Return(resp, nil)

	viewMgr := ViewIndexManager{
		mgmtProvider: mockProvider,
		bucketName:   "mock",
		tracer:       &NoopTracer{},
		meter:        &meterWrapper{meter: &NoopMeter{}},
	}

	err := viewMgr.PublishDesignDocument(ddocName, &PublishDesignDocumentOptions{
		Timeout: 1 * time.Second,
	})
	if !errors.Is(err, ErrDesignDocumentNotFound) {
		suite.T().Fatalf("Expected design document not found: %s", err)
	}
}

func (suite *UnitTestSuite) TestViewIndexManagerDropDoesntExist() {
	ddocName := "ddoc"
	retErr := `{"error": "not_found", "reason": "missing}`
	resp := &mgmtResponse{
		Endpoint:   "http://localhost:8092/default",
		StatusCode: 404,
		Body:       io.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	mockProvider := new(mockMgmtProvider)
	mockProvider.
		On("executeMgmtRequest", nil, mock.AnythingOfType("mgmtRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(1).(mgmtRequest)

			suite.Assert().Equal("/_design/"+ddocName, req.Path)
			suite.Assert().Equal(ServiceTypeViews, req.Service)
			suite.Assert().False(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("DELETE", req.Method)
			suite.Assert().Nil(req.RetryStrategy)
		}).
		Return(resp, nil)

	viewMgr := ViewIndexManager{
		mgmtProvider: mockProvider,
		bucketName:   "mock",
		tracer:       &NoopTracer{},
		meter:        &meterWrapper{meter: &NoopMeter{}},
	}

	err := viewMgr.DropDesignDocument(ddocName, DesignDocumentNamespaceProduction, &DropDesignDocumentOptions{
		Timeout: 1 * time.Second,
	})
	if !errors.Is(err, ErrDesignDocumentNotFound) {
		suite.T().Fatalf("Expected design document not found: %s", err)
	}
}

func (suite *UnitTestSuite) TestViewIndexManagerGetAllDesignDocumentsFiltersCorrectlyProduction() {
	payload, err := loadRawTestDataset("views_response_70")
	suite.Require().Nil(err)

	resp := &mgmtResponse{
		Endpoint:   "http://localhost:8092/default",
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(payload)),
	}

	mockProvider := new(mockMgmtProvider)
	mockProvider.
		On("executeMgmtRequest", nil, mock.AnythingOfType("mgmtRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(1).(mgmtRequest)

			suite.Assert().Equal("/pools/default/buckets/mock/ddocs", req.Path)
			suite.Assert().Equal(ServiceTypeManagement, req.Service)
			suite.Assert().True(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("GET", req.Method)
			suite.Assert().Nil(req.RetryStrategy)
		}).
		Return(resp, nil)

	viewMgr := ViewIndexManager{
		mgmtProvider: mockProvider,
		bucketName:   "mock",
		tracer:       &NoopTracer{},
		meter:        &meterWrapper{meter: &NoopMeter{}},
	}

	ddocs, err := viewMgr.GetAllDesignDocuments(DesignDocumentNamespaceProduction, &GetAllDesignDocumentsOptions{
		Timeout: 1 * time.Second,
	})
	suite.Require().Nil(err)

	suite.Require().Len(ddocs, 1)
	suite.Assert().Equal("aaa", ddocs[0].Name)
}

func (suite *UnitTestSuite) TestViewIndexManagerGetAllDesignDocumentsFiltersCorrectlyDevelopment() {
	payload, err := loadRawTestDataset("views_response_70")
	suite.Require().Nil(err)

	resp := &mgmtResponse{
		Endpoint:   "http://localhost:8092/default",
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(payload)),
	}

	mockProvider := new(mockMgmtProvider)
	mockProvider.
		On("executeMgmtRequest", nil, mock.AnythingOfType("mgmtRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(1).(mgmtRequest)

			suite.Assert().Equal("/pools/default/buckets/mock/ddocs", req.Path)
			suite.Assert().Equal(ServiceTypeManagement, req.Service)
			suite.Assert().True(req.IsIdempotent)
			suite.Assert().Equal(1*time.Second, req.Timeout)
			suite.Assert().Equal("GET", req.Method)
			suite.Assert().Nil(req.RetryStrategy)
		}).
		Return(resp, nil)

	viewMgr := ViewIndexManager{
		mgmtProvider: mockProvider,
		bucketName:   "mock",
		tracer:       &NoopTracer{},
		meter:        &meterWrapper{meter: &NoopMeter{}},
	}

	ddocs, err := viewMgr.GetAllDesignDocuments(DesignDocumentNamespaceDevelopment, &GetAllDesignDocumentsOptions{
		Timeout: 1 * time.Second,
	})
	suite.Require().Nil(err)

	suite.Require().Len(ddocs, 3)
	suite.Assert().Equal("aaa", ddocs[0].Name)
	suite.Assert().Equal("test", ddocs[1].Name)
	suite.Assert().Equal("test12", ddocs[2].Name)
}
