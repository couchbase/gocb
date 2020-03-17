package gocb

import (
	"bytes"
	"errors"
	"io/ioutil"
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
	}, DesignDocumentNamespaceDevelopment, &UpsertDesignDocumentOptions{
		Timeout: 1 * time.Second,
	})
	suite.Require().Nil(err, err)

	var designdoc *DesignDocument
	success := suite.tryUntil(time.Now().Add(5*time.Second), 500*time.Millisecond, func() bool {
		designdoc, err = mgr.GetDesignDocument("test", DesignDocumentNamespaceDevelopment, &GetDesignDocumentOptions{
			Timeout: 1 * time.Second,
		})
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

	designdocs, err := mgr.GetAllDesignDocuments(DesignDocumentNamespaceDevelopment, &GetAllDesignDocumentsOptions{
		Timeout: 1 * time.Second,
	})
	suite.Require().Nil(err, err)

	suite.Require().GreaterOrEqual(len(designdocs), 1)

	err = mgr.PublishDesignDocument("test", &PublishDesignDocumentOptions{
		Timeout: 1 * time.Second,
	})
	suite.Require().Nil(err, err)

	// It can take time for the published doc to come online
	success = suite.tryUntil(time.Now().Add(5*time.Second), 500*time.Millisecond, func() bool {
		designdoc, err = mgr.GetDesignDocument("test", DesignDocumentNamespaceProduction, &GetDesignDocumentOptions{
			Timeout: 1 * time.Second,
		})
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

	err = mgr.DropDesignDocument("test", DesignDocumentNamespaceProduction, &DropDesignDocumentOptions{
		Timeout: 1 * time.Second,
	})
	suite.Require().Nil(err, err)
}

func (suite *UnitTestSuite) TestViewIndexManagerGetDoesntExist() {
	ddocName := "ddoc"
	retErr := `{"error": "not_found", "reason": "missing}`
	resp := &mgmtResponse{
		Endpoint:   "http://localhost:8092/default",
		StatusCode: 404,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	mockProvider := new(mockMgmtProvider)
	mockProvider.
		On("executeMgmtRequest", mock.AnythingOfType("mgmtRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(0).(mgmtRequest)

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
		tracer:       &noopTracer{},
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
		Body:       ioutil.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	mockProvider := new(mockMgmtProvider)
	mockProvider.
		On("executeMgmtRequest", mock.AnythingOfType("mgmtRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(0).(mgmtRequest)

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
		tracer:       &noopTracer{},
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
		Body:       ioutil.NopCloser(bytes.NewReader([]byte(retErr))),
	}

	mockProvider := new(mockMgmtProvider)
	mockProvider.
		On("executeMgmtRequest", mock.AnythingOfType("mgmtRequest")).
		Run(func(args mock.Arguments) {
			req := args.Get(0).(mgmtRequest)

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
		tracer:       &noopTracer{},
	}

	err := viewMgr.DropDesignDocument(ddocName, DesignDocumentNamespaceProduction, &DropDesignDocumentOptions{
		Timeout: 1 * time.Second,
	})
	if !errors.Is(err, ErrDesignDocumentNotFound) {
		suite.T().Fatalf("Expected design document not found: %s", err)
	}
}
