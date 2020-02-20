package gocb

import "time"

func (suite *IntegrationTestSuite) TestViewIndexManagerCrud() {
	if !globalCluster.SupportsFeature(ViewFeature) {
		suite.T().Skip("Skipping test as view indexes not supported")
	}

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
	suite.Require().Nil(err)

	designdoc, err := mgr.GetDesignDocument("test", DesignDocumentNamespaceDevelopment, &GetDesignDocumentOptions{
		Timeout: 1 * time.Second,
	})
	suite.Require().Nil(err, err)

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

	suite.Require().GreaterOrEqual(1, len(designdocs))

	err = mgr.PublishDesignDocument("test", &PublishDesignDocumentOptions{
		Timeout: 1 * time.Second,
	})
	suite.Require().Nil(err, err)

	designdoc, err = mgr.GetDesignDocument("test", DesignDocumentNamespaceProduction, &GetDesignDocumentOptions{
		Timeout: 1 * time.Second,
	})
	suite.Require().Nil(err, err)

	suite.Assert().Equal("test", designdoc.Name)
	suite.Require().Equal(1, len(designdoc.Views))

	err = mgr.DropDesignDocument("test", DesignDocumentNamespaceProduction, &DropDesignDocumentOptions{
		Timeout: 1 * time.Second,
	})
	suite.Require().Nil(err, err)
}
