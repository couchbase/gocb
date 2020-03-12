package gocb

import (
	"errors"
)

func (suite *IntegrationTestSuite) TestAnalyticsIndexesCrud() {
	suite.skipIfUnsupported(AnalyticsIndexFeature)

	mgr := globalCluster.AnalyticsIndexes()

	err := mgr.CreateDataverse("testaverse", nil)
	if err != nil {
		suite.T().Fatalf("Expected CreateDataverse to not error %v", err)
	}

	err = mgr.CreateDataverse("testaverse", &CreateAnalyticsDataverseOptions{
		IgnoreIfExists: true,
	})
	if err != nil {
		suite.T().Fatalf("Expected CreateDataverse to not error %v", err)
	}

	err = mgr.CreateDataverse("testaverse", nil)
	if err == nil {
		suite.T().Fatalf("Expected CreateDataverse to error")
	}

	if !errors.Is(err, ErrDataverseExists) {
		suite.T().Fatalf("Expected error to be dataverse already exists but was %v", err)
	}

	err = mgr.CreateDataset("testaset", globalBucket.Name(), &CreateAnalyticsDatasetOptions{
		DataverseName: "testaverse",
	})
	if err != nil {
		suite.T().Fatalf("Expected CreateDataset to not error %v", err)
	}

	err = mgr.CreateDataset("testaset", globalBucket.Name(), &CreateAnalyticsDatasetOptions{
		IgnoreIfExists: true,
		DataverseName:  "testaverse",
	})
	if err != nil {
		suite.T().Fatalf("Expected CreateDataset to not error %v", err)
	}

	err = mgr.CreateDataset("testaset", globalBucket.Name(), &CreateAnalyticsDatasetOptions{
		DataverseName: "testaverse",
	})
	if err == nil {
		suite.T().Fatalf("Expected CreateDataverse to error")
	}

	if !errors.Is(err, ErrDatasetExists) {
		suite.T().Fatalf("Expected error to be dataset already exists but was %v", err)
	}

	err = mgr.CreateIndex("testaset", "testindex", map[string]string{
		"testkey": "string",
	}, &CreateAnalyticsIndexOptions{
		IgnoreIfExists: true,
		DataverseName:  "testaverse",
	})
	if err != nil {
		suite.T().Fatalf("Expected CreateIndex to not error %v", err)
	}

	err = mgr.CreateIndex("testaset", "testindex", map[string]string{
		"testkey": "string",
	}, &CreateAnalyticsIndexOptions{
		IgnoreIfExists: true,
		DataverseName:  "testaverse",
	})
	if err != nil {
		suite.T().Fatalf("Expected CreateIndex to not error %v", err)
	}

	err = mgr.CreateIndex("testaset", "testindex", map[string]string{
		"testkey": "string",
	}, &CreateAnalyticsIndexOptions{
		IgnoreIfExists: false,
		DataverseName:  "testaverse",
	})
	if err == nil {
		suite.T().Fatalf("Expected CreateIndex to error")
	}

	if !errors.Is(err, ErrIndexExists) {
		suite.T().Fatalf("Expected error to be index already exists but was %v", err)
	}

	err = mgr.ConnectLink(nil)
	if err != nil {
		suite.T().Fatalf("Expected ConnectLink to not error %v", err)
	}

	datasets, err := mgr.GetAllDatasets(nil)
	if err != nil {
		suite.T().Fatalf("Expected GetAllDatasets to not error %v", err)
	}

	if len(datasets) == 0 {
		suite.T().Fatalf("Expected datasets length to be greater than 0")
	}

	indexes, err := mgr.GetAllIndexes(nil)
	if err != nil {
		suite.T().Fatalf("Expected GetAllIndexes to not error %v", err)
	}

	if len(indexes) == 0 {
		suite.T().Fatalf("Expected indexes length to be greater than 0")
	}

	if globalCluster.SupportsFeature(AnalyticsIndexPendingMutationsFeature) {
		_, err = mgr.GetPendingMutations(nil)
		if err != nil {
			suite.T().Fatalf("Expected GetPendingMutations to not error %v", err)
		}
	}

	err = mgr.DisconnectLink(nil)
	if err != nil {
		suite.T().Fatalf("Expected DisconnectLink to not error %v", err)
	}

	err = mgr.DropIndex("testaset", "testindex", &DropAnalyticsIndexOptions{
		IgnoreIfNotExists: true,
		DataverseName:     "testaverse",
	})
	if err != nil {
		suite.T().Fatalf("Expected DropIndex to not error %v", err)
	}

	err = mgr.DropIndex("testaset", "testindex", &DropAnalyticsIndexOptions{
		IgnoreIfNotExists: true,
		DataverseName:     "testaverse",
	})
	if err != nil {
		suite.T().Fatalf("Expected DropIndex to not error %v", err)
	}

	err = mgr.DropIndex("testaset", "testindex", &DropAnalyticsIndexOptions{
		DataverseName: "testaverse",
	})
	if err == nil {
		suite.T().Fatalf("Expected DropIndex to error")
	}

	if !errors.Is(err, ErrIndexNotFound) {
		suite.T().Fatalf("Expected error to be index not found but was %v", err)
	}

	err = mgr.DropDataset("testaset", &DropAnalyticsDatasetOptions{
		DataverseName: "testaverse",
	})
	if err != nil {
		suite.T().Fatalf("Expected DropDataset to not error %v", err)
	}

	err = mgr.DropDataset("testaset", &DropAnalyticsDatasetOptions{
		IgnoreIfNotExists: true,
		DataverseName:     "testaverse",
	})
	if err != nil {
		suite.T().Fatalf("Expected DropDataset to not error %v", err)
	}

	err = mgr.DropDataset("testaset", &DropAnalyticsDatasetOptions{
		DataverseName: "testaverse",
	})
	if err == nil {
		suite.T().Fatalf("Expected DropDataset to error")
	}

	if !errors.Is(err, ErrDatasetNotFound) {
		suite.T().Fatalf("Expected error to be dataset not found but was %v", err)
	}

	err = mgr.DropDataverse("testaverse", nil)
	if err != nil {
		suite.T().Fatalf("Expected DropDataverse to not error %v", err)
	}

	err = mgr.DropDataverse("testaverse", &DropAnalyticsDataverseOptions{
		IgnoreIfNotExists: true,
	})
	if err != nil {
		suite.T().Fatalf("Expected DropDataverse to not error %v", err)
	}

	err = mgr.DropDataverse("testaverse", nil)
	if err == nil {
		suite.T().Fatalf("Expected DropDataverse to error")
	}

	if !errors.Is(err, ErrDataverseNotFound) {
		suite.T().Fatalf("Expected error to be dataverse not found but was %v", err)
	}
}
