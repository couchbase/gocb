package gocb

import (
	"errors"
	"net/url"
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

	spans := 22
	if globalCluster.SupportsFeature(AnalyticsIndexPendingMutationsFeature) {
		spans = 23
	}

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(spans, len(nilParents))
	span := suite.RequireQueryMgmtOpSpan(nilParents[0], "manager_analytics_create_dataverse", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: true, // The first operation on new cluster can initially fail with temporary failure - allow that
			hasEncoding:             true,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[3], "manager_analytics_create_dataset", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[6], "manager_analytics_create_index", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[9], "manager_analytics_connect_link", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[10], "manager_analytics_get_all_datasets", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[11], "manager_analytics_get_all_indexes", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	offset := 0
	if globalCluster.SupportsFeature(AnalyticsIndexPendingMutationsFeature) {
		offset = 1
		suite.AssertHTTPOpSpan(nilParents[12], "manager_analytics_get_pending_mutations",
			HTTPOpSpanExpectations{
				operationID:             "GET /analytics/node/agg/stats/remaining",
				numDispatchSpans:        1,
				atLeastNumDispatchSpans: false,
				hasEncoding:             false,
				service:                 "management",
			})
	}
	span = suite.RequireQueryMgmtOpSpan(nilParents[12+offset], "manager_analytics_disconnect_link", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[13+offset], "manager_analytics_drop_index", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[16+offset], "manager_analytics_drop_dataset", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[19+offset], "manager_analytics_drop_dataverse", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})

	numResponses := 22
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_create_dataverse"), 3, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_create_dataset"), 3, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_create_index"), 3, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_connect_link"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_get_all_datasets"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_get_all_indexes"), 1, false)
	if globalCluster.SupportsFeature(AnalyticsIndexPendingMutationsFeature) {
		numResponses++
		suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_get_pending_mutations"), 1, false)
	}
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_disconnect_link"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_drop_dataverse"), 3, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_drop_dataset"), 3, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_drop_index"), 3, false)
}

func (suite *IntegrationTestSuite) TestAnalyticsIndexesCrudCompoundNames() {
	suite.skipIfUnsupported(AnalyticsIndexFeature)
	suite.skipIfUnsupported(AnalyticsIndexLinksScopesFeature)

	mgr := globalCluster.AnalyticsIndexes()

	dataverse := "namepart1/namepart2"
	err := mgr.CreateDataverse(dataverse, nil)
	if err != nil {
		suite.T().Fatalf("Expected CreateDataverse to not error %v", err)
	}

	err = mgr.CreateDataset("testaset", globalBucket.Name(), &CreateAnalyticsDatasetOptions{
		DataverseName: dataverse,
	})
	if err != nil {
		suite.T().Fatalf("Expected CreateDataset to not error %v", err)
	}

	err = mgr.CreateIndex("testaset", "testindex", map[string]string{
		"testkey": "string",
	}, &CreateAnalyticsIndexOptions{
		IgnoreIfExists: true,
		DataverseName:  dataverse,
	})
	if err != nil {
		suite.T().Fatalf("Expected CreateIndex to not error %v", err)
	}

	err = mgr.ConnectLink(&ConnectAnalyticsLinkOptions{
		DataverseName: dataverse,
	})
	if err != nil {
		suite.T().Fatalf("Expected ConnectLink to not error %v", err)
	}

	err = mgr.DisconnectLink(&DisconnectAnalyticsLinkOptions{
		DataverseName: dataverse,
	})
	if err != nil {
		suite.T().Fatalf("Expected DisconnectLink to not error %v", err)
	}

	err = mgr.DropIndex("testaset", "testindex", &DropAnalyticsIndexOptions{
		IgnoreIfNotExists: true,
		DataverseName:     dataverse,
	})
	if err != nil {
		suite.T().Fatalf("Expected DropIndex to not error %v", err)
	}

	err = mgr.DropDataset("testaset", &DropAnalyticsDatasetOptions{
		DataverseName: dataverse,
	})
	if err != nil {
		suite.T().Fatalf("Expected DropDataset to not error %v", err)
	}

	err = mgr.DropDataverse(dataverse, nil)
	if err != nil {
		suite.T().Fatalf("Expected DropDataverse to not error %v", err)
	}

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(8, len(nilParents))
	span := suite.RequireQueryMgmtOpSpan(nilParents[0], "manager_analytics_create_dataverse", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[1], "manager_analytics_create_dataset", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[2], "manager_analytics_create_index", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[3], "manager_analytics_connect_link", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[4], "manager_analytics_disconnect_link", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[5], "manager_analytics_drop_index", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[6], "manager_analytics_drop_dataset", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	span = suite.RequireQueryMgmtOpSpan(nilParents[7], "manager_analytics_drop_dataverse", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_create_dataverse"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_create_dataset"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_create_index"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_connect_link"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_disconnect_link"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_drop_dataverse"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_drop_dataset"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_drop_index"), 1, false)
}

func (suite *IntegrationTestSuite) TestAnalyticsIndexesS3Links() {
	suite.skipIfUnsupported(AnalyticsIndexFeature)
	suite.skipIfUnsupported(AnalyticsIndexLinksFeature)

	mgr := globalCluster.AnalyticsIndexes()

	dataverse := "scopeslinkaverse"
	err := mgr.CreateDataverse(dataverse, &CreateAnalyticsDataverseOptions{
		IgnoreIfExists: true,
	})
	suite.Require().Nil(err, err)
	defer mgr.DropDataverse(dataverse, nil)

	link := NewS3ExternalAnalyticsLink("s3Link", dataverse, "accesskey",
		"secretKey", "us-east-1", nil)

	link2 := NewS3ExternalAnalyticsLink("s3Link2", dataverse, "2",
		"secretKey2", "us-east-1", &NewS3ExternalAnalyticsLinkOptions{ServiceEndpoint: "end"})

	err = mgr.CreateLink(link, nil)
	suite.Require().Nil(err, err)
	err = mgr.CreateLink(link2, nil)
	suite.Require().Nil(err, err)

	err = mgr.CreateLink(link, nil)
	if !errors.Is(err, ErrAnalyticsLinkExists) {
		suite.T().Fatalf("Expected error to be link already exists but was %v", err)
	}

	links, err := mgr.GetLinks(nil)
	suite.Require().Nil(err, err)

	resultLink1 := &S3ExternalAnalyticsLink{
		Dataverse:       link.Dataverse,
		LinkName:        link.Name(),
		AccessKeyID:     link.AccessKeyID,
		Region:          link.Region,
		ServiceEndpoint: link.ServiceEndpoint,
	}
	resultLink2 := &S3ExternalAnalyticsLink{
		Dataverse:       link2.Dataverse,
		LinkName:        link2.Name(),
		AccessKeyID:     link2.AccessKeyID,
		Region:          link2.Region,
		ServiceEndpoint: link2.ServiceEndpoint,
	}

	suite.Require().Len(links, 2)
	suite.Assert().Contains(links, resultLink1)
	suite.Assert().Contains(links, resultLink2)

	links, err = mgr.GetLinks(&GetAnalyticsLinksOptions{
		Dataverse: dataverse,
		Name:      link.Name(),
		LinkType:  AnalyticsLinkTypeS3External,
	})
	suite.Require().Nil(err, err)

	suite.Require().Len(links, 1)
	suite.Assert().Contains(links, resultLink1)

	rLink := NewS3ExternalAnalyticsLink("s3Link", dataverse, "accesskey2",
		"secretKey", "us-east-1", nil)

	err = mgr.ReplaceLink(rLink, nil)
	suite.Require().Nil(err, err)

	links, err = mgr.GetLinks(nil)
	suite.Require().Nil(err, err)

	suite.Require().Len(links, 2)

	err = mgr.DropLink("s3Link", dataverse, nil)
	suite.Require().Nil(err, err)
	err = mgr.DropLink("s3Link2", dataverse, nil)
	suite.Require().Nil(err, err)

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(10, len(nilParents))
	span := suite.RequireQueryMgmtOpSpan(nilParents[0], "manager_analytics_create_dataverse", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	suite.AssertHTTPOpSpan(nilParents[1], "manager_analytics_create_link",
		HTTPOpSpanExpectations{
			operationID:             "POST /analytics/link",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[2], "manager_analytics_create_link",
		HTTPOpSpanExpectations{
			operationID:             "POST /analytics/link",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[3], "manager_analytics_create_link",
		HTTPOpSpanExpectations{
			operationID:             "POST /analytics/link",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[4], "manager_analytics_get_all_links",
		HTTPOpSpanExpectations{
			operationID:             "GET /analytics/link",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[5], "manager_analytics_get_all_links",
		HTTPOpSpanExpectations{
			operationID:             "GET /analytics/link?dataverse=" + dataverse + "&name=" + link.LinkName + "&type=s3",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[6], "manager_analytics_replace_link",
		HTTPOpSpanExpectations{
			operationID:             "PUT /analytics/link",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[7], "manager_analytics_get_all_links",
		HTTPOpSpanExpectations{
			operationID:             "GET /analytics/link",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[8], "manager_analytics_drop_link",
		HTTPOpSpanExpectations{
			operationID:             "DELETE /analytics/link",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[9], "manager_analytics_drop_link",
		HTTPOpSpanExpectations{
			operationID:             "DELETE /analytics/link",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			service:                 "management",
		})

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_create_dataverse"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_create_link"), 3, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_replace_link"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_get_all_links"), 3, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_drop_link"), 2, false)
}

func (suite *IntegrationTestSuite) TestAnalyticsIndexesS3LinksScopes() {
	suite.skipIfUnsupported(AnalyticsIndexFeature)
	suite.skipIfUnsupported(AnalyticsIndexLinksFeature)
	suite.skipIfUnsupported(AnalyticsIndexLinksScopesFeature)

	mgr := globalCluster.AnalyticsIndexes()

	dataverse := globalBucket.Name() + "/" + globalScope.Name()
	err := mgr.CreateDataverse(dataverse, &CreateAnalyticsDataverseOptions{
		IgnoreIfExists: true,
	})
	suite.Require().Nil(err, err)
	defer mgr.DropDataverse(dataverse, nil)

	link := NewS3ExternalAnalyticsLink("s3LinkScope", dataverse, "accesskey",
		"secretKey", "us-east-1", nil)

	link2 := NewS3ExternalAnalyticsLink("s3LinkScope2", dataverse, "2",
		"secretKey2", "us-east-1", &NewS3ExternalAnalyticsLinkOptions{ServiceEndpoint: "end"})

	err = mgr.CreateLink(link, nil)
	suite.Require().Nil(err, err)
	err = mgr.CreateLink(link2, nil)
	suite.Require().Nil(err, err)

	err = mgr.CreateLink(link, nil)
	if !errors.Is(err, ErrAnalyticsLinkExists) {
		suite.T().Fatalf("Expected error to be link already exists but was %v", err)
	}

	links, err := mgr.GetLinks(nil)
	suite.Require().Nil(err, err)

	resultLink1 := &S3ExternalAnalyticsLink{
		Dataverse:       link.Dataverse,
		LinkName:        link.LinkName,
		AccessKeyID:     link.AccessKeyID,
		Region:          link.Region,
		ServiceEndpoint: link.ServiceEndpoint,
	}
	resultLink2 := &S3ExternalAnalyticsLink{
		Dataverse:       link2.Dataverse,
		LinkName:        link2.LinkName,
		AccessKeyID:     link2.AccessKeyID,
		Region:          link2.Region,
		ServiceEndpoint: link2.ServiceEndpoint,
	}

	suite.Require().Len(links, 2)
	suite.Assert().Contains(links, resultLink1)
	suite.Assert().Contains(links, resultLink2)

	links, err = mgr.GetLinks(&GetAnalyticsLinksOptions{
		Dataverse: dataverse,
		Name:      link.Name(),
		LinkType:  AnalyticsLinkTypeS3External,
	})
	suite.Require().Nil(err, err)

	suite.Require().Len(links, 1)
	suite.Assert().Contains(links, resultLink1)

	rLink := NewS3ExternalAnalyticsLink("s3LinkScope", dataverse, "accesskey2",
		"secretKey", "us-east-1", nil)

	err = mgr.ReplaceLink(rLink, nil)
	suite.Require().Nil(err, err)

	links, err = mgr.GetLinks(nil)
	suite.Require().Nil(err, err)

	suite.Require().Len(links, 2)

	err = mgr.DropLink("s3LinkScope", dataverse, nil)
	suite.Require().Nil(err, err)
	err = mgr.DropLink("s3LinkScope2", dataverse, nil)
	suite.Require().Nil(err, err)

	escapedScope := url.PathEscape(dataverse)

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(10, len(nilParents))
	span := suite.RequireQueryMgmtOpSpan(nilParents[0], "manager_analytics_create_dataverse", "analytics")
	suite.AssertHTTPOpSpan(span, "analytics",
		HTTPOpSpanExpectations{
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			dispatchOperationID:     "any",
			service:                 "analytics",
			statement:               "any",
		})
	suite.AssertHTTPOpSpan(nilParents[1], "manager_analytics_create_link",
		HTTPOpSpanExpectations{
			operationID:             "POST /analytics/link/" + escapedScope + "/s3LinkScope",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[2], "manager_analytics_create_link",
		HTTPOpSpanExpectations{
			operationID:             "POST /analytics/link/" + escapedScope + "/s3LinkScope2",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[3], "manager_analytics_create_link",
		HTTPOpSpanExpectations{
			operationID:             "POST /analytics/link/" + escapedScope + "/s3LinkScope",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[4], "manager_analytics_get_all_links",
		HTTPOpSpanExpectations{
			operationID:             "GET /analytics/link",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[5], "manager_analytics_get_all_links",
		HTTPOpSpanExpectations{
			operationID:             "GET /analytics/link/" + escapedScope + "/s3LinkScope" + "?type=s3",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[6], "manager_analytics_replace_link",
		HTTPOpSpanExpectations{
			operationID:             "PUT /analytics/link/" + escapedScope + "/s3LinkScope",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[7], "manager_analytics_get_all_links",
		HTTPOpSpanExpectations{
			operationID:             "GET /analytics/link",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[8], "manager_analytics_drop_link",
		HTTPOpSpanExpectations{
			operationID:             "DELETE /analytics/link/" + escapedScope + "/s3LinkScope",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			service:                 "management",
		})
	suite.AssertHTTPOpSpan(nilParents[9], "manager_analytics_drop_link",
		HTTPOpSpanExpectations{
			operationID:             "DELETE /analytics/link/" + escapedScope + "/s3LinkScope2",
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			service:                 "management",
		})

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_create_dataverse"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_create_link"), 3, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_replace_link"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_get_all_links"), 3, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_analytics_drop_link"), 2, false)
}

func (suite *UnitTestSuite) TestAnalyticsIndexesCouchbaseLinksFormEncode() {
	link := NewCouchbaseRemoteAnalyticsLink("link", "host", "scope",
		&NewCouchbaseRemoteAnalyticsLinkOptions{
			Username: "username",
			Password: "password",
		})

	body, err := link.FormEncode()
	suite.Require().Nil(err, err)
	data := string(body)
	q, err := url.ParseQuery(data)
	suite.Require().Nil(err)

	suite.Assert().Equal("host", q.Get("hostname"))
	suite.Assert().Equal(string(AnalyticsLinkTypeCouchbaseRemote), q.Get("type"))
	suite.Assert().Equal("none", q.Get("encryption"))
	suite.Assert().Equal("username", q.Get("username"))
	suite.Assert().Equal("password", q.Get("password"))

	link = NewCouchbaseRemoteAnalyticsLink("link", "host", "scope",
		&NewCouchbaseRemoteAnalyticsLinkOptions{
			Encryption: CouchbaseRemoteAnalyticsEncryptionSettings{
				EncryptionLevel:   AnalyticsEncryptionLevelFull,
				Certificate:       []byte("certificate"),
				ClientCertificate: []byte("clientcertificate"),
				ClientKey:         []byte("clientkey"),
			},
		})

	body, err = link.FormEncode()
	suite.Require().Nil(err, err)
	data = string(body)
	q, err = url.ParseQuery(data)
	suite.Require().Nil(err)

	suite.Assert().Equal("host", q.Get("hostname"))
	suite.Assert().Equal(string(AnalyticsLinkTypeCouchbaseRemote), q.Get("type"))
	suite.Assert().Equal("full", q.Get("encryption"))
	suite.Assert().Equal("certificate", q.Get("certificate"))
	suite.Assert().Equal("clientcertificate", q.Get("clientCertificate"))
	suite.Assert().Equal("clientkey", q.Get("clientKey"))
}
