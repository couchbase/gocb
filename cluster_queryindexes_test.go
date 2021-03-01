package gocb

import (
	"errors"
	"time"
)

func (suite *IntegrationTestSuite) TestQueryIndexesCrud() {
	suite.skipIfUnsupported(QueryIndexFeature)

	bucketMgr := globalCluster.Buckets()
	bucketName := "testIndexes"

	err := bucketMgr.CreateBucket(CreateBucketSettings{
		BucketSettings: BucketSettings{
			Name:        bucketName,
			RAMQuotaMB:  100,
			NumReplicas: 0,
			BucketType:  CouchbaseBucketType,
		},
	}, nil)
	suite.Require().Nil(err, err)
	defer bucketMgr.DropBucket(bucketName, nil)

	mgr := globalCluster.QueryIndexes()

	deadline := time.Now().Add(5 * time.Second)
	for {
		err = mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
			IgnoreIfExists: true,
		})
		if err == nil {
			break
		}

		suite.T().Logf("Failed to create primary index: %s", err)

		sleepDeadline := time.Now().Add(500 * time.Millisecond)
		if sleepDeadline.After(deadline) {
			sleepDeadline = deadline
		}
		time.Sleep(sleepDeadline.Sub(time.Now()))

		if sleepDeadline == deadline {
			suite.T().Errorf("timed out waiting for create index to succeed")
			return
		}
	}

	err = mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: false,
	})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexExists) {
		suite.T().Fatalf("Expected index exists error but was %s", err)
	}

	err = mgr.CreateIndex(bucketName, "testIndex", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().Nil(err, err)

	err = mgr.CreateIndex(bucketName, "testIndex", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: false,
	})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexExists) {
		suite.T().Fatalf("Expected index exists error but was %s", err)
	}

	// We create this first to give it a chance to be created by the time we need it.
	err = mgr.CreateIndex(bucketName, "testIndexDeferred", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: false,
		Deferred:       true,
	})
	suite.Require().Nil(err, err)

	indexNames, err := mgr.BuildDeferredIndexes(bucketName, nil)
	suite.Require().Nil(err, err)

	suite.Assert().Len(indexNames, 1)

	err = mgr.WatchIndexes(bucketName, []string{"testIndexDeferred"}, 10*time.Second, nil)
	suite.Require().Nil(err, err)

	indexes, err := mgr.GetAllIndexes(bucketName, nil)
	suite.Require().Nil(err, err)

	suite.Assert().Len(indexes, 3)
	var index QueryIndex
	for _, idx := range indexes {
		if idx.Name == "testIndex" {
			index = idx
			break
		}
	}
	suite.Assert().Equal("testIndex", index.Name)
	suite.Assert().False(index.IsPrimary)
	suite.Assert().Equal(QueryIndexTypeGsi, index.Type)
	suite.Assert().Equal("online", index.State)
	suite.Assert().Equal("testIndexes", index.Keyspace)
	suite.Assert().Equal("default", index.Namespace)
	if suite.Assert().Len(index.IndexKey, 1) {
		suite.Assert().Equal("`field`", index.IndexKey[0])
	}
	suite.Assert().Empty(index.Condition)
	suite.Assert().Empty(index.Partition)

	err = mgr.DropIndex(bucketName, "testIndex", nil)
	suite.Require().Nil(err, err)

	err = mgr.DropIndex(bucketName, "testIndex", nil)
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexNotFound) {
		suite.T().Fatalf("Expected index not found error but was %s", err)
	}

	err = mgr.DropPrimaryIndex(bucketName, nil)
	suite.Require().Nil(err, err)

	err = mgr.DropPrimaryIndex(bucketName, nil)
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexNotFound) {
		suite.T().Fatalf("Expected index not found error but was %s", err)
	}

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_bucket_create_bucket"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_query_create_primary_index"), 2, true)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_query_create_index"), 3, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_query_build_deferred_indexes"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_query_watch_indexes"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_query_get_all_indexes"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_query_drop_primary_index"), 2, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_query_drop_index"), 2, false)
	suite.AssertMetrics(makeMetricsKey(meterNameResponses, "query", ""), 12, true)
	suite.AssertMetrics(makeMetricsKey(meterNameResponses, "management", ""), 1, false)
}

type testQueryIndexDataset struct {
	Results []map[string]interface{}
	jsonQueryResponse
}

type mockQueryIndexRowReader struct {
	Dataset []map[string]interface{}
	mockQueryRowReaderBase
}

func (arr *mockQueryIndexRowReader) NextRow() []byte {
	if arr.idx == len(arr.Dataset) {
		return nil
	}

	idx := arr.idx
	arr.idx++

	return arr.Suite.mustConvertToBytes(arr.Dataset[idx])
}

func (suite *UnitTestSuite) TestQueryIndexesParsing() {
	var dataset testQueryIndexDataset
	err := loadJSONTestDataset("query_index_response", &dataset)
	suite.Require().Nil(err, err)

	reader := &mockQueryIndexRowReader{
		Dataset: dataset.Results,
		mockQueryRowReaderBase: mockQueryRowReaderBase{
			Meta:  suite.mustConvertToBytes(dataset.jsonQueryResponse),
			Suite: suite,
		},
	}

	var cluster *Cluster
	cluster = suite.queryCluster(false, reader, nil)

	mgr := QueryIndexManager{
		provider: cluster,
		tracer:   &NoopTracer{},
		meter:    &NoopMeter{},
	}

	res, err := mgr.GetAllIndexes("mybucket", nil)
	suite.Require().Nil(err, err)

	suite.Require().Len(res, 1)
	index := res[0]
	suite.Assert().Equal("ih", index.Name)
	suite.Assert().False(index.IsPrimary)
	suite.Assert().Equal(QueryIndexTypeGsi, index.Type)
	suite.Assert().Equal("online", index.State)
	suite.Assert().Equal("test", index.Keyspace)
	suite.Assert().Equal("default", index.Namespace)
	if suite.Assert().Len(index.IndexKey, 1) {
		suite.Assert().Equal("`_type`", index.IndexKey[0])
	}
	suite.Assert().Empty(index.Condition)
	suite.Assert().Equal("HASH(`_type`)", index.Partition)
}
