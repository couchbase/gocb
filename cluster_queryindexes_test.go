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
	suite.Assert().Equal("testIndexes", index.BucketName)
	suite.Assert().Equal("", index.ScopeName)
	suite.Assert().Equal("", index.CollectionName)
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
}

func (suite *IntegrationTestSuite) TestQueryIndexesCrudCollections() {
	suite.skipIfUnsupported(QueryIndexFeature)
	suite.skipIfUnsupported(CollectionsFeature)

	bucketMgr := globalCluster.Buckets()
	bucketName := "testCollectionIndexes"

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

	scopeName := "testQueryIndexesScope"
	colName := "testQueryIndexesCollection"

	bucket := globalCluster.Bucket(bucketName)
	colmgr := bucket.Collections()
	err = colmgr.CreateScope(scopeName, nil)
	suite.Require().Nil(err, err)
	defer colmgr.DropScope(scopeName, nil)

	err = colmgr.CreateCollection(CollectionSpec{
		ScopeName: scopeName,
		Name:      colName,
	}, nil)
	suite.Require().Nil(err, err)

	mgr := globalCluster.QueryIndexes()

	deadline := time.Now().Add(60 * time.Second)
	for {
		err = mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
			IgnoreIfExists: true,
			ScopeName:      scopeName,
			CollectionName: colName,
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
		ScopeName:      scopeName,
		CollectionName: colName,
	})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexExists) {
		suite.T().Fatalf("Expected index exists error but was %s", err)
	}

	err = mgr.CreateIndex(bucketName, "testIndex", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: true,
		ScopeName:      scopeName,
		CollectionName: colName,
	})
	suite.Require().Nil(err, err)

	err = mgr.CreateIndex(bucketName, "testIndex", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: false,
		ScopeName:      scopeName,
		CollectionName: colName,
	})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexExists) {
		suite.T().Fatalf("Expected index exists error but was %s", err)
	}

	// We create this first to give it a chance to be created by the time we need it.
	err = mgr.CreateIndex(bucketName, "testIndexDeferred", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: false,
		Deferred:       true,
		ScopeName:      scopeName,
		CollectionName: colName,
	})
	suite.Require().Nil(err, err)

	indexNames, err := mgr.BuildDeferredIndexes(bucketName, &BuildDeferredQueryIndexOptions{
		ScopeName:      scopeName,
		CollectionName: colName,
	})
	suite.Require().Nil(err, err)

	suite.Assert().Len(indexNames, 1)

	err = mgr.WatchIndexes(bucketName, []string{"testIndexDeferred"}, 10*time.Second, &WatchQueryIndexOptions{
		ScopeName:      scopeName,
		CollectionName: colName,
	})
	suite.Require().Nil(err, err)

	indexes, err := mgr.GetAllIndexes(bucketName, &GetAllQueryIndexesOptions{
		ScopeName:      scopeName,
		CollectionName: colName,
	})
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
	suite.Assert().Equal(colName, index.Keyspace)
	suite.Assert().Equal("default", index.Namespace)
	suite.Assert().Equal(scopeName, index.ScopeName)
	suite.Assert().Equal(colName, index.CollectionName)
	suite.Assert().Equal(bucketName, index.BucketName)
	if suite.Assert().Len(index.IndexKey, 1) {
		suite.Assert().Equal("`field`", index.IndexKey[0])
	}
	suite.Assert().Empty(index.Condition)
	suite.Assert().Empty(index.Partition)

	err = mgr.DropIndex(bucketName, "testIndex", &DropQueryIndexOptions{
		ScopeName:      scopeName,
		CollectionName: colName,
	})
	suite.Require().Nil(err, err)

	err = mgr.DropIndex(bucketName, "testIndex", &DropQueryIndexOptions{
		ScopeName:      scopeName,
		CollectionName: colName,
	})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexNotFound) {
		suite.T().Fatalf("Expected index not found error but was %s", err)
	}

	err = mgr.DropPrimaryIndex(bucketName, &DropPrimaryQueryIndexOptions{
		ScopeName:      scopeName,
		CollectionName: colName,
	})
	suite.Require().Nil(err, err)

	err = mgr.DropPrimaryIndex(bucketName, &DropPrimaryQueryIndexOptions{
		ScopeName:      scopeName,
		CollectionName: colName,
	})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexNotFound) {
		suite.T().Fatalf("Expected index not found error but was %s", err)
	}

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_collections_create_scope"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_collections_create_collection"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_query_create_primary_index"), 2, true)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_query_create_index"), 3, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_query_build_deferred_indexes"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_query_watch_indexes"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_query_get_all_indexes"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_query_drop_primary_index"), 2, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_query_drop_index"), 2, false)
}

func (suite *IntegrationTestSuite) TestQueryIndexesBuildDeferredSameNamespaceNamesBucketOnly() {
	suite.skipIfUnsupported(QueryIndexFeature)
	suite.skipIfUnsupported(CollectionsFeature)

	bucketMgr := globalCluster.Buckets()
	bucketName := "testIndexesBuildDeferredBucketOnly"

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

	collections := globalCluster.Bucket(bucketName).Collections()
	err = collections.CreateCollection(CollectionSpec{
		ScopeName: "_default",
		Name:      bucketName,
	}, nil)
	suite.Require().Nil(err, err)

	mgr := globalCluster.QueryIndexes()

	suite.tryUntil(time.Now().Add(5*time.Second), 100*time.Millisecond, func() bool {
		err = mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
			Deferred: true,
		})
		if err == nil || errors.Is(err, ErrIndexExists) {
			return true
		}

		suite.T().Logf("Unexpected error, retrying: %v", err)
		return false
	})

	suite.tryUntil(time.Now().Add(5*time.Second), 100*time.Millisecond, func() bool {
		err = mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
			Deferred:       true,
			ScopeName:      "_default",
			CollectionName: bucketName,
		})
		if err == nil || errors.Is(err, ErrIndexExists) {
			return true
		}

		suite.T().Logf("Unexpected error, retrying: %v", err)
		return false
	})

	names, err := mgr.BuildDeferredIndexes(bucketName, nil)
	suite.Require().Nil(err, err)

	suite.Assert().Equal([]string{"#primary"}, names)

	suite.Eventually(func() bool {
		indexes, err := mgr.GetAllIndexes(bucketName, nil)
		suite.Require().Nil(err, err)

		suite.Assert().Len(indexes, 2)

		for _, index := range indexes {
			if index.CollectionName == "" {
				if !(index.State == "building" || index.State == "online") {
					return false
				}
			} else {
				suite.Assert().Equal("deferred", index.State)
			}
		}

		return true
	}, 5*time.Second, 100*time.Millisecond)
}

func (suite *IntegrationTestSuite) TestQueryIndexesBuildDeferredSameNamespaceNamesCollectionOnly() {
	suite.skipIfUnsupported(QueryIndexFeature)
	suite.skipIfUnsupported(CollectionsFeature)

	bucketMgr := globalCluster.Buckets()
	bucketName := "testIndexesBuildDeferredCollectionOnly"

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

	collections := globalCluster.Bucket(bucketName).Collections()
	err = collections.CreateCollection(CollectionSpec{
		ScopeName: "_default",
		Name:      bucketName,
	}, nil)
	suite.Require().Nil(err, err)

	mgr := globalCluster.QueryIndexes()

	suite.tryUntil(time.Now().Add(5*time.Second), 100*time.Millisecond, func() bool {
		err = mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
			Deferred: true,
		})
		if err == nil || errors.Is(err, ErrIndexExists) {
			return true
		}

		suite.T().Logf("Unexpected error, retrying: %v", err)
		return false
	})

	suite.tryUntil(time.Now().Add(5*time.Second), 100*time.Millisecond, func() bool {
		err = mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
			Deferred:       true,
			ScopeName:      "_default",
			CollectionName: bucketName,
		})
		if err == nil || errors.Is(err, ErrIndexExists) {
			return true
		}

		suite.T().Logf("Unexpected error, retrying: %v", err)
		return false
	})

	names, err := mgr.BuildDeferredIndexes(bucketName, &BuildDeferredQueryIndexOptions{
		ScopeName:      "_default",
		CollectionName: bucketName,
	})
	suite.Require().Nil(err, err)

	suite.Assert().Equal([]string{"#primary"}, names)

	suite.Eventually(func() bool {
		indexes, err := mgr.GetAllIndexes(bucketName, nil)
		suite.Require().Nil(err, err)

		suite.Assert().Len(indexes, 2)

		for _, index := range indexes {
			if index.CollectionName == "" {
				suite.Assert().Equal("deferred", index.State)
			} else {
				if !(index.State == "building" || index.State == "online") {
					return false
				}
			}
		}

		return true
	}, 5*time.Second, 100*time.Millisecond)
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
		meter:    &meterWrapper{meter: &NoopMeter{}},
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
