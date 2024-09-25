package gocb

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

func (suite *IntegrationTestSuite) TestQueryIndexesCrud() {
	suite.skipIfUnsupported(QueryIndexFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	mgr := globalCluster.QueryIndexes()
	var bucketName string
	if globalCluster.SupportsFeature(BucketMgrFeature) {
		bucketMgr := globalCluster.Buckets()
		bucketName = "testIndexes" + uuid.NewString()[:6]

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

		suite.EnsureBucketOnAllIndexesAndNodes(time.Now().Add(30*time.Second), bucketName)
	} else {
		bucketName = globalBucket.Name()
		indexes, err := mgr.GetAllIndexes(bucketName, &GetAllQueryIndexesOptions{
			ScopeName: "_default",
		})
		suite.Require().Nil(err, err)

		for _, index := range indexes {
			if index.IsPrimary {
				err := mgr.DropPrimaryIndex(bucketName, &DropPrimaryQueryIndexOptions{
					ScopeName:      "_default",
					CollectionName: "_default",
				})
				suite.Require().NoError(err, err)
			} else {
				err := mgr.DropIndex(bucketName, index.Name, &DropQueryIndexOptions{
					ScopeName:      "_default",
					CollectionName: "_default",
				})
				suite.Require().NoError(err, err)
			}
		}

		globalTracer.Reset()
		globalMeter.Reset()
	}

	err := mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().NoError(err)

	suite.EnsureIndexOnAllNodes(time.Now().Add(20*time.Second), "#primary", bucketName, "", "", func(row queryRow) bool {
		return row.State == "online"
	})

	err = mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: false,
	})
	suite.Require().ErrorIs(err, ErrIndexExists)

	err = mgr.CreateIndex(bucketName, "testIndex", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().Nil(err, err)

	suite.EnsureIndexOnAllNodes(time.Now().Add(20*time.Second), "testIndex", bucketName, "", "", func(row queryRow) bool {
		return row.State == "online"
	})

	err = mgr.CreateIndex(bucketName, "testIndex", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: false,
	})
	suite.Require().ErrorIs(err, ErrIndexExists)

	// We create this first to give it a chance to be created by the time we need it.
	err = mgr.CreateIndex(bucketName, "testIndexDeferred", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: false,
		Deferred:       true,
	})
	suite.Require().Nil(err, err)

	suite.EnsureIndexOnAllNodes(time.Now().Add(20*time.Second), "testIndexDeferred", bucketName, "", "", nil)

	indexNames, err := mgr.BuildDeferredIndexes(bucketName, nil)
	suite.Require().Nil(err, err)

	suite.Assert().Len(indexNames, 1)

	err = mgr.WatchIndexes(bucketName, []string{"testIndexDeferred"}, 60*time.Second, nil)
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
	if !globalCluster.IsProtostellar() {
		suite.Assert().Equal(bucketName, index.Keyspace)
		suite.Assert().Equal("default", index.Namespace)
	}
	suite.Assert().Equal(bucketName, index.BucketName)
	if index.ScopeName != "" && index.ScopeName != "_default" {
		suite.T().Logf("Expected scope name to be _default or empty, was %s", index.ScopeName)
		suite.T().Fail()
	}
	if index.CollectionName != "" && index.CollectionName != "_default" {
		suite.T().Logf("Expected collection name to be _default or empty, was %s", index.CollectionName)
		suite.T().Fail()
	}
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

	if globalCluster.SupportsFeature(BucketMgrFeature) {
		suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_bucket_create_bucket"), 1, false)
	}
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
	suite.skipIfUnsupported(CollectionsManagerFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)
	suite.skipIfUnsupported(QueryMB57673Feature)

	suite.dropAllIndexes()
	bucketName := globalBucket.Name()

	scopeName := "testQueryIndexesScope"
	colName := "testQueryIndexesCollection"

	colmgr := globalBucket.CollectionsV2()
	err := colmgr.CreateScope(scopeName, nil)
	suite.Require().Nil(err, err)
	defer colmgr.DropScope(scopeName, nil)

	suite.EnsureScopeOnAllNodes(scopeName)

	err = colmgr.CreateCollection(scopeName, colName, nil, nil)
	suite.Require().Nil(err, err)

	suite.EnsureCollectionsOnAllNodes(scopeName, []string{colName})
	suite.EnsureCollectionOnAllIndexesAndNodes(time.Now().Add(30*time.Second), globalBucket.Name(), scopeName, colName)

	mgr := globalCluster.QueryIndexes()

	err = mgr.CreatePrimaryIndex(globalBucket.Name(), &CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: true,
		ScopeName:      scopeName,
		CollectionName: colName,
	})
	suite.Require().NoError(err)

	suite.EnsureIndexOnAllNodes(time.Now().Add(20*time.Second), "#primary", bucketName, scopeName, colName, func(row queryRow) bool {
		return row.State == "online"
	})

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

	suite.EnsureIndexOnAllNodes(time.Now().Add(20*time.Second), "testIndex", bucketName, scopeName, colName, func(row queryRow) bool {
		return row.State == "online"
	})

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

	suite.EnsureIndexOnAllNodes(time.Now().Add(20*time.Second), "testIndexDeferred", bucketName, scopeName, colName, nil)

	indexNames, err := mgr.BuildDeferredIndexes(bucketName, &BuildDeferredQueryIndexOptions{
		ScopeName:      scopeName,
		CollectionName: colName,
	})
	suite.Require().Nil(err, err)

	suite.Assert().Len(indexNames, 1)

	err = mgr.WatchIndexes(bucketName, []string{"testIndexDeferred"}, 60*time.Second, &WatchQueryIndexOptions{
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
	if !globalCluster.IsProtostellar() {
		suite.Assert().Equal(colName, index.Keyspace)
		suite.Assert().Equal("default", index.Namespace)
	}
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
	suite.skipIfUnsupported(CollectionsManagerFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)
	suite.skipIfUnsupported(QueryMB57673Feature)

	suite.dropAllIndexes()
	bucketName := globalBucket.Name()

	colName := uuid.NewString()[:6]
	collections := globalBucket.CollectionsV2()
	err := collections.CreateCollection("_default", colName, nil, nil)
	suite.Require().Nil(err, err)

	suite.EnsureCollectionOnAllIndexesAndNodes(time.Now().Add(20*time.Second), bucketName, "_default", colName)

	mgr := globalCluster.QueryIndexes()

	err = mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
		Deferred:       true,
		IgnoreIfExists: true,
	})
	suite.Require().NoError(err)

	err = mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
		Deferred:       true,
		ScopeName:      "_default",
		CollectionName: colName,
		IgnoreIfExists: true,
	})
	suite.Require().NoError(err)

	suite.EnsureIndexOnAllNodes(time.Now().Add(20*time.Second), "#primary", bucketName, "", "", nil)
	suite.EnsureIndexOnAllNodes(time.Now().Add(20*time.Second), "#primary", bucketName, "_default", colName, nil)

	names, err := mgr.BuildDeferredIndexes(bucketName, nil)
	suite.Require().Nil(err, err)

	// Protostellar builds all indexes on the bucket, classic only builds the default collection.
	if globalCluster.IsProtostellar() {
		suite.Assert().Equal([]string{"default._default._default.#primary", "default._default." + colName + ".#primary"}, names)
	} else {
		suite.Assert().Equal([]string{"#primary"}, names)
	}

	suite.Eventually(func() bool {
		indexes, err := mgr.GetAllIndexes(bucketName, nil)
		suite.Require().Nil(err, err)

		suite.Assert().Len(indexes, 2)

		for _, index := range indexes {
			if index.CollectionName == "" || globalCluster.IsProtostellar() {
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
	suite.skipIfUnsupported(CollectionsManagerFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)
	suite.skipIfUnsupported(QueryMB57673Feature)

	suite.dropAllIndexes()
	bucketName := globalBucket.Name()
	collectionName := uuid.NewString()

	collections := globalBucket.CollectionsV2()
	err := collections.CreateCollection("_default", collectionName, nil, nil)
	suite.Require().Nil(err, err)

	suite.EnsureCollectionOnAllIndexesAndNodes(time.Now().Add(20*time.Second), bucketName, "_default", collectionName)

	mgr := globalCluster.QueryIndexes()

	err = mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
		Deferred:       true,
		IgnoreIfExists: true,
	})
	suite.Require().NoError(err)

	err = mgr.CreatePrimaryIndex(bucketName, &CreatePrimaryQueryIndexOptions{
		Deferred:       true,
		ScopeName:      "_default",
		CollectionName: collectionName,
		IgnoreIfExists: true,
	})
	suite.Require().NoError(err)

	suite.EnsureIndexOnAllNodes(time.Now().Add(20*time.Second), "#primary", bucketName, "", "", nil)
	suite.EnsureIndexOnAllNodes(time.Now().Add(20*time.Second), "#primary", bucketName, "_default", collectionName, nil)

	names, err := mgr.BuildDeferredIndexes(bucketName, &BuildDeferredQueryIndexOptions{
		ScopeName:      "_default",
		CollectionName: collectionName,
	})
	suite.Require().Nil(err, err)

	name := "#primary"
	if globalCluster.IsProtostellar() {
		name = "default._default." + collectionName + "." + name
	}
	suite.Assert().Equal([]string{name}, names)

	suite.Eventually(func() bool {
		indexes, err := mgr.GetAllIndexes(bucketName, nil)
		suite.Require().Nil(err, err)

		suite.Assert().Len(indexes, 2)

		for _, index := range indexes {
			if index.CollectionName == "" || index.CollectionName == "_default" {
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

func (suite *IntegrationTestSuite) TestQueryIndexesIncludesDefaultCollection() {
	suite.skipIfUnsupported(QueryIndexFeature)
	suite.skipIfUnsupported(ClusterLevelQueryFeature)

	mgr := globalCluster.QueryIndexes()
	var bucketName string
	if globalCluster.SupportsFeature(BucketMgrFeature) {
		bucketMgr := globalCluster.Buckets()
		bucketName = "testIndexes" + uuid.NewString()

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

		suite.EnsureBucketOnAllIndexesAndNodes(time.Now().Add(30*time.Second), bucketName)
	} else {
		bucketName = globalBucket.Name()
		indexes, err := mgr.GetAllIndexes(bucketName, &GetAllQueryIndexesOptions{
			ScopeName: "_default",
		})
		suite.Require().Nil(err, err)

		for _, index := range indexes {
			if index.IsPrimary {
				err := mgr.DropPrimaryIndex(bucketName, &DropPrimaryQueryIndexOptions{
					ScopeName:      "_default",
					CollectionName: "_default",
				})
				suite.Require().NoError(err, err)
			} else {
				err := mgr.DropIndex(bucketName, index.Name, &DropQueryIndexOptions{
					ScopeName:      "_default",
					CollectionName: "_default",
				})
				suite.Require().NoError(err, err)
			}
		}
	}

	err := mgr.CreateIndex(bucketName, "myindex", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().NoError(err)

	suite.EnsureIndexOnAllNodes(time.Now().Add(20*time.Second), "myindex", bucketName, "", "", nil)

	indexes, err := mgr.GetAllIndexes(bucketName, &GetAllQueryIndexesOptions{
		ScopeName: "_default",
	})
	suite.Require().Nil(err, err)

	suite.Require().Len(indexes, 1)
	var index QueryIndex
	for _, idx := range indexes {
		if idx.Name == "myindex" {
			index = idx
			break
		}
	}
	suite.Assert().Equal("myindex", index.Name)
	suite.Assert().False(index.IsPrimary)
	suite.Assert().Equal(QueryIndexTypeGsi, index.Type)
	suite.Assert().Equal("online", index.State)
	if !globalCluster.IsProtostellar() {
		suite.Assert().Equal(bucketName, index.Keyspace)
		suite.Assert().Equal("default", index.Namespace)
	}
	suite.Assert().Equal(bucketName, index.BucketName)
	if index.ScopeName != "" && index.ScopeName != "_default" {
		suite.T().Logf("Expected scope name to be _default or empty, was %s", index.ScopeName)
		suite.T().Fail()
	}
	if index.CollectionName != "" && index.CollectionName != "_default" {
		suite.T().Logf("Expected collection name to be _default or empty, was %s", index.CollectionName)
		suite.T().Fail()
	}
	if suite.Assert().Len(index.IndexKey, 1) {
		suite.Assert().Equal("`field`", index.IndexKey[0])
	}
	suite.Assert().Empty(index.Condition)
	suite.Assert().Empty(index.Partition)
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
	provider, _ := suite.newMockQueryProvider(false, reader)

	mgr := QueryIndexManager{
		controller: &providerController[queryIndexProvider]{
			get: func() (queryIndexProvider, error) {
				return &queryProviderCore{
					provider: provider,
					tracer:   newTracerWrapper(&NoopTracer{}),
					meter:    newMeterWrapper(&NoopMeter{}),
				}, nil
			},
			opController: mockOpController{},
		},
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
