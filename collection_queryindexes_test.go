package gocb

import (
	"errors"
	"time"
)

func (suite *IntegrationTestSuite) TestCollectionQueryIndexManagerCrud() {
	suite.skipIfUnsupported(QueryIndexFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	bucketName := globalBucket.Name()

	scopeName := generateDocId("scope")
	colName := generateDocId("collection")

	colmgr := globalBucket.CollectionsV2()
	err := colmgr.CreateScope(scopeName, nil)
	suite.Require().Nil(err, err)
	defer colmgr.DropScope(scopeName, nil)

	suite.EnsureScopeOnAllNodes(scopeName)

	err = colmgr.CreateCollection(scopeName, colName, nil, nil)
	suite.Require().Nil(err, err)

	suite.EnsureCollectionsOnAllNodes(scopeName, []string{colName})
	suite.EnsureCollectionOnAllIndexesAndNodes(time.Now().Add(30*time.Second), bucketName, scopeName, colName)

	mgr := globalBucket.Scope(scopeName).Collection(colName).QueryIndexes()

	err = mgr.CreatePrimaryIndex(&CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().NoError(err)

	err = mgr.CreatePrimaryIndex(&CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: false,
	})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexExists) {
		suite.T().Fatalf("Expected index exists error but was %s", err)
	}

	err = mgr.CreateIndex("testIndex", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().Nil(err, err)

	err = mgr.CreateIndex("testIndex", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: false,
	})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexExists) {
		suite.T().Fatalf("Expected index exists error but was %s", err)
	}

	// We create this first to give it a chance to be created by the time we need it.
	err = mgr.CreateIndex("testIndexDeferred", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: false,
		Deferred:       true,
	})
	suite.Require().Nil(err, err)

	indexNames, err := mgr.BuildDeferredIndexes(&BuildDeferredQueryIndexOptions{})
	suite.Require().Nil(err, err)

	suite.Assert().Len(indexNames, 1)

	err = mgr.WatchIndexes([]string{"testIndexDeferred"}, 30*time.Second, &WatchQueryIndexOptions{})
	suite.Require().Nil(err, err)

	indexes, err := mgr.GetAllIndexes(&GetAllQueryIndexesOptions{})
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
	// Protostellar doesn't support keyspace or namespace.
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

	err = mgr.DropIndex("testIndex", &DropQueryIndexOptions{})
	suite.Require().Nil(err, err)

	err = mgr.DropIndex("testIndex", &DropQueryIndexOptions{})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexNotFound) {
		suite.T().Fatalf("Expected index not found error but was %s", err)
	}

	err = mgr.DropPrimaryIndex(&DropPrimaryQueryIndexOptions{})
	suite.Require().Nil(err, err)

	err = mgr.DropPrimaryIndex(&DropPrimaryQueryIndexOptions{})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexNotFound) {
		suite.T().Fatalf("Expected index not found error but was %s", err)
	}

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_collections_create_scope"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_collections_create_collection"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "query", "manager_query_create_primary_index"), 2, true)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "query", "manager_query_create_index"), 3, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "query", "manager_query_build_deferred_indexes"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "query", "manager_query_watch_indexes"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "query", "manager_query_get_all_indexes"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "query", "manager_query_drop_primary_index"), 2, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "query", "manager_query_drop_index"), 2, false)
}

func (suite *IntegrationTestSuite) TestCollectionQueryIndexManagerCrudDefaultScopeCollection() {
	suite.skipIfUnsupported(QueryIndexFeature)
	suite.skipIfUnsupported(CollectionsFeature)

	suite.dropAllIndexesAtCollectionLevel()

	mgr := globalBucket.DefaultCollection().QueryIndexes()

	err := mgr.CreatePrimaryIndex(&CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().NoError(err)

	err = mgr.CreatePrimaryIndex(&CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: false,
	})
	if !errors.Is(err, ErrIndexExists) {
		suite.T().Fatalf("Expected index exists error but was %s", err)
	}

	err = mgr.CreateIndex("testIndex", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().Nil(err, err)

	err = mgr.CreateIndex("testIndex", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: false,
	})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexExists) {
		suite.T().Fatalf("Expected index exists error but was %s", err)
	}

	// We create this first to give it a chance to be created by the time we need it.
	err = mgr.CreateIndex("testIndexDeferred", []string{"field"}, &CreateQueryIndexOptions{
		IgnoreIfExists: false,
		Deferred:       true,
	})
	suite.Require().Nil(err, err)

	indexNames, err := mgr.BuildDeferredIndexes(&BuildDeferredQueryIndexOptions{})
	suite.Require().Nil(err, err)

	suite.Assert().Len(indexNames, 1)

	err = mgr.WatchIndexes([]string{"testIndexDeferred"}, 30*time.Second, &WatchQueryIndexOptions{})
	suite.Require().Nil(err, err)

	indexes, err := mgr.GetAllIndexes(&GetAllQueryIndexesOptions{})
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
		suite.Assert().Equal(globalBucket.bucketName, index.Keyspace)
		suite.Assert().Equal("default", index.Namespace)
	}
	if index.ScopeName != "" && index.ScopeName != "_default" {
		suite.T().Logf("Expected scope name to be _default or empty, was %s", index.ScopeName)
		suite.T().Fail()
	}
	if index.CollectionName != "" && index.CollectionName != "_default" {
		suite.T().Logf("Expected collection name to be _default or empty, was %s", index.CollectionName)
		suite.T().Fail()
	}
	suite.Assert().Equal(globalBucket.Name(), index.BucketName)
	if suite.Assert().Len(index.IndexKey, 1) {
		suite.Assert().Equal("`field`", index.IndexKey[0])
	}
	suite.Assert().Empty(index.Condition)
	suite.Assert().Empty(index.Partition)

	err = mgr.DropIndex("testIndex", &DropQueryIndexOptions{})
	suite.Require().Nil(err, err)

	err = mgr.DropIndex("testIndex", &DropQueryIndexOptions{})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexNotFound) {
		suite.T().Fatalf("Expected index not found error but was %s", err)
	}

	err = mgr.DropPrimaryIndex(&DropPrimaryQueryIndexOptions{})
	suite.Require().Nil(err, err)

	err = mgr.DropPrimaryIndex(&DropPrimaryQueryIndexOptions{})
	suite.Require().NotNil(err, err)
	if !errors.Is(err, ErrIndexNotFound) {
		suite.T().Fatalf("Expected index not found error but was %s", err)
	}
}
