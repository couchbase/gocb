package gocb

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/stretchr/testify/mock"
)

func (suite *IntegrationTestSuite) runCollectionManagerCrudTest(v2 bool) {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	scopeName := generateDocId("testScope")
	collectionName := generateDocId("testCollection")

	mgr := globalBucket.Collections()
	mgrV2 := globalBucket.CollectionsV2()

	var err error
	if v2 {
		err = mgrV2.CreateScope(scopeName, nil)
	} else {
		err = mgr.CreateScope(scopeName, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	if v2 {
		err = mgrV2.CreateScope(scopeName, nil)
	} else {
		err = mgr.CreateScope(scopeName, nil)
	}
	if !errors.Is(err, ErrScopeExists) {
		suite.T().Fatalf("Expected create scope to error with ScopeExists but was %v", err)
	}

	if v2 {
		err = mgrV2.CreateCollection(scopeName, collectionName, &CreateCollectionSettings{
			MaxExpiry: 5 * time.Second,
		}, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      collectionName,
			ScopeName: scopeName,
			MaxExpiry: 5 * time.Second,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	if v2 {
		err = mgrV2.CreateCollection(scopeName, collectionName, nil, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      collectionName,
			ScopeName: scopeName,
		}, nil)
	}

	if !errors.Is(err, ErrCollectionExists) {
		suite.T().Fatalf("Expected create collection to error with CollectionExists but was %v", err)
	}

	var scopes []ScopeSpec
	if v2 {
		scopes, err = mgrV2.GetAllScopes(nil)
	} else {
		scopes, err = mgr.GetAllScopes(nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to GetAllScopes %v", err)
	}

	if len(scopes) < 2 {
		suite.T().Fatalf("Expected scopes to contain at least 2 scopes but was %v", scopes)
	}

	var found bool
	for _, scope := range scopes {
		if scope.Name != scopeName {
			continue
		}

		found = true
		if suite.Assert().Len(scope.Collections, 1) {
			col := scope.Collections[0]
			suite.Assert().Equal(collectionName, col.Name)
			suite.Assert().Equal(scopeName, col.ScopeName)
			suite.Assert().Equal(5*time.Second, col.MaxExpiry)
		}
		break
	}
	suite.Assert().True(found)

	if v2 {
		err = mgrV2.DropCollection(scopeName, collectionName, nil)
	} else {
		err = mgr.DropCollection(CollectionSpec{
			Name:      collectionName,
			ScopeName: scopeName,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Expected DropCollection to not error but was %v", err)
	}

	if v2 {
		err = mgrV2.DropCollection(scopeName, collectionName, nil)
	} else {
		err = mgr.DropCollection(CollectionSpec{
			Name:      collectionName,
			ScopeName: scopeName,
		}, nil)
	}
	if !errors.Is(err, ErrCollectionNotFound) {
		suite.T().Fatalf("Expected drop collection to error with ErrCollectionNotFound but was %v", err)
	}

	if v2 {
		err = mgrV2.DropScope(scopeName, nil)
	} else {
		err = mgr.DropScope(scopeName, nil)
	}
	if err != nil {
		suite.T().Fatalf("Expected DropScope to not error but was %v", err)
	}

	if v2 {
		err = mgrV2.DropScope(scopeName, nil)
	} else {
		err = mgr.DropScope(scopeName, nil)
	}
	if !errors.Is(err, ErrScopeNotFound) {
		suite.T().Fatalf("Expected drop scope to error with ErrScopeNotFound but was %v", err)
	}

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(9, len(nilParents))

	isProtostellar := globalCluster.IsProtostellar()
	var operationID string
	var numDispatchSpans int
	var dispatchOperationID string
	if isProtostellar {
		operationID = "CreateScope"
		numDispatchSpans = 1
	} else {
		operationID = "POST " + fmt.Sprintf("/pools/default/buckets/%s/scopes", globalConfig.Bucket)
		numDispatchSpans = 1
		dispatchOperationID = "any"
	}

	suite.AssertHTTPOpSpan(nilParents[0], "manager_collections_create_scope",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			scope:                   scopeName,
			service:                 "management",
			operationID:             operationID,
			numDispatchSpans:        numDispatchSpans,
			atLeastNumDispatchSpans: false,
			hasEncoding:             !isProtostellar,
			dispatchOperationID:     dispatchOperationID,
		})
	if isProtostellar {
		operationID = "CreateCollection"
	} else {
		operationID = "POST " + fmt.Sprintf("/pools/default/buckets/%s/scopes/%s/collections", globalConfig.Bucket, scopeName)
	}
	suite.AssertHTTPOpSpan(nilParents[2], "manager_collections_create_collection",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			scope:                   scopeName,
			collection:              collectionName,
			service:                 "management",
			operationID:             operationID,
			numDispatchSpans:        numDispatchSpans,
			atLeastNumDispatchSpans: false,
			hasEncoding:             !isProtostellar,
			dispatchOperationID:     dispatchOperationID,
		})
	if isProtostellar {
		operationID = "ListCollections"
	} else {
		operationID = "GET " + fmt.Sprintf("/pools/default/buckets/%s/scopes", globalConfig.Bucket)
	}
	suite.AssertHTTPOpSpan(nilParents[4], "manager_collections_get_all_scopes",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			service:                 "management",
			operationID:             operationID,
			numDispatchSpans:        numDispatchSpans,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     dispatchOperationID,
		})
	if isProtostellar {
		operationID = "DeleteCollection"
	} else {
		operationID = "DELETE " + fmt.Sprintf("/pools/default/buckets/%s/scopes/%s/collections/%s", globalConfig.Bucket, scopeName, collectionName)
	}
	suite.AssertHTTPOpSpan(nilParents[5], "manager_collections_drop_collection",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			scope:                   scopeName,
			collection:              collectionName,
			service:                 "management",
			operationID:             operationID,
			numDispatchSpans:        numDispatchSpans,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     dispatchOperationID,
		})
	if isProtostellar {
		operationID = "DeleteScope"
	} else {
		operationID = "DELETE " + fmt.Sprintf("/pools/default/buckets/%s/scopes/%s", globalConfig.Bucket, scopeName)
	}
	suite.AssertHTTPOpSpan(nilParents[7], "manager_collections_drop_scope",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			scope:                   scopeName,
			service:                 "management",
			operationID:             operationID,
			numDispatchSpans:        numDispatchSpans,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     dispatchOperationID,
		})

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_collections_create_scope"), 2, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_collections_create_collection"), 2, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_collections_get_all_scopes"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_collections_drop_scope"), 2, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_collections_drop_collection"), 2, false)
}

func (suite *IntegrationTestSuite) runDropNonExistentScopeTest(v2 bool) {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	scopeName := generateDocId("testDropScopeX")

	mgr := globalBucket.Collections()
	mgrV2 := globalBucket.CollectionsV2()

	var err error
	if v2 {
		err = mgrV2.CreateScope(scopeName, nil)
	} else {
		err = mgr.CreateScope(scopeName, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}
	if v2 {
		err = mgrV2.CreateCollection(scopeName, generateDocId("testDropCollection"), nil, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      generateDocId("testDropCollection"),
			ScopeName: scopeName,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	if v2 {
		err = mgrV2.DropScope(generateDocId("testScopeX"), nil)
	} else {
		err = mgr.DropScope(generateDocId("testScopeX"), nil)
	}
	if err == nil {
		suite.T().Fatalf("Expected error to be non-nil")
	}
}

func (suite *IntegrationTestSuite) TestDropNotExistentScope() {
	suite.runDropNonExistentScopeTest(false)
}

func (suite *IntegrationTestSuite) runTestDropNonExistentCollectionTest(v2 bool) {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	scopeName := generateDocId("testDropScopeY")

	mgr := globalBucket.Collections()
	mgrV2 := globalBucket.CollectionsV2()

	var err error
	if v2 {
		err = mgrV2.CreateScope(scopeName, nil)
	} else {
		err = mgr.CreateScope(scopeName, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	suite.EnsureScopeOnAllNodes(scopeName)

	if v2 {
		err = mgrV2.CreateCollection(scopeName, generateDocId("testDropCollectionY"), nil, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      generateDocId("testDropCollectionY"),
			ScopeName: scopeName,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	if v2 {
		err = mgrV2.DropCollection(scopeName, generateDocId("testCollectionZ"), nil)
	} else {
		err = mgr.DropCollection(CollectionSpec{
			Name:      generateDocId("testCollectionZ"),
			ScopeName: scopeName,
		}, nil)
	}
	if err == nil {
		suite.T().Fatalf("Expected error to be non-nil")
	}
}

func (suite *IntegrationTestSuite) TestDropNonExistentCollection() {
	suite.runTestDropNonExistentCollectionTest(false)
}

func (suite *IntegrationTestSuite) runCollectionsAreNotPresentTest(v2 bool) {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	scopeName1 := generateDocId("scope-1-")
	scopeName2 := generateDocId("scope-2-")
	collectionName1 := generateDocId("collection-1-")
	collectionName2 := generateDocId("collection-2-")

	mgr := globalBucket.Collections()
	mgrV2 := globalBucket.CollectionsV2()

	var err error
	if v2 {
		err = mgrV2.CreateScope(scopeName1, nil)
	} else {
		err = mgr.CreateScope(scopeName1, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	if v2 {
		err = mgrV2.CreateScope(scopeName2, nil)
	} else {
		err = mgr.CreateScope(scopeName2, nil)

	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	suite.EnsureScopeOnAllNodes(scopeName1)
	suite.EnsureScopeOnAllNodes(scopeName2)

	if v2 {
		err = mgrV2.CreateCollection(scopeName1, collectionName1, nil, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      collectionName1,
			ScopeName: scopeName1,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	if v2 {
		err = mgrV2.CreateCollection(scopeName2, collectionName2, nil, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      collectionName2,
			ScopeName: scopeName2,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	if v2 {
		err = mgrV2.DropCollection(scopeName1, collectionName1, nil)
	} else {
		err = mgr.DropCollection(CollectionSpec{
			Name:      collectionName1,
			ScopeName: scopeName1,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Expected DropCollection to not error but was %v", err)
	}

	if v2 {
		err = mgrV2.DropCollection(scopeName2, collectionName2, nil)
	} else {
		err = mgr.DropCollection(CollectionSpec{
			Name:      collectionName2,
			ScopeName: scopeName2,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Expected DropCollection to not error but was %v", err)
	}

	if v2 {
		err = mgrV2.DropCollection(scopeName2, collectionName2, nil)
	} else {
		err = mgr.DropCollection(CollectionSpec{
			Name:      collectionName2,
			ScopeName: scopeName2,
		}, nil)
	}
	if err == nil {
		suite.T().Fatalf("Expected error to be non-nil")
	}

}

func (suite *IntegrationTestSuite) TestCollectionsAreNotPresent() {
	suite.runCollectionsAreNotPresentTest(false)
}

func (suite *IntegrationTestSuite) runDropScopesAreNotExistTest(v2 bool) {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	scopeName1 := "testDropScope1"
	scopeName2 := "testDropScope2"
	collectionName1 := "testDropCollection1"
	collectionName2 := "testDropCollection2"

	mgr := globalBucket.Collections()
	mgrV2 := globalBucket.CollectionsV2()

	var err error
	if v2 {
		err = mgrV2.CreateScope(scopeName1, nil)
	} else {
		err = mgr.CreateScope(scopeName1, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	if v2 {
		err = mgrV2.CreateScope(scopeName2, nil)
	} else {
		err = mgr.CreateScope(scopeName2, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	if v2 {
		err = mgrV2.CreateCollection(scopeName1, collectionName1, nil, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      collectionName1,
			ScopeName: scopeName1,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	if v2 {
		err = mgrV2.CreateCollection(scopeName2, collectionName2, nil, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      collectionName2,
			ScopeName: scopeName2,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	if v2 {
		err = mgrV2.DropScope(scopeName1, nil)
	} else {
		err = mgr.DropScope(scopeName1, nil)
	}
	if err != nil {
		suite.T().Fatalf("Expected DropScope to not error but was %v", err)
	}

	if v2 {
		err = mgrV2.DropScope(scopeName2, nil)
	} else {
		err = mgr.DropScope(scopeName2, nil)
	}
	if err != nil {
		suite.T().Fatalf("Expected DropScope to not error but was %v", err)
	}

	if v2 {
		err = mgrV2.DropScope(scopeName1, nil)
	} else {
		err = mgr.DropScope(scopeName1, nil)
	}
	if err == nil {
		suite.T().Fatalf("Expected error to be non-nil")
	}

	if v2 {
		err = mgrV2.DropCollection(scopeName1, collectionName1, nil)
	} else {
		err = mgr.DropCollection(CollectionSpec{
			Name:      collectionName1,
			ScopeName: scopeName1,
		}, nil)
	}
	if err == nil {
		suite.T().Fatalf("Expected error to be non-nil")
	}
}

func (suite *IntegrationTestSuite) TestDropScopesAreNotExist() {
	suite.runDropScopesAreNotExistTest(false)
}

func (suite *IntegrationTestSuite) runGetAllScopesTest(v2 bool) {
	suite.skipIfUnsupported(CollectionsManagerFeature)

	mgr := globalBucket.Collections()
	mgrV2 := globalBucket.CollectionsV2()

	var err error
	if v2 {
		err = mgrV2.CreateScope(generateDocId("testScopeX1"), nil)
	} else {
		err = mgr.CreateScope(generateDocId("testScopeX1"), nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	if v2 {
		err = mgrV2.CreateScope(generateDocId("testScopeX2"), nil)
	} else {
		err = mgr.CreateScope(generateDocId("testScopeX2"), nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	if v2 {
		err = mgrV2.CreateScope(generateDocId("testScopeX3"), nil)
	} else {
		err = mgr.CreateScope(generateDocId("testScopeX3"), nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	if v2 {
		err = mgrV2.CreateScope(generateDocId("testScopeX4"), nil)
	} else {
		err = mgr.CreateScope(generateDocId("testScopeX4"), nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	if v2 {
		err = mgrV2.CreateScope(generateDocId("testScopeX5"), nil)
	} else {
		err = mgr.CreateScope(generateDocId("testScopeX5"), nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	var scopes []ScopeSpec
	if v2 {
		scopes, err = mgrV2.GetAllScopes(nil)
	} else {
		scopes, err = mgr.GetAllScopes(nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to GetAllScopes %v", err)
	}

	if len(scopes) < 5 {
		suite.T().Fatalf("Expected scopes to contain total of 5 scopes but was %v", scopes)
	}
}

func (suite *IntegrationTestSuite) TestGetAllScopes() {
	suite.runGetAllScopesTest(false)
}

func (suite *IntegrationTestSuite) runCollectionsInBucketTest(v2 bool) {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	scopeName := generateDocId("collectionsInBucketScope")

	mgr := globalBucket.Collections()
	mgrV2 := globalBucket.CollectionsV2()

	var err error
	if v2 {
		err = mgrV2.CreateScope(scopeName, nil)
	} else {
		err = mgr.CreateScope(scopeName, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}
	suite.EnsureScopeOnAllNodes(scopeName)

	if v2 {
		err = mgrV2.CreateCollection(scopeName, generateDocId("testCollection1-"), nil, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      generateDocId("testCollection1-"),
			ScopeName: scopeName,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	if v2 {
		err = mgrV2.CreateCollection(scopeName, generateDocId("testCollection2-"), nil, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      generateDocId("testCollection2-"),
			ScopeName: scopeName,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	if v2 {
		err = mgrV2.CreateCollection(scopeName, generateDocId("testCollection3-"), nil, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      generateDocId("testCollection3-"),
			ScopeName: scopeName,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	if v2 {
		err = mgrV2.CreateCollection(scopeName, generateDocId("testCollection4-"), nil, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      generateDocId("testCollection4-"),
			ScopeName: scopeName,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	if v2 {
		err = mgrV2.CreateCollection(scopeName, generateDocId("testCollection5-"), nil, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      generateDocId("testCollection5-"),
			ScopeName: scopeName,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	success := suite.tryUntil(time.Now().Add(5*time.Second), 500*time.Millisecond, func() bool {
		var scopes []ScopeSpec
		if v2 {
			scopes, err = mgrV2.GetAllScopes(nil)
		} else {
			scopes, err = mgr.GetAllScopes(nil)
		}
		if err != nil {
			suite.T().Fatalf("Failed to GetAllScopes %v", err)
		}

		var scope *ScopeSpec
		for i, s := range scopes {
			if s.Name == scopeName {
				scope = &scopes[i]
			}
		}
		suite.Require().NotNil(scope)

		if len(scope.Collections) != 5 {
			suite.T().Logf("Expected collections in scope should be 5 but was %v", scope)
			return false
		}

		return true
	})
	suite.Require().True(success)
}

func (suite *IntegrationTestSuite) TestCollectionsInBucket() {
	suite.runCollectionsInBucketTest(false)
}

func (suite *IntegrationTestSuite) runNumberOfCollectionInScopeTest(v2 bool) {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	scopeName1 := generateDocId("numCollectionsScope1-")
	scopeName2 := generateDocId("numCollectionsScope2-")

	mgr := globalBucket.Collections()
	mgrV2 := globalBucket.CollectionsV2()

	var err error
	if v2 {
		err = mgrV2.CreateScope(scopeName1, nil)
	} else {
		err = mgr.CreateScope(scopeName1, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	if v2 {
		err = mgrV2.CreateScope(scopeName2, nil)
	} else {
		err = mgr.CreateScope(scopeName2, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	// Create 5 collections in the first scope
	for i := 1; i <= 5; i++ {
		collectionName := generateDocId(fmt.Sprintf("testCollection%d-", i))
		if v2 {
			err = mgrV2.CreateCollection(scopeName1, collectionName, nil, nil)
		} else {
			err = mgr.CreateCollection(CollectionSpec{
				Name:      collectionName,
				ScopeName: scopeName1,
			}, nil)
		}
		if err != nil {
			suite.T().Fatalf("Failed to create collection %v", err)
		}
	}

	// Create 2 collections in the second scope
	for i := 1; i <= 2; i++ {
		collectionName := generateDocId(fmt.Sprintf("testCollection%d-", i))
		if v2 {
			err = mgrV2.CreateCollection(scopeName2, collectionName, nil, nil)
		} else {
			err = mgr.CreateCollection(CollectionSpec{
				Name:      collectionName,
				ScopeName: scopeName2,
			}, nil)
		}
		if err != nil {
			suite.T().Fatalf("Failed to create collection %v", err)
		}
	}

	var scopes []ScopeSpec
	if v2 {
		scopes, err = mgrV2.GetAllScopes(nil)
	} else {
		scopes, err = mgr.GetAllScopes(nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to GetAllScopes %v", err)
	}

	var scope *ScopeSpec
	for i, s := range scopes {
		if s.Name == scopeName1 {
			scope = &scopes[i]
		}
	}
	suite.Require().NotNil(scope)

	if len(scope.Collections) != 5 {
		suite.T().Fatalf("Expected collections in scope should be 5 but was %v", scope)
	}
}

func (suite *IntegrationTestSuite) TestNumberOfCollectionsInScope() {
	suite.runNumberOfCollectionInScopeTest(false)
}

func (suite *IntegrationTestSuite) runMaxNumberOfCollectionsInScopeTest(v2 bool) {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)
	suite.skipIfUnsupported(CollectionsManagerMaxCollectionsFeature)

	mgr := globalBucket.Collections()
	mgrV2 := globalBucket.CollectionsV2()

	scopeName := generateDocId("singleScope")

	var err error
	if v2 {
		err = mgrV2.CreateScope(scopeName, nil)
	} else {
		err = mgr.CreateScope(scopeName, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}
	for i := 0; i < 1000; i++ {
		if v2 {
			err = mgrV2.CreateCollection(scopeName, strconv.Itoa(1000+i), nil, nil)
		} else {
			err = mgr.CreateCollection(CollectionSpec{
				Name:      strconv.Itoa(1000 + i),
				ScopeName: scopeName,
			}, nil)
		}
		if err != nil {
			suite.T().Fatalf("Failed to create collection %v", err)
		}
	}

	success := suite.tryUntil(time.Now().Add(15*time.Second), 100*time.Millisecond, func() bool {
		var scopes []ScopeSpec
		if v2 {
			scopes, err = mgrV2.GetAllScopes(nil)
		} else {
			scopes, err = mgr.GetAllScopes(nil)
		}
		if err != nil {
			suite.T().Logf("Failed to GetAllScopes %v", err)
			return false
		}
		var scope *ScopeSpec
		for i, s := range scopes {
			if s.Name == scopeName {
				scope = &scopes[i]
			}
		}
		if scope == nil {
			suite.T().Log("scope not found")
			return false
		}

		if len(scope.Collections) != 1000 {
			suite.T().Logf("Expected collections in scope should be 1000 but was %v", len(scope.Collections))
			return false
		}

		return true
	})

	suite.Require().True(success)
}

func (suite *IntegrationTestSuite) TestMaxNumberOfCollectionsInScope() {
	suite.runMaxNumberOfCollectionsInScopeTest(false)
}

func (suite *IntegrationTestSuite) runCollectionHistoryRetentionTest(v2 bool) {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(HistoryRetentionFeature)

	cluster, err := Connect(globalConfig.connstr, ClusterOptions{
		Authenticator:  globalConfig.Auth,
		SecurityConfig: globalConfig.SecurityConfig,
	})
	suite.Require().NoError(err)
	defer cluster.Close(nil)

	bMgr := cluster.Buckets()
	bName := "a" + uuid.NewString()[:6]
	settings := BucketSettings{
		Name:           bName,
		RAMQuotaMB:     1024,
		BucketType:     CouchbaseBucketType,
		StorageBackend: StorageBackendMagma,
	}

	err = bMgr.CreateBucket(CreateBucketSettings{
		BucketSettings:         settings,
		ConflictResolutionType: ConflictResolutionTypeSequenceNumber,
	}, nil)
	suite.Require().NoError(err)
	defer bMgr.DropBucket(bName, nil)

	mgr := cluster.Bucket(bName).Collections()
	mgrV2 := cluster.Bucket(bName).CollectionsV2()

	scopeName := "a" + uuid.NewString()[:6]
	if v2 {
		err = mgrV2.CreateScope(scopeName, nil)
	} else {
		err = mgr.CreateScope(scopeName, nil)
	}
	suite.Require().NoError(err)

	colName := "a" + uuid.NewString()[:6]
	if v2 {
		err = mgrV2.CreateCollection(scopeName, colName, &CreateCollectionSettings{
			History: &CollectionHistorySettings{
				Enabled: true,
			},
		}, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      colName,
			ScopeName: scopeName,
			History: &CollectionHistorySettings{
				Enabled: true,
			},
		}, nil)
	}
	suite.Require().NoError(err)

	var collection *CollectionSpec
	success := suite.tryUntil(time.Now().Add(15*time.Second), 100*time.Millisecond, func() bool {
		var scopes []ScopeSpec
		if v2 {
			scopes, err = mgrV2.GetAllScopes(nil)
		} else {
			scopes, err = mgr.GetAllScopes(nil)
		}
		if err != nil {
			suite.T().Logf("Failed to GetAllScopes %v", err)
			return false
		}
		var scope *ScopeSpec
		for i, s := range scopes {
			if s.Name == scopeName {
				scope = &scopes[i]
			}
		}
		if scope == nil {
			suite.T().Log("scope not found")
			return false
		}

		for _, c := range scope.Collections {
			if c.Name == colName {
				collection = &c
				return true
			}
		}

		suite.T().Log("collection not found")
		return false
	})
	suite.Require().True(success)

	if suite.Assert().NotNil(collection.History) {
		suite.Assert().True(collection.History.Enabled)
	}

	if v2 {
		settings := UpdateCollectionSettings{
			History: &CollectionHistorySettings{
				Enabled: false,
			},
		}
		err = mgrV2.UpdateCollection(collection.ScopeName, collection.Name, settings, nil)
	} else {
		collection.History = &CollectionHistorySettings{
			Enabled: false,
		}
		err = mgr.UpdateCollection(*collection, nil)
	}
	suite.Require().NoError(err)
}

func (suite *IntegrationTestSuite) TestCollectionHistoryRetentionTest() {
	suite.runCollectionHistoryRetentionTest(false)
}

func (suite *IntegrationTestSuite) runCollectionHistoryRetentionUnsupportedTest(v2 bool) {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(HistoryRetentionFeature)

	cluster, err := Connect(globalConfig.connstr, ClusterOptions{
		Authenticator:  globalConfig.Auth,
		SecurityConfig: globalConfig.SecurityConfig,
	})
	suite.Require().NoError(err)
	defer cluster.Close(nil)

	bMgr := cluster.Buckets()
	bName := "a" + uuid.NewString()[:6]
	settings := BucketSettings{
		Name:           bName,
		RAMQuotaMB:     256,
		BucketType:     CouchbaseBucketType,
		StorageBackend: StorageBackendCouchstore,
	}

	err = bMgr.CreateBucket(CreateBucketSettings{
		BucketSettings:         settings,
		ConflictResolutionType: ConflictResolutionTypeSequenceNumber,
	}, nil)
	suite.Require().NoError(err)

	bucket := cluster.Bucket(bName)
	err = bucket.WaitUntilReady(10*time.Second, &WaitUntilReadyOptions{
		ServiceTypes: []ServiceType{ServiceTypeKeyValue},
	})

	mgr := bucket.Collections()
	mgrV2 := bucket.CollectionsV2()

	scopeName := "a" + uuid.NewString()[:6]
	if v2 {
		err = mgrV2.CreateScope(scopeName, nil)
	} else {
		err = mgr.CreateScope(scopeName, nil)
	}
	suite.Require().NoError(err)
	if v2 {
		defer mgrV2.DropScope(scopeName, nil)
	} else {
		defer mgr.DropScope(scopeName, nil)
	}

	colName := "a" + uuid.NewString()[:6]
	if v2 {
		err = mgrV2.CreateCollection(scopeName, colName, &CreateCollectionSettings{
			History: &CollectionHistorySettings{
				Enabled: true,
			},
		}, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      colName,
			ScopeName: scopeName,
			History: &CollectionHistorySettings{
				Enabled: true,
			},
		}, nil)
	}
	suite.Require().ErrorIs(err, ErrFeatureNotAvailable)
}

func (suite *IntegrationTestSuite) TestCollectionHistoryRetentionUnsupported() {
	suite.runCollectionHistoryRetentionUnsupportedTest(false)
}

func (suite *IntegrationTestSuite) runCreateCollectionWithMaxExpiryAsNoExpiryTest(v2 bool) {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)
	suite.skipIfUnsupported(CollectionMaxExpiryNoExpiryFeature)

	scopeName := generateDocId("testScope")
	collectionName := generateDocId("testCollection")

	mgr := globalBucket.Collections()
	mgrV2 := globalBucket.CollectionsV2()

	var err error
	if v2 {
		err = mgrV2.CreateScope(scopeName, nil)
	} else {
		err = mgr.CreateScope(scopeName, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	suite.EnsureScopeOnAllNodes(scopeName)

	if v2 {
		err = mgrV2.CreateCollection(scopeName, collectionName, &CreateCollectionSettings{
			MaxExpiry: -1 * time.Second,
		}, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      collectionName,
			ScopeName: scopeName,
			MaxExpiry: -1 * time.Second,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	suite.EnsureCollectionsOnAllNodes(scopeName, []string{collectionName})

	var scopes []ScopeSpec
	if v2 {
		scopes, err = mgrV2.GetAllScopes(nil)
	} else {
		scopes, err = mgr.GetAllScopes(nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to GetAllScopes %v", err)
	}

	if len(scopes) < 2 {
		suite.T().Fatalf("Expected scopes to contain at least 2 scopes but was %v", scopes)
	}

	var found bool
	for _, scope := range scopes {
		if scope.Name != scopeName {
			continue
		}

		found = true
		if suite.Assert().Len(scope.Collections, 1) {
			col := scope.Collections[0]
			suite.Assert().Equal(collectionName, col.Name)
			suite.Assert().Equal(scopeName, col.ScopeName)
			suite.Assert().Equal(-1*time.Second, col.MaxExpiry)
		}
		break
	}
	suite.Assert().True(found)

	if v2 {
		err = mgrV2.DropScope(scopeName, nil)
	} else {
		err = mgr.DropScope(scopeName, nil)
	}
	if err != nil {
		suite.T().Fatalf("Expected DropScope to not error but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestCreateCollectionWithMaxExpiryAsNoExpiry() {
	suite.runCreateCollectionWithMaxExpiryAsNoExpiryTest(false)
}

func (suite *IntegrationTestSuite) runUpdateCollectionWithMaxExpiryAsNoExpiryTest(v2 bool) {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)
	suite.skipIfUnsupported(CollectionMaxExpiryNoExpiryFeature)
	suite.skipIfUnsupported(CollectionUpdateMaxExpiryFeature)

	scopeName := generateDocId("testScope")
	collectionName := generateDocId("testCollection")

	mgr := globalBucket.Collections()
	mgrV2 := globalBucket.CollectionsV2()

	var err error
	if v2 {
		err = mgrV2.CreateScope(scopeName, nil)
	} else {
		err = mgr.CreateScope(scopeName, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	suite.EnsureScopeOnAllNodes(scopeName)

	if v2 {
		err = mgrV2.CreateCollection(scopeName, collectionName, &CreateCollectionSettings{
			MaxExpiry: 20 * time.Second,
		}, nil)
	} else {
		err = mgr.CreateCollection(CollectionSpec{
			Name:      collectionName,
			ScopeName: scopeName,
			MaxExpiry: 20 * time.Second,
		}, nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	suite.EnsureCollectionsOnAllNodes(scopeName, []string{collectionName})

	if v2 {
		err = mgrV2.UpdateCollection(scopeName, collectionName, UpdateCollectionSettings{
			MaxExpiry: -1 * time.Second,
		}, nil)
	} else {
		err = mgr.UpdateCollection(CollectionSpec{
			Name:      collectionName,
			ScopeName: scopeName,
			MaxExpiry: -1 * time.Second,
		}, nil)
	}

	if err != nil {
		suite.T().Fatalf("Failed to update collection %v", err)
	}

	var scopes []ScopeSpec
	if v2 {
		scopes, err = mgrV2.GetAllScopes(nil)
	} else {
		scopes, err = mgr.GetAllScopes(nil)
	}
	if err != nil {
		suite.T().Fatalf("Failed to GetAllScopes %v", err)
	}

	if len(scopes) < 2 {
		suite.T().Fatalf("Expected scopes to contain at least 2 scopes but was %v", scopes)
	}

	var found bool
	for _, scope := range scopes {
		if scope.Name != scopeName {
			continue
		}

		found = true
		if suite.Assert().Len(scope.Collections, 1) {
			col := scope.Collections[0]
			suite.Assert().Equal(collectionName, col.Name)
			suite.Assert().Equal(scopeName, col.ScopeName)
			suite.Assert().Equal(-1*time.Second, col.MaxExpiry)
		}
		break
	}
	suite.Assert().True(found)

	if v2 {
		err = mgrV2.DropScope(scopeName, nil)
	} else {
		err = mgr.DropScope(scopeName, nil)
	}
	if err != nil {
		suite.T().Fatalf("Expected DropScope to not error but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestUpdateCollectionWithMaxExpiryAsNoExpiry() {
	suite.runUpdateCollectionWithMaxExpiryAsNoExpiryTest(false)
}

func (suite *UnitTestSuite) runGetAllScopesMgmtRequestFailsTest(v2 bool) {
	provider := new(mockMgmtProvider)
	provider.On("executeMgmtRequest", nil, mock.AnythingOfType("gocb.mgmtRequest")).Return(nil, errors.New("http send failure"))

	mgrV2 := CollectionManagerV2{
		controller: &providerController[collectionsManagementProvider]{
			get: func() (collectionsManagementProvider, error) {
				return &collectionsManagementProviderCore{
					mgmtProvider: provider,
					tracer:       &NoopTracer{},
					meter:        &meterWrapper{meter: &NoopMeter{}, isNoopMeter: true},
				}, nil
			},
			opController: mockOpController{},
		},
	}

	mgr := CollectionManager{
		managerV2: &mgrV2,
	}

	var err error
	var scopes []ScopeSpec
	if v2 {
		scopes, err = mgrV2.GetAllScopes(nil)
	} else {
		scopes, err = mgr.GetAllScopes(nil)
	}
	suite.Require().NotNil(err)
	suite.Require().Nil(scopes)
}

func (suite *UnitTestSuite) TestGetAllScopesMgmtRequestFails() {
	suite.runGetAllScopesMgmtRequestFailsTest(false)
}
