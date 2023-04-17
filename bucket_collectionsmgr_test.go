package gocb

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/mock"
	"strconv"
	"time"
)

func (suite *IntegrationTestSuite) TestCollectionManagerCrud() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	scopeName := generateDocId("testScope")
	collectionName := generateDocId("testCollection")

	mgr := globalBucket.Collections()

	err := mgr.CreateScope(scopeName, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = mgr.CreateScope(scopeName, nil)
	if !errors.Is(err, ErrScopeExists) {
		suite.T().Fatalf("Expected create scope to error with ScopeExists but was %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{
		Name:      collectionName,
		ScopeName: scopeName,
		MaxExpiry: 5 * time.Second,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{
		Name:      collectionName,
		ScopeName: scopeName,
	}, nil)
	if !errors.Is(err, ErrCollectionExists) {
		suite.T().Fatalf("Expected create collection to error with CollectionExists but was %v", err)
	}

	scopes, err := mgr.GetAllScopes(nil)
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

	err = mgr.DropCollection(CollectionSpec{
		Name:      collectionName,
		ScopeName: scopeName,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected DropCollection to not error but was %v", err)
	}

	err = mgr.DropCollection(CollectionSpec{
		Name:      collectionName,
		ScopeName: scopeName,
	}, nil)
	if !errors.Is(err, ErrCollectionNotFound) {
		suite.T().Fatalf("Expected drop collection to error with ErrCollectionNotFound but was %v", err)
	}

	err = mgr.DropScope(scopeName, nil)
	if err != nil {
		suite.T().Fatalf("Expected DropScope to not error but was %v", err)
	}

	err = mgr.DropScope(scopeName, nil)
	if !errors.Is(err, ErrScopeNotFound) {
		suite.T().Fatalf("Expected drop scope to error with ErrScopeNotFound but was %v", err)
	}

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(9, len(nilParents))
	suite.AssertHTTPOpSpan(nilParents[0], "manager_collections_create_scope",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			scope:                   scopeName,
			service:                 "management",
			operationID:             "POST " + fmt.Sprintf("/pools/default/buckets/%s/scopes", globalConfig.Bucket),
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			dispatchOperationID:     "any",
		})
	suite.AssertHTTPOpSpan(nilParents[2], "manager_collections_create_collection",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			scope:                   scopeName,
			collection:              collectionName,
			service:                 "management",
			operationID:             "POST " + fmt.Sprintf("/pools/default/buckets/%s/scopes/%s/collections", globalConfig.Bucket, scopeName),
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             true,
			dispatchOperationID:     "any",
		})
	suite.AssertHTTPOpSpan(nilParents[4], "manager_collections_get_all_scopes",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			service:                 "management",
			operationID:             "GET " + fmt.Sprintf("/pools/default/buckets/%s/scopes", globalConfig.Bucket),
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
		})
	suite.AssertHTTPOpSpan(nilParents[5], "manager_collections_drop_collection",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			scope:                   scopeName,
			collection:              collectionName,
			service:                 "management",
			operationID:             "DELETE " + fmt.Sprintf("/pools/default/buckets/%s/scopes/%s/collections/%s", globalConfig.Bucket, scopeName, collectionName),
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
		})
	suite.AssertHTTPOpSpan(nilParents[7], "manager_collections_drop_scope",
		HTTPOpSpanExpectations{
			bucket:                  globalConfig.Bucket,
			scope:                   scopeName,
			service:                 "management",
			operationID:             "DELETE " + fmt.Sprintf("/pools/default/buckets/%s/scopes/%s", globalConfig.Bucket, scopeName),
			numDispatchSpans:        1,
			atLeastNumDispatchSpans: false,
			hasEncoding:             false,
			dispatchOperationID:     "any",
		})

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_collections_create_scope"), 2, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_collections_create_collection"), 2, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_collections_get_all_scopes"), 1, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_collections_drop_scope"), 2, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, "management", "manager_collections_drop_collection"), 2, false)
}

func (suite *IntegrationTestSuite) TestDropNonExistentScope() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	scopeName := generateDocId("testDropScopeX")

	mgr := globalBucket.Collections()

	err := mgr.CreateScope(scopeName, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}
	err = mgr.CreateCollection(CollectionSpec{
		Name:      generateDocId("testDropCollection"),
		ScopeName: scopeName,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = mgr.DropScope(generateDocId("testScopeX"), nil)
	if err == nil {
		suite.T().Fatalf("Expected error to be non-nil")
	}
}

func (suite *IntegrationTestSuite) TestDropNonExistentCollection() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	scopeName := generateDocId("testDropScopeY")

	mgr := globalBucket.Collections()
	err := mgr.CreateScope(scopeName, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{
		Name:      generateDocId("testDropCollectionY"),
		ScopeName: scopeName,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = mgr.DropCollection(CollectionSpec{
		Name:      generateDocId("testCollectionZ"),
		ScopeName: scopeName,
	}, nil)
	if err == nil {
		suite.T().Fatalf("Expected error to be non-nil")
	}
}

func (suite *IntegrationTestSuite) TestCollectionsAreNotPresent() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	scopeName1 := generateDocId("scope-1-")
	scopeName2 := generateDocId("scope-2-")
	collectionName1 := generateDocId("collection-1-")
	collectionName2 := generateDocId("collection-2-")

	mgr := globalBucket.Collections()

	err := mgr.CreateScope(scopeName1, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = mgr.CreateScope(scopeName2, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{
		Name:      collectionName1,
		ScopeName: scopeName1,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{
		Name:      collectionName2,
		ScopeName: scopeName2,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = mgr.DropCollection(CollectionSpec{
		Name:      collectionName1,
		ScopeName: scopeName1,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected DropCollection to not error but was %v", err)
	}

	err = mgr.DropCollection(CollectionSpec{
		Name:      collectionName2,
		ScopeName: scopeName2,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected DropCollection to not error but was %v", err)
	}

	err = mgr.DropCollection(CollectionSpec{
		Name:      collectionName2,
		ScopeName: scopeName2,
	}, nil)
	if err == nil {
		suite.T().Fatalf("Expected error to be non-nil")
	}

}

func (suite *IntegrationTestSuite) TestDropScopesAreNotExist() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	mgr := globalBucket.Collections()

	err := mgr.CreateScope("testDropScope1", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = mgr.CreateScope("testDropScope2", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{
		Name:      "testDropCollection1",
		ScopeName: "testDropScope1",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{
		Name:      "testDropCollection2",
		ScopeName: "testDropScope2",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = mgr.DropScope("testDropScope1", nil)
	if err != nil {
		suite.T().Fatalf("Expected DropScope to not error but was %v", err)
	}

	err = mgr.DropScope("testDropScope2", nil)
	if err != nil {
		suite.T().Fatalf("Expected DropScope to not error but was %v", err)
	}

	err = mgr.DropScope("testDropScope1", nil)
	if err == nil {
		suite.T().Fatalf("Expected error to be non-nil")
	}

	err = mgr.DropCollection(CollectionSpec{
		Name:      "testDropCollection1",
		ScopeName: "testDropScope1",
	}, nil)
	if err == nil {
		suite.T().Fatalf("Expected error to be non-nil")
	}
}
func (suite *IntegrationTestSuite) TestGetAllScopes() {
	suite.skipIfUnsupported(CollectionsFeature)

	bucket1 := globalBucket.Collections()

	err := bucket1.CreateScope(generateDocId("testScopeX1"), nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = bucket1.CreateScope(generateDocId("testScopeX2"), nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = bucket1.CreateScope(generateDocId("testScopeX3"), nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = bucket1.CreateScope(generateDocId("testScopeX4"), nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = bucket1.CreateScope(generateDocId("testScopeX5"), nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	scopes, err := bucket1.GetAllScopes(nil)
	if err != nil {
		suite.T().Fatalf("Failed to GetAllScopes %v", err)
	}

	if len(scopes) < 5 {
		suite.T().Fatalf("Expected scopes to contain total of 5 scopes but was %v", scopes)
	}
}

func (suite *IntegrationTestSuite) TestCollectionsInBucket() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	scopeName := generateDocId("collectionsInBucketScope")

	bucket1 := globalBucket.Collections()

	err := bucket1.CreateScope(scopeName, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = bucket1.CreateCollection(CollectionSpec{
		Name:      generateDocId("testCollection1-"),
		ScopeName: scopeName,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucket1.CreateCollection(CollectionSpec{
		Name:      generateDocId("testCollection2-"),
		ScopeName: scopeName,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucket1.CreateCollection(CollectionSpec{
		Name:      generateDocId("testCollection3-"),
		ScopeName: scopeName,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucket1.CreateCollection(CollectionSpec{
		Name:      generateDocId("testCollection4-"),
		ScopeName: scopeName,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucket1.CreateCollection(CollectionSpec{
		Name:      generateDocId("testCollection5-"),
		ScopeName: scopeName,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	success := suite.tryUntil(time.Now().Add(5*time.Second), 500*time.Millisecond, func() bool {
		scopes, err := bucket1.GetAllScopes(nil)
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

func (suite *IntegrationTestSuite) TestNumberOfCollectionInScope() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	scopeName1 := generateDocId("numCollectionsScope1-")
	scopeName2 := generateDocId("numCollectionsScope2-")

	bucketX := globalBucket.Collections()

	err := bucketX.CreateScope(scopeName1, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = bucketX.CreateScope(scopeName2, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = bucketX.CreateCollection(CollectionSpec{
		Name:      generateDocId("testCollection1-"),
		ScopeName: scopeName1,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucketX.CreateCollection(CollectionSpec{
		Name:      generateDocId("testCollection2-"),
		ScopeName: scopeName1,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucketX.CreateCollection(CollectionSpec{
		Name:      generateDocId("testCollection3-"),
		ScopeName: scopeName1,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucketX.CreateCollection(CollectionSpec{
		Name:      generateDocId("testCollection4-"),
		ScopeName: scopeName1,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucketX.CreateCollection(CollectionSpec{
		Name:      generateDocId("testCollection5-"),
		ScopeName: scopeName1,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucketX.CreateCollection(CollectionSpec{
		Name:      generateDocId("testCollection6-"),
		ScopeName: scopeName2,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucketX.CreateCollection(CollectionSpec{
		Name:      generateDocId("testCollection6-"),
		ScopeName: scopeName2,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	scopes, err := bucketX.GetAllScopes(nil)
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

func (suite *IntegrationTestSuite) TestMaxNumberOfCollectionInScope() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)
	suite.skipIfUnsupported(CollectionsManagerMaxCollectionsFeature)

	testBucket1 := globalBucket.Collections()
	err := testBucket1.CreateScope("singleScope", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}
	for i := 0; i < 1000; i++ {
		err = testBucket1.CreateCollection(CollectionSpec{
			Name:      strconv.Itoa(1000 + i),
			ScopeName: "singleScope",
		}, nil)
		if err != nil {
			suite.T().Fatalf("Failed to create collection %v", err)
		}
	}

	success := suite.tryUntil(time.Now().Add(15*time.Second), 100*time.Millisecond, func() bool {
		scopes, err := testBucket1.GetAllScopes(nil)
		if err != nil {
			suite.T().Logf("Failed to GetAllScopes %v", err)
			return false
		}
		var scope *ScopeSpec
		for i, s := range scopes {
			if s.Name == "singleScope" {
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

func (suite *UnitTestSuite) TestGetAllScopesMgmtRequestFails() {
	provider := new(mockMgmtProvider)
	provider.On("executeMgmtRequest", nil, mock.AnythingOfType("gocb.mgmtRequest")).Return(nil, errors.New("http send failure"))

	mgr := CollectionManager{
		mgmtProvider: provider,
		tracer:       &NoopTracer{},
		meter:        &meterWrapper{meter: &NoopMeter{}, isNoopMeter: true},
	}

	scopes, err := mgr.GetAllScopes(nil)
	suite.Require().NotNil(err)
	suite.Require().Nil(scopes)
}
