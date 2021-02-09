package gocb

import (
	"errors"
	"strconv"
	"time"
)

func (suite *IntegrationTestSuite) TestCollectionManagerCrud() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	mgr := globalBucket.Collections()

	err := mgr.CreateScope("testScope", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = mgr.CreateScope("testScope", nil)
	if !errors.Is(err, ErrScopeExists) {
		suite.T().Fatalf("Expected create scope to error with ScopeExists but was %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{
		Name:      "testCollection",
		ScopeName: "testScope",
		MaxExpiry: 5 * time.Second,
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{
		Name:      "testCollection",
		ScopeName: "testScope",
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
		if scope.Name != "testScope" {
			continue
		}

		found = true
		if suite.Assert().Len(scope.Collections, 1) {
			col := scope.Collections[0]
			suite.Assert().Equal("testCollection", col.Name)
			suite.Assert().Equal("testScope", col.ScopeName)
			suite.Assert().Equal(5*time.Second, col.MaxExpiry)
		}
		break
	}
	suite.Assert().True(found)

	err = mgr.DropCollection(CollectionSpec{
		Name:      "testCollection",
		ScopeName: "testScope",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected DropCollection to not error but was %v", err)
	}

	err = mgr.DropCollection(CollectionSpec{
		Name:      "testCollection",
		ScopeName: "testScope",
	}, nil)
	if !errors.Is(err, ErrCollectionNotFound) {
		suite.T().Fatalf("Expected drop collection to error with ErrCollectionNotFound but was %v", err)
	}

	err = mgr.DropScope("testScope", nil)
	if err != nil {
		suite.T().Fatalf("Expected DropScope to not error but was %v", err)
	}

	err = mgr.DropScope("testScope", nil)
	if !errors.Is(err, ErrScopeNotFound) {
		suite.T().Fatalf("Expected drop scope to error with ErrScopeNotFound but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestDropNonExistentScope() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	mgr := globalBucket.Collections()

	err := mgr.CreateScope("testDropScopeX", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}
	err = mgr.CreateCollection(CollectionSpec{
		Name:      "testDropCollection",
		ScopeName: "testDropScopeX",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = mgr.DropScope("testScopeX", nil)
	if err == nil {
		suite.T().Fatalf("Expected error to be non-nil")
	}
}

func (suite *IntegrationTestSuite) TestDropNonExistentCollection() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	mgr := globalBucket.Collections()
	err := mgr.CreateScope("testDropScopeY", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{
		Name:      "testDropCollectionY",
		ScopeName: "testDropScopeY",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = mgr.DropCollection(CollectionSpec{
		Name:      "testCollectionZ",
		ScopeName: "testDropScopeY",
	}, nil)
	if err == nil {
		suite.T().Fatalf("Expected error to be non-nil")
	}
}

func (suite *IntegrationTestSuite) TestCollectionsAreNotPresent() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(CollectionsManagerFeature)

	mgr := globalBucket.Collections()

	err := mgr.CreateScope("testScope1", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = mgr.CreateScope("testScope2", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{
		Name:      "testCollection1",
		ScopeName: "testScope1",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{
		Name:      "testCollection2",
		ScopeName: "testScope2",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = mgr.DropCollection(CollectionSpec{
		Name:      "testCollection1",
		ScopeName: "testScope1",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected DropCollection to not error but was %v", err)
	}

	err = mgr.DropCollection(CollectionSpec{
		Name:      "testCollection2",
		ScopeName: "testScope2",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected DropCollection to not error but was %v", err)
	}

	err = mgr.DropCollection(CollectionSpec{
		Name:      "testCollection2",
		ScopeName: "testScope2",
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

	err := bucket1.CreateScope("testScopeX1", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = bucket1.CreateScope("testScopeX2", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = bucket1.CreateScope("testScopeX3", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = bucket1.CreateScope("testScopeX4", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = bucket1.CreateScope("testScopeX5", nil)
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

	bucket1 := globalBucket.Collections()

	err := bucket1.CreateScope("collectionsInBucketScope", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = bucket1.CreateCollection(CollectionSpec{
		Name:      "testCollection1",
		ScopeName: "collectionsInBucketScope",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucket1.CreateCollection(CollectionSpec{
		Name:      "testCollection2",
		ScopeName: "collectionsInBucketScope",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucket1.CreateCollection(CollectionSpec{
		Name:      "testCollection3",
		ScopeName: "collectionsInBucketScope",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucket1.CreateCollection(CollectionSpec{
		Name:      "testCollection4",
		ScopeName: "collectionsInBucketScope",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucket1.CreateCollection(CollectionSpec{
		Name:      "testCollection5",
		ScopeName: "collectionsInBucketScope",
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
			if s.Name == "collectionsInBucketScope" {
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

	bucketX := globalBucket.Collections()

	err := bucketX.CreateScope("numCollectionsScope1", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = bucketX.CreateScope("numCollectionsScope2", nil)
	if err != nil {
		suite.T().Fatalf("Failed to create scope %v", err)
	}

	err = bucketX.CreateCollection(CollectionSpec{
		Name:      "testCollection1",
		ScopeName: "numCollectionsScope1",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucketX.CreateCollection(CollectionSpec{
		Name:      "testCollection2",
		ScopeName: "numCollectionsScope1",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucketX.CreateCollection(CollectionSpec{
		Name:      "testCollection3",
		ScopeName: "numCollectionsScope1",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucketX.CreateCollection(CollectionSpec{
		Name:      "testCollection4",
		ScopeName: "numCollectionsScope1",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucketX.CreateCollection(CollectionSpec{
		Name:      "testCollection5",
		ScopeName: "numCollectionsScope1",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucketX.CreateCollection(CollectionSpec{
		Name:      "testCollection6",
		ScopeName: "numCollectionsScope2",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Failed to create collection %v", err)
	}

	err = bucketX.CreateCollection(CollectionSpec{
		Name:      "testCollection7",
		ScopeName: "numCollectionsScope2",
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
		if s.Name == "numCollectionsScope1" {
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
