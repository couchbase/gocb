package gocb

import (
	"errors"
	"time"
)

func (suite *IntegrationTestSuite) TestCollectionManagerCrud() {
	suite.skipIfUnsupported(CollectionsFeature)

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

	err = mgr.DropCollection(CollectionSpec{
		Name:      "testCollection",
		ScopeName: "testScope",
	}, nil)
	if err != nil {
		suite.T().Fatalf("Expected DropCollection to not error but was %v", err)
	}

	err = mgr.DropScope("testScope", nil)
	if err != nil {
		suite.T().Fatalf("Expected DropScope to not error but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestCollectionManagerCrudUnsupported() {
	if globalCluster.SupportsFeature(CollectionsFeature) {
		suite.T().Skip("Skipping as collections are supported")
	}

	mgr := globalBucket.Collections()
	err := mgr.CreateScope("testScope", nil)
	suite.Assert().Equal(err, ErrFeatureNotAvailable)

}
