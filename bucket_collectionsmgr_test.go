package gocb

import (
	"errors"
	"testing"
)

func TestCollectionManagerCrud(t *testing.T) {
	if !globalCluster.SupportsFeature(CollectionsFeature) {
		t.Skip("Skipping test as collections not supported")
	}

	mgr, err := globalBucket.CollectionManager()
	if err != nil {
		t.Fatalf("Failed to get collections manager %v", err)
	}

	err = mgr.CreateScope("testScope", nil)
	if err != nil {
		t.Fatalf("Failed to create scope %v", err)
	}

	err = mgr.CreateScope("testScope", nil)
	if !errors.Is(err, ErrScopeExists) {
		t.Fatalf("Expected create scope to error with ScopeExists but was %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{
		Name:      "testCollection",
		ScopeName: "testScope",
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create collection %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{
		Name:      "testCollection",
		ScopeName: "testScope",
	}, nil)
	if !errors.Is(err, ErrCollectionExists) {
		t.Fatalf("Expected create collection to error with CollectionExists but was %v", err)
	}

	scopes, err := mgr.GetAllScopes(nil)
	if err != nil {
		t.Fatalf("Failed to GetAllScopes %v", err)
	}

	if len(scopes) < 2 {
		t.Fatalf("Expected scopes to contain at least 2 scopes but was %v", scopes)
	}

	scope, err := mgr.GetScope("testScope", nil)
	if err != nil {
		t.Fatalf("Failed to GetScope %v", err)
	}

	if scope.Name != "testScope" {
		t.Fatalf("Expected scope name to be testScope but was %s", scope.Name)
	}

	if len(scope.Collections) != 1 {
		t.Fatalf("Expected scope to contain 1 collection but was %v", scope.Collections)
	}

	collection := scope.Collections[0]
	if collection.Name != "testCollection" {
		t.Fatalf("Expected collection name to be testCollection but was %s", collection.Name)
	}
	if collection.ScopeName != "testScope" {
		t.Fatalf("Expected collection scope name to be testScope but was %s", collection.ScopeName)
	}

	err = mgr.DropCollection(CollectionSpec{
		Name:      "testCollection",
		ScopeName: "testScope",
	}, nil)
	if err != nil {
		t.Fatalf("Expected DropCollection to not error but was %v", err)
	}

	err = mgr.DropScope("testScope", nil)
	if err != nil {
		t.Fatalf("Expected DropScope to not error but was %v", err)
	}
}
