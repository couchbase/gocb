package gocb

import (
	"errors"
	"testing"
)

func TestCollectionManagerCrud(t *testing.T) {
	if !globalCluster.SupportsFeature(CollectionsFeature) {
		t.Skip("Skipping test as collections not supported")
	}

	mgr := globalBucket.Collections()

	err := mgr.CreateScope("testScope", nil)
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
