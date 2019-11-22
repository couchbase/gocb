package gocb

import (
	"errors"
	"strings"
	"testing"
)

func TestInsertLookupIn(t *testing.T) {
	type beerWithCountable struct {
		testBeerDocument
		Countable []string `json:"countable"`
	}
	var doc beerWithCountable
	err := loadJSONTestDataset("beer_sample_single", &doc.testBeerDocument)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	doc.Countable = []string{"one", "two"}

	mutRes, err := globalCollection.Insert("lookupDoc", doc, nil)
	if err != nil {
		t.Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	result, err := globalCollection.LookupIn("lookupDoc", []LookupInSpec{
		GetSpec("name", nil),
		GetSpec("description", nil),
		ExistsSpec("doesnt", nil),
		ExistsSpec("style", nil),
		GetSpec("doesntexist", nil),
		CountSpec("countable", nil),
	}, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	if result.Exists(2) {
		t.Fatalf("Expected doesnt field to not exist")
	}

	if !result.Exists(3) {
		t.Fatalf("Expected style field to exist")
	}

	var name string
	err = result.ContentAt(0, &name)
	if err != nil {
		t.Fatalf("Failed to get name from LookupInResult, %v", err)
	}

	if name != doc.Name {
		t.Fatalf("Expected name to be %s but was %s", doc.Name, name)
	}

	var desc string
	err = result.ContentAt(1, &desc)
	if err != nil {
		t.Fatalf("Failed to get description from LookupInResult, %v", err)
	}

	if desc != doc.Description {
		t.Fatalf("Expected description to be %s but was %s", doc.Description, desc)
	}

	var idontexist string
	err = result.ContentAt(4, &idontexist)
	if err == nil {
		t.Fatalf("Expected lookup on a non existent field to return error")
	}

	if !errors.Is(err, ErrPathNotFound) {
		t.Fatalf("Expected error to be path not found but was %+v", err)
	}

	var count int
	err = result.ContentAt(5, &count)
	if err != nil {
		t.Fatalf("Failed to get count from LookupInResult, %v", err)
	}

	if count != 2 {
		t.Fatalf("LookupIn Result count should have be 2 but was %d", count)
	}
}

func TestMutateInBasicCrud(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert("mutateIn", doc, nil)
	if err != nil {
		t.Fatalf("Insert failed, error: %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	fishName := "blobfish"
	newName := "fishy beer"
	newStyle := "fishy"
	subRes, err := globalCollection.MutateIn("mutateIn", []MutateInSpec{
		InsertSpec("fish", fishName, nil),
		UpsertSpec("name", newName, nil),
		UpsertSpec("newName", newName, nil),
		ReplaceSpec("style", newStyle, nil),
		RemoveSpec("type", nil),
	}, nil)
	if err != nil {
		t.Fatalf("MutateIn failed, error was %v", err)
	}

	if subRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	getRes, err := globalCollection.Get("mutateIn", nil)
	if err != nil {
		t.Fatalf("Getting document errored: %v", err)
	}

	type fishBeerDocument struct {
		testBeerDocument
		NewName string `json:"newName"`
		Fish    string `json:"fish"`
	}

	var actualDoc fishBeerDocument
	err = getRes.Content(&actualDoc)
	if err != nil {
		t.Fatalf("Getting content errored: %v", err)
	}

	expectedDoc := fishBeerDocument{
		testBeerDocument: doc,
		NewName:          newName,
		Fish:             fishName,
	}
	expectedDoc.Name = newName
	expectedDoc.Style = newStyle
	expectedDoc.Type = ""

	if actualDoc != expectedDoc {
		t.Fatalf("results did not match, expected %#v but was %#v", expectedDoc, actualDoc)
	}
}

func TestMutateInBasicArray(t *testing.T) {
	doc := struct {
		Fish []string `json:"array"`
	}{
		[]string{},
	}
	mutRes, err := globalCollection.Insert("mutateInArray", doc, nil)
	if err != nil {
		t.Fatalf("Insert failed, error: %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	subRes, err := globalCollection.MutateIn("mutateInArray", []MutateInSpec{
		ArrayAppendSpec("array", "clownfish", nil),
		ArrayPrependSpec("array", "whaleshark", nil),
		ArrayInsertSpec("array[1]", "catfish", nil),
		ArrayAppendSpec("array", []string{"manta ray", "stingray"}, &ArrayAppendSpecOptions{HasMultiple: true}),
		ArrayPrependSpec("array", []string{"carp", "goldfish"}, &ArrayPrependSpecOptions{HasMultiple: true}),
		ArrayInsertSpec("array[1]", []string{"eel", "stonefish"}, &ArrayInsertSpecOptions{HasMultiple: true}),
	}, nil)
	if err != nil {
		t.Fatalf("MutateIn failed, error was %v", err)
	}

	if subRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	getRes, err := globalCollection.Get("mutateInArray", nil)
	if err != nil {
		t.Fatalf("Getting document errored: %v", err)
	}

	type fishBeerDocument struct {
		Fish []string `json:"array"`
	}

	var actualDoc fishBeerDocument
	err = getRes.Content(&actualDoc)
	if err != nil {
		t.Fatalf("Getting content errored: %v", err)
	}

	expectedDoc := fishBeerDocument{
		Fish: []string{"carp", "eel", "stonefish", "goldfish", "whaleshark", "catfish", "clownfish", "manta ray", "stingray"},
	}

	if len(expectedDoc.Fish) != len(actualDoc.Fish) {
		t.Fatalf("results did not match, expected %v but was %v", expectedDoc, actualDoc)
	}
	for i, fish := range expectedDoc.Fish {
		if fish != actualDoc.Fish[i] {
			t.Fatalf("results did not match, expected %s at index %d but was %s", fish, i, actualDoc.Fish[i])
		}
	}
}

func TestMutateInLookupInXattr(t *testing.T) {
	if !globalCluster.SupportsFeature(XattrFeature) {
		t.Skip("Skipping test as xattrs not supported")
	}

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert("mutateInFullInsertInsertXattr", doc, nil)
	if err != nil {
		t.Fatalf("Insert failed, error: %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	fishName := "flounder"
	doc.Name = "namename"
	subRes, err := globalCollection.MutateIn("mutateInFullInsertInsertXattr", []MutateInSpec{
		InsertSpec("fish", fishName, &InsertSpecOptions{IsXattr: true}),
		ReplaceSpec("", doc, nil),
	}, nil)
	if err != nil {
		t.Fatalf("MutateIn failed, error was %v", err)
	}

	if subRes.Cas() == 0 {
		t.Fatalf("MutateIn CAS was 0")
	}

	result, err := globalCollection.LookupIn("mutateInFullInsertInsertXattr", []LookupInSpec{
		GetSpec("fish", &GetSpecOptions{IsXattr: true}),
		GetSpec("name", nil),
	}, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var fish string
	err = result.ContentAt(0, &fish)
	if err != nil {
		t.Fatalf("Failed to get name from LookupInResult, %v", err)
	}

	if fish != fishName {
		t.Fatalf("Expected fish to be %s but was %s", fishName, fish)
	}

	var name string
	err = result.ContentAt(1, &name)
	if err != nil {
		t.Fatalf("Failed to get name from LookupInResult, %v", err)
	}

	if name != doc.Name {
		t.Fatalf("Expected name to be %s but was %s", doc.Name, name)
	}
}

func TestInsertLookupInInsertGetFull(t *testing.T) {
	if !globalCluster.SupportsFeature(XattrFeature) {
		t.Skip("Skipping test as xattrs not supported")
	}
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	subRes, err := globalCollection.MutateIn("lookupDocGetFull", []MutateInSpec{
		InsertSpec("xattrpath", "xattrvalue", &InsertSpecOptions{IsXattr: true}),
		ReplaceSpec("", doc, nil),
	}, &MutateInOptions{StoreSemantic: StoreSemanticsUpsert, Expiry: 20})
	if err != nil {
		t.Fatalf("MutateIn failed, error was %v", err)
	}

	if subRes.Cas() == 0 {
		t.Fatalf("MutateIn CAS was 0")
	}

	result, err := globalCollection.LookupIn("lookupDocGetFull", []LookupInSpec{
		GetSpec("$document.exptime", &GetSpecOptions{IsXattr: true}),
		GetSpec("", nil),
	}, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var exptime int
	err = result.ContentAt(0, &exptime)
	if err != nil {
		t.Fatalf("Failed to get expiry from LookupInResult, %v", err)
	}

	if exptime == 0 {
		t.Fatalf("Expected expiry to be non zero")
	}

	var actualDoc testBeerDocument
	err = result.ContentAt(1, &actualDoc)
	if err != nil {
		t.Fatalf("Failed to get name from LookupInResult, %v", err)
	}

	if actualDoc != doc {
		t.Fatalf("Expected doc to be %v but was %v", doc, actualDoc)
	}
}

func TestMutateInLookupInCounters(t *testing.T) {
	doc := struct {
		Counter int `json:"counter"`
	}{
		Counter: 20,
	}

	mutRes, err := globalCollection.Insert("mutateInLookupInCounters", doc, nil)
	if err != nil {
		t.Fatalf("Insert failed, error: %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	subRes, err := globalCollection.MutateIn("mutateInLookupInCounters", []MutateInSpec{
		IncrementSpec("counter", 10, nil),
		DecrementSpec("counter", 5, nil),
	}, nil)
	if err != nil {
		t.Fatalf("Increment failed, error was %v", err)
	}

	if subRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	result, err := globalCollection.LookupIn("mutateInLookupInCounters", []LookupInSpec{
		GetSpec("counter", nil),
	}, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var counter int
	err = result.ContentAt(0, &counter)
	if err != nil {
		t.Fatalf("Failed to get counter from LookupInResult, %v", err)
	}

	if counter != 25 {
		t.Fatalf("Expected counter to be 25 but was %d", counter)
	}
}

func TestMutateInLookupInMacro(t *testing.T) {
	if !globalCluster.SupportsFeature(ExpandMacrosFeature) {
		t.Skip("Skipping test as macros not supported")
	}

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert("mutateInInsertMacro", doc, nil)
	if err != nil {
		t.Fatalf("Insert failed, error: %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	subRes, err := globalCollection.MutateIn("mutateInInsertMacro", []MutateInSpec{
		InsertSpec("caspath", MutationMacroCAS, nil),
	}, nil)
	if err != nil {
		t.Fatalf("MutateIn failed, error was %v", err)
	}

	if subRes.Cas() == 0 {
		t.Fatalf("MutateIn CAS was 0")
	}

	result, err := globalCollection.LookupIn("mutateInInsertMacro", []LookupInSpec{
		GetSpec("caspath", &GetSpecOptions{IsXattr: true}),
	}, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var caspath string
	err = result.ContentAt(0, &caspath)
	if err != nil {
		t.Fatalf("Failed to get caspath from LookupInResult, %v", err)
	}

	if !strings.HasPrefix(caspath, "0x") {
		t.Fatalf("Expected caspath to start with 0x but was %s", caspath)
	}
}
