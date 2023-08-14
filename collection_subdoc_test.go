package gocb

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/couchbase/gocbcore/v10/memd"
)

func (suite *IntegrationTestSuite) TestInsertLookupIn() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(SubdocFeature)

	docId := generateDocId("lookupDoc")

	type beerWithCountable struct {
		testBeerDocument
		Countable []string `json:"countable"`
	}
	var doc beerWithCountable
	err := loadJSONTestDataset("beer_sample_single", &doc.testBeerDocument)
	suite.Require().NoError(err, "Could not read test dataset")

	doc.Countable = []string{"one", "two"}

	mutRes, err := globalCollection.Insert(docId, doc, nil)
	suite.Require().NoError(err, "Insert failed")

	suite.Assert().NotZero(mutRes.Cas(), "Insert CAS was 0")

	result, err := globalCollection.LookupIn(docId, []LookupInSpec{
		GetSpec("name", nil),
		GetSpec("description", nil),
		ExistsSpec("doesnt", nil),
		ExistsSpec("style", nil),
		GetSpec("doesntexist", nil),
		CountSpec("countable", nil),
	}, nil)
	suite.Require().NoError(err, "LookupIn failed")

	suite.Assert().False(result.Exists(2), "Expected doesnt field to not exist")

	var shouldNotExist bool
	err = result.ContentAt(2, &shouldNotExist)
	suite.Require().NoError(err)

	suite.Assert().False(shouldNotExist)

	suite.Assert().True(result.Exists(3), "Expected style field to exist")

	var shouldExist bool
	err = result.ContentAt(3, &shouldExist)
	suite.Require().NoError(err)

	suite.Assert().True(shouldExist, "Expected style field to assign true")

	var name string
	err = result.ContentAt(0, &name)
	suite.Require().NoError(err, "Failed to get name from LookupInResult")

	suite.Assert().Equal(doc.Name, name)

	var desc string
	err = result.ContentAt(1, &desc)
	suite.Require().NoError(err, "Failed to get description from LookupInResult")

	suite.Assert().Equal(doc.Description, desc)

	var idontexist string
	err = result.ContentAt(4, &idontexist)
	suite.Require().ErrorIs(err, ErrPathNotFound)

	var count int
	err = result.ContentAt(5, &count)
	suite.Require().NoError(err, "Failed to get count from LookupInResult")

	suite.Assert().Equal(2, count)

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 2)
	suite.AssertKvOpSpan(nilParents[0], "insert", memd.CmdAdd.Name(), true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "lookup_in", memd.CmdSubDocMultiLookup.Name(), false, DurabilityLevelNone)
}

func (suite *IntegrationTestSuite) TestMutateInBasicCrud() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(SubdocFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("mutateIn", doc, nil)
	if err != nil {
		suite.T().Fatalf("Upsert failed, error: %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	fishName := "blobfish"
	newName := "fishy beer"
	newStyle := "fishy"
	subRes, err := globalCollection.MutateIn("mutateIn", []MutateInSpec{
		InsertSpec("fish", fishName, nil),
		UpsertSpec("name", newName, nil),
		UpsertSpec("newName", newName, nil),
		UpsertSpec("description", nil, nil),
		ReplaceSpec("style", newStyle, nil),
		ReplaceSpec("category", nil, nil),
		RemoveSpec("type", nil),
		InsertSpec("x.foo.bar", "ddd", &InsertSpecOptions{CreatePath: true}),
		UpsertSpec("x.foo.barbar", "barbar", &UpsertSpecOptions{}),
		ReplaceSpec("x.foo.bar", "eee", &ReplaceSpecOptions{}),
	}, nil)
	if err != nil {
		suite.T().Fatalf("MutateIn failed, error was %v", err)
	}

	if subRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	getRes, err := globalCollection.Get("mutateIn", nil)
	if err != nil {
		suite.T().Fatalf("Getting document errored: %v", err)
	}

	type fishBeerDocument struct {
		testBeerDocument
		NewName string `json:"newName"`
		Fish    string `json:"fish"`
	}

	var actualDoc fishBeerDocument
	err = getRes.Content(&actualDoc)
	if err != nil {
		suite.T().Fatalf("Getting content errored: %v", err)
	}
	rawMap := make(map[string]interface{})
	err = getRes.Content(&rawMap)
	if err != nil {
		suite.T().Fatalf("Getting content to raw map errored: %v", err)
	}

	if rawMap["brewery_id"] != doc.BreweryID {
		suite.T().Fatalf("raw map content did not match, expected %#v but was %#v", doc.BreweryID, rawMap["brewery_id"])
	}
	if rawMap["category"] != nil {
		suite.T().Fatalf("raw map content did not match, expected %#v but was %#v", nil, rawMap["category"])
	}
	if rawMap["description"] != nil {
		suite.T().Fatalf("raw map content did not match, expected %#v but was %#v", nil, rawMap["description"])
	}

	expectedDoc := fishBeerDocument{
		testBeerDocument: doc,
		NewName:          newName,
		Fish:             fishName,
	}
	expectedDoc.Name = newName
	expectedDoc.Style = newStyle
	expectedDoc.Type = ""
	expectedDoc.Category = ""
	expectedDoc.Description = ""

	if actualDoc != expectedDoc {
		suite.T().Fatalf("results did not match, expected %#v but was %#v", expectedDoc, actualDoc)
	}

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 3)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), true, DurabilityLevelNone)
	suite.AssertKvSpan(nilParents[1], "mutate_in", DurabilityLevelNone)
	suite.AssertEncodingSpansEq(nilParents[1].Spans, 10)
	suite.AssertCmdSpans(nilParents[1].Spans, memd.CmdSubDocMultiMutation.Name())
	suite.AssertKvOpSpan(nilParents[2], "get", memd.CmdGet.Name(), false, DurabilityLevelNone)
}

func (suite *IntegrationTestSuite) TestMutateInBasicArray() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(SubdocFeature)

	docId := generateDocId("mutateInArray")

	doc := struct {
	}{}
	mutRes, err := globalCollection.Insert(docId, doc, nil)
	if err != nil {
		suite.T().Fatalf("Insert failed, error: %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	subRes, err := globalCollection.MutateIn(docId, []MutateInSpec{
		ArrayAppendSpec("array", "clownfish", &ArrayAppendSpecOptions{
			CreatePath: true,
		}),
		ArrayPrependSpec("array", "whaleshark", nil),
		ArrayInsertSpec("array[1]", "catfish", nil),
		ArrayAppendSpec("array", []string{"manta ray", "stingray"}, &ArrayAppendSpecOptions{HasMultiple: true}),
		ArrayPrependSpec("array", []string{"carp", "goldfish"}, &ArrayPrependSpecOptions{HasMultiple: true}),
		ArrayInsertSpec("array[1]", []string{"eel", "stonefish"}, &ArrayInsertSpecOptions{HasMultiple: true}),
	}, nil)
	if err != nil {
		suite.T().Fatalf("MutateIn failed, error was %v", err)
	}

	if subRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	getRes, err := globalCollection.Get(docId, nil)
	if err != nil {
		suite.T().Fatalf("Getting document errored: %v", err)
	}

	type fishBeerDocument struct {
		Fish []string `json:"array"`
	}

	var actualDoc fishBeerDocument
	err = getRes.Content(&actualDoc)
	if err != nil {
		suite.T().Fatalf("Getting content errored: %v", err)
	}

	expectedDoc := fishBeerDocument{
		Fish: []string{"carp", "eel", "stonefish", "goldfish", "whaleshark", "catfish", "clownfish", "manta ray", "stingray"},
	}

	if len(expectedDoc.Fish) != len(actualDoc.Fish) {
		suite.T().Fatalf("results did not match, expected %v but was %v", expectedDoc, actualDoc)
	}
	for i, fish := range expectedDoc.Fish {
		if fish != actualDoc.Fish[i] {
			suite.T().Fatalf("results did not match, expected %s at index %d but was %s", fish, i, actualDoc.Fish[i])
		}
	}
}

func (suite *IntegrationTestSuite) TestInsertLookupInInsertGetFull() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(SubdocFeature)
	suite.skipIfUnsupported(XattrFeature)

	docId := generateDocId("lookupDocGetFull")

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	subRes, err := globalCollection.MutateIn(docId, []MutateInSpec{
		InsertSpec("xattrpath", "xattrvalue", &InsertSpecOptions{IsXattr: true}),
		ReplaceSpec("", doc, nil),
	}, &MutateInOptions{StoreSemantic: StoreSemanticsUpsert, Expiry: 20 * time.Second})
	if err != nil {
		suite.T().Fatalf("MutateIn failed, error was %v", err)
	}

	if subRes.Cas() == 0 {
		suite.T().Fatalf("MutateIn CAS was 0")
	}

	result, err := globalCollection.LookupIn(docId, []LookupInSpec{
		GetSpec("$document.exptime", &GetSpecOptions{IsXattr: true}),
		GetSpec("", nil),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var exptime int
	err = result.ContentAt(0, &exptime)
	if err != nil {
		suite.T().Fatalf("Failed to get expiry from LookupInResult, %v", err)
	}

	if exptime == 0 {
		suite.T().Fatalf("Expected expiry to be non zero")
	}

	var actualDoc testBeerDocument
	err = result.ContentAt(1, &actualDoc)
	if err != nil {
		suite.T().Fatalf("Failed to get name from LookupInResult, %v", err)
	}

	if actualDoc != doc {
		suite.T().Fatalf("Expected doc to be %v but was %v", doc, actualDoc)
	}
}

func (suite *IntegrationTestSuite) TestMutateInLookupInCounters() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(SubdocFeature)

	docId := generateDocId("mutateInLookupInCounters")

	doc := struct {
		Counter int `json:"counter"`
	}{
		Counter: 20,
	}

	mutRes, err := globalCollection.Insert(docId, doc, nil)
	if err != nil {
		suite.T().Fatalf("Insert failed, error: %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	subRes, err := globalCollection.MutateIn(docId, []MutateInSpec{
		IncrementSpec("counter", 10, nil),
		DecrementSpec("counter", 5, nil),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Increment failed, error was %v", err)
	}

	if subRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	result, err := globalCollection.LookupIn(docId, []LookupInSpec{
		GetSpec("counter", nil),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var counter int
	err = result.ContentAt(0, &counter)
	if err != nil {
		suite.T().Fatalf("Failed to get counter from LookupInResult, %v", err)
	}

	if counter != 25 {
		suite.T().Fatalf("Expected counter to be 25 but was %d", counter)
	}
}

func (suite *IntegrationTestSuite) TestMutateInLookupInMacro() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(SubdocFeature)
	suite.skipIfUnsupported(ExpandMacrosFeature)

	docId := generateDocId("mutateInInsertMacro")

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert(docId, doc, nil)
	if err != nil {
		suite.T().Fatalf("Insert failed, error: %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	subRes, err := globalCollection.MutateIn(docId, []MutateInSpec{
		InsertSpec("caspath", MutationMacroCAS, nil),
	}, nil)
	if err != nil {
		suite.T().Fatalf("MutateIn failed, error was %v", err)
	}

	if subRes.Cas() == 0 {
		suite.T().Fatalf("MutateIn CAS was 0")
	}

	result, err := globalCollection.LookupIn(docId, []LookupInSpec{
		GetSpec("caspath", &GetSpecOptions{IsXattr: true}),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var caspath string
	err = result.ContentAt(0, &caspath)
	if err != nil {
		suite.T().Fatalf("Failed to get caspath from LookupInResult, %v", err)
	}

	if !strings.HasPrefix(caspath, "0x") {
		suite.T().Fatalf("Expected caspath to start with 0x but was %s", caspath)
	}
}
func (suite *IntegrationTestSuite) TestSubdocNil() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(SubdocFeature)

	_, err := globalCollection.Upsert("nullvalues", struct{}{}, nil)
	suite.Require().Nil(err, err)

	mutateOps := []MutateInSpec{
		InsertSpec("insert", nil, nil),
		UpsertSpec("upsert", nil, nil),
		ReplaceSpec("upsert", nil, nil),
		ArrayAppendSpec("array", nil, &ArrayAppendSpecOptions{
			CreatePath: true,
		}),
		ArrayPrependSpec("array", []interface{}{nil, nil, nil}, &ArrayPrependSpecOptions{
			HasMultiple: true,
		}),
		ArrayInsertSpec("array[1]", nil, nil),
	}

	mutRes, err := globalCollection.MutateIn("nullvalues", mutateOps, nil)
	suite.Require().Nil(err, err)

	suite.Assert().NotZero(mutRes.Cas())

	lookupOps := []LookupInSpec{
		GetSpec("insert", nil),
		GetSpec("upsert", nil),
		GetSpec("array", nil),
	}

	lookupRes, err := globalCollection.LookupIn("nullvalues", lookupOps, nil)
	suite.Require().Nil(err, err)

	suite.Assert().NotZero(lookupRes.Cas())

	var insertVal interface{}
	if suite.Assert().Nil(lookupRes.ContentAt(0, &insertVal)) {
		suite.Assert().Nil(insertVal)
	}

	var upsertVal interface{}
	if suite.Assert().Nil(lookupRes.ContentAt(1, &upsertVal)) {
		suite.Assert().Nil(upsertVal)
	}

	var arrayVal []interface{}
	if suite.Assert().Nil(lookupRes.ContentAt(2, &arrayVal)) {
		if suite.Assert().Len(arrayVal, 5) {
			suite.Assert().Nil(arrayVal[0], nil)
			suite.Assert().Nil(arrayVal[1], nil)
			suite.Assert().Nil(arrayVal[2], nil)
			suite.Assert().Nil(arrayVal[3], nil)
			suite.Assert().Nil(arrayVal[4], nil)
		}
	}

	mutateOps = []MutateInSpec{
		ReplaceSpec("", nil, nil),
	}

	mutRes, err = globalCollection.MutateIn("nullvalues", mutateOps, &MutateInOptions{})
	suite.Require().Nil(err, err)

	lookupOps = []LookupInSpec{
		GetSpec("", nil),
	}

	lookupRes, err = globalCollection.LookupIn("nullvalues", lookupOps, nil)
	suite.Require().Nil(err, err)

	var doc interface{}
	if suite.Assert().Nil(lookupRes.ContentAt(0, &doc)) {
		suite.Assert().Nil(doc)
	}
}

func (suite *IntegrationTestSuite) TestMutateInBlankPathRemove() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(SubdocFeature)
	suite.skipIfUnsupported(XattrFeature)

	doc := struct {
		Thing string `json:"thing"`
	}{
		Thing: "from the depths",
	}

	mutRes, err := globalCollection.Upsert("mutateInBlankPathRemove", doc, nil)
	suite.Require().Nil(err)

	suite.Assert().NotZero(mutRes.Cas())

	subRes, err := globalCollection.MutateIn("mutateInBlankPathRemove", []MutateInSpec{
		RemoveSpec("", nil),
	}, nil)
	suite.Require().Nil(err)

	suite.Assert().NotZero(subRes.Cas())

	_, err = globalCollection.Get("mutateInBlankPathRemove", nil)
	if !errors.Is(err, ErrDocumentNotFound) {
		suite.T().Fatalf("Expected error to be doc not found but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestPreserveExpiryMutateIn() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(XattrFeature)
	suite.skipIfUnsupported(PreserveExpiryFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	suite.Require().Nil(err, err)

	start := time.Now()
	mutRes, err := globalCollection.Upsert("preservettlmutatein", doc, &UpsertOptions{Expiry: 25 * time.Second})
	suite.Require().Nil(err, err)

	suite.Assert().NotZero(mutRes.Cas())

	mutInRes, err := globalCollection.MutateIn("preservettlmutatein", []MutateInSpec{
		UpsertSpec("test", "test", nil),
	}, &MutateInOptions{PreserveExpiry: true})
	suite.Require().Nil(err, err)

	suite.Assert().NotZero(mutInRes.Cas())

	mutatedDoc, err := globalCollection.Get("preservettlmutatein", &GetOptions{WithExpiry: true})
	suite.Require().Nil(err, err)

	suite.Assert().InDelta(start.Add(25*time.Second).Unix(), mutatedDoc.ExpiryTime().Unix(), 5)

	_, err = globalCollection.MutateIn("preservettlmutatein", []MutateInSpec{
		UpsertSpec("test", "test", nil),
	}, &MutateInOptions{PreserveExpiry: true, StoreSemantic: StoreSemanticsInsert})
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Expected invalid args error but was %v", err)
	}

	_, err = globalCollection.MutateIn("preservettlmutatein", []MutateInSpec{
		UpsertSpec("test", "test", nil),
	}, &MutateInOptions{PreserveExpiry: true, Expiry: 5 * time.Second})
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("Expected invalid args error but was %v", err)
	}
}

// GOCBC-1019: Due to a previous bug in gocbcore we need to convert cas mismatch back to exists.
func (suite *IntegrationTestSuite) TestCasMismatchConvertedToExists() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(SubdocFeature)

	docId := generateDocId("subdocCasMismatchDoc")

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert(docId, doc, &InsertOptions{Expiry: 10 * time.Second})
	suite.Require().Nil(err, err)

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	_, err = globalCollection.MutateIn(docId, []MutateInSpec{
		ReplaceSpec("brewery_id", "test", nil),
	}, &MutateInOptions{
		Cas: Cas(123),
	})
	if !errors.Is(err, ErrDocumentExists) {
		suite.T().Fatalf("Expected error to be exists but was: %v", err)
	}
}

func (suite *IntegrationTestSuite) TestLookupInBadComboConvertedToInvalidArgs() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(SubdocFeature)

	docId := uuid.NewString()

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	suite.Require().NoError(err, "Could not read test dataset")

	mutRes, err := globalCollection.Upsert(docId, doc, &UpsertOptions{Expiry: 10 * time.Second})
	suite.Require().Nil(err, err)

	suite.Assert().NotZero(mutRes.Cas())

	_, err = globalCollection.LookupIn(docId, []LookupInSpec{
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
		GetSpec("test", nil),
	}, nil)
	suite.Require().ErrorIs(err, ErrInvalidArgument)
}

func (suite *IntegrationTestSuite) TestBadComboConvertedToInvalidArgs() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(SubdocFeature)

	docId := uuid.NewString()

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	suite.Require().NoError(err, "Could not read test dataset")

	mutRes, err := globalCollection.Upsert(docId, doc, &UpsertOptions{Expiry: 10 * time.Second})
	suite.Require().Nil(err, err)

	suite.Assert().NotZero(mutRes.Cas())

	_, err = globalCollection.MutateIn(docId, []MutateInSpec{
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
		ReplaceSpec("brewery_id", "test", nil),
	}, &MutateInOptions{})
	suite.Require().ErrorIs(err, ErrInvalidArgument)
}

func (suite *IntegrationTestSuite) TestMutateInLookupInXattrs() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(SubdocFeature)
	suite.skipIfUnsupported(XattrFeature)

	type beerWithCountable struct {
		testBeerDocument
		Countable []string `json:"countable"`
	}
	var doc beerWithCountable
	err := loadJSONTestDataset("beer_sample_single", &doc.testBeerDocument)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	docId := generateDocId("lookupXattrDoc")

	_, err = globalCollection.Upsert(docId, doc, nil)
	suite.Require().Nil(err, err)

	doc.Countable = []string{"one", "two"}

	mutRes, err := globalCollection.MutateIn(docId, []MutateInSpec{
		InsertSpec("x.name", doc.Name, &InsertSpecOptions{IsXattr: true, CreatePath: true}),
		InsertSpec("x.description", "ddd", &InsertSpecOptions{IsXattr: true}),
		UpsertSpec("x.style", doc.Style, &UpsertSpecOptions{IsXattr: true}),
		UpsertSpec("x.countable", doc.Countable, &UpsertSpecOptions{IsXattr: true}),
		ReplaceSpec("x.description", doc.Description, &ReplaceSpecOptions{IsXattr: true}),
		InsertSpec("x.foo.bar", "ddd", &InsertSpecOptions{IsXattr: true, CreatePath: true}),
		UpsertSpec("x.foo.barbar", "barbar", &UpsertSpecOptions{IsXattr: true}),
		ReplaceSpec("x.foo.bar", "eee", &ReplaceSpecOptions{IsXattr: true}),
	}, nil)
	if err != nil {
		suite.T().Fatalf("MutateIn failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("MutateIn CAS was 0")
	}

	result, err := globalCollection.LookupIn(docId, []LookupInSpec{
		GetSpec("x.name", &GetSpecOptions{IsXattr: true}),
		GetSpec("x.description", &GetSpecOptions{IsXattr: true}),
		ExistsSpec("x.doesnt", &ExistsSpecOptions{IsXattr: true}),
		ExistsSpec("x.style", &ExistsSpecOptions{IsXattr: true}),
		GetSpec("x.doesntexist", &GetSpecOptions{IsXattr: true}),
		CountSpec("x.countable", &CountSpecOptions{IsXattr: true}),
		GetSpec("x.foo.bar", &GetSpecOptions{IsXattr: true}),
		GetSpec("x.foo.barbar", &GetSpecOptions{IsXattr: true}),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	if result.Exists(2) {
		suite.T().Fatalf("Expected doesnt field to not exist")
	}

	if !result.Exists(3) {
		suite.T().Fatalf("Expected style field to exist")
	}

	var name string
	err = result.ContentAt(0, &name)
	if err != nil {
		suite.T().Fatalf("Failed to get name from LookupInResult, %v", err)
	}

	if name != doc.Name {
		suite.T().Fatalf("Expected name to be %s but was %s", doc.Name, name)
	}

	var desc string
	err = result.ContentAt(1, &desc)
	if err != nil {
		suite.T().Fatalf("Failed to get description from LookupInResult, %v", err)
	}

	if desc != doc.Description {
		suite.T().Fatalf("Expected description to be %s but was %s", doc.Description, desc)
	}

	var idontexist string
	err = result.ContentAt(4, &idontexist)
	if err == nil {
		suite.T().Fatalf("Expected lookup on a non existent field to return error")
	}

	if !errors.Is(err, ErrPathNotFound) {
		suite.T().Fatalf("Expected error to be path not found but was %+v", err)
	}

	var count int
	err = result.ContentAt(5, &count)
	if err != nil {
		suite.T().Fatalf("Failed to get count from LookupInResult, %v", err)
	}

	if count != 2 {
		suite.T().Fatalf("LookupIn Result count should have be 2 but was %d", count)
	}

	var bar string
	err = result.ContentAt(6, &bar)
	if err != nil {
		suite.T().Fatalf("Failed to get description from LookupInResult, %v", err)
	}

	if bar != "eee" {
		suite.T().Fatalf("Expected description to be %s but was %s", doc.Description, desc)
	}

	var barbar string
	err = result.ContentAt(7, &barbar)
	if err != nil {
		suite.T().Fatalf("Failed to get description from LookupInResult, %v", err)
	}

	if barbar != "barbar" {
		suite.T().Fatalf("Expected description to be %s but was %s", doc.Description, desc)
	}
}

func (suite *IntegrationTestSuite) TestMutateInBasicArrayXattrs() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(SubdocFeature)

	docId := generateDocId("mutateInArrayXattr")

	_, err := globalCollection.Upsert(docId, "{}", nil)
	suite.Require().Nil(err, err)

	subRes, err := globalCollection.MutateIn(docId, []MutateInSpec{
		ArrayAppendSpec("array", "clownfish", &ArrayAppendSpecOptions{IsXattr: true, CreatePath: true}),
		ArrayPrependSpec("array", "whaleshark", &ArrayPrependSpecOptions{IsXattr: true}),
		ArrayInsertSpec("array[1]", "catfish", &ArrayInsertSpecOptions{IsXattr: true}),
		ArrayAppendSpec("array", []string{"manta ray", "stingray"}, &ArrayAppendSpecOptions{IsXattr: true, HasMultiple: true}),
		ArrayPrependSpec("array", []string{"carp", "goldfish"}, &ArrayPrependSpecOptions{IsXattr: true, HasMultiple: true}),
		ArrayInsertSpec("array[1]", []string{"eel", "stonefish"}, &ArrayInsertSpecOptions{IsXattr: true, HasMultiple: true}),
	}, nil)
	if err != nil {
		suite.T().Fatalf("MutateIn failed, error was %v", err)
	}

	if subRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	result, err := globalCollection.LookupIn(docId, []LookupInSpec{
		GetSpec("array", &GetSpecOptions{IsXattr: true}),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var actualDoc []string
	err = result.ContentAt(0, &actualDoc)
	if err != nil {
		suite.T().Fatalf("Failed to get actualDoc from LookupInResult, %v", err)
	}

	expectedDoc := []string{"carp", "eel", "stonefish", "goldfish", "whaleshark", "catfish", "clownfish", "manta ray", "stingray"}

	if len(expectedDoc) != len(actualDoc) {
		suite.T().Fatalf("results did not match, expected %v but was %v", expectedDoc, actualDoc)
	}
	for i, fish := range expectedDoc {
		if fish != actualDoc[i] {
			suite.T().Fatalf("results did not match, expected %s at index %d but was %s", fish, i, actualDoc[i])
		}
	}
}

func (suite *IntegrationTestSuite) TestMutateInLookupInCountersXattrs() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(SubdocFeature)

	docId := generateDocId("mutateInLookupInCountersXattrs")

	_, err := globalCollection.Upsert(docId, "{}", nil)
	suite.Require().Nil(err, err)

	subRes, err := globalCollection.MutateIn(docId, []MutateInSpec{
		InsertSpec("count", 10, &InsertSpecOptions{IsXattr: true}),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Increment failed, error was %v", err)
	}

	if subRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	subRes, err = globalCollection.MutateIn(docId, []MutateInSpec{
		DecrementSpec("count", 3, &CounterSpecOptions{IsXattr: true}),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Increment failed, error was %v", err)
	}

	if subRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	result, err := globalCollection.LookupIn(docId, []LookupInSpec{
		GetSpec("count", &GetSpecOptions{
			IsXattr: true,
		}),
	}, nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var counter int
	err = result.ContentAt(0, &counter)
	if err != nil {
		suite.T().Fatalf("Failed to get counter from LookupInResult, %v", err)
	}

	if counter != 7 {
		suite.T().Fatalf("Expected counter to be 25 but was %v", counter)
	}
}

func (suite *IntegrationTestSuite) TestUpsertReplicateToLookupInAnyReplica() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(ReplicasFeature)
	suite.skipIfUnsupported(SubdocReplicaReadsFeature)

	docId := generateDocId("insertReplicaLookupInDoc")

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	suite.Require().NoError(err, err)

	mutRes, err := globalCollection.Upsert(docId, doc, &UpsertOptions{
		PersistTo: 1,
		Timeout:   5 * time.Second,
	})
	suite.Require().NoError(err, err)
	suite.Assert().NotZero(mutRes.Cas())

	result, err := globalCollection.LookupInAnyReplica(docId, []LookupInSpec{
		GetSpec("name", nil),
	}, nil)
	suite.Require().NoError(err, err)

	var name string
	err = result.ContentAt(0, &name)
	suite.Require().NoError(err, err)

	suite.Assert().Equal(doc.Name, name)

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 2)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), true, DurabilityLevelNone)

	span := nilParents[1]
	suite.AssertKvSpan(span, "lookup_in_any_replica", DurabilityLevelNone)

	suite.Require().Equal(len(span.Spans), 1)
	suite.Require().Contains(span.Spans, "lookup_in_all_replicas")
	allReplicasSpans := span.Spans["lookup_in_all_replicas"]

	suite.Require().GreaterOrEqual(len(allReplicasSpans), 1)
	suite.Require().Contains(allReplicasSpans[0].Spans, "lookup_in")
	getReplicaSpans := allReplicasSpans[0].Spans["lookup_in"]
	suite.Require().GreaterOrEqual(len(getReplicaSpans), 2)
	// We don't actually know which of these will win.
	for _, span := range getReplicaSpans {
		suite.Require().Equal(1, len(span.Spans))
		suite.AssertKvSpan(span, "lookup_in", DurabilityLevelNone)
		// We don't know which span was actually cancelled so we don't check the CMD spans.
	}

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "lookup_in_any_replica", 1, false)

	// We can't reliably check the metrics for the get cmd spans, as we don't know which one will have won.
}

func (suite *IntegrationTestSuite) TestUpsertReplicateToGetAllReplicas() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(ReplicasFeature)
	suite.skipIfUnsupported(SubdocReplicaReadsFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	suite.Require().NoError(err, err)

	prov, err := globalCollection.getKvProvider()
	suite.Require().NoError(err, err)

	agent, ok := prov.(*kvProviderCore)
	suite.Require().True(ok)

	snapshot, err := agent.snapshotProvider.WaitForConfigSnapshot(context.Background(), time.Now().Add(5*time.Second))
	suite.Require().NoError(err, err)

	numReplicas, err := snapshot.NumReplicas()
	suite.Require().NoError(err, err)

	expectedReplicas := numReplicas + 1

	docID := uuid.NewString()[:6]
	mutRes, err := globalCollection.Upsert(docID, doc, &UpsertOptions{
		PersistTo: uint(expectedReplicas),
	})
	suite.Require().NoError(err, err)

	suite.Assert().NotZero(mutRes.Cas())

	stream, err := globalCollection.LookupInAllReplicas(docID, []LookupInSpec{
		GetSpec("name", nil),
	}, &LookupInAllReplicaOptions{
		Timeout: 25 * time.Second,
	})
	suite.Require().NoError(err, err)

	actualReplicas := 0
	numMasters := 0

	for {
		upsertedDoc := stream.Next()
		if upsertedDoc == nil {
			break
		}

		actualReplicas++

		if !upsertedDoc.IsReplica() {
			numMasters++
		}

		var name string
		err = upsertedDoc.ContentAt(0, &name)
		suite.Require().NoError(err, err)

		suite.Assert().Equal(doc.Name, name)
	}

	err = stream.Close()
	suite.Require().NoError(err, err)

	suite.Assert().Equal(expectedReplicas, actualReplicas)
	suite.Assert().Equal(1, numMasters)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "lookup_in_all_replicas", 1, false)
}

func (suite *IntegrationTestSuite) TestLookupInAnyReplicaKeyNotFound() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(ReplicasFeature)
	suite.skipIfUnsupported(SubdocReplicaReadsFeature)

	docId := uuid.NewString()[:6]

	_, err := globalCollection.LookupInAnyReplica(docId, []LookupInSpec{
		GetSpec("name", nil),
	}, nil)
	suite.Require().ErrorIs(err, ErrDocumentUnretrievable)
}
