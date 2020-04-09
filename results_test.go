package gocb

import (
	"encoding/json"
	"errors"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v9"
)

func (suite *UnitTestSuite) TestGetResultCas() {
	cas := Cas(10)
	res := GetResult{
		Result: Result{
			cas: cas,
		},
	}

	if res.Cas() != cas {
		suite.T().Fatalf("Cas value should have been %d but was %d", cas, res.Cas())
	}
}

func (suite *UnitTestSuite) TestGetResultHasExpiry() {
	res := GetResult{}

	if res.Expiry() != nil {
		suite.T().Fatalf("Expiry should have returned nil but returned %d", *res.Expiry())
	}

	expiry := 32 * time.Second
	res.expiry = &expiry

	if *res.Expiry() == 0 {
		suite.T().Fatalf("HasExpiry should have returned not 0")
	}
}

func (suite *UnitTestSuite) TestGetResultExpiry() {
	expiry := 10 * time.Second
	res := GetResult{
		expiry: &expiry,
	}

	if res.Expiry() == nil {
		suite.T().Fatalf("Expiry should have not returned nil")
	}

	if *res.Expiry() != 10*time.Second {
		suite.T().Fatalf("Expiry value should have been 10 but was %d", res.Expiry())
	}
}

func (suite *UnitTestSuite) TestGetResultContent() {
	dataset, err := loadRawTestDataset("beer_sample_single")
	if err != nil {
		suite.T().Fatalf("Failed to load dataset: %v", err)
	}

	var expected testBeerDocument
	err = json.Unmarshal(dataset, &expected)
	if err != nil {
		suite.T().Fatalf("Failed to unmarshal dataset: %v", err)
	}

	res := GetResult{
		contents:   dataset,
		transcoder: NewJSONTranscoder(),
	}

	var doc testBeerDocument
	err = res.Content(&doc)
	if err != nil {
		suite.T().Fatalf("Failed to get content: %v", err)
	}

	// expected := "512_brewing_company (512) Bruin North American Ale"
	if doc != expected {
		suite.T().Fatalf("Document value should have been %+v but was %+v", expected, doc)
	}
}

func (suite *UnitTestSuite) TestGetResultFromSubDoc() {
	ops := []LookupInSpec{
		{
			path: "id",
		},
		{
			path: "name",
		},
		{
			path: "address.house.number",
		},
	}

	results := &LookupInResult{
		contents: make([]lookupInPartial, 3),
	}

	var err error
	results.contents[0].data, err = json.Marshal("key")
	if err != nil {
		suite.T().Fatalf("Failed to marshal content: %v", err)
	}
	results.contents[1].data, err = json.Marshal("barry")
	if err != nil {
		suite.T().Fatalf("Failed to marshal content: %v", err)
	}
	results.contents[2].data, err = json.Marshal(11)
	if err != nil {
		suite.T().Fatalf("Failed to marshal content: %v", err)
	}

	type house struct {
		Number int `json:"number"`
	}
	type address struct {
		House house `json:"house"`
	}
	type person struct {
		ID      string
		Name    string
		Address address `json:"address"`
	}
	var doc person
	getResult := GetResult{transcoder: NewJSONTranscoder()}
	err = getResult.fromSubDoc(ops, results)
	if err != nil {
		suite.T().Fatalf("Failed to create result from subdoc: %v", err)
	}

	err = getResult.Content(&doc)
	if err != nil {
		suite.T().Fatalf("Failed to get content: %v", err)
	}

	if doc.ID != "key" {
		suite.T().Fatalf("Document value should have been %s but was %s", "key", doc.ID)
	}

	if doc.Name != "barry" {
		suite.T().Fatalf("Document value should have been %s but was %s", "barry", doc.ID)
	}

	if doc.Address.House.Number != 11 {
		suite.T().Fatalf("Document value should have been %d but was %d", 11, doc.Address.House.Number)
	}
}

func (suite *UnitTestSuite) TestLookupInResultCas() {
	cas := Cas(10)
	res := LookupInResult{
		Result: Result{
			cas: cas,
		},
	}

	if res.Cas() != cas {
		suite.T().Fatalf("Cas value should have been %d but was %d", cas, res.Cas())
	}
}

func (suite *UnitTestSuite) TestLookupInResultContentAt() {
	var dataset testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &dataset)
	if err != nil {
		suite.T().Fatalf("Failed to load dataset: %v", err)
	}

	contents1, err := json.Marshal(dataset.Name)
	if err != nil {
		suite.T().Fatalf("Failed to marshal data, %v", err)
	}

	contents2, err := json.Marshal(dataset.Description)
	if err != nil {
		suite.T().Fatalf("Failed to marshal data, %v", err)
	}

	type fakeBeer struct {
		Name string `json:"name"`
	}
	contentAsStruct := fakeBeer{
		"beer",
	}
	contents3, err := json.Marshal(contentAsStruct)
	if err != nil {
		suite.T().Fatalf("Failed to marshal data, %v", err)
	}

	res := LookupInResult{
		contents: []lookupInPartial{
			{
				data: contents1,
			},
			{
				data: contents2,
			},
			{
				data: contents3,
			},
		},
	}

	var name string
	err = res.ContentAt(0, &name)
	if err != nil {
		suite.T().Fatalf("Failed to get contentat: %v", err)
	}

	if name != dataset.Name {
		suite.T().Fatalf("Name value should have been %s but was %s", dataset.Name, name)
	}

	if !res.Exists(0) {
		suite.T().Fatalf("Content value at 0 should have existed but didn't")
	}

	var description string
	err = res.ContentAt(1, &description)
	if err != nil {
		suite.T().Fatalf("Failed to get contentat: %v", err)
	}

	if description != dataset.Description {
		suite.T().Fatalf("Name value should have been %s but was %s", dataset.Description, description)
	}

	if !res.Exists(1) {
		suite.T().Fatalf("Content value at 1 should have existed but didn't")
	}

	var fake fakeBeer
	err = res.ContentAt(2, &fake)
	if err != nil {
		suite.T().Fatalf("Failed to get contentat: %v", err)
	}

	if fake != contentAsStruct {
		suite.T().Fatalf("Struct value should have been %v but was %v", contentAsStruct, fake)
	}

	if !res.Exists(2) {
		suite.T().Fatalf("Decode value at 2 should have existed but didn't")
	}

	var shouldFail string
	err = res.ContentAt(3, &shouldFail)
	if !errors.Is(err, ErrInvalidArgument) {
		suite.T().Fatalf("ContentAt should have failed with InvalidIndexError, was %v", err)
	}

	if res.Exists(3) {
		suite.T().Fatalf("Content value at 3 shouldn't have existed")
	}
}

func (suite *UnitTestSuite) TestExistsResultCas() {
	cas := Cas(10)
	res := ExistsResult{
		Result: Result{
			cas: Cas(cas),
		},
	}

	if res.Cas() != cas {
		suite.T().Fatalf("Cas value should have been %d but was %d", cas, res.Cas())
	}
}

func (suite *UnitTestSuite) TestExistsResultNotFound() {
	res := ExistsResult{
		docExists: false,
	}

	if res.Exists() {
		suite.T().Fatalf("Expected result to not exist")
	}
}

func (suite *UnitTestSuite) TestExistsResultExists() {
	res := ExistsResult{
		docExists: true,
	}

	if !res.Exists() {
		suite.T().Fatalf("Expected result to exist")
	}
}

func (suite *UnitTestSuite) TestMutationResultCas() {
	cas := Cas(10)
	res := MutationResult{
		Result: Result{
			cas: Cas(cas),
		},
	}

	if res.Cas() != cas {
		suite.T().Fatalf("Cas value should have been %d but was %d", cas, res.Cas())
	}
}

func (suite *UnitTestSuite) TestMutationResultMutationToken() {
	token := &MutationToken{
		bucketName: "name",
		token:      gocbcore.MutationToken{},
	}
	res := MutationResult{
		mt: token,
	}

	if res.MutationToken() != token {
		suite.T().Fatalf("Token value should have been %v but was %v", token, res.MutationToken())
	}
}

func (suite *UnitTestSuite) TestCounterResultCas() {
	cas := Cas(10)
	res := CounterResult{
		MutationResult: MutationResult{
			Result: Result{
				cas: Cas(cas),
			},
		},
	}

	if res.Cas() != cas {
		suite.T().Fatalf("Cas value should have been %d but was %d", cas, res.Cas())
	}
}

func (suite *UnitTestSuite) TestCounterResultMutationToken() {
	token := &MutationToken{
		bucketName: "name",
		token:      gocbcore.MutationToken{},
	}
	res := CounterResult{
		MutationResult: MutationResult{
			mt: token,
		},
	}

	if res.MutationToken() != token {
		suite.T().Fatalf("Token value should have been %v but was %v", token, res.MutationToken())
	}
}

func (suite *UnitTestSuite) TestCounterResultContent() {
	res := CounterResult{
		content: 64,
	}

	if res.Content() != 64 {
		suite.T().Fatalf("Content value should have been %d but was %d", 64, res.Content())
	}
}

func (suite *UnitTestSuite) TestMutateInResultCas() {
	cas := Cas(10)
	res := MutateInResult{
		MutationResult: MutationResult{
			Result: Result{
				cas: Cas(cas),
			},
		},
	}

	if res.Cas() != cas {
		suite.T().Fatalf("Cas value should have been %d but was %d", cas, res.Cas())
	}
}

func (suite *UnitTestSuite) TestMutateInResultMutationToken() {
	token := &MutationToken{
		bucketName: "name",
		token:      gocbcore.MutationToken{},
	}
	res := MutateInResult{
		MutationResult: MutationResult{
			mt: token,
		},
	}

	if res.MutationToken() != token {
		suite.T().Fatalf("Token value should have been %v but was %v", token, res.MutationToken())
	}
}

func (suite *UnitTestSuite) TestMutateInResultContentAt() {
	results := &MutateInResult{
		contents: make([]mutateInPartial, 2),
	}

	var err error
	results.contents[0].data, err = json.Marshal(23)
	if err != nil {
		suite.T().Fatalf("Failed to marshal content: %v", err)
	}
	results.contents[1].data, err = json.Marshal(1)
	if err != nil {
		suite.T().Fatalf("Failed to marshal content: %v", err)
	}

	var count int
	err = results.ContentAt(0, &count)
	if err != nil {
		suite.T().Fatalf("Failed to get contentat: %v", err)
	}

	if count != 23 {
		suite.T().Fatalf("Expected count to be %d but was %d", 23, count)
	}

	err = results.ContentAt(1, &count)
	if err != nil {
		suite.T().Fatalf("Failed to get contentat: %v", err)
	}

	if count != 1 {
		suite.T().Fatalf("Expected count to be %d but was %d", 1, count)
	}
}
