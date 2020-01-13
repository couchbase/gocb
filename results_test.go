package gocb

import (
	"encoding/json"
	"errors"
	"testing"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

func TestGetResultCas(t *testing.T) {
	cas := Cas(10)
	res := GetResult{
		Result: Result{
			cas: cas,
		},
	}

	if res.Cas() != cas {
		t.Fatalf("Cas value should have been %d but was %d", cas, res.Cas())
	}
}

func TestGetResultHasExpiry(t *testing.T) {
	res := GetResult{}

	if res.Expiry() != nil {
		t.Fatalf("Expiry should have returned nil but returned %d", *res.Expiry())
	}

	expiry := uint32(32)
	res.expiry = &expiry

	if *res.Expiry() == 0 {
		t.Fatalf("HasExpiry should have returned not 0")
	}
}

func TestGetResultExpiry(t *testing.T) {
	expiry := uint32(10)
	res := GetResult{
		expiry: &expiry,
	}

	if res.Expiry() == nil {
		t.Fatalf("Expiry should have not returned nil")
	}

	if *res.Expiry() != 10 {
		t.Fatalf("Expiry value should have been 10 but was %d", res.Expiry())
	}
}

func TestGetResultContent(t *testing.T) {
	dataset, err := loadRawTestDataset("beer_sample_single")
	if err != nil {
		t.Fatalf("Failed to load dataset: %v", err)
	}

	var expected testBeerDocument
	err = json.Unmarshal(dataset, &expected)
	if err != nil {
		t.Fatalf("Failed to unmarshal dataset: %v", err)
	}

	res := GetResult{
		contents:   dataset,
		transcoder: NewJSONTranscoder(),
	}

	var doc testBeerDocument
	err = res.Content(&doc)
	if err != nil {
		t.Fatalf("Failed to get content: %v", err)
	}

	// expected := "512_brewing_company (512) Bruin North American Ale"
	if doc != expected {
		t.Fatalf("Document value should have been %+v but was %+v", expected, doc)
	}
}

func TestGetResultFromSubDoc(t *testing.T) {
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
		t.Fatalf("Failed to marshal content: %v", err)
	}
	results.contents[1].data, err = json.Marshal("barry")
	if err != nil {
		t.Fatalf("Failed to marshal content: %v", err)
	}
	results.contents[2].data, err = json.Marshal(11)
	if err != nil {
		t.Fatalf("Failed to marshal content: %v", err)
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
		t.Fatalf("Failed to create result from subdoc: %v", err)
	}

	err = getResult.Content(&doc)
	if err != nil {
		t.Fatalf("Failed to get content: %v", err)
	}

	if doc.ID != "key" {
		t.Fatalf("Document value should have been %s but was %s", "key", doc.ID)
	}

	if doc.Name != "barry" {
		t.Fatalf("Document value should have been %s but was %s", "barry", doc.ID)
	}

	if doc.Address.House.Number != 11 {
		t.Fatalf("Document value should have been %d but was %d", 11, doc.Address.House.Number)
	}
}

func TestLookupInResultCas(t *testing.T) {
	cas := Cas(10)
	res := LookupInResult{
		Result: Result{
			cas: cas,
		},
	}

	if res.Cas() != cas {
		t.Fatalf("Cas value should have been %d but was %d", cas, res.Cas())
	}
}

func TestLookupInResultContentAt(t *testing.T) {
	var dataset testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &dataset)
	if err != nil {
		t.Fatalf("Failed to load dataset: %v", err)
	}

	contents1, err := json.Marshal(dataset.Name)
	if err != nil {
		t.Fatalf("Failed to marshal data, %v", err)
	}

	contents2, err := json.Marshal(dataset.Description)
	if err != nil {
		t.Fatalf("Failed to marshal data, %v", err)
	}

	type fakeBeer struct {
		Name string `json:"name"`
	}
	contentAsStruct := fakeBeer{
		"beer",
	}
	contents3, err := json.Marshal(contentAsStruct)
	if err != nil {
		t.Fatalf("Failed to marshal data, %v", err)
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
		t.Fatalf("Failed to get contentat: %v", err)
	}

	if name != dataset.Name {
		t.Fatalf("Name value should have been %s but was %s", dataset.Name, name)
	}

	if !res.Exists(0) {
		t.Fatalf("Content value at 0 should have existed but didn't")
	}

	var description string
	err = res.ContentAt(1, &description)
	if err != nil {
		t.Fatalf("Failed to get contentat: %v", err)
	}

	if description != dataset.Description {
		t.Fatalf("Name value should have been %s but was %s", dataset.Description, description)
	}

	if !res.Exists(1) {
		t.Fatalf("Content value at 1 should have existed but didn't")
	}

	var fake fakeBeer
	err = res.ContentAt(2, &fake)
	if err != nil {
		t.Fatalf("Failed to get contentat: %v", err)
	}

	if fake != contentAsStruct {
		t.Fatalf("Struct value should have been %v but was %v", contentAsStruct, fake)
	}

	if !res.Exists(2) {
		t.Fatalf("Decode value at 2 should have existed but didn't")
	}

	var shouldFail string
	err = res.ContentAt(3, &shouldFail)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ContentAt should have failed with InvalidIndexError, was %v", err)
	}

	if res.Exists(3) {
		t.Fatalf("Content value at 3 shouldn't have existed")
	}
}

func TestExistsResultCas(t *testing.T) {
	cas := Cas(10)
	res := ExistsResult{
		Result: Result{
			cas: Cas(cas),
		},
	}

	if res.Cas() != cas {
		t.Fatalf("Cas value should have been %d but was %d", cas, res.Cas())
	}
}

func TestExistsResultNotFound(t *testing.T) {
	res := ExistsResult{
		docExists: false,
	}

	if res.Exists() {
		t.Fatalf("Expected result to not exist")
	}
}

func TestExistsResultExists(t *testing.T) {
	res := ExistsResult{
		docExists: true,
	}

	if !res.Exists() {
		t.Fatalf("Expected result to exist")
	}
}

func TestMutationResultCas(t *testing.T) {
	cas := Cas(10)
	res := MutationResult{
		Result: Result{
			cas: Cas(cas),
		},
	}

	if res.Cas() != cas {
		t.Fatalf("Cas value should have been %d but was %d", cas, res.Cas())
	}
}

func TestMutationResultMutationToken(t *testing.T) {
	token := &MutationToken{
		bucketName: "name",
		token:      gocbcore.MutationToken{},
	}
	res := MutationResult{
		mt: token,
	}

	if res.MutationToken() != token {
		t.Fatalf("Token value should have been %v but was %v", token, res.MutationToken())
	}
}

func TestCounterResultCas(t *testing.T) {
	cas := Cas(10)
	res := CounterResult{
		MutationResult: MutationResult{
			Result: Result{
				cas: Cas(cas),
			},
		},
	}

	if res.Cas() != cas {
		t.Fatalf("Cas value should have been %d but was %d", cas, res.Cas())
	}
}

func TestCounterResultMutationToken(t *testing.T) {
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
		t.Fatalf("Token value should have been %v but was %v", token, res.MutationToken())
	}
}

func TestCounterResultContent(t *testing.T) {
	res := CounterResult{
		content: 64,
	}

	if res.Content() != 64 {
		t.Fatalf("Content value should have been %d but was %d", 64, res.Content())
	}
}

func TestMutateInResultCas(t *testing.T) {
	cas := Cas(10)
	res := MutateInResult{
		MutationResult: MutationResult{
			Result: Result{
				cas: Cas(cas),
			},
		},
	}

	if res.Cas() != cas {
		t.Fatalf("Cas value should have been %d but was %d", cas, res.Cas())
	}
}

func TestMutateInResultMutationToken(t *testing.T) {
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
		t.Fatalf("Token value should have been %v but was %v", token, res.MutationToken())
	}
}

func TestMutateInResultContentAt(t *testing.T) {
	results := &MutateInResult{
		contents: make([]mutateInPartial, 2),
	}

	var err error
	results.contents[0].data, err = json.Marshal(23)
	if err != nil {
		t.Fatalf("Failed to marshal content: %v", err)
	}
	results.contents[1].data, err = json.Marshal(1)
	if err != nil {
		t.Fatalf("Failed to marshal content: %v", err)
	}

	var count int
	err = results.ContentAt(0, &count)
	if err != nil {
		t.Fatalf("Failed to get contentat: %v", err)
	}

	if count != 23 {
		t.Fatalf("Expected count to be %d but was %d", 23, count)
	}

	err = results.ContentAt(1, &count)
	if err != nil {
		t.Fatalf("Failed to get contentat: %v", err)
	}

	if count != 1 {
		t.Fatalf("Expected count to be %d but was %d", 1, count)
	}
}
