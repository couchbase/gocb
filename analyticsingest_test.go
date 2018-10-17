package gocb

import (
	"encoding/json"
	"errors"
	"testing"
)

type testIngestQueryRunner struct {
	data []interface{}
}

func (runner *testIngestQueryRunner) ExecuteQuery(bucket *Bucket, query *AnalyticsQuery, params []interface{}) (AnalyticsResults, error) {
	var rawMessages []json.RawMessage
	for _, doc := range runner.data {
		message, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}

		rawMessages = append(rawMessages, json.RawMessage(message))
	}
	return &analyticsResults{
		rows: &analyticsRows{
			index: -1,
			rows:  rawMessages,
		},
	}, nil
}

// This test currently mocks out the analytics query execution but does perform the ingest step
func TestAnalyticsIngest(t *testing.T) {
	var beers []testBeer
	err := loadTestDataset("beer", &beers)
	if err != nil {
		t.Fatal(err)
	}

	var iBeers []interface{}
	for _, beer := range beers {
		iBeers = append(iBeers, beer)
	}

	idGen := func(doc interface{}) (string, error) {
		beer, ok := doc.(testBeer)
		if !ok {
			return "", errors.New("doc was not a testBeer")
		}

		return beer.ID + beer.Type, nil
	}

	converter := func(docBytes []byte) (interface{}, error) {
		var doc testBeer
		err := json.Unmarshal(docBytes, &doc)
		if err != nil {
			return nil, err
		}
		return doc, nil
	}

	opts := DefaultAnalyticsIngestOptions().IdGenerator(idGen).IgnoreIngestError(false).DataConverter(converter)
	query := NewAnalyticsQuery("select id, name, `type` from allbeers")
	runner := &testIngestQueryRunner{data: iBeers}
	err = globalBucket.analyticsIngest(runner, query, nil, opts)
	if err != nil {
		t.Fatal(err)
	}

	for _, beer := range beers {
		var resBeer testBeer
		_, err := globalBucket.Get(beer.ID+beer.Type, &resBeer)
		if err != nil {
			t.Logf("Failed to find %s: %s", beer.ID+beer.Type, err.Error())
			t.Fail()
			continue
		}

		if beer.ID != resBeer.ID {
			t.Logf("Expected %s but got %s", beer.ID, resBeer.ID)
			t.Fail()
		}

		if beer.Name != resBeer.Name {
			t.Logf("Expected %s but got %s", beer.Name, resBeer.Name)
			t.Fail()
		}

		if beer.Type != resBeer.Type {
			t.Logf("Expected %s but got %s", beer.Type, resBeer.Type)
			t.Fail()
		}
	}
}

func TestAnalyticsIngestDefaultConverter(t *testing.T) {
	var beers []testBeer
	err := loadTestDataset("beer", &beers)
	if err != nil {
		t.Fatal(err)
	}

	var iBeers []interface{}
	for _, beer := range beers {
		iBeers = append(iBeers, beer)
	}

	createdBeers := make(map[string]map[string]interface{})
	method := func(bucket *Bucket, key string, val interface{}) error {
		beer, ok := val.(map[string]interface{})
		if !ok {
			return errors.New("doc was not a map[string]interface{} in ingest method")
		}
		createdBeers[key] = beer

		return nil
	}

	idGen := func(doc interface{}) (string, error) {
		beer, ok := doc.(map[string]interface{})
		if !ok {
			return "", errors.New("doc was not a map[string]interface{} in id generator")
		}
		id := beer["id"].(string)
		beerName := beer["name"].(string)

		return id + beerName, nil
	}

	opts := DefaultAnalyticsIngestOptions().IdGenerator(idGen).IgnoreIngestError(false).IngestMethod(method)
	query := NewAnalyticsQuery("select id, name, `type` from allbeers")
	runner := &testIngestQueryRunner{data: iBeers}
	err = globalBucket.analyticsIngest(runner, query, nil, opts)
	if err != nil {
		t.Fatal(err)
	}

	for _, beer := range beers {
		resBeer, ok := createdBeers[beer.ID+beer.Name]
		if !ok {
			t.Logf("Failed to find %s: %s", beer.ID+beer.Name, err.Error())
			t.Fail()
			continue
		}

		if beer.ID != resBeer["id"].(string) {
			t.Logf("Expected %s but got %s", beer.ID, resBeer["id"].(string))
			t.Fail()
		}

		if beer.Name != resBeer["name"].(string) {
			t.Logf("Expected %s but got %s", beer.Name, resBeer["name"].(string))
			t.Fail()
		}

		if beer.Type != resBeer["type"].(string) {
			t.Logf("Expected %s but got %s", beer.Type, resBeer["type"].(string))
			t.Fail()
		}
	}
}

func TestAnalyticsIngestDoesntIgnoreErrorsIdGen(t *testing.T) {
	var beers []testBeer
	err := loadTestDataset("beer", &beers)
	if err != nil {
		t.Fatal(err)
	}

	var iBeers []interface{}
	for _, beer := range beers {
		iBeers = append(iBeers, beer)
	}

	method := func(bucket *Bucket, key string, val interface{}) error {
		return nil
	}

	idGen := func(doc interface{}) (string, error) {
		return "", errors.New("bad id")
	}

	opts := DefaultAnalyticsIngestOptions().IdGenerator(idGen).IgnoreIngestError(false).IngestMethod(method)
	query := NewAnalyticsQuery("select id, name, `type` from allbeers")
	runner := &testIngestQueryRunner{data: iBeers}
	err = globalBucket.analyticsIngest(runner, query, nil, opts)
	if err == nil {
		t.Fatal("Expected error but was nil")
	}
}

func TestAnalyticsIngestDoesntIgnoreErrorsConverter(t *testing.T) {
	var beers []testBeer
	err := loadTestDataset("beer", &beers)
	if err != nil {
		t.Fatal(err)
	}

	var iBeers []interface{}
	for _, beer := range beers {
		iBeers = append(iBeers, beer)
	}

	method := func(bucket *Bucket, key string, val interface{}) error {
		return nil
	}

	converter := func(docBytes []byte) (interface{}, error) {
		return "", errors.New("bad data")
	}

	opts := DefaultAnalyticsIngestOptions().DataConverter(converter).IgnoreIngestError(false).IngestMethod(method)
	query := NewAnalyticsQuery("select id, name, `type` from allbeers")
	runner := &testIngestQueryRunner{data: iBeers}
	err = globalBucket.analyticsIngest(runner, query, nil, opts)
	if err == nil {
		t.Fatal("Expected error but was nil")
	}
}

func TestAnalyticsIngestDoesntIgnoreErrorsIngestMethod(t *testing.T) {
	var beers []testBeer
	err := loadTestDataset("beer", &beers)
	if err != nil {
		t.Fatal(err)
	}

	var iBeers []interface{}
	for _, beer := range beers {
		iBeers = append(iBeers, beer)
	}

	method := func(bucket *Bucket, key string, val interface{}) error {
		return errors.New("ingest bad")
	}

	opts := DefaultAnalyticsIngestOptions().IgnoreIngestError(false).IngestMethod(method)
	query := NewAnalyticsQuery("select id, name, `type` from allbeers")
	runner := &testIngestQueryRunner{data: iBeers}
	err = globalBucket.analyticsIngest(runner, query, nil, opts)
	if err == nil {
		t.Fatal("Expected error but was nil")
	}
}

func TestAnalyticsIngestDoesIgnoreErrorsIdGen(t *testing.T) {
	var beers []testBeer
	err := loadTestDataset("beer", &beers)
	if err != nil {
		t.Fatal(err)
	}

	var iBeers []interface{}
	for _, beer := range beers {
		iBeers = append(iBeers, beer)
	}

	method := func(bucket *Bucket, key string, val interface{}) error {
		return nil
	}

	i := 0
	idGen := func(doc interface{}) (string, error) {
		i++
		return "", errors.New("bad id")
	}

	opts := DefaultAnalyticsIngestOptions().IdGenerator(idGen).IngestMethod(method)
	query := NewAnalyticsQuery("select id, name, `type` from allbeers")
	runner := &testIngestQueryRunner{data: iBeers}
	err = globalBucket.analyticsIngest(runner, query, nil, opts)
	if err != nil {
		t.Fatal(err)
	}

	if i != len(beers) {
		t.Fatalf("Expected converter to run %d times but ran %d times", len(beers), i)
	}
}

func TestAnalyticsIngestDoesIgnoreErrorsConverter(t *testing.T) {
	var beers []testBeer
	err := loadTestDataset("beer", &beers)
	if err != nil {
		t.Fatal(err)
	}

	var iBeers []interface{}
	for _, beer := range beers {
		iBeers = append(iBeers, beer)
	}

	method := func(bucket *Bucket, key string, val interface{}) error {
		return nil
	}

	i := 0
	converter := func(docBytes []byte) (interface{}, error) {
		i++
		return "", errors.New("bad data")
	}

	opts := DefaultAnalyticsIngestOptions().DataConverter(converter).IngestMethod(method)
	query := NewAnalyticsQuery("select id, name, `type` from allbeers")
	runner := &testIngestQueryRunner{data: iBeers}
	err = globalBucket.analyticsIngest(runner, query, nil, opts)
	if err != nil {
		t.Fatal(err)
	}

	if i != len(beers) {
		t.Fatalf("Expected converter to run %d times but ran %d times", len(beers), i)
	}
}

func TestAnalyticsIngestDoesIgnoreErrorsIngestMethod(t *testing.T) {
	var beers []testBeer
	err := loadTestDataset("beer", &beers)
	if err != nil {
		t.Fatal(err)
	}

	var iBeers []interface{}
	for _, beer := range beers {
		iBeers = append(iBeers, beer)
	}

	i := 0
	method := func(bucket *Bucket, key string, val interface{}) error {
		i++
		return errors.New("ingest bad")
	}

	opts := DefaultAnalyticsIngestOptions().IngestMethod(method)
	query := NewAnalyticsQuery("select id, name, `type` from allbeers")
	runner := &testIngestQueryRunner{data: iBeers}
	err = globalBucket.analyticsIngest(runner, query, nil, opts)
	if err != nil {
		t.Fatal(err)
	}

	if i != len(beers) {
		t.Fatalf("Expected converter to run %d times but ran %d times", len(beers), i)
	}
}

func TestUUIDIdGeneratorFunction(t *testing.T) {
	var doc interface{}
	id, err := UUIDIdGeneratorFunction(doc)
	if err != nil {
		t.Fatal(err)
	}

	if id == "" {
		t.Fatalf("Expected id to be not empty")
	}
}

func TestPassthroughDataConverterFunction(t *testing.T) {
	var beers []testBeer
	err := loadTestDataset("beer", &beers)
	if err != nil {
		t.Fatal(err)
	}

	beer := beers[0]
	beerBytes, err := json.Marshal(beer)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := PassthroughDataConverterFunction(beerBytes)
	if err != nil {
		t.Fatal(err)
	}

	resBeer, ok := doc.(map[string]interface{})
	if !ok {
		t.Fatal("doc was not a map[string]interface{}")
	}

	if beer.ID != resBeer["id"].(string) {
		t.Logf("Expected %s but got %s", beer.ID, resBeer["id"].(string))
		t.Fail()
	}

	if beer.Name != resBeer["name"].(string) {
		t.Logf("Expected %s but got %s", beer.Name, resBeer["name"].(string))
		t.Fail()
	}

	if beer.Type != resBeer["type"].(string) {
		t.Logf("Expected %s but got %s", beer.Type, resBeer["type"].(string))
		t.Fail()
	}
}
