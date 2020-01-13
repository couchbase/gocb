package gocb

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestErrorNonExistant(t *testing.T) {
	res, err := globalCollection.Get("doesnt-exist", nil)
	if err == nil {
		t.Fatalf("Expected error to be non-nil")
	}

	if res != nil {
		t.Fatalf("Expected result to be nil but was %v", res)
	}
}

func TestErrorDoubleInsert(t *testing.T) {
	_, err := globalCollection.Insert("doubleInsert", "test", nil)
	if err != nil {
		t.Fatalf("Expected error to be nil but was %v", err)
	}
	_, err = globalCollection.Insert("doubleInsert", "test", nil)
	if err == nil {
		t.Fatalf("Expected error to be non-nil")
	}

	if !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("Expected error to be DocumentExists but is %s", err)
	}
}

func TestInsertGetWithExpiry(t *testing.T) {
	if globalCluster.NotSupportsFeature(XattrFeature) {
		t.Skip("Skipping test as xattrs not supported.")
	}

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert("expiryDoc", doc, &InsertOptions{Expiry: 10})
	if err != nil {
		t.Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	insertedDoc, err := globalCollection.Get("expiryDoc", &GetOptions{WithExpiry: true})
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != insertedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}

	if *insertedDoc.Expiry() == 0 {
		t.Fatalf("Expected expiry value to be populated")
	}
}

func TestInsertGetProjection(t *testing.T) {
	type PersonDimensions struct {
		Height int `json:"height"`
		Weight int `json:"weight"`
	}
	type Location struct {
		Lat  float32 `json:"lat"`
		Long float32 `json:"long"`
	}
	type HobbyDetails struct {
		Location Location `json:"location"`
	}
	type PersonHobbies struct {
		Type    string       `json:"type"`
		Name    string       `json:"name"`
		Details HobbyDetails `json:"details,omitempty"`
	}
	type PersonAttributes struct {
		Hair       string           `json:"hair"`
		Dimensions PersonDimensions `json:"dimensions"`
		Hobbies    []PersonHobbies  `json:"hobbies"`
	}
	type Person struct {
		Name       string           `json:"name"`
		Age        int              `json:"age"`
		Animals    []string         `json:"animals"`
		Attributes PersonAttributes `json:"attributes"`
	}

	var person Person
	err := loadJSONTestDataset("projection_doc", &person)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("projectDoc", person, nil)
	if err != nil {
		t.Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	type tCase struct {
		name     string
		project  []string
		expected Person
	}

	testCases := []tCase{
		{
			name:     "string",
			project:  []string{"name"},
			expected: Person{Name: person.Name},
		},
		{
			name:     "int",
			project:  []string{"age"},
			expected: Person{Age: person.Age},
		},
		{
			name:     "array",
			project:  []string{"animals"},
			expected: Person{Animals: person.Animals},
		},
		{
			name:     "array-index1",
			project:  []string{"animals[0]"},
			expected: Person{Animals: []string{person.Animals[0]}},
		},
		{
			name:     "array-index2",
			project:  []string{"animals[1]"},
			expected: Person{Animals: []string{person.Animals[1]}},
		},
		{
			name:     "array-index3",
			project:  []string{"animals[2]"},
			expected: Person{Animals: []string{person.Animals[2]}},
		},
		{
			name:     "full-object-field",
			project:  []string{"attributes"},
			expected: Person{Attributes: person.Attributes},
		},
		{
			name:    "nested-object-field1",
			project: []string{"attributes.hair"},
			expected: Person{
				Attributes: PersonAttributes{
					Hair: person.Attributes.Hair,
				},
			},
		},
		{
			name:    "nested-object-field2",
			project: []string{"attributes.dimensions"},
			expected: Person{
				Attributes: PersonAttributes{
					Dimensions: person.Attributes.Dimensions,
				},
			},
		},
		{
			name:    "nested-object-field3",
			project: []string{"attributes.dimensions.height"},
			expected: Person{
				Attributes: PersonAttributes{
					Dimensions: PersonDimensions{
						Height: person.Attributes.Dimensions.Height,
					},
				},
			},
		},
		{
			name:    "nested-object-field3",
			project: []string{"attributes.dimensions.weight"},
			expected: Person{
				Attributes: PersonAttributes{
					Dimensions: PersonDimensions{
						Weight: person.Attributes.Dimensions.Weight,
					},
				},
			},
		},
		{
			name:    "nested-object-field4",
			project: []string{"attributes.hobbies"},
			expected: Person{
				Attributes: PersonAttributes{
					Hobbies: person.Attributes.Hobbies,
				},
			},
		},
		{
			name:    "nested-array-object-field1",
			project: []string{"attributes.hobbies[0].type"},
			expected: Person{
				Attributes: PersonAttributes{
					Hobbies: []PersonHobbies{
						PersonHobbies{
							Type: person.Attributes.Hobbies[0].Type,
						},
					},
				},
			},
		},
		{
			name:    "nested-array-object-field2",
			project: []string{"attributes.hobbies[1].type"},
			expected: Person{
				Attributes: PersonAttributes{
					Hobbies: []PersonHobbies{
						PersonHobbies{
							Type: person.Attributes.Hobbies[1].Type,
						},
					},
				},
			},
		},
		{
			name:    "nested-array-object-field3",
			project: []string{"attributes.hobbies[0].name"},
			expected: Person{
				Attributes: PersonAttributes{
					Hobbies: []PersonHobbies{
						PersonHobbies{
							Name: person.Attributes.Hobbies[0].Name,
						},
					},
				},
			},
		},
		{
			name:    "nested-array-object-field4",
			project: []string{"attributes.hobbies[1].name"},
			expected: Person{
				Attributes: PersonAttributes{
					Hobbies: []PersonHobbies{
						PersonHobbies{
							Name: person.Attributes.Hobbies[1].Name,
						},
					},
				},
			},
		},
		{
			name:    "nested-array-object-field5",
			project: []string{"attributes.hobbies[1].details"},
			expected: Person{
				Attributes: PersonAttributes{
					Hobbies: []PersonHobbies{
						PersonHobbies{
							Details: person.Attributes.Hobbies[1].Details,
						},
					},
				},
			},
		},
	}

	// a bug in Mock prevents these cases from working correctly
	if globalCluster.SupportsFeature(SubdocMockBugFeature) {
		testCases = append(testCases,
			tCase{
				name:    "nested-array-object-nested-field1",
				project: []string{"attributes.hobbies[1].details.location"},
				expected: Person{
					Attributes: PersonAttributes{
						Hobbies: []PersonHobbies{
							PersonHobbies{
								Details: HobbyDetails{
									Location: person.Attributes.Hobbies[1].Details.Location,
								},
							},
						},
					},
				},
			},
			tCase{
				name:    "nested-array-object-nested-nested-field1",
				project: []string{"attributes.hobbies[1].details.location.lat"},
				expected: Person{
					Attributes: PersonAttributes{
						Hobbies: []PersonHobbies{
							PersonHobbies{
								Details: HobbyDetails{
									Location: Location{
										Lat: person.Attributes.Hobbies[1].Details.Location.Lat,
									},
								},
							},
						},
					},
				},
			},
			tCase{
				name:    "nested-array-object-nested-nested-field2",
				project: []string{"attributes.hobbies[1].details.location.long"},
				expected: Person{
					Attributes: PersonAttributes{
						Hobbies: []PersonHobbies{
							PersonHobbies{
								Details: HobbyDetails{
									Location: Location{
										Long: person.Attributes.Hobbies[1].Details.Location.Long,
									},
								},
							},
						},
					},
				},
			},
		)
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			doc, err := globalCollection.Get("projectDoc", &GetOptions{
				Project: testCase.project,
			})
			if err != nil {
				t.Fatalf("Get failed, error was %v", err)
			}

			var actual Person
			err = doc.Content(&actual)
			if err != nil {
				t.Fatalf("Content failed, error was %v", err)
			}

			if !reflect.DeepEqual(actual, testCase.expected) {
				t.Fatalf("Projection failed, expected %+v but was %+v", testCase.expected, actual)
			}
		})
	}
}

func TestInsertGetProjection18Fields(t *testing.T) {
	type docType struct {
		Field1  int `json:"field1"`
		Field2  int `json:"field2"`
		Field3  int `json:"field3"`
		Field4  int `json:"field4"`
		Field5  int `json:"field5"`
		Field6  int `json:"field6"`
		Field7  int `json:"field7"`
		Field8  int `json:"field8"`
		Field9  int `json:"field9"`
		Field10 int `json:"field10"`
		Field11 int `json:"field11"`
		Field12 int `json:"field12"`
		Field13 int `json:"field13"`
		Field14 int `json:"field14"`
		Field15 int `json:"field15"`
		Field16 int `json:"field16"`
		Field17 int `json:"field17"`
		Field18 int `json:"field18"`
	}
	doc := docType{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
	}

	mutRes, err := globalCollection.Insert("projectDocTooManyFields", doc, nil)
	if err != nil {
		t.Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	getDoc, err := globalCollection.Get("projectDocTooManyFields", &GetOptions{
		Project: []string{"field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9",
			"field10", "field11", "field12", "field13", "field14", "field15", "field16", "field17"},
	})
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var getDocContent map[string]interface{}
	err = getDoc.Content(&getDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	bytes, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal failed, error was %v", err)
	}

	var originalDocContent map[string]interface{}
	err = json.Unmarshal(bytes, &originalDocContent)
	if err != nil {
		t.Fatalf("Unmarshal failed, error was %v", err)
	}

	if len(getDocContent) != 17 {
		t.Fatalf("Expected doc content to have 17 fields, had %d", len(getDocContent))
	}

	if _, ok := getDocContent["field18"]; ok {
		t.Fatalf("Expected doc to not contain field18")
	}

	for k, v := range originalDocContent {
		if v != getDocContent[k] && k != "field18" {
			t.Fatalf("%s not equal, expected %d but was %d", k, v, originalDocContent[k])
		}
	}
}

func TestInsertGetProjection16FieldsExpiry(t *testing.T) {
	if globalCluster.NotSupportsFeature(XattrFeature) {
		t.Skip("Skipping test as xattrs not supported.")
	}

	type docType struct {
		Field1  int `json:"field1"`
		Field2  int `json:"field2"`
		Field3  int `json:"field3"`
		Field4  int `json:"field4"`
		Field5  int `json:"field5"`
		Field6  int `json:"field6"`
		Field7  int `json:"field7"`
		Field8  int `json:"field8"`
		Field9  int `json:"field9"`
		Field10 int `json:"field10"`
		Field11 int `json:"field11"`
		Field12 int `json:"field12"`
		Field13 int `json:"field13"`
		Field14 int `json:"field14"`
		Field15 int `json:"field15"`
		Field16 int `json:"field16"`
		Field17 int `json:"field17"`
		Field18 int `json:"field18"`
	}
	doc := docType{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
	}

	mutRes, err := globalCollection.Upsert("projectDocTooManyFieldsExpiry", doc, &UpsertOptions{
		Expiry: 60,
	})
	if err != nil {
		t.Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	insertedDoc, err := globalCollection.Get("projectDocTooManyFieldsExpiry", &GetOptions{
		Project: []string{"field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9",
			"field10", "field11", "field12", "field13", "field14", "field15", "field16"},
		WithExpiry: true,
	})
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var insertedDocContent map[string]interface{}
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	bytes, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal failed, error was %v", err)
	}

	var originalDocContent map[string]interface{}
	err = json.Unmarshal(bytes, &originalDocContent)
	if err != nil {
		t.Fatalf("Unmarshal failed, error was %v", err)
	}

	if len(insertedDocContent) != 16 {
		t.Fatalf("Expected doc content to have 16 fields, had %d", len(insertedDocContent))
	}

	for k, v := range originalDocContent {
		if v != originalDocContent[k] {
			t.Fatalf("%s not equal, expected %d but was %d", k, v, originalDocContent[k])
		}
	}

	if *insertedDoc.Expiry() == 0 {
		t.Fatalf("Expected expiry value to be populated")
	}
}

func TestInsertGetProjectionPathMissing(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert("projectMissingDoc", doc, nil)
	if err != nil {
		t.Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	_, err = globalCollection.Get("projectMissingDoc", &GetOptions{
		Project: []string{"name", "thisfielddoesntexist"},
	})
	if err == nil {
		t.Fatalf("Get should have failed")
	}
}

func TestInsertGet(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert("insertDoc", doc, nil)
	if err != nil {
		t.Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	insertedDoc, err := globalCollection.Get("insertDoc", nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != insertedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}
}

// Following test tests that if a collection is deleted and recreated midway through a set of operations
// then the operations will still succeed due to the cid being refreshed under the hood.
func TestCollectionRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	if globalCluster.NotSupportsFeature(CollectionsFeature) {
		t.Skip("Skipping test as collections not supported.")
	}

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	collectionName := "insertRetry"

	// cli := globalBucket.sb.getCachedClient()
	mgr := globalBucket.Collections()

	err = mgr.CreateCollection(CollectionSpec{ScopeName: "_default", Name: collectionName}, nil)
	if err != nil {
		t.Fatalf("Could not create collection: %v", err)
	}

	err = waitForCollection(globalBucket, collectionName)
	if err != nil {
		t.Fatalf("Failed waiting for collection: %v", err)
	}

	col := globalBucket.Collection(collectionName)

	// Make sure we've connected to the collection ok
	mutRes, err := col.Upsert("insertRetryDoc", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	// The following delete and create will recreate a collection with the same name but a different cid.
	err = mgr.DropCollection(CollectionSpec{ScopeName: "_default", Name: collectionName}, nil)
	if err != nil {
		t.Fatalf("Could not drop collection: %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{ScopeName: "_default", Name: collectionName}, nil)
	if err != nil {
		t.Fatalf("Could not create collection: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// We've wiped the collection so we need to recreate this doc
	mutRes, err = col.Upsert("insertRetryDoc", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	insertedDoc, err := col.Get("insertRetryDoc", nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != insertedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}
}

func TestUpsertGetRemove(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("upsertDoc", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	upsertedDoc, err := globalCollection.Get("upsertDoc", nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var upsertedDocContent testBeerDocument
	err = upsertedDoc.Content(&upsertedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != upsertedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, upsertedDocContent)
	}

	existsRes, err := globalCollection.Exists("upsertDoc", nil)
	if err != nil {
		t.Fatalf("Exists failed, error was %v", err)
	}

	if !existsRes.Exists() {
		t.Fatalf("Expected exists to return true")
	}

	_, err = globalCollection.Remove("upsertDoc", nil)
	if err != nil {
		t.Fatalf("Remove failed, error was %v", err)
	}

	existsRes, err = globalCollection.Exists("upsertDoc", nil)
	if err != nil {
		t.Fatalf("Exists failed, error was %v", err)
	}

	if existsRes.Exists() {
		t.Fatalf("Expected exists to return false")
	}
}

type upsertRetriesStrategy struct {
	retries int
}

func (rts *upsertRetriesStrategy) RetryAfter(req RetryRequest, reason RetryReason) RetryAction {
	rts.retries++
	return &WithDurationRetryAction{100 * time.Millisecond}
}

func TestUpsertRetries(t *testing.T) {
	if testing.Short() {
		t.Log("Skipping test in short mode")
		return
	}

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("getRetryDoc", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	_, err = globalCollection.GetAndLock("getRetryDoc", 1*time.Second, nil)
	if err != nil {
		t.Fatalf("GetAndLock failed, error was %v", err)
	}

	retryStrategy := &upsertRetriesStrategy{}
	mutRes, err = globalCollection.Upsert("getRetryDoc", doc, &UpsertOptions{
		Timeout:       2100 * time.Millisecond, // Timeout has to be long due to how the server handles unlocking.
		RetryStrategy: retryStrategy,
	})
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	if retryStrategy.retries <= 1 {
		t.Fatalf("Expected retries to be > 1")
	}
}

func TestRemoveWithCas(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("removeWithCas", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	existsRes, err := globalCollection.Exists("removeWithCas", nil)
	if err != nil {
		t.Fatalf("Exists failed, error was %v", err)
	}

	if !existsRes.Exists() {
		t.Fatalf("Expected exists to return true")
	}

	_, err = globalCollection.Remove("removeWithCas", &RemoveOptions{Cas: mutRes.Cas() + 0xFECA})
	if err == nil {
		t.Fatalf("Expected remove to fail")
	}

	if !errors.Is(err, ErrCasMismatch) {
		t.Fatalf("Expected error to be CasMismatch but is %s", err)
	}

	existsRes, err = globalCollection.Exists("removeWithCas", nil)
	if err != nil {
		t.Fatalf("Exists failed, error was %v", err)
	}

	if !existsRes.Exists() {
		t.Fatalf("Expected exists to return true")
	}

	_, err = globalCollection.Remove("removeWithCas", &RemoveOptions{Cas: mutRes.Cas()})
	if err != nil {
		t.Fatalf("Remove failed, error was %v", err)
	}

	existsRes, err = globalCollection.Exists("removeWithCas", nil)
	if err != nil {
		t.Fatalf("Exists failed, error was %v", err)
	}

	if existsRes.Exists() {
		t.Fatalf("Expected exists to return false")
	}
}

func TestUpsertAndReplace(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("upsertAndReplace", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	insertedDoc, err := globalCollection.Get("upsertAndReplace", nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != insertedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}

	doc.Name = "replaced"
	mutRes, err = globalCollection.Replace("upsertAndReplace", doc, &ReplaceOptions{Cas: mutRes.Cas()})
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	replacedDoc, err := globalCollection.Get("upsertAndReplace", nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var replacedDocContent testBeerDocument
	err = replacedDoc.Content(&replacedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != replacedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}
}

func TestGetAndTouch(t *testing.T) {
	if globalCluster.NotSupportsFeature(XattrFeature) {
		t.Skip("Skipping test as xattrs not supported.")
	}

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("getAndTouch", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndTouch("getAndTouch", 10, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	expireDoc, err := globalCollection.Get("getAndTouch", &GetOptions{WithExpiry: true})
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	if *expireDoc.Expiry() == 0 {
		t.Fatalf("Expected doc to have an expiry > 0, was %d", expireDoc.Expiry())
	}

	var expireDocContent testBeerDocument
	err = expireDoc.Content(&expireDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != expireDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}
}

func TestGetAndLock(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("getAndLock", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndLock("getAndLock", 1*time.Second, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	mutRes, err = globalCollection.Upsert("getAndLock", doc, &UpsertOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	})
	if err == nil {
		t.Fatalf("Expected error but was nil")
	}

	if !errors.Is(err, ErrDocumentLocked) {
		t.Fatalf("Expected error to be DocumentLocked but is %s", err)
	}

	globalCluster.TimeTravel(2000 * time.Millisecond)

	mutRes, err = globalCollection.Upsert("getAndLock", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}
}

func TestUnlock(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("unlock", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndLock("unlock", 1, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	_, err = globalCollection.Unlock("unlock", lockedDoc.Cas(), nil)
	if err != nil {
		t.Fatalf("Unlock failed, error was %v", err)
	}

	mutRes, err = globalCollection.Upsert("unlock", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}
}

func TestUnlockInvalidCas(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("unlockInvalidCas", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndLock("unlockInvalidCas", 2, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	_, err = globalCollection.Unlock("unlockInvalidCas", lockedDoc.Cas()+1, &UnlockOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	})
	if err == nil {
		t.Fatalf("Unlock should have failed")
	}

	// The server and the mock do not agree on the error for locked documents.
	if !errors.Is(err, ErrDocumentLocked) && !errors.Is(err, ErrTemporaryFailure) {
		t.Fatalf("Expected error to be DocumentLocked or TemporaryFailure but was %s", err)
	}

	_, err = globalCollection.Unlock("unlockInvalidCas", lockedDoc.Cas()+1, &UnlockOptions{
		RetryStrategy: NewBestEffortRetryStrategy(nil),
		Timeout:       10 * time.Millisecond,
	})
	if err == nil {
		t.Fatalf("Unlock should have failed")
	}

	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("Expected error to be TimeoutError but was %s", err)
	}
}

func TestDoubleLockFail(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("doubleLock", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndLock("doubleLock", 1, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	_, err = globalCollection.GetAndLock("doubleLock", 1, &GetAndLockOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	})
	if err == nil {
		t.Fatalf("Expected GetAndLock to fail")
	}

	// The server and the mock do not agree on the error for locked documents.
	if !errors.Is(err, ErrDocumentLocked) && !errors.Is(err, ErrTemporaryFailure) {
		t.Fatalf("Expected error to be DocumentLocked or TemporaryFailure but was %s", err)
	}
}

func TestUnlockMissingDocFail(t *testing.T) {
	_, err := globalCollection.Unlock("unlockMissing", 123, nil)
	if err == nil {
		t.Fatalf("Expected Unlock to fail")
	}

	if !errors.Is(err, ErrDocumentNotFound) {
		t.Fatalf("Expected error to be DocumentNotFound but was %v", err)
	}
}

func TestTouch(t *testing.T) {
	if globalCluster.NotSupportsFeature(XattrFeature) {
		t.Skip("Skipping test as xattrs not supported.")
	}

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("touch", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndTouch("touch", 2, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	globalCluster.TimeTravel(1 * time.Second)

	touchOut, err := globalCollection.Touch("touch", 3, nil)
	if err != nil {
		t.Fatalf("Touch failed, error was %v", err)
	}

	if touchOut.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	globalCluster.TimeTravel(2 * time.Second)

	expireDoc, err := globalCollection.Get("touch", &GetOptions{WithExpiry: true})
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	if *expireDoc.Expiry() == 0 {
		t.Fatalf("Expected doc to have an expiry > 0, was %d", expireDoc.Expiry())
	}

	var expireDocContent testBeerDocument
	err = expireDoc.Content(&expireDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != expireDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}
}

func TestTouchMissingDocFail(t *testing.T) {
	_, err := globalCollection.Touch("touchMissing", 3, nil)
	if err == nil {
		t.Fatalf("Touch should have failed")
	}

	if !errors.Is(err, ErrDocumentNotFound) {
		t.Fatalf("Expected error to be KeyNotFoundError but was %v", err)
	}
}

func TestInsertReplicateToGetAnyReplica(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert("insertReplicaDoc", doc, &InsertOptions{
		PersistTo: 1,
	})
	if err != nil {
		t.Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	insertedDoc, err := globalCollection.GetAnyReplica("insertReplicaDoc", nil)
	if err != nil {
		t.Fatalf("GetFromReplica failed, error was %v", err)
	}

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != insertedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}
}

func TestInsertReplicateToGetAllReplicas(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("insertAllReplicaDoc", doc, &UpsertOptions{
		PersistTo: 1,
	})
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	stream, err := globalCollection.GetAllReplicas("insertAllReplicaDoc", &GetAllReplicaOptions{
		Timeout: 25 * time.Second,
	})
	if err != nil {
		t.Fatalf("GetAllReplicas failed, error was %v", err)
	}

	agent, err := globalCollection.getKvProvider()
	if err != nil {
		t.Fatalf("Failed to get kv provider, was %v", err)
	}

	expectedReplicas := agent.NumReplicas() + 1
	actualReplicas := 0
	numMasters := 0

	for {
		insertedDoc := stream.Next()
		if insertedDoc == nil {
			break
		}

		actualReplicas++

		if !insertedDoc.IsReplica() {
			numMasters++
		}

		var insertedDocContent testBeerDocument
		err = insertedDoc.Content(&insertedDocContent)
		if err != nil {
			t.Fatalf("Content failed, error was %v", err)
		}

		if doc != insertedDocContent {
			t.Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
		}
	}

	err = stream.Close()
	if err != nil {
		t.Fatalf("Expected stream close to not error, was %v", err)
	}

	if expectedReplicas != actualReplicas {
		t.Fatalf("Expected replicas to be %d but was %d", expectedReplicas, actualReplicas)
	}

	if numMasters != 1 {
		t.Fatalf("Expected number of masters to be 1 but was %d", numMasters)
	}
}

func TestDurabilityGetFromAnyReplica(t *testing.T) {
	if !globalCluster.SupportsFeature(DurabilityFeature) {
		t.Skip("Skipping test as durability not supported")
	}

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	type CasResult interface {
		Cas() Cas
	}

	type tCase struct {
		name              string
		method            string
		args              []interface{}
		expectCas         bool
		expectedError     error
		expectKeyNotFound bool
	}

	testCases := []tCase{
		{
			name:   "insertDurabilityMajorityDoc",
			method: "Insert",
			args: []interface{}{"insertDurabilityMajorityDoc", doc, &InsertOptions{
				DurabilityLevel: DurabilityLevelMajority,
			}},
			expectCas:         true,
			expectedError:     nil,
			expectKeyNotFound: false,
		},
	}

	for _, tCase := range testCases {
		t.Run(tCase.name, func(te *testing.T) {
			args := make([]reflect.Value, len(tCase.args))
			for i := range tCase.args {
				args[i] = reflect.ValueOf(tCase.args[i])
			}

			retVals := reflect.ValueOf(globalCollection).MethodByName(tCase.method).Call(args)
			if len(retVals) != 2 {
				te.Fatalf("Method call should have returned 2 values but returned %d", len(retVals))
			}

			var retErr error
			if retVals[1].Interface() != nil {
				var ok bool
				retErr, ok = retVals[1].Interface().(error)
				if ok {
					if err != nil {
						te.Fatalf("Method call returned error: %v", err)
					}
				} else {
					te.Fatalf("Could not type assert second returned value to error")
				}
			}

			if retErr != tCase.expectedError {
				te.Fatalf("Expected error to be %v but was %v", tCase.expectedError, retErr)
			}

			if tCase.expectCas {
				if val, ok := retVals[0].Interface().(CasResult); ok {
					if val.Cas() == 0 {
						te.Fatalf("CAS value was 0")
					}
				} else {
					te.Fatalf("Could not assert result to CasResult type")
				}
			}

			_, err := globalCollection.GetAnyReplica(tCase.name, nil)
			if tCase.expectKeyNotFound {
				if !errors.Is(err, ErrDocumentNotFound) {
					t.Fatalf("Expected GetFromReplica to not find a key but got error %v", err)
				}
			} else {
				if err != nil {
					te.Fatalf("GetFromReplica failed, error was %v", err)
				}
			}
		})
	}
}

func TestGetErrorCollectionUnknown(t *testing.T) {
	var doc testBreweryDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not load dataset: %v", err)
	}

	provider := &mockKvProvider{
		err:   ErrCollectionNotFound,
		value: make([]byte, 0),
	}
	col := testGetCollection(t, provider)

	res, err := col.Get("getDocErrCollectionUnknown", nil)
	if err == nil {
		t.Fatalf("Get didn't error")
	}

	if res != nil {
		t.Fatalf("Result should have been nil")
	}

	if !errors.Is(err, ErrCollectionNotFound) {
		t.Fatalf("Error should have been collection missing but was %v", err)
	}
}
