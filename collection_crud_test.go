package gocb

import (
	"context"
	"reflect"
	"testing"
	"time"

	"gopkg.in/couchbase/gocbcore.v8"
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

	if !IsKeyExistsError(err) {
		t.Fatalf("Expected error to be KeyExistsError but is %s", reflect.TypeOf(err).String())
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

	mutRes, err := globalCollection.Insert("expiryDoc", doc, &InsertOptions{Expiration: 10})
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

	if !insertedDoc.HasExpiration() {
		t.Fatalf("Expected document to have an expiry")
	}

	if insertedDoc.Expiration() == 0 {
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
				Project: &ProjectOptions{
					Fields: testCase.project,
				},
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

func TestInsertGetProjection17Fields(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert("projectDocTooManyFields", doc, nil)
	if err != nil {
		t.Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	insertedDoc, err := globalCollection.Get("projectDocTooManyFields", &GetOptions{
		Project: &ProjectOptions{
			Fields: []string{"field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9",
				"field1", "field10", "field12", "field13", "field14", "field15", "field16", "field17"},
		},
	})
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if insertedDocContent != doc {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}
}

func TestInsertGetProjection16FieldsExpiry(t *testing.T) {
	if globalCluster.NotSupportsFeature(XattrFeature) {
		t.Skip("Skipping test as xattrs not supported.")
	}

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("projectDocTooManyFieldsExpiry", doc, &UpsertOptions{
		Expiration: 60,
	})
	if err != nil {
		t.Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	insertedDoc, err := globalCollection.Get("projectDocTooManyFieldsExpiry", &GetOptions{
		Project: &ProjectOptions{
			Fields: []string{"field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9",
				"field1", "field10", "field12", "field13", "field14", "field15", "field16"},
		},
		WithExpiry: true,
	})
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if insertedDocContent != doc {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}

	if !insertedDoc.HasExpiration() {
		t.Fatalf("Expected document to have an expiry")
	}

	if insertedDoc.Expiration() == 0 {
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
		Project: &ProjectOptions{
			Fields:                 []string{"name", "thisfielddoesntexist"},
			IgnorePathMissingError: false,
		},
	})
	if err == nil {
		t.Fatalf("Get should have failed")
	}
}

func TestInsertGetProjectionIgnorePathMissing(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert("projectIgnoreMissingDoc", doc, nil)
	if err != nil {
		t.Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	insertedDoc, err := globalCollection.Get("projectIgnoreMissingDoc", &GetOptions{
		Project: &ProjectOptions{
			Fields:                 []string{"name", "thisfielddoesntexist"},
			IgnorePathMissingError: true,
		},
	})
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	expectedDoc := testBeerDocument{
		Name: doc.Name,
	}

	if insertedDocContent != expectedDoc {
		t.Fatalf("Expected resulting doc to be %v but was %v", expectedDoc, insertedDocContent)
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

// func TestInsertGetRetryInsert(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip("Skipping test in short mode.")
// 	}
//
// 	if globalCluster.NotSupportsFeature(CollectionsFeature) {
// 		t.Skip("Skipping test as collections not supported.")
// 	}
//
// 	var doc testBeerDocument
// 	err := loadJSONTestDataset("beer_sample_single", &doc)
// 	if err != nil {
// 		t.Fatalf("Could not read test dataset: %v", err)
// 	}
//
// 	cli := globalBucket.sb.getCachedClient()
// 	_, err = testCreateCollection("insertRetry", "_default", globalBucket, cli)
// 	if err != nil {
// 		t.Fatalf("Failed to create collection, error was %v", err)
// 	}
// 	defer testDeleteCollection("insertRetry", "_default", globalBucket, cli, true)
//
// 	col := globalBucket.Collection("_default", "insertRetry", nil)
//
// 	_, err = testDeleteCollection("insertRetry", "_default", globalBucket, cli, true)
// 	if err != nil {
// 		t.Fatalf("Failed to delete collection, error was %v", err)
// 	}
//
// 	_, err = testCreateCollection("insertRetry", "_default", globalBucket, cli)
// 	if err != nil {
// 		t.Fatalf("Failed to create collection, error was %v", err)
// 	}
//
// 	mutRes, err := col.Insert("nonDefaultInsertDoc", doc, nil)
// 	if err != nil {
// 		t.Fatalf("Insert failed, error was %v", err)
// 	}
//
// 	if mutRes.Cas() == 0 {
// 		t.Fatalf("Insert CAS was 0")
// 	}
//
// 	insertedDoc, err := col.Get("nonDefaultInsertDoc", nil)
// 	if err != nil {
// 		t.Fatalf("Get failed, error was %v", err)
// 	}
//
// 	var insertedDocContent testBeerDocument
// 	err = insertedDoc.Content(&insertedDocContent)
// 	if err != nil {
// 		t.Fatalf("Content failed, error was %v", err)
// 	}
//
// 	if doc != insertedDocContent {
// 		t.Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
// 	}
// }

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

	if mutRes.Cas() == 0 {
		t.Fatalf("Remove CAS was 0")
	}

	existsRes, err = globalCollection.Exists("upsertDoc", nil)
	if err != nil {
		t.Fatalf("Exists failed, error was %v", err)
	}

	if existsRes.Exists() {
		t.Fatalf("Expected exists to return false")
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

	if !IsKeyExistsError(err) {
		t.Fatalf("Expected error to be KeyExistsError but is %s", reflect.TypeOf(err).String())
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

	if !expireDoc.HasExpiration() {
		t.Fatalf("Expected doc to have an expiry")
	}

	if expireDoc.Expiration() == 0 {
		t.Fatalf("Expected doc to have an expiry > 0, was %d", expireDoc.Expiration())
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

	lockedDoc, err := globalCollection.GetAndLock("getAndLock", 1, nil)
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

	mutRes, err = globalCollection.Upsert("getAndLock", doc, nil)
	if err == nil {
		t.Fatalf("Expected error but was nil")
	}

	if !IsKeyExistsError(err) {
		t.Fatalf("Expected error to be KeyExistsError but is %s", reflect.TypeOf(err).String())
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

	_, err = globalCollection.Unlock("unlock", &UnlockOptions{Cas: lockedDoc.Cas()})
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

	lockedDoc, err := globalCollection.GetAndLock("unlockInvalidCas", 1, nil)
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

	_, err = globalCollection.Unlock("unlockInvalidCas", &UnlockOptions{Cas: lockedDoc.Cas() + 1})
	if err == nil {
		t.Fatalf("Unlock should have failed")
	}

	if !IsTempFailError(err) {
		t.Fatalf("Expected error to be TempFailError but was %s", reflect.TypeOf(err).String())
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

	_, err = globalCollection.GetAndLock("doubleLock", 1, nil)
	if err == nil {
		t.Fatalf("Expected GetAndLock to fail")
	}

	if !IsTempFailError(err) {
		t.Fatalf("Expected error to be TempFailError but was %v", err)
	}
}

func TestUnlockMissingDocFail(t *testing.T) {
	_, err := globalCollection.Unlock("unlockMissing", &UnlockOptions{Cas: 123})
	if err == nil {
		t.Fatalf("Expected Unlock to fail")
	}

	if !IsKeyNotFoundError(err) {
		t.Fatalf("Expected error to be KeyNotFoundError but was %v", err)
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

	if !expireDoc.HasExpiration() {
		t.Fatalf("Expected doc to have an expiry")
	}

	if expireDoc.Expiration() == 0 {
		t.Fatalf("Expected doc to have an expiry > 0, was %d", expireDoc.Expiration())
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

	if !IsKeyNotFoundError(err) {
		t.Fatalf("Expected error to be KeyNotFoundError but was %v", err)
	}
}

func TestInsertReplicateToGetFromReplica(t *testing.T) {
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

	insertedDoc, err := globalCollection.GetFromReplica("insertReplicaDoc", 1, nil)
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

func TestInsertReplicateToGetFromAnyReplica(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert("insertAnyReplicaDoc", doc, &InsertOptions{
		PersistTo: 1,
	})
	if err != nil {
		t.Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	insertedDoc, err := globalCollection.GetFromReplica("insertAnyReplicaDoc", 0, nil)
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

// In this test it is expected that the operation will timeout and ctx.Err() will be DeadlineExceeded.
func TestGetContextTimeout1(t *testing.T) {
	var doc testBreweryDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not load dataset: %v", err)
	}

	provider := &mockKvOperator{
		cas:                   gocbcore.Cas(0),
		datatype:              1,
		value:                 nil,
		opWait:                3000 * time.Millisecond,
		opCancellationSuccess: true,
	}
	col := testGetCollection(t, provider)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()
	opts := GetOptions{Context: ctx, Timeout: 2000 * time.Millisecond}
	_, err = col.Get("getDocTimeout", &opts)
	if err == nil {
		t.Fatalf("Get succeeded, should have timedout")
	}

	if !IsTimeoutError(err) {
		t.Fatalf("Error should have been timeout error, was %s", reflect.TypeOf(err).Name())
	}

	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Error should have been DeadlineExceeded error but was %v", ctx.Err())
	}
}

// In this test it is expected that the operation will timeout but ctx.Err() will be nil as it is the timeout value
// that is hit.
func TestGetContextTimeout2(t *testing.T) {
	var doc testBreweryDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not load dataset: %v", err)
	}

	provider := &mockKvOperator{
		cas:                   gocbcore.Cas(0),
		datatype:              1,
		value:                 nil,
		opWait:                2000 * time.Millisecond,
		opCancellationSuccess: true,
	}
	col := testGetCollection(t, provider)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	opts := GetOptions{Context: ctx, Timeout: 2 * time.Millisecond}
	_, err = col.Get("getDocTimeout", &opts)
	if err == nil {
		t.Fatalf("Insert succeeded, should have timedout")
	}

	if !IsTimeoutError(err) {
		t.Fatalf("Error should have been timeout error, was %s", reflect.TypeOf(err).Name())
	}

	if ctx.Err() != nil {
		t.Fatalf("Context error should have been nil")
	}
}

func TestGetErrorCollectionUnknown(t *testing.T) {
	var doc testBreweryDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not load dataset: %v", err)
	}

	provider := &mockKvOperator{
		err: &gocbcore.KvError{Code: gocbcore.StatusCollectionUnknown},
	}
	col := testGetCollection(t, provider)

	res, err := col.Get("getDocErrCollectionUnknown", nil)
	if err == nil {
		t.Fatalf("Get didn't error")
	}

	if res != nil {
		t.Fatalf("Result should have been nil")
	}

	if !IsCollectionMissingError(err) {
		t.Fatalf("Error should have been collection missing but was %v", err)
	}
}

// In this test it is expected that the operation will timeout and ctx.Err() will be DeadlineExceeded.
func TestInsertContextTimeout1(t *testing.T) {
	provider := &mockKvOperator{
		cas:                   gocbcore.Cas(0),
		datatype:              1,
		value:                 nil,
		opWait:                3000 * time.Millisecond,
		opCancellationSuccess: true,
	}
	col := testGetCollection(t, provider)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()
	opts := InsertOptions{Context: ctx, Timeout: 2000 * time.Millisecond}
	_, err := col.Insert("insertDocTimeout", "test", &opts)
	if err == nil {
		t.Fatalf("Insert succeeded, should have timedout")
	}

	if !IsTimeoutError(err) {
		t.Fatalf("Error should have been timeout error, was %s", reflect.TypeOf(err).Name())
	}

	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("Error should have been DeadlineExceeded error but was %v", ctx.Err())
	}
}

// In this test it is expected that the operation will timeout but ctx.Err() will be nil as it is the timeout value
// that is hit.
func TestInsertContextTimeout2(t *testing.T) {
	provider := &mockKvOperator{
		cas:                   gocbcore.Cas(0),
		datatype:              1,
		value:                 nil,
		opWait:                2000 * time.Millisecond,
		opCancellationSuccess: true,
	}
	col := testGetCollection(t, provider)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	opts := InsertOptions{Context: ctx, Timeout: 2 * time.Millisecond}
	_, err := col.Insert("insertDocTimeout", "test", &opts)
	if err == nil {
		t.Fatalf("Insert succeeded, should have timedout")
	}

	if !IsTimeoutError(err) {
		t.Fatalf("Error should have been timeout error, was %s", reflect.TypeOf(err).Name())
	}

	if ctx.Err() != nil {
		t.Fatalf("Context error should have been nil")
	}
}

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

	spec := LookupInSpec{}
	result, err := globalCollection.LookupIn("lookupDoc", []LookupInOp{
		spec.Get("name"),
		spec.Get("description"),
		spec.Exists("doesnt"),
		spec.Exists("style"),
		spec.Get("doesntexist"),
		spec.Count("countable"),
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

	if !IsPathNotFoundError(err) {
		t.Fatalf("Expected error to be path not found but was %v", err)
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

func TestInsertLookupInXattr(t *testing.T) {
	if !globalCluster.SupportsFeature(XattrFeature) {
		t.Skip("Skipping test as xattrs not supported")
	}
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

	mutRes, err := globalCollection.Insert("lookupDocXattr", doc, &InsertOptions{Expiration: 20})
	if err != nil {
		t.Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	spec := LookupInSpec{}

	result, err := globalCollection.LookupIn("lookupDocXattr", []LookupInOp{
		spec.XAttr("$document.exptime"),
		spec.Get("name"),
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

	var name string
	err = result.ContentAt(1, &name)
	if err != nil {
		t.Fatalf("Failed to get name from LookupInResult, %v", err)
	}

	if name != doc.Name {
		t.Fatalf("Expected name to be %s but was %s", doc.Name, name)
	}
}

// TODO: MutateIn!
