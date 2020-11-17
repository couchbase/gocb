package gocb

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/couchbase/gocbcore/v9"
)

func (suite *IntegrationTestSuite) TestErrorNonExistant() {
	suite.skipIfUnsupported(KeyValueFeature)

	res, err := globalCollection.Get("doesnt-exist", nil)
	if err == nil {
		suite.T().Fatalf("Expected error to be non-nil")
	}

	if res != nil {
		suite.T().Fatalf("Expected result to be nil but was %v", res)
	}
}

func (suite *IntegrationTestSuite) TestErrorDoubleInsert() {
	suite.skipIfUnsupported(KeyValueFeature)

	_, err := globalCollection.Insert("doubleInsert", "test", nil)
	if err != nil {
		suite.T().Fatalf("Expected error to be nil but was %v", err)
	}
	_, err = globalCollection.Insert("doubleInsert", "test", nil)
	if err == nil {
		suite.T().Fatalf("Expected error to be non-nil")
	}

	if !errors.Is(err, ErrDocumentExists) {
		suite.T().Fatalf("Expected error to be DocumentExists but is %s", err)
	}
}

func (suite *IntegrationTestSuite) TestInsertGetWithExpiry() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(XattrFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	start := time.Now()
	mutRes, err := globalCollection.Insert("expiryDoc", doc, &InsertOptions{Expiry: 10 * time.Second})
	if err != nil {
		suite.T().Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	insertedDoc, err := globalCollection.Get("expiryDoc", &GetOptions{WithExpiry: true})
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}
	end := time.Now()

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != insertedDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}

	if suite.Assert().NotNil(insertedDoc.Expiry()) {
		suite.Assert().InDelta(end.Sub(start).Seconds(), insertedDoc.Expiry().Seconds(), float64(1*time.Second))
	}
	suite.Assert().InDelta(start.Add(10*time.Second).Second(), insertedDoc.ExpiryTime().Second(), float64(1*time.Second))
}

func (suite *IntegrationTestSuite) TestUpsertGetWithExpiryTranscoder() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(XattrFeature)

	b := []byte("abinarydocument")

	tcoder := NewRawBinaryTranscoder()

	start := time.Now()
	mutRes, err := globalCollection.Upsert("expiryTranscoderDoc", b, &UpsertOptions{
		Expiry:     10 * time.Second,
		Transcoder: tcoder,
	})
	if err != nil {
		suite.T().Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	insertedDoc, err := globalCollection.Get("expiryTranscoderDoc", &GetOptions{
		WithExpiry: true,
		Transcoder: tcoder,
	})
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}
	end := time.Now()

	var actualB []byte
	suite.Require().Nil(insertedDoc.Content(&actualB))
	suite.Assert().Equal(string(b), string(actualB))

	if suite.Assert().NotNil(insertedDoc.Expiry()) {
		suite.Assert().InDelta(end.Sub(start).Seconds(), insertedDoc.Expiry().Seconds(), float64(1*time.Second))
	}
	suite.Assert().InDelta(start.Add(10*time.Second).Second(), insertedDoc.ExpiryTime().Second(), float64(1*time.Second))
}

func (suite *IntegrationTestSuite) TestInsertGetProjection() {
	suite.skipIfUnsupported(KeyValueFeature)

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
	type Tracking struct {
		Locations [][]Location `json:"locations"`
		Raw       [][]float32  `json:"raw"`
	}
	type Person struct {
		Name       string           `json:"name"`
		Age        int              `json:"age"`
		Animals    []string         `json:"animals"`
		Attributes PersonAttributes `json:"attributes"`
		Tracking   Tracking         `json:"tracking"`
	}

	var person Person
	err := loadJSONTestDataset("projection_doc", &person)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("projectDoc", person, nil)
	if err != nil {
		suite.T().Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
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
						{
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
						{
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
						{
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
						{
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
						{
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
							{
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
							{
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
							{
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
			tCase{
				name:    "array-of-arrays-object",
				project: []string{"tracking.locations[1][1].lat"},
				expected: Person{
					Tracking: Tracking{
						Locations: [][]Location{
							{
								Location{
									Lat: person.Tracking.Locations[1][1].Lat,
								},
							},
						},
					},
				},
			},
			tCase{
				name:    "array-of-arrays-native",
				project: []string{"tracking.raw[1][1]"},
				expected: Person{
					Tracking: Tracking{
						Raw: [][]float32{
							{
								person.Tracking.Raw[1][1],
							},
						},
					},
				},
			},
		)
	}

	for _, testCase := range testCases {
		suite.T().Run(testCase.name, func(t *testing.T) {
			doc, err := globalCollection.Get("projectDoc", &GetOptions{
				Project: testCase.project,
			})
			if err != nil {
				suite.T().Fatalf("Get failed, error was %v", err)
			}

			var actual Person
			err = doc.Content(&actual)
			if err != nil {
				suite.T().Fatalf("Content failed, error was %v", err)
			}

			if !reflect.DeepEqual(actual, testCase.expected) {
				suite.T().Fatalf("Projection failed, expected %+v but was %+v", testCase.expected, actual)
			}
		})
	}
}

func (suite *IntegrationTestSuite) TestInsertGetProjection18Fields() {
	suite.skipIfUnsupported(KeyValueFeature)

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
		suite.T().Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	getDoc, err := globalCollection.Get("projectDocTooManyFields", &GetOptions{
		Project: []string{"field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9",
			"field10", "field11", "field12", "field13", "field14", "field15", "field16", "field17"},
	})
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var getDocContent map[string]interface{}
	err = getDoc.Content(&getDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	bytes, err := json.Marshal(doc)
	if err != nil {
		suite.T().Fatalf("Marshal failed, error was %v", err)
	}

	var originalDocContent map[string]interface{}
	err = json.Unmarshal(bytes, &originalDocContent)
	if err != nil {
		suite.T().Fatalf("Unmarshal failed, error was %v", err)
	}

	if len(getDocContent) != 17 {
		suite.T().Fatalf("Expected doc content to have 17 fields, had %d", len(getDocContent))
	}

	if _, ok := getDocContent["field18"]; ok {
		suite.T().Fatalf("Expected doc to not contain field18")
	}

	for k, v := range originalDocContent {
		if v != getDocContent[k] && k != "field18" {
			suite.T().Fatalf("%s not equal, expected %d but was %d", k, v, originalDocContent[k])
		}
	}
}

func (suite *IntegrationTestSuite) TestInsertGetProjection16FieldsExpiry() {
	suite.skipIfUnsupported(XattrFeature)
	suite.skipIfUnsupported(KeyValueFeature)

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

	start := time.Now()
	mutRes, err := globalCollection.Upsert("projectDocTooManyFieldsExpiry", doc, &UpsertOptions{
		Expiry: 60 * time.Second,
	})
	if err != nil {
		suite.T().Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	insertedDoc, err := globalCollection.Get("projectDocTooManyFieldsExpiry", &GetOptions{
		Project: []string{"field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9",
			"field10", "field11", "field12", "field13", "field14", "field15", "field16"},
		WithExpiry: true,
	})
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}
	end := time.Now()

	var insertedDocContent map[string]interface{}
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	bytes, err := json.Marshal(doc)
	if err != nil {
		suite.T().Fatalf("Marshal failed, error was %v", err)
	}

	var originalDocContent map[string]interface{}
	err = json.Unmarshal(bytes, &originalDocContent)
	if err != nil {
		suite.T().Fatalf("Unmarshal failed, error was %v", err)
	}

	if len(insertedDocContent) != 16 {
		suite.T().Fatalf("Expected doc content to have 16 fields, had %d", len(insertedDocContent))
	}

	for k, v := range originalDocContent {
		if v != originalDocContent[k] {
			suite.T().Fatalf("%s not equal, expected %d but was %d", k, v, originalDocContent[k])
		}
	}

	if suite.Assert().NotNil(insertedDoc.Expiry()) {
		suite.Assert().InDelta(end.Sub(start).Seconds(), insertedDoc.Expiry().Seconds(), float64(1*time.Second))
	}
	suite.Assert().InDelta(start.Add(10*time.Second).Second(), insertedDoc.ExpiryTime().Second(), float64(1*time.Second))
}

func (suite *IntegrationTestSuite) TestInsertGetProjectionPathMissing() {
	suite.skipIfUnsupported(KeyValueFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert("projectMissingDoc", doc, nil)
	if err != nil {
		suite.T().Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	_, err = globalCollection.Get("projectMissingDoc", &GetOptions{
		Project: []string{"name", "thisfielddoesntexist"},
	})
	if err == nil {
		suite.T().Fatalf("Get should have failed")
	}
}

func (suite *IntegrationTestSuite) TestInsertGetProjectionTranscoders() {
	suite.skipIfUnsupported(KeyValueFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	_, err = globalCollection.Upsert("projectTranscoders", doc, &UpsertOptions{
		Expiry: 60 * time.Second,
	})

	type ABVdoc struct {
		ABV float32 `json:"abv"`
	}

	type tCase struct {
		name       string
		transcoder Transcoder
		expectErr  bool
		expected   interface{}
	}

	expectedBytes, err := json.Marshal(ABVdoc{ABV: 7.6})
	suite.Require().Nil(err)

	testCases := []tCase{
		{
			name:       "Binary",
			transcoder: NewRawBinaryTranscoder(),
			expectErr:  true,
		},
		{
			name:       "String",
			transcoder: NewRawStringTranscoder(),
			expectErr:  true,
		},
		{
			name:       "JSON",
			transcoder: NewJSONTranscoder(),
			expected: ABVdoc{
				ABV: 7.6,
			},
		},
		{
			name:       "Legacy",
			transcoder: NewLegacyTranscoder(),
			expected: ABVdoc{
				ABV: 7.6,
			},
		},
		{
			name:       "RawJSON",
			transcoder: NewRawJSONTranscoder(),
			expected:   expectedBytes,
		},
	}
	for _, testCase := range testCases {
		suite.T().Run(testCase.name, func(t *testing.T) {
			res, err := globalCollection.Get("projectTranscoders", &GetOptions{
				Project:    []string{"abv"},
				Transcoder: testCase.transcoder,
			})

			if suite.Assert().Nil(err, err) {
				if reflect.TypeOf(testCase.transcoder) == reflect.TypeOf(NewRawJSONTranscoder()) {
					var actual []byte
					err = res.Content(&actual)
					if suite.Assert().Nil(err, err) {
						suite.Assert().Equal(testCase.expected, actual)
					}
					return
				}
				var actual ABVdoc
				err = res.Content(&actual)
				if testCase.expectErr {
					suite.Assert().NotNil(err, err)
				} else {
					if suite.Assert().Nil(err, err) {
						suite.Assert().Equal(testCase.expected, actual)
					}
				}
			}
		})
	}
}

func (suite *IntegrationTestSuite) TestInsertGet() {
	suite.skipIfUnsupported(KeyValueFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert("insertDoc", doc, nil)
	if err != nil {
		suite.T().Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	insertedDoc, err := globalCollection.Get("insertDoc", nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != insertedDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}
}

func (suite *IntegrationTestSuite) TestExists() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(GetMetaFeature)

	_, err := globalCollection.Insert("exists", "test", nil)
	if err != nil {
		suite.T().Fatalf("Insert failed, error was %v", err)
	}

	existsRes, err := globalCollection.Exists("exists", nil)
	if err != nil {
		suite.T().Fatalf("Exists failed, error was %v", err)
	}

	if !existsRes.Exists() {
		suite.T().Fatalf("Expected exists to return true")
	}

	_, err = globalCollection.Remove("exists", nil)
	if err != nil {
		suite.T().Fatalf("Remove failed, error was %v", err)
	}

	existsRes, err = globalCollection.Exists("exists", nil)
	if err != nil {
		suite.T().Fatalf("Exists failed, error was %v", err)
	}

	if existsRes.Exists() {
		suite.T().Fatalf("Expected exists to return false")
	}
}

// Following test tests that if a collection is deleted and recreated midway through a set of operations
// then the operations will still succeed due to the cid being refreshed under the hood.
func (suite *IntegrationTestSuite) TestCollectionRetry() {
	suite.skipIfUnsupported(CollectionsFeature)
	suite.skipIfUnsupported(KeyValueFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	collectionName := "insertRetry"

	// cli := globalBucket.sb.getCachedClient()
	mgr := globalBucket.Collections()

	err = mgr.CreateCollection(CollectionSpec{ScopeName: "_default", Name: collectionName}, nil)
	if err != nil {
		suite.T().Fatalf("Could not create collection: %v", err)
	}

	col := globalBucket.Collection(collectionName)

	// Make sure we've connected to the collection ok
	success := suite.tryUntil(time.Now().Add(10*time.Second), 500*time.Millisecond, func() bool {
		mutRes, err := col.Upsert("insertRetryDoc", doc, nil)
		if err != nil {
			suite.T().Logf("Upsert failed, error was %v", err)
			return false
		}

		if mutRes.Cas() == 0 {
			suite.T().Fatalf("Upsert CAS was 0")
		}

		return true
	})

	suite.Require().True(success)

	// The following delete and create will recreate a collection with the same name but a different cid.
	err = mgr.DropCollection(CollectionSpec{ScopeName: "_default", Name: collectionName}, nil)
	if err != nil {
		suite.T().Fatalf("Could not drop collection: %v", err)
	}

	err = mgr.CreateCollection(CollectionSpec{ScopeName: "_default", Name: collectionName}, nil)
	if err != nil {
		suite.T().Fatalf("Could not create collection: %v", err)
	}

	// We've wiped the collection so we need to recreate this doc
	// We know that this operation can take a bit longer than normal as collections take time to come online.
	_, err = col.Upsert("insertRetryDoc", doc, &UpsertOptions{
		Timeout: 10000 * time.Millisecond,
	})
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	insertedDoc, err := col.Get("insertRetryDoc", nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != insertedDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}
}

func (suite *IntegrationTestSuite) TestUpsertGetRemove() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(GetMetaFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("upsertDoc", doc, nil)
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	upsertedDoc, err := globalCollection.Get("upsertDoc", nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var upsertedDocContent testBeerDocument
	err = upsertedDoc.Content(&upsertedDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != upsertedDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, upsertedDocContent)
	}

	existsRes, err := globalCollection.Exists("upsertDoc", nil)
	if err != nil {
		suite.T().Fatalf("Exists failed, error was %v", err)
	}

	if !existsRes.Exists() {
		suite.T().Fatalf("Expected exists to return true")
	}

	_, err = globalCollection.Remove("upsertDoc", nil)
	if err != nil {
		suite.T().Fatalf("Remove failed, error was %v", err)
	}

	existsRes, err = globalCollection.Exists("upsertDoc", nil)
	if err != nil {
		suite.T().Fatalf("Exists failed, error was %v", err)
	}

	if existsRes.Exists() {
		suite.T().Fatalf("Expected exists to return false")
	}
}

type upsertRetriesStrategy struct {
	retries int
}

func (rts *upsertRetriesStrategy) RetryAfter(RetryRequest, RetryReason) RetryAction {
	rts.retries++
	return &WithDurationRetryAction{100 * time.Millisecond}
}

func (suite *IntegrationTestSuite) TestUpsertRetries() {
	suite.skipIfUnsupported(KeyValueFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("getRetryDoc", doc, nil)
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	_, err = globalCollection.GetAndLock("getRetryDoc", 1*time.Second, nil)
	if err != nil {
		suite.T().Fatalf("GetAndLock failed, error was %v", err)
	}

	retryStrategy := &upsertRetriesStrategy{}
	mutRes, err = globalCollection.Upsert("getRetryDoc", doc, &UpsertOptions{
		Timeout:       2100 * time.Millisecond, // Timeout has to be long due to how the server handles unlocking.
		RetryStrategy: retryStrategy,
	})
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	if retryStrategy.retries <= 1 {
		suite.T().Fatalf("Expected retries to be > 1")
	}
}

func (suite *IntegrationTestSuite) TestRemoveWithCas() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(GetMetaFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("removeWithCas", doc, nil)
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	existsRes, err := globalCollection.Exists("removeWithCas", nil)
	if err != nil {
		suite.T().Fatalf("Exists failed, error was %v", err)
	}

	if !existsRes.Exists() {
		suite.T().Fatalf("Expected exists to return true")
	}

	_, err = globalCollection.Remove("removeWithCas", &RemoveOptions{Cas: mutRes.Cas() + 0xFECA})
	if err == nil {
		suite.T().Fatalf("Expected remove to fail")
	}

	if !errors.Is(err, ErrCasMismatch) {
		suite.T().Fatalf("Expected error to be CasMismatch but is %s", err)
	}

	existsRes, err = globalCollection.Exists("removeWithCas", nil)
	if err != nil {
		suite.T().Fatalf("Exists failed, error was %v", err)
	}

	if !existsRes.Exists() {
		suite.T().Fatalf("Expected exists to return true")
	}

	_, err = globalCollection.Remove("removeWithCas", &RemoveOptions{Cas: mutRes.Cas()})
	if err != nil {
		suite.T().Fatalf("Remove failed, error was %v", err)
	}

	existsRes, err = globalCollection.Exists("removeWithCas", nil)
	if err != nil {
		suite.T().Fatalf("Exists failed, error was %v", err)
	}

	if existsRes.Exists() {
		suite.T().Fatalf("Expected exists to return false")
	}
}

func (suite *IntegrationTestSuite) TestUpsertAndReplace() {
	suite.skipIfUnsupported(KeyValueFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("upsertAndReplace", doc, nil)
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	insertedDoc, err := globalCollection.Get("upsertAndReplace", nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != insertedDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}

	doc.Name = "replaced"
	mutRes, err = globalCollection.Replace("upsertAndReplace", doc, &ReplaceOptions{Cas: mutRes.Cas()})
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	replacedDoc, err := globalCollection.Get("upsertAndReplace", nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var replacedDocContent testBeerDocument
	err = replacedDoc.Content(&replacedDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != replacedDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}
}

func (suite *IntegrationTestSuite) TestGetAndTouch() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(XattrFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("getAndTouch", doc, nil)
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	start := time.Now()
	lockedDoc, err := globalCollection.GetAndTouch("getAndTouch", 10*time.Second, nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	expireDoc, err := globalCollection.Get("getAndTouch", &GetOptions{WithExpiry: true})
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}
	end := time.Now()

	if suite.Assert().NotNil(expireDoc.Expiry()) {
		suite.Assert().InDelta(end.Sub(start).Seconds(), expireDoc.Expiry().Seconds(), float64(1*time.Second))
	}
	suite.Assert().InDelta(start.Add(10*time.Second).Second(), expireDoc.ExpiryTime().Second(), float64(1*time.Second))

	var expireDocContent testBeerDocument
	err = expireDoc.Content(&expireDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != expireDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}
}

func (suite *IntegrationTestSuite) TestGetAndLock() {
	suite.skipIfUnsupported(KeyValueFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("getAndLock", doc, nil)
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndLock("getAndLock", 1*time.Second, nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	mutRes, err = globalCollection.Upsert("getAndLock", doc, &UpsertOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	})
	if err == nil {
		suite.T().Fatalf("Expected error but was nil")
	}

	if !errors.Is(err, ErrDocumentLocked) {
		suite.T().Fatalf("Expected error to be DocumentLocked but is %s", err)
	}

	globalCluster.TimeTravel(2000 * time.Millisecond)

	mutRes, err = globalCollection.Upsert("getAndLock", doc, nil)
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}
}

func (suite *IntegrationTestSuite) TestUnlock() {
	suite.skipIfUnsupported(KeyValueFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("unlock", doc, nil)
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndLock("unlock", 1, nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	err = globalCollection.Unlock("unlock", lockedDoc.Cas(), nil)
	if err != nil {
		suite.T().Fatalf("Unlock failed, error was %v", err)
	}

	mutRes, err = globalCollection.Upsert("unlock", doc, nil)
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}
}

func (suite *IntegrationTestSuite) TestUnlockInvalidCas() {
	suite.skipIfUnsupported(KeyValueFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("unlockInvalidCas", doc, nil)
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndLock("unlockInvalidCas", 2, nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	err = globalCollection.Unlock("unlockInvalidCas", lockedDoc.Cas()+1, &UnlockOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	})
	if err == nil {
		suite.T().Fatalf("Unlock should have failed")
	}

	// The server and the mock do not agree on the error for locked documents.
	if !errors.Is(err, ErrCasMismatch) && !errors.Is(err, ErrTemporaryFailure) {
		suite.T().Fatalf("Expected error to be DocumentLocked or TemporaryFailure but was %s", err)
	}
}

func (suite *IntegrationTestSuite) TestDoubleLockFail() {
	suite.skipIfUnsupported(KeyValueFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("doubleLock", doc, nil)
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndLock("doubleLock", 1, nil)
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	_, err = globalCollection.GetAndLock("doubleLock", 1, &GetAndLockOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	})
	if err == nil {
		suite.T().Fatalf("Expected GetAndLock to fail")
	}

	// The server and the mock do not agree on the error for locked documents.
	if !errors.Is(err, ErrDocumentLocked) && !errors.Is(err, ErrTemporaryFailure) {
		suite.T().Fatalf("Expected error to be DocumentLocked or TemporaryFailure but was %s", err)
	}
}

func (suite *IntegrationTestSuite) TestUnlockMissingDocFail() {
	suite.skipIfUnsupported(KeyValueFeature)

	err := globalCollection.Unlock("unlockMissing", 123, nil)
	if err == nil {
		suite.T().Fatalf("Expected Unlock to fail")
	}

	if !errors.Is(err, ErrDocumentNotFound) {
		suite.T().Fatalf("Expected error to be DocumentNotFound but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestTouch() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(XattrFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("touch", doc, nil)
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	start := time.Now()
	touchOut, err := globalCollection.Touch("touch", 3*time.Second, nil)
	if err != nil {
		suite.T().Fatalf("Touch failed, error was %v", err)
	}

	if touchOut.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	expireDoc, err := globalCollection.Get("touch", &GetOptions{WithExpiry: true})
	if err != nil {
		suite.T().Fatalf("Get failed, error was %v", err)
	}
	end := time.Now()

	if suite.Assert().NotNil(expireDoc.Expiry()) {
		suite.Assert().InDelta(end.Sub(start).Seconds(), expireDoc.Expiry().Seconds(), float64(1*time.Second))
	}
	suite.Assert().InDelta(start.Add(10*time.Second).Second(), expireDoc.ExpiryTime().Second(), float64(1*time.Second))

	var expireDocContent testBeerDocument
	err = expireDoc.Content(&expireDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != expireDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, expireDocContent)
	}
}

func (suite *IntegrationTestSuite) TestTouchMissingDocFail() {
	suite.skipIfUnsupported(KeyValueFeature)

	_, err := globalCollection.Touch("touchMissing", 3, nil)
	if err == nil {
		suite.T().Fatalf("Touch should have failed")
	}

	if !errors.Is(err, ErrDocumentNotFound) {
		suite.T().Fatalf("Expected error to be KeyNotFoundError but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestInsertReplicateToGetAnyReplica() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(ReplicasFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert("insertReplicaDoc", doc, &InsertOptions{
		PersistTo: 1,
		Timeout:   5 * time.Second,
	})
	if err != nil {
		suite.T().Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	insertedDoc, err := globalCollection.GetAnyReplica("insertReplicaDoc", nil)
	if err != nil {
		suite.T().Fatalf("GetFromReplica failed, error was %v", err)
	}

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		suite.T().Fatalf("Content failed, error was %v", err)
	}

	if doc != insertedDocContent {
		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}
}

func (suite *IntegrationTestSuite) TestInsertReplicateToGetAllReplicas() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(ReplicasFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	agent, err := globalCollection.getKvProvider()
	if err != nil {
		suite.T().Fatalf("Failed to get kv provider, was %v", err)
	}

	snapshot, err := agent.ConfigSnapshot()
	if err != nil {
		suite.T().Fatalf("Failed to get config snapshot, was %v", err)
	}

	numReplicas, err := snapshot.NumReplicas()
	if err != nil {
		suite.T().Fatalf("Failed to get numReplicas, was %v", err)
	}

	expectedReplicas := numReplicas + 1

	mutRes, err := globalCollection.Upsert("insertAllReplicaDoc", doc, &UpsertOptions{
		PersistTo: uint(expectedReplicas),
	})
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Insert CAS was 0")
	}

	stream, err := globalCollection.GetAllReplicas("insertAllReplicaDoc", &GetAllReplicaOptions{
		Timeout: 25 * time.Second,
	})
	if err != nil {
		suite.T().Fatalf("GetAllReplicas failed, error was %v", err)
	}

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
			suite.T().Fatalf("Content failed, error was %v", err)
		}

		if doc != insertedDocContent {
			suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
		}
	}

	err = stream.Close()
	if err != nil {
		suite.T().Fatalf("Expected stream close to not error, was %v", err)
	}

	if expectedReplicas != actualReplicas {
		suite.T().Fatalf("Expected replicas to be %d but was %d", expectedReplicas, actualReplicas)
	}

	if numMasters != 1 {
		suite.T().Fatalf("Expected number of masters to be 1 but was %d", numMasters)
	}
}

func (suite *IntegrationTestSuite) TestDurabilityGetFromAnyReplica() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(DurabilityFeature)
	suite.skipIfUnsupported(ReplicasFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
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
			name:   "upsertDurabilityMajorityDoc",
			method: "Upsert",
			args: []interface{}{"upsertDurabilityMajorityDoc", doc, &UpsertOptions{
				DurabilityLevel: DurabilityLevelMajority,
			}},
			expectCas:         true,
			expectedError:     nil,
			expectKeyNotFound: false,
		},
		{
			name:   "insertDurabilityLevelPersistToMajority",
			method: "Insert",
			args: []interface{}{"insertDurabilityLevelPersistToMajority", doc, &InsertOptions{
				DurabilityLevel: DurabilityLevelPersistToMajority,
			}},
			expectCas:         true,
			expectedError:     nil,
			expectKeyNotFound: false,
		},
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
		{
			name:   "insertDurabilityMajorityAndPersistOnMasterDoc",
			method: "Insert",
			args: []interface{}{"insertDurabilityMajorityAndPersistOnMasterDoc", doc, &InsertOptions{
				DurabilityLevel: DurabilityLevelMajorityAndPersistOnMaster,
			}},
			expectCas:         true,
			expectedError:     nil,
			expectKeyNotFound: false,
		},
		{
			name:   "upsertDurabilityLevelPersistToMajority",
			method: "Upsert",
			args: []interface{}{"upsertDurabilityLevelPersistToMajority", doc, &UpsertOptions{
				DurabilityLevel: DurabilityLevelPersistToMajority,
			}},
			expectCas:         true,
			expectedError:     nil,
			expectKeyNotFound: false,
		},
		{
			name:   "upsertDurabilityMajorityAndPersistOnMasterDoc",
			method: "Upsert",
			args: []interface{}{"upsertDurabilityMajorityAndPersistOnMasterDoc", doc, &UpsertOptions{
				DurabilityLevel: DurabilityLevelMajorityAndPersistOnMaster,
			}},
			expectCas:         true,
			expectedError:     nil,
			expectKeyNotFound: false,
		},
	}

	for _, tCase := range testCases {
		suite.T().Run(tCase.name, func(te *testing.T) {
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
					suite.T().Fatalf("Expected GetFromReplica to not find a key but got error %v", err)
				}
			} else {
				if err != nil {
					te.Fatalf("GetFromReplica failed, error was %v", err)
				}
			}
		})
	}
}

func (suite *UnitTestSuite) TestGetErrorCollectionUnknown() {
	var doc testBreweryDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not load dataset: %v", err)
	}

	pendingOp := new(mockPendingOp)
	pendingOp.AssertNotCalled(suite.T(), "Cancel", mock.AnythingOfType("error"))

	provider := new(mockKvProvider)
	provider.
		On("Get", mock.AnythingOfType("gocbcore.GetOptions"), mock.AnythingOfType("gocbcore.GetCallback")).
		Run(func(args mock.Arguments) {
			cb := args.Get(1).(gocbcore.GetCallback)
			cb(nil, gocbcore.ErrCollectionNotFound)
		}).
		Return(pendingOp, nil)

	col := &Collection{
		bucket:         &Bucket{bucketName: "mock"},
		collectionName: "",
		scope:          "",

		getKvProvider: suite.kvProvider(provider, nil),
		timeoutsConfig: kvTimeoutsConfig{
			KVTimeout: 2500 * time.Millisecond,
		},
		transcoder:           NewJSONTranscoder(),
		tracer:               &noopTracer{},
		retryStrategyWrapper: newRetryStrategyWrapper(NewBestEffortRetryStrategy(nil)),
	}

	res, err := col.Get("getDocErrCollectionUnknown", nil)
	if err == nil {
		suite.T().Fatalf("Get didn't error")
	}

	if res != nil {
		suite.T().Fatalf("Result should have been nil")
	}

	if !errors.Is(err, ErrCollectionNotFound) {
		suite.T().Fatalf("Error should have been collection missing but was %v", err)
	}
}

func (suite *UnitTestSuite) TestGetErrorProperties() {
	pendingOp := new(mockPendingOp)
	pendingOp.AssertNotCalled(suite.T(), "Cancel", mock.AnythingOfType("error"))

	expectedErr := &gocbcore.KeyValueError{
		InnerError:         gocbcore.ErrDocumentNotFound, // Doesn't map perfectly but it's good enough
		StatusCode:         0x01,
		DocumentKey:        "someid",
		BucketName:         "default",
		ScopeName:          "_default",
		CollectionName:     "_default",
		CollectionID:       0,
		ErrorName:          "KEY_ENOENT ",
		ErrorDescription:   "Not Found",
		Opaque:             0x25,
		Context:            "",
		Ref:                "",
		RetryReasons:       []gocbcore.RetryReason{gocbcore.KVTemporaryFailureRetryReason},
		RetryAttempts:      1,
		LastDispatchedTo:   "10.112.194.102:11210",
		LastDispatchedFrom: "10.112.194.1:11210",
		LastConnectionID:   "d80aa17aa2577d3d/87aa643382554811",
	}

	provider := new(mockKvProvider)
	provider.
		On("Get", mock.AnythingOfType("gocbcore.GetOptions"), mock.AnythingOfType("gocbcore.GetCallback")).
		Run(func(args mock.Arguments) {
			cb := args.Get(1).(gocbcore.GetCallback)
			cb(nil, expectedErr)
		}).
		Return(pendingOp, nil)

	col := &Collection{
		bucket: &Bucket{bucketName: "mock"},

		collectionName: "",
		scope:          "",

		getKvProvider: suite.kvProvider(provider, nil),
		timeoutsConfig: kvTimeoutsConfig{
			KVTimeout: 2500 * time.Millisecond,
		},
		transcoder:           NewJSONTranscoder(),
		tracer:               &noopTracer{},
		retryStrategyWrapper: newRetryStrategyWrapper(NewBestEffortRetryStrategy(nil)),
	}

	res, err := col.Get("someid", nil)
	if !errors.Is(err, ErrDocumentNotFound) {
		suite.T().Fatalf("Error should have been document not found but was %s", err)
	}
	var kvErr *KeyValueError
	if !errors.As(err, &kvErr) {
		suite.T().Fatalf("Error should have been KeyValueError but was %s", err)
	}

	suite.Assert().Equal(expectedErr.StatusCode, kvErr.StatusCode)
	suite.Assert().Equal(expectedErr.DocumentKey, kvErr.DocumentID)
	suite.Assert().Equal(expectedErr.BucketName, kvErr.BucketName)
	suite.Assert().Equal(expectedErr.ScopeName, kvErr.ScopeName)
	suite.Assert().Equal(expectedErr.CollectionName, kvErr.CollectionName)
	suite.Assert().Equal(expectedErr.CollectionID, kvErr.CollectionID)
	suite.Assert().Equal(expectedErr.ErrorName, kvErr.ErrorName)
	suite.Assert().Equal(expectedErr.ErrorDescription, kvErr.ErrorDescription)
	suite.Assert().Equal(expectedErr.Opaque, kvErr.Opaque)
	suite.Assert().Equal(expectedErr.Context, kvErr.Context)
	suite.Assert().Equal(expectedErr.Ref, kvErr.Ref)
	suite.Assert().Equal([]RetryReason{KVTemporaryFailureRetryReason}, kvErr.RetryReasons)
	suite.Assert().Equal(expectedErr.RetryAttempts, kvErr.RetryAttempts)
	suite.Assert().Equal(expectedErr.LastDispatchedTo, kvErr.LastDispatchedTo)
	suite.Assert().Equal(expectedErr.LastDispatchedFrom, kvErr.LastDispatchedFrom)
	suite.Assert().Equal(expectedErr.LastConnectionID, kvErr.LastConnectionID)

	suite.Assert().Nil(res)
}
