package gocb

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"

	"github.com/stretchr/testify/mock"

	"github.com/couchbase/gocbcore/v10"
)

func (suite *IntegrationTestSuite) TestErrorNonExistant() {
	suite.skipIfUnsupported(KeyValueFeature)

	res, err := globalCollection.Get("doesnt-exist", nil)
	if !errors.Is(err, ErrDocumentNotFound) {
		suite.T().Fatalf("Expected error to be non-nil")
	}

	suite.Assert().Nil(res)

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 1)
	suite.AssertKvOpSpan(nilParents[0], "get", memd.CmdGet.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 2)
	suite.AssertKvOpSpan(nilParents[0], "insert", memd.CmdAdd.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[0], "insert", memd.CmdAdd.Name(), 1, false, true, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "insert", 2, false)
}

func (suite *IntegrationTestSuite) TestExpiryConversions() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(XattrFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	type tCase struct {
		name   string
		expiry time.Duration
	}
	tCases := []tCase{
		{
			name:   "TestExpiryConversionsUnder30Days",
			expiry: 5 * time.Second,
		},
		{
			name:   "TestExpiryConversions30Days",
			expiry: 30 * (24 * time.Hour),
		},
		{
			name:   "TestExpiryConversionsOver30Days",
			expiry: 31 * (24 * time.Hour),
		},
	}

	for _, tCase := range tCases {
		suite.T().Run(tCase.name, func(te *testing.T) {
			res, err := globalCollection.Upsert(tCase.name, doc, &UpsertOptions{
				Expiry: tCase.expiry,
			})
			if err != nil {
				te.Fatalf("Error running Upsert: %v", err)
			}

			if res.Cas() == 0 {
				te.Fatalf("Insert CAS was 0")
			}

			start := time.Now()
			spec := []LookupInSpec{
				GetSpec("$document", &GetSpecOptions{IsXattr: true}),
			}

			if globalCluster.SupportsFeature(HLCFeature) {
				spec = append(spec, GetSpec("$vbucket.HLC", &GetSpecOptions{IsXattr: true}))
			}

			lookupRes, err := globalCollection.LookupIn(
				tCase.name,
				spec,
				nil,
			)
			if err != nil {
				te.Fatalf("Error running LookupIn: %v", err)
				return
			}

			exp := struct {
				Expiration int64 `json:"exptime"`
			}{}
			err = lookupRes.ContentAt(0, &exp)
			if err != nil {
				te.Fatalf("Error running ContentAt: %v", err)
			}

			var actualExpirySecs float64
			if globalCluster.SupportsFeature(HLCFeature) {
				hlcStr := struct {
					Now string `json:"now"`
				}{}
				err = lookupRes.ContentAt(1, &hlcStr)
				if err != nil {
					te.Fatalf("Error running ContentAt: %v", err)
				}

				hlc, err := strconv.Atoi(hlcStr.Now)
				if err != nil {
					te.Fatalf("Error running Atoi: %v", err)
				}

				actualExpirySecs = time.Unix(exp.Expiration, 0).Sub(time.Unix(int64(hlc), 0)).Seconds()
			} else {
				actualExpirySecs = time.Unix(exp.Expiration, 0).Sub(start).Seconds()
			}

			if actualExpirySecs > (tCase.expiry + (1000 * time.Millisecond)).Seconds() {
				te.Fatalf("Expected expiry to be less than %f but was %f", tCase.expiry.Seconds(),
					actualExpirySecs)
			}

			if actualExpirySecs < (tCase.expiry - (2500 * time.Millisecond)).Seconds() {
				te.Fatalf("Expected expiry to be greater than %f but was %f", tCase.expiry.Seconds(),
					actualExpirySecs)
			}
		})
	}
	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 3, false)
	suite.AssertKVMetrics(meterNameCBOperations, "lookup_in", 3, false)
}

func (suite *IntegrationTestSuite) TestPreserveExpiry() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(XattrFeature)
	suite.skipIfUnsupported(PreserveExpiryFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	suite.Require().Nil(err, err)

	start := time.Now()
	mutRes, err := globalCollection.Upsert("preservettl", doc, &UpsertOptions{Expiry: 25 * time.Second})
	suite.Require().Nil(err, err)

	suite.Assert().NotZero(mutRes.Cas())

	mutRes, err = globalCollection.Upsert("preservettl", doc, &UpsertOptions{PreserveExpiry: true})
	suite.Require().Nil(err, err)

	suite.Assert().NotZero(mutRes.Cas())

	insertedDoc, err := globalCollection.Get("preservettl", &GetOptions{WithExpiry: true})
	suite.Require().Nil(err, err)

	suite.Assert().InDelta(start.Add(25*time.Second).Unix(), insertedDoc.ExpiryTime().Unix(), 5)

	mutRes, err = globalCollection.Replace("preservettl", doc, &ReplaceOptions{PreserveExpiry: true})
	suite.Require().Nil(err, err)

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Replace CAS was 0")
	}

	replacedDoc, err := globalCollection.Get("preservettl", &GetOptions{WithExpiry: true})
	suite.Require().Nil(err, err)

	suite.Assert().InDelta(start.Add(25*time.Second).Unix(), replacedDoc.ExpiryTime().Unix(), 5)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 2)
	suite.AssertKvOpSpan(nilParents[0], "insert", memd.CmdAdd.Name(), 1, false, true, DurabilityLevelNone)
	span := nilParents[1]
	suite.AssertKvSpan(span, "get", DurabilityLevelNone)

	suite.Require().Equal(len(span.Spans), 1)
	suite.Require().Contains(span.Spans, "lookup_in")
	lookupSpans := span.Spans["lookup_in"]

	suite.Require().Equal(len(lookupSpans), 1)
	suite.AssertKvOpSpan(lookupSpans[0], "lookup_in", memd.CmdSubDocMultiLookup.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "insert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 2)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), 1, false, true, DurabilityLevelNone)
	span := nilParents[1]
	suite.AssertKvSpan(span, "get", DurabilityLevelNone)

	suite.Require().Equal(len(span.Spans), 1)
	suite.Require().Contains(span.Spans, "lookup_in")
	lookupSpans := span.Spans["lookup_in"]

	suite.Require().Equal(len(lookupSpans), 1)
	suite.AssertKvOpSpan(lookupSpans[0], "lookup_in", memd.CmdSubDocMultiLookup.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
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
		tCase{
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
		{
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
		{
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
		{
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
		{
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
		{
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
	}

	for _, testCase := range testCases {
		globalTracer.Reset()
		globalMeter.Reset()
		suite.T().Run(testCase.name, func(t *testing.T) {
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

			suite.Require().Contains(globalTracer.GetSpans(), nil)
			nilParents := globalTracer.GetSpans()[nil]
			suite.Require().Equal(len(nilParents), 1)
			span := nilParents[0]
			suite.AssertKvSpan(span, "get", DurabilityLevelNone)

			suite.Require().Equal(len(span.Spans), 1)
			suite.Require().Contains(span.Spans, "lookup_in")
			lookupSpans := span.Spans["lookup_in"]

			suite.Require().Equal(len(lookupSpans), 1)
			suite.AssertKvOpSpan(lookupSpans[0], "lookup_in", memd.CmdSubDocMultiLookup.Name(), 1, false, false, DurabilityLevelNone)

			suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 2)
	suite.AssertKvOpSpan(nilParents[0], "insert", memd.CmdAdd.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "get", "", 0, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "insert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 2)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), 1, false, true, DurabilityLevelNone)
	span := nilParents[1]
	suite.AssertKvSpan(span, "get", DurabilityLevelNone)

	suite.Require().Equal(len(span.Spans), 1)
	suite.Require().Contains(span.Spans, "lookup_in")
	lookupSpans := span.Spans["lookup_in"]

	suite.Require().Equal(len(lookupSpans), 1)
	suite.AssertKvOpSpan(lookupSpans[0], "lookup_in", memd.CmdSubDocMultiLookup.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 2)
	suite.AssertKvOpSpan(nilParents[0], "insert", memd.CmdAdd.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "get", "", 0, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "insert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 2)
	suite.AssertKvOpSpan(nilParents[0], "insert", memd.CmdAdd.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "get", memd.CmdGet.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "insert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 4)
	suite.AssertKvOpSpan(nilParents[0], "insert", memd.CmdAdd.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "exists", memd.CmdGetMeta.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[2], "remove", memd.CmdDelete.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[3], "exists", memd.CmdGetMeta.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "insert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "remove", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "exists", 2, false)
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

	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, meterValueServiceManagement, "manager_collections_create_collection"), 2, false)
	suite.AssertMetrics(makeMetricsKey(meterNameCBOperations, meterValueServiceManagement, "manager_collections_drop_collection"), 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 2, true)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 5)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "get", memd.CmdGet.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[2], "exists", memd.CmdGetMeta.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[3], "remove", memd.CmdDelete.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[4], "exists", memd.CmdGetMeta.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "remove", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "exists", 2, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 3)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "get_and_lock", memd.CmdGetLocked.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[2], "upsert", memd.CmdSet.Name(), retryStrategy.retries+1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 2, true)
	suite.AssertKVMetrics(meterNameCBOperations, "get_and_lock", 1, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 6)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "exists", memd.CmdGetMeta.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[2], "remove", memd.CmdDelete.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[3], "exists", memd.CmdGetMeta.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[4], "remove", memd.CmdDelete.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[5], "exists", memd.CmdGetMeta.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "exists", 3, false)
	suite.AssertKVMetrics(meterNameCBOperations, "remove", 2, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 4)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "get", memd.CmdGet.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[2], "replace", memd.CmdReplace.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[3], "get", memd.CmdGet.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 2, false)
	suite.AssertKVMetrics(meterNameCBOperations, "replace", 1, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 3)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "get_and_touch", memd.CmdGAT.Name(), 1, false, false, DurabilityLevelNone)
	span := nilParents[2]
	suite.AssertKvSpan(span, "get", DurabilityLevelNone)

	suite.Require().Equal(len(span.Spans), 1)
	suite.Require().Contains(span.Spans, "lookup_in")
	lookupSpans := span.Spans["lookup_in"]

	suite.Require().Equal(len(lookupSpans), 1)
	suite.AssertKvOpSpan(lookupSpans[0], "lookup_in", memd.CmdSubDocMultiLookup.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get_and_touch", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
}

func (suite *IntegrationTestSuite) TestExpires() {
	suite.skipIfUnsupported(KeyValueFeature)

	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		suite.T().Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("expires", doc, &UpsertOptions{
		Expiry: 1 * time.Second,
	})
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	globalCluster.TimeTravel(3000 * time.Millisecond)

	_, err = globalCollection.Get("expires", nil)
	if !errors.Is(err, ErrDocumentNotFound) {
		suite.T().Fatalf("Get should have failed with doc not found but was: %v", err)
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

	mutRes, err = globalCollection.Upsert("getAndLock", doc, &UpsertOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	})
	if err != nil {
		suite.T().Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		suite.T().Fatalf("Upsert CAS was 0")
	}

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 4)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "get_and_lock", memd.CmdGetLocked.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[2], "upsert", memd.CmdSet.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[3], "upsert", memd.CmdSet.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 3, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get_and_lock", 1, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 4)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "get_and_lock", memd.CmdGetLocked.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[2], "unlock", memd.CmdUnlockKey.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[3], "upsert", memd.CmdSet.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 2, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get_and_lock", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "unlock", 1, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 3)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "get_and_lock", memd.CmdGetLocked.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[2], "unlock", memd.CmdUnlockKey.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get_and_lock", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "unlock", 1, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 3)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "get_and_lock", memd.CmdGetLocked.Name(), 1, false, false, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[2], "get_and_lock", memd.CmdGetLocked.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get_and_lock", 2, false)
}

func (suite *IntegrationTestSuite) TestUnlockMissingDocFail() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfServerVersionEquals(srvVer750)

	err := globalCollection.Unlock("unlockMissing", 123, nil)
	if err == nil {
		suite.T().Fatalf("Expected Unlock to fail")
	}

	if !errors.Is(err, ErrDocumentNotFound) {
		suite.T().Fatalf("Expected error to be DocumentNotFound but was %v", err)
	}

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 1)
	suite.AssertKvOpSpan(nilParents[0], "unlock", memd.CmdUnlockKey.Name(), 1, false, false, DurabilityLevelNone)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 3)
	suite.AssertKvOpSpan(nilParents[0], "upsert", memd.CmdSet.Name(), 1, false, true, DurabilityLevelNone)
	suite.AssertKvOpSpan(nilParents[1], "touch", memd.CmdTouch.Name(), 1, false, false, DurabilityLevelNone)
	span := nilParents[2]
	suite.AssertKvSpan(span, "get", DurabilityLevelNone)

	suite.Require().Equal(len(span.Spans), 1)
	suite.Require().Contains(span.Spans, "lookup_in")
	lookupSpans := span.Spans["lookup_in"]

	suite.Require().Equal(len(lookupSpans), 1)
	suite.AssertKvOpSpan(lookupSpans[0], "lookup_in", memd.CmdSubDocMultiLookup.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "upsert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "touch", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get", 1, false)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 1)
	suite.AssertKvOpSpan(nilParents[0], "touch", memd.CmdTouch.Name(), 1, false, false, DurabilityLevelNone)

	suite.AssertKVMetrics(meterNameCBOperations, "touch", 1, false)
}

func (suite *IntegrationTestSuite) TestInsertReplicateStartupWait() {
	suite.skipIfUnsupported(KeyValueFeature)

	globalCollection.Get("touchMissing", nil)
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

	suite.Require().Contains(globalTracer.GetSpans(), nil)
	nilParents := globalTracer.GetSpans()[nil]
	suite.Require().Equal(len(nilParents), 2)
	suite.AssertKvOpSpan(nilParents[0], "insert", memd.CmdAdd.Name(), 1, false, true, DurabilityLevelNone)

	span := nilParents[1]
	suite.AssertKvSpan(span, "get_any_replica", DurabilityLevelNone)

	suite.Require().Equal(len(span.Spans), 1)
	suite.Require().Contains(span.Spans, "get_all_replicas")
	allReplicasSpans := span.Spans["get_all_replicas"]

	suite.Require().GreaterOrEqual(len(allReplicasSpans), 1)
	suite.Require().Contains(allReplicasSpans[0].Spans, "get_replica")
	getReplicaSpans := allReplicasSpans[0].Spans["get_replica"]
	suite.Require().GreaterOrEqual(len(getReplicaSpans), 2)
	// We don't actually know which of these will win.
	for _, span := range getReplicaSpans {
		suite.Require().Equal(1, len(span.Spans))
		suite.AssertKvSpan(span, "get_replica", DurabilityLevelNone)
		// We don't know which span was actually cancelled so we don't check the CMD spans.
	}

	suite.AssertKVMetrics(meterNameCBOperations, "insert", 1, false)
	suite.AssertKVMetrics(meterNameCBOperations, "get_any_replica", 1, false)

	// We can't reliably check the metrics for the get cmd spans, as we don't know which one will have won.
}

func (suite *IntegrationTestSuite) TestInsertReplicateToGetAllReplicas() {
	// suite.skipIfUnsupported(KeyValueFeature)
	// suite.skipIfUnsupported(ReplicasFeature)

	// var doc testBeerDocument
	// err := loadJSONTestDataset("beer_sample_single", &doc)
	// if err != nil {
	// 	suite.T().Fatalf("Could not read test dataset: %v", err)
	// }

	// agent, err := globalCollection.getKvProvider()
	// if err != nil {
	// 	suite.T().Fatalf("Failed to get kv provider, was %v", err)
	// }

	// snapshot, err := globalCollection.waitForConfigSnapshot(context.Background(), time.Now().Add(5*time.Second), agent)
	// if err != nil {
	// 	suite.T().Fatalf("Failed to get config snapshot, was %v", err)
	// }

	// numReplicas, err := snapshot.NumReplicas()
	// if err != nil {
	// 	suite.T().Fatalf("Failed to get numReplicas, was %v", err)
	// }

	// expectedReplicas := numReplicas + 1

	// mutRes, err := globalCollection.Upsert("insertAllReplicaDoc", doc, &UpsertOptions{
	// 	PersistTo: uint(expectedReplicas),
	// })
	// if err != nil {
	// 	suite.T().Fatalf("Upsert failed, error was %v", err)
	// }

	// if mutRes.Cas() == 0 {
	// 	suite.T().Fatalf("Insert CAS was 0")
	// }

	// stream, err := globalCollection.GetAllReplicas("insertAllReplicaDoc", &GetAllReplicaOptions{
	// 	Timeout: 25 * time.Second,
	// })
	// if err != nil {
	// 	suite.T().Fatalf("GetAllReplicas failed, error was %v", err)
	// }

	// actualReplicas := 0
	// numMasters := 0

	// for {
	// 	insertedDoc := stream.Next()
	// 	if insertedDoc == nil {
	// 		break
	// 	}

	// 	actualReplicas++

	// 	if !insertedDoc.IsReplica() {
	// 		numMasters++
	// 	}

	// 	var insertedDocContent testBeerDocument
	// 	err = insertedDoc.Content(&insertedDocContent)
	// 	if err != nil {
	// 		suite.T().Fatalf("Content failed, error was %v", err)
	// 	}

	// 	if doc != insertedDocContent {
	// 		suite.T().Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	// 	}
	// }

	// err = stream.Close()
	// if err != nil {
	// 	suite.T().Fatalf("Expected stream close to not error, was %v", err)
	// }

	// if expectedReplicas != actualReplicas {
	// 	suite.T().Fatalf("Expected replicas to be %d but was %d", expectedReplicas, actualReplicas)
	// }

	// if numMasters != 1 {
	// 	suite.T().Fatalf("Expected number of masters to be 1 but was %d", numMasters)
	// }

	// suite.AssertKVMetrics(meterNameCBOperations, "upsert", 1, false)
	// suite.AssertKVMetrics(meterNameCBOperations, "get_all_replicas", 1, false)
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
		name               string
		method             string
		args               []interface{}
		expectCas          bool
		expectedError      error
		expectKeyNotFound  bool
		expectedDurability DurabilityLevel
	}

	testCases := []tCase{
		{
			name:   "upsertDurabilityMajorityDoc",
			method: "Upsert",
			args: []interface{}{"upsertDurabilityMajorityDoc", doc, &UpsertOptions{
				DurabilityLevel: DurabilityLevelMajority,
				// MB-41616: For some reason the first durable request after a lot of collections activity takes longer.
				Timeout: 10 * time.Second,
			}},
			expectCas:          true,
			expectedError:      nil,
			expectKeyNotFound:  false,
			expectedDurability: DurabilityLevelMajority,
		},
		{
			name:   "insertDurabilityLevelPersistToMajority",
			method: "Insert",
			args: []interface{}{"insertDurabilityLevelPersistToMajority", doc, &InsertOptions{
				DurabilityLevel: DurabilityLevelPersistToMajority,
			}},
			expectCas:          true,
			expectedError:      nil,
			expectKeyNotFound:  false,
			expectedDurability: DurabilityLevelPersistToMajority,
		},
		{
			name:   "insertDurabilityMajorityDoc",
			method: "Insert",
			args: []interface{}{"insertDurabilityMajorityDoc", doc, &InsertOptions{
				DurabilityLevel: DurabilityLevelMajority,
			}},
			expectCas:          true,
			expectedError:      nil,
			expectKeyNotFound:  false,
			expectedDurability: DurabilityLevelMajority,
		},
		{
			name:   "insertDurabilityMajorityAndPersistOnMasterDoc",
			method: "Insert",
			args: []interface{}{"insertDurabilityMajorityAndPersistOnMasterDoc", doc, &InsertOptions{
				DurabilityLevel: DurabilityLevelMajorityAndPersistOnMaster,
			}},
			expectCas:          true,
			expectedError:      nil,
			expectKeyNotFound:  false,
			expectedDurability: DurabilityLevelMajorityAndPersistOnMaster,
		},
		{
			name:   "upsertDurabilityLevelPersistToMajority",
			method: "Upsert",
			args: []interface{}{"upsertDurabilityLevelPersistToMajority", doc, &UpsertOptions{
				DurabilityLevel: DurabilityLevelPersistToMajority,
			}},
			expectCas:          true,
			expectedError:      nil,
			expectKeyNotFound:  false,
			expectedDurability: DurabilityLevelPersistToMajority,
		},
		{
			name:   "upsertDurabilityMajorityAndPersistOnMasterDoc",
			method: "Upsert",
			args: []interface{}{"upsertDurabilityMajorityAndPersistOnMasterDoc", doc, &UpsertOptions{
				DurabilityLevel: DurabilityLevelMajorityAndPersistOnMaster,
			}},
			expectCas:          true,
			expectedError:      nil,
			expectKeyNotFound:  false,
			expectedDurability: DurabilityLevelMajorityAndPersistOnMaster,
		},
	}

	for _, tCase := range testCases {
		suite.T().Run(tCase.name, func(te *testing.T) {
			globalTracer.Reset()
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

			var cmdName string
			if tCase.method == "Upsert" {
				cmdName = memd.CmdSet.Name()
			} else {
				cmdName = memd.CmdAdd.Name()
			}

			suite.Require().Contains(globalTracer.GetSpans(), nil)
			nilParents := globalTracer.GetSpans()[nil]
			suite.Require().Equal(len(nilParents), 2)
			suite.AssertKvOpSpan(nilParents[0], strings.ToLower(tCase.method), cmdName, 1, false,
				true, tCase.expectedDurability)
			// GetAnyReplica tracing is a pain to test and we do it elsewhere so we don't bother here.
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
		On("Get", mock.Anything).
		// Run(func(args mock.Arguments) {
		// 	cb := args.Get(1).(gocbcore.GetCallback)
		// 	cb(nil, gocbcore.ErrCollectionNotFound)
		// }).
		Return(pendingOp, nil)

	col := suite.collection("mock", "", "", provider)

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

func (suite *IntegrationTestSuite) TestBasicCrudContext() {
	suite.skipIfUnsupported(KeyValueFeature)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := globalCollection.Upsert("context", "test", &UpsertOptions{
		Context: ctx,
	})
	if !errors.Is(err, ErrRequestCanceled) {
		suite.T().Fatalf("Expected error to be canceled but was %v", err)
	}
	suite.Require().Nil(res)

	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(1*time.Nanosecond))
	defer cancel()

	// This is ugly but caves on windows seems to be able to run ops instantaneously (time accuracy).
	time.Sleep(10 * time.Millisecond)

	res, err = globalCollection.Upsert("context", "test", &UpsertOptions{
		Context: ctx,
	})
	if !errors.Is(err, ErrRequestCanceled) {
		suite.T().Fatalf("Expected error to be canceled but was %v", err)
	}
	suite.Require().Nil(res)

	ctx, cancel = context.WithCancel(context.Background())
	cancel()

	getRes, err := globalCollection.Get("context", &GetOptions{
		Context: ctx,
	})
	if !errors.Is(err, ErrRequestCanceled) {
		suite.T().Fatalf("Expected error to be canceled but was %v", err)
	}
	suite.Require().Nil(getRes)

	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(1*time.Nanosecond))
	defer cancel()

	// This is ugly but caves on windows seems to be able to run ops instantaneously (time accuracy).
	time.Sleep(10 * time.Millisecond)

	getRes, err = globalCollection.Get("context", &GetOptions{
		Context: ctx,
	})
	if !errors.Is(err, ErrRequestCanceled) {
		suite.T().Fatalf("Expected error to be canceled but was %v", err)
	}
	suite.Require().Nil(getRes)
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
		On("Get", mock.Anything).
		Return(nil, expectedErr)

	col := suite.collection("mock", "", "", provider)

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

func (suite *UnitTestSuite) TestExpiryConversion5Seconds() {
	pendingOp := new(mockPendingOp)
	pendingOp.AssertNotCalled(suite.T(), "Cancel", mock.AnythingOfType("error"))

	provider := new(mockKvProvider)
	provider.
		On("Set", mock.AnythingOfType("gocbcore.SetOptions"), mock.AnythingOfType("gocbcore.StoreCallback")).
		Run(func(args mock.Arguments) {
			opts := args.Get(0).(gocbcore.SetOptions)
			cb := args.Get(1).(gocbcore.StoreCallback)

			suite.Assert().Equal(uint32(5), opts.Expiry)
			cb(&gocbcore.StoreResult{
				Cas: gocbcore.Cas(123),
			}, nil)
		}).
		Return(pendingOp, nil)

	col := suite.collection("mock", "", "", provider)

	res, err := col.Upsert("someid", "someval", &UpsertOptions{
		Expiry: 5 * time.Second,
	})
	suite.Require().Nil(err, err)

	suite.Assert().Equal(Cas(123), res.Cas())
}

func (suite *UnitTestSuite) TestExpiryConversion500Milliseconds() {
	pendingOp := new(mockPendingOp)
	pendingOp.AssertNotCalled(suite.T(), "Cancel", mock.AnythingOfType("error"))

	provider := new(mockKvProvider)
	provider.
		On("Set", mock.AnythingOfType("gocbcore.SetOptions"), mock.AnythingOfType("gocbcore.StoreCallback")).
		Run(func(args mock.Arguments) {
			opts := args.Get(0).(gocbcore.SetOptions)
			cb := args.Get(1).(gocbcore.StoreCallback)

			suite.Assert().Equal(uint32(1), opts.Expiry)
			cb(&gocbcore.StoreResult{
				Cas: gocbcore.Cas(123),
			}, nil)
		}).
		Return(pendingOp, nil)

	col := suite.collection("mock", "", "", provider)

	res, err := col.Upsert("someid", "someval", &UpsertOptions{
		Expiry: 500 * time.Millisecond,
	})
	suite.Require().Nil(err, err)

	suite.Assert().Equal(Cas(123), res.Cas())
}

func (suite *UnitTestSuite) TestExpiryConversion30Days() {
	pendingOp := new(mockPendingOp)
	pendingOp.AssertNotCalled(suite.T(), "Cancel", mock.AnythingOfType("error"))

	expectedTime := time.Now().Add(30 * 24 * time.Hour)
	provider := new(mockKvProvider)
	provider.
		On("Set", mock.AnythingOfType("gocbcore.SetOptions"), mock.AnythingOfType("gocbcore.StoreCallback")).
		Run(func(args mock.Arguments) {
			opts := args.Get(0).(gocbcore.SetOptions)
			cb := args.Get(1).(gocbcore.StoreCallback)

			if opts.Expiry < uint32(expectedTime.Add(-1*time.Second).Unix()) {
				suite.T().Fatalf("Expected expiry to be %d but was %d", expectedTime.Second(), opts.Expiry)
			}

			if opts.Expiry > uint32(expectedTime.Unix()) {
				suite.T().Fatalf("Expected expiry to be %d but was %d", expectedTime.Second(), opts.Expiry)
			}

			cb(&gocbcore.StoreResult{
				Cas: gocbcore.Cas(123),
			}, nil)
		}).
		Return(pendingOp, nil)

	col := suite.collection("mock", "", "", provider)

	res, err := col.Upsert("someid", "someval", &UpsertOptions{
		Expiry: 30 * 24 * time.Hour,
	})
	suite.Require().Nil(err, err)

	suite.Assert().Equal(Cas(123), res.Cas())
}

func (suite *UnitTestSuite) TestExpiryConversion31Days() {
	pendingOp := new(mockPendingOp)
	pendingOp.AssertNotCalled(suite.T(), "Cancel", mock.AnythingOfType("error"))

	expectedTime := time.Now().Add(31 * 24 * time.Hour)
	provider := new(mockKvProvider)
	provider.
		On("Set", mock.AnythingOfType("gocbcore.SetOptions"), mock.AnythingOfType("gocbcore.StoreCallback")).
		Run(func(args mock.Arguments) {
			opts := args.Get(0).(gocbcore.SetOptions)
			cb := args.Get(1).(gocbcore.StoreCallback)

			if opts.Expiry < uint32(expectedTime.Add(-1*time.Second).Unix()) {
				suite.T().Fatalf("Expected expiry to be %d but was %d", expectedTime.Second(), opts.Expiry)
			}

			if opts.Expiry > uint32(expectedTime.Unix()) {
				suite.T().Fatalf("Expected expiry to be %d but was %d", expectedTime.Second(), opts.Expiry)
			}

			cb(&gocbcore.StoreResult{
				Cas: gocbcore.Cas(123),
			}, nil)
		}).
		Return(pendingOp, nil)

	col := suite.collection("mock", "", "", provider)

	res, err := col.Upsert("someid", "someval", &UpsertOptions{
		Expiry: 31 * 24 * time.Hour,
	})
	suite.Require().Nil(err, err)

	suite.Assert().Equal(Cas(123), res.Cas())
}
