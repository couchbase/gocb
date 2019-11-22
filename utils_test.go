package gocb

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/pkg/errors"
)

type testBeerDocument struct {
	ABV         float32 `json:"abv,omitempty"`
	BreweryID   string  `json:"brewery_id,omitempty"`
	Category    string  `json:"category,omitempty"`
	Description string  `json:"description,omitempty"`
	IBU         int     `json:"IBU,omitempty"`
	Name        string  `json:"name,omitempty"`
	SRM         int     `json:"srm,omitempty"`
	Style       string  `json:"style,omitempty"`
	Type        string  `json:"type,omitempty"`
	UPC         int     `json:"upc,omitempty"`
	Updated     string  `json:"updated,omitempty"`
}

type testBreweryGeo struct {
	Accuracy string  `json:"accuracy,omitempty"`
	Lat      float32 `json:"lat,omitempty"`
	Lon      float32 `json:"lon,omitempty"`
}

type testBreweryDocument struct {
	City        string         `json:"city,omitempty"`
	Code        string         `json:"code,omitempty"`
	Country     string         `json:"country,omitempty"`
	Description string         `json:"description,omitempty"`
	Geo         testBreweryGeo `json:"geo,omitempty"`
	Name        string         `json:"name,omitempty"`
	Phone       string         `json:"phone,omitempty"`
	State       string         `json:"state,omitempty"`
	Type        string         `json:"type,omitempty"`
	Updated     string         `json:"updated,omitempty"`
	Website     string         `json:"website,omitempty"`
}

type testMetadata struct {
}

func loadRawTestDataset(dataset string) ([]byte, error) {
	return ioutil.ReadFile("testdata/" + dataset + ".json")
}

func loadJSONTestDataset(dataset string, valuePtr interface{}) error {
	bytes, err := loadRawTestDataset(dataset)
	if err != nil {
		return err
	}

	err = json.Unmarshal(bytes, &valuePtr)
	if err != nil {
		return err
	}

	return nil
}

func loadSDKTestDataset(dataset string) (*testMetadata, []byte, error) {
	var testdata map[string]interface{}
	err := loadJSONTestDataset("sdk-testcases/"+dataset, &testdata)
	if err != nil {
		return nil, nil, err
	}

	_, ok := testdata["metadata"]
	if !ok {
		return nil, nil, errors.New("test dataset missing metadata")
	}

	data, ok := testdata["data"]
	if !ok {
		return nil, nil, errors.New("test dataset missing data")
	}

	b, err := json.Marshal(data)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not remarshal test data")
	}

	return nil, b, nil
}

func marshal(t *testing.T, value interface{}) []byte {
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Could not marshal value: %v", err)
	}

	return b
}

type testReadCloser struct {
	io.Reader
	closeErr error
}

func (trc *testReadCloser) Close() error {
	return trc.closeErr
}

// Not a test, just gets a collection instance.
func testGetCollection(t *testing.T, provider *mockKvProvider) *Collection {
	clients := make(map[string]client)
	cli := &mockClient{
		bucketName:        "mock",
		collectionID:      0,
		scopeID:           0,
		useMutationTokens: true,
		mockKvProvider:    provider,
	}
	clients["mock"] = cli

	b := &Bucket{
		sb: stateBlock{
			clientStateBlock: clientStateBlock{
				BucketName: "mock",
			},

			cachedClient:     cli,
			AnalyticsTimeout: 75000 * time.Millisecond,
			QueryTimeout:     75000 * time.Millisecond,
			SearchTimeout:    75000 * time.Millisecond,
			ViewTimeout:      75000 * time.Millisecond,
			KvTimeout:        2500 * time.Millisecond,
			Transcoder:       NewJSONTranscoder(),
			Tracer:           &noopTracer{},
		},
	}
	col := b.DefaultCollection()
	return col
}
