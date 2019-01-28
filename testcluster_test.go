package gocb

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/couchbase/gocbcore.v8"

	"gopkg.in/couchbaselabs/gojcbmock.v1"
)

type FeatureCode int

var (
	srvVer180   = NodeVersion{1, 8, 0, 0, "", false}
	srvVer200   = NodeVersion{2, 0, 0, 0, "", false}
	srvVer250   = NodeVersion{2, 5, 0, 0, "", false}
	srvVer300   = NodeVersion{3, 0, 0, 0, "", false}
	srvVer400   = NodeVersion{4, 0, 0, 0, "", false}
	srvVer450   = NodeVersion{4, 5, 0, 0, "", false}
	srvVer500   = NodeVersion{5, 0, 0, 0, "", false}
	srvVer550   = NodeVersion{5, 5, 0, 0, "", false}
	srvVer551   = NodeVersion{5, 5, 1, 0, "", false}
	srvVer552   = NodeVersion{5, 5, 2, 0, "", false}
	srvVer553   = NodeVersion{5, 5, 3, 0, "", false}
	srvVer600   = NodeVersion{6, 0, 0, 0, "", false}
	srvVer650   = NodeVersion{6, 5, 0, 0, "", false}
	mockVer156  = NodeVersion{1, 5, 6, 0, "", true}
	mockVer1513 = NodeVersion{1, 5, 13, 0, "", true}
	mockVer1515 = NodeVersion{1, 5, 15, 0, "", true}
)

var (
	KeyValueFeature          = FeatureCode(1)
	ViewFeature              = FeatureCode(2)
	CccpFeature              = FeatureCode(3)
	SslFeature               = FeatureCode(4)
	DcpFeature               = FeatureCode(5)
	SpatialViewFeature       = FeatureCode(6)
	N1qlFeature              = FeatureCode(7)
	SubdocFeature            = FeatureCode(8)
	KvErrorMapFeature        = FeatureCode(9)
	RbacFeature              = FeatureCode(10)
	FtsFeature               = FeatureCode(11)
	EnhancedErrorsFeature    = FeatureCode(12)
	FtsIndexFeature          = FeatureCode(13)
	CompressionFeature       = FeatureCode(14)
	ServerSideTracingFeature = FeatureCode(15)
	AnalyticsFeature         = FeatureCode(16)
	XattrFeature             = FeatureCode(17)
	CollectionsFeature       = FeatureCode(18)
	SubdocMockBugFeature     = FeatureCode(19)
	AdjoinFeature            = FeatureCode(20)
)

type testCluster struct {
	*Cluster
	Mock    *gojcbmock.Mock
	Version *NodeVersion
}

func (c *testCluster) isMock() bool {
	return c.Mock != nil
}

func (c *testCluster) SupportsFeature(feature FeatureCode) bool {
	supported := false
	if c.Version.IsMock {
		supported = true

		switch feature {
		case RbacFeature:
			supported = !c.Version.Lower(mockVer156)
		case FtsIndexFeature:
			supported = false
		case CompressionFeature:
			supported = !c.Version.Lower(mockVer1513)
		case ServerSideTracingFeature:
			supported = !c.Version.Lower(mockVer1515)
		case AnalyticsFeature:
			supported = false
		case N1qlFeature:
			supported = false
		case XattrFeature:
			supported = false
		case CollectionsFeature:
			supported = false
		case SubdocMockBugFeature:
			supported = false
		}
	} else {
		switch feature {
		case KeyValueFeature:
			supported = !c.Version.Lower(srvVer180)
		case ViewFeature:
			supported = !c.Version.Lower(srvVer200)
		case CccpFeature:
			supported = !c.Version.Lower(srvVer250)
		case SslFeature:
			supported = !c.Version.Lower(srvVer300)
		case DcpFeature:
			supported = !c.Version.Lower(srvVer400)
		case SpatialViewFeature:
			supported = !c.Version.Lower(srvVer400)
		case N1qlFeature:
			supported = !c.Version.Lower(srvVer400)
		case SubdocFeature:
			supported = !c.Version.Lower(srvVer450)
		case XattrFeature:
			supported = !c.Version.Lower(srvVer450)
		case KvErrorMapFeature:
			supported = !c.Version.Lower(srvVer500)
		case RbacFeature:
			supported = !c.Version.Lower(srvVer500)
		case FtsFeature:
			supported = !c.Version.Lower(srvVer500)
		case EnhancedErrorsFeature:
			supported = !c.Version.Lower(srvVer500)
		case FtsIndexFeature:
			supported = !c.Version.Lower(srvVer500)
		case CompressionFeature:
			supported = !c.Version.Lower(srvVer550)
		case ServerSideTracingFeature:
			supported = !c.Version.Lower(srvVer550)
		case AnalyticsFeature:
			supported = !c.Version.Lower(srvVer600)
		case CollectionsFeature:
			supported = !c.Version.Lower(srvVer650)
		case SubdocMockBugFeature:
			supported = true
		case AdjoinFeature:
			return !c.Version.Equal(srvVer551) && !c.Version.Equal(srvVer552) && !c.Version.Equal(srvVer553)
		}
	}

	return supported
}

func (c *testCluster) NotSupportsFeature(feature FeatureCode) bool {
	return !c.SupportsFeature(feature)
}

func (c *testCluster) TimeTravel(waitDura time.Duration) {
	if c.isMock() {
		waitSecs := int(math.Ceil(float64(waitDura) / float64(time.Second)))
		c.Mock.Control(gojcbmock.NewCommand(gojcbmock.CTimeTravel, map[string]interface{}{
			"Offset": waitSecs,
		}))
	} else {
		time.Sleep(waitDura)
	}
}

func (c *testCluster) DefaultCollection(bucket *Bucket) *Collection {
	return bucket.DefaultCollection(nil)
}

func (c *testCluster) CreateBreweryDataset(col *Collection) error {
	var dataset []testBreweryDocument
	err := loadJSONTestDataset("beer_sample_brewery_five", &dataset)
	if err != nil {
		return errors.Wrap(err, "could not read test dataset")
	}

	for _, doc := range dataset {
		_, err = col.Upsert(doc.Name, doc, nil)
		if err != nil {
			return errors.Wrap(err, "could not create dataset")
		}
	}

	return nil
}

// These functions are likely temporary.

func testCreateCollection(name, scopeName string, bucket *Bucket, cli client) (*Collection, error) {
	return testCreate(name, scopeName, bucket, cli)
}

func testCreate(name, scopeName string, bucket *Bucket, cli client) (*Collection, error) {
	provider, err := cli.getHTTPProvider()
	if err != nil {
		return nil, err
	}
	data := url.Values{}
	data.Set("name", name)

	req := &gocbcore.HttpRequest{
		Service: gocbcore.MgmtService,
		Path:    fmt.Sprintf("/pools/default/buckets/%s/collections/%s/", bucket.Name(), scopeName),
		Method:  "POST",
		Body:    []byte(data.Encode()),
		Headers: make(map[string]string),
	}

	req.Headers["Content-Type"] = "application/x-www-form-urlencoded"

	resp, err := provider.DoHttpRequest(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("could not create collection, status code: %d", resp.StatusCode)
	}

	respBody := struct {
		Uid uint64 `json:"uid"`
	}{}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&respBody)
	if err != nil {
		return nil, err
	}
	err = resp.Body.Close()
	if err != nil {
		return nil, err
	}

	timer := time.NewTimer(20 * time.Second)

	for {
		select {
		case <-timer.C:
			return nil, errors.New("wait time for collection to become available expired")
		default:
			col := bucket.Collection(scopeName, name, nil)
			_, err := col.Get("test", nil)
			if err != nil {
				if IsCollectionMissingError(err) {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				if !IsKeyNotFoundError(err) {
					return nil, err
				}
			}

			return col, nil
		}
	}
}

func testDeleteCollection(name, scopeName string, bucket *Bucket, cli client, waitForDeletion bool) (uint64, error) {
	provider, err := cli.getHTTPProvider()
	if err != nil {
		return 0, nil
	}
	data := url.Values{}
	data.Set("name", name)

	req := &gocbcore.HttpRequest{
		Service: gocbcore.MgmtService,
		Path:    fmt.Sprintf("/pools/default/buckets/%s/collections/%s/%s", bucket.Name(), scopeName, name),
		Method:  "DELETE",
		Headers: make(map[string]string),
	}

	resp, err := provider.DoHttpRequest(req)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("could not delete, status code: %d", resp.StatusCode)
	}

	respBody := struct {
		Uid uint64 `json:"uid"`
	}{}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&respBody)
	if err != nil {
		return 0, err
	}
	err = resp.Body.Close()
	if err != nil {
		return 0, err
	}

	if waitForDeletion {
		timer := time.NewTimer(10 * time.Second)

		for {
			select {
			case <-timer.C:
				return 0, errors.New("wait time for collection to become unavailable expired")
			default:
				col := bucket.Collection(scopeName, name, nil)
				_, err := col.Get("test", nil)

				if err == nil || IsKeyNotFoundError(err) {
					continue
				}
			}

			return respBody.Uid, nil
		}
	} else {
		return respBody.Uid, nil
	}
}
