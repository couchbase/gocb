package gocb

import (
	"errors"
	"math"
	"time"

	"github.com/couchbaselabs/gojcbmock"
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
	KeyValueFeature                       = FeatureCode(1)
	ViewFeature                           = FeatureCode(2)
	CccpFeature                           = FeatureCode(3)
	SslFeature                            = FeatureCode(4)
	DcpFeature                            = FeatureCode(5)
	SpatialViewFeature                    = FeatureCode(6)
	QueryFeature                          = FeatureCode(7)
	SubdocFeature                         = FeatureCode(8)
	KvErrorMapFeature                     = FeatureCode(9)
	RbacFeature                           = FeatureCode(10)
	SearchFeature                         = FeatureCode(11)
	EnhancedErrorsFeature                 = FeatureCode(12)
	SearchIndexFeature                    = FeatureCode(13)
	CompressionFeature                    = FeatureCode(14)
	ServerSideTracingFeature              = FeatureCode(15)
	AnalyticsFeature                      = FeatureCode(16)
	XattrFeature                          = FeatureCode(17)
	CollectionsFeature                    = FeatureCode(18)
	SubdocMockBugFeature                  = FeatureCode(19)
	AdjoinFeature                         = FeatureCode(20)
	ExpandMacrosFeature                   = FeatureCode(21)
	DurabilityFeature                     = FeatureCode(22)
	UserGroupFeature                      = FeatureCode(23)
	UserManagerFeature                    = FeatureCode(24)
	AnalyticsIndexFeature                 = FeatureCode(25)
	BucketMgrFeature                      = FeatureCode(26)
	SearchAnalyzeFeature                  = FeatureCode(27)
	AnalyticsIndexPendingMutationsFeature = FeatureCode(28)
	GetMetaFeature                        = FeatureCode(29)
)

type testClusterErrorWrap struct {
	InnerError error
	Message    string
}

func (e testClusterErrorWrap) Error() string {
	return e.Message + ": " + e.InnerError.Error()
}

func (e testClusterErrorWrap) Unwrap() error {
	return e.InnerError
}

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
		case SearchIndexFeature:
			supported = false
		case CompressionFeature:
			supported = !c.Version.Lower(mockVer1513)
		case ServerSideTracingFeature:
			supported = !c.Version.Lower(mockVer1515)
		case AnalyticsFeature:
			supported = false
		case QueryFeature:
			supported = false
		case XattrFeature:
			supported = false
		case CollectionsFeature:
			supported = false
		case SubdocMockBugFeature:
			supported = false
		case ExpandMacrosFeature:
			supported = false
		case DurabilityFeature:
			supported = false
		case UserGroupFeature:
			supported = false
		case UserManagerFeature:
			supported = false
		case AnalyticsIndexFeature:
			supported = false
		case BucketMgrFeature:
			supported = false
		case SearchAnalyzeFeature:
			supported = false
		case AnalyticsIndexPendingMutationsFeature:
			supported = false
		case GetMetaFeature:
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
		case QueryFeature:
			supported = !c.Version.Lower(srvVer400)
		case SubdocFeature:
			supported = !c.Version.Lower(srvVer450)
		case XattrFeature:
			supported = !c.Version.Lower(srvVer450)
		case KvErrorMapFeature:
			supported = !c.Version.Lower(srvVer500)
		case RbacFeature:
			supported = !c.Version.Lower(srvVer500)
		case SearchFeature:
			supported = !c.Version.Lower(srvVer500)
		case EnhancedErrorsFeature:
			supported = !c.Version.Lower(srvVer500)
		case SearchIndexFeature:
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
		case ExpandMacrosFeature:
			supported = !c.Version.Lower(srvVer450)
		case AdjoinFeature:
			supported = !c.Version.Equal(srvVer551) && !c.Version.Equal(srvVer552) && !c.Version.Equal(srvVer553)
		case DurabilityFeature:
			supported = !c.Version.Lower(srvVer650)
		case UserGroupFeature:
			supported = !c.Version.Lower(srvVer650)
		case UserManagerFeature:
			supported = !c.Version.Lower(srvVer500)
		case AnalyticsIndexFeature:
			supported = !c.Version.Lower(srvVer600)
		case BucketMgrFeature:
			supported = true
		case SearchAnalyzeFeature:
			supported = !c.Version.Lower(srvVer650)
		case AnalyticsIndexPendingMutationsFeature:
			supported = !c.Version.Lower(srvVer650)
		case GetMetaFeature:
			supported = true
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
	return bucket.DefaultCollection()
}

func (c *testCluster) CreateBreweryDataset(col *Collection) error {
	var dataset []testBreweryDocument
	err := loadJSONTestDataset("beer_sample_brewery_five", &dataset)
	if err != nil {
		return testClusterErrorWrap{
			InnerError: err,
			Message:    "could not read test dataset"}
	}

	for _, doc := range dataset {
		_, err = col.Upsert(doc.Name, doc, nil)
		if err != nil {
			return testClusterErrorWrap{
				InnerError: err,
				Message:    "could not create dataset"}
		}
	}

	return nil
}

func waitForCollection(bucket *Bucket, name string) error {
	timer := time.NewTimer(1 * time.Second)

	for {
		select {
		case <-timer.C:
			return errors.New("wait time for collection to become available expired")
		default:
			col := bucket.Collection(name)
			_, err := col.Get("test", nil)
			if err != nil {
				if errors.Is(err, ErrCollectionNotFound) {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				if !errors.Is(err, ErrDocumentNotFound) {
					return err
				}
			}

			return nil
		}
	}
}
