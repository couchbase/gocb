package gocb

import "gopkg.in/couchbaselabs/gojcbmock.v1"

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
	srvVer600   = NodeVersion{5, 5, 0, 0, "", false}
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
		}
	}

	return supported
}

func (c *testCluster) NotSupportsFeature(feature FeatureCode) bool {
	return !c.SupportsFeature(feature)
}
