package gocb

type FeatureCode int

var (
	KeyValueFeature       = FeatureCode(1)
	ViewFeature           = FeatureCode(2)
	CccpFeature           = FeatureCode(3)
	SslFeature            = FeatureCode(4)
	DcpFeature            = FeatureCode(5)
	SpatialViewFeature    = FeatureCode(6)
	N1qlFeature           = FeatureCode(7)
	SubdocFeature         = FeatureCode(8)
	KvErrorMapFeature     = FeatureCode(9)
	RbacFeature           = FeatureCode(10)
	FtsFeature            = FeatureCode(11)
	EnhancedErrorsFeature = FeatureCode(12)
)

func (b *Bucket) NodeVersions() ([]NodeVersion, error) {
	return nil, nil
}

// TODO: I feel strange about this possibly erroring...
func (b *Bucket) ClusterVersion() (NodeVersion, error) {
	versions, err := b.NodeVersions()
	if err != nil {
		return NodeVersion{}, err
	}

	if len(versions) == 0 {
		return NodeVersion{}, ErrCliInternalError
	}

	minVersion := versions[0]

	for i := 1; i < len(versions); i++ {
		if minVersion.Higher(versions[i]) {
			minVersion = versions[i]
		}
	}

	return minVersion, nil
}

var (
	srvVer180 = NodeVersion{1, 8, 0, 0, ""}
	srvVer200 = NodeVersion{2, 0, 0, 0, ""}
	srvVer250 = NodeVersion{2, 5, 0, 0, ""}
	srvVer300 = NodeVersion{3, 0, 0, 0, ""}
	srvVer400 = NodeVersion{4, 0, 0, 0, ""}
	srvVer450 = NodeVersion{4, 5, 0, 0, ""}
	srvVer500 = NodeVersion{5, 0, 0, 0, ""}
)

func (b *Bucket) SupportsFeature(feature FeatureCode) (bool, error) {
	version, err := b.ClusterVersion()
	if err != nil {
		return false, err
	}

	switch feature {
	case KeyValueFeature:
		return !version.Lower(srvVer180), nil
	case ViewFeature:
		return !version.Lower(srvVer200), nil
	case CccpFeature:
		return !version.Lower(srvVer250), nil
	case SslFeature:
		return !version.Lower(srvVer300), nil
	case DcpFeature:
		return !version.Lower(srvVer400), nil
	case SpatialViewFeature:
		return !version.Lower(srvVer400), nil
	case N1qlFeature:
		return !version.Lower(srvVer400), nil
	case SubdocFeature:
		return !version.Lower(srvVer450), nil
	case KvErrorMapFeature:
		return !version.Lower(srvVer500), nil
	case RbacFeature:
		return !version.Lower(srvVer500), nil
	case FtsFeature:
		return !version.Lower(srvVer500), nil
	case EnhancedErrorsFeature:
		return !version.Lower(srvVer500), nil
	}

	return false, ErrInvalidFeature
}
