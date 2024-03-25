package gocb

import (
	"time"

	cavescli "github.com/couchbaselabs/gocaves/client"
)

var (
	srvVer180       = NodeVersion{1, 8, 0, 0, 0, "", false}
	srvVer200       = NodeVersion{2, 0, 0, 0, 0, "", false}
	srvVer250       = NodeVersion{2, 5, 0, 0, 0, "", false}
	srvVer300       = NodeVersion{3, 0, 0, 0, 0, "", false}
	srvVer400       = NodeVersion{4, 0, 0, 0, 0, "", false}
	srvVer450       = NodeVersion{4, 5, 0, 0, 0, "", false}
	srvVer500       = NodeVersion{5, 0, 0, 0, 0, "", false}
	srvVer550       = NodeVersion{5, 5, 0, 0, 0, "", false}
	srvVer551       = NodeVersion{5, 5, 1, 0, 0, "", false}
	srvVer552       = NodeVersion{5, 5, 2, 0, 0, "", false}
	srvVer553       = NodeVersion{5, 5, 3, 0, 0, "", false}
	srvVer600       = NodeVersion{6, 0, 0, 0, 0, "", false}
	srvVer650       = NodeVersion{6, 5, 0, 0, 0, "", false}
	srvVer650DP     = NodeVersion{6, 5, 0, 0, 0, "dp", false}
	srvVer660       = NodeVersion{6, 6, 0, 0, 0, "", false}
	srvVer700       = NodeVersion{7, 0, 0, 0, 0, "", false}
	srvVer710       = NodeVersion{7, 1, 0, 0, 0, "", false}
	srvVer711       = NodeVersion{7, 1, 1, 0, 0, "", false}
	srvVer710DP     = NodeVersion{7, 1, 0, 0, 0, "dp", false}
	srvVer720       = NodeVersion{7, 2, 0, 0, 0, "", false}
	protostellarVer = NodeVersion{7, 5, 0, 0, ProtostellarNodeEdition, "", false}
	srvVer750       = NodeVersion{7, 5, 0, 0, 0, "", false}
	srvVer760       = NodeVersion{7, 6, 0, 0, 0, "", false}
	mockVer156      = NodeVersion{1, 5, 6, 0, 0, "", true}
	mockVer1513     = NodeVersion{1, 5, 13, 0, 0, "", true}
	mockVer1515     = NodeVersion{1, 5, 15, 0, 0, "", true}
)

type FeatureCode string

var (
	KeyValueFeature                           = FeatureCode("keyvalue")
	ViewFeature                               = FeatureCode("view")
	QueryFeature                              = FeatureCode("query")
	ClusterLevelQueryFeature                  = FeatureCode("clusterQuery")
	SubdocFeature                             = FeatureCode("subdoc")
	SearchFeature                             = FeatureCode("search")
	SearchIndexFeature                        = FeatureCode("searchindex")
	AnalyticsFeature                          = FeatureCode("analytics")
	XattrFeature                              = FeatureCode("xattrs")
	CollectionsFeature                        = FeatureCode("collections")
	CollectionsManagerMaxCollectionsFeature   = FeatureCode("collectionsmgrmaxcollections")
	CollectionsManagerFeature                 = FeatureCode("collectionsmgr")
	AdjoinFeature                             = FeatureCode("adjoin")
	ExpandMacrosFeature                       = FeatureCode("expandmacros")
	DurabilityFeature                         = FeatureCode("durability")
	UserGroupFeature                          = FeatureCode("usergroup")
	UserManagerFeature                        = FeatureCode("usermanager")
	AnalyticsIndexFeature                     = FeatureCode("analyticsindex")
	BucketMgrFeature                          = FeatureCode("bucketmgr")
	SearchAnalyzeFeature                      = FeatureCode("searchanalyze")
	AnalyticsIndexPendingMutationsFeature     = FeatureCode("analyticspending")
	GetMetaFeature                            = FeatureCode("getmeta")
	PingFeature                               = FeatureCode("ping")
	ViewIndexUpsertBugFeature                 = FeatureCode("viewinsertupsertbug")
	ReplicasFeature                           = FeatureCode("replicas")
	PingAnalyticsFeature                      = FeatureCode("pinganalytics")
	WaitUntilReadyFeature                     = FeatureCode("waituntilready")
	WaitUntilReadyFastFailFeature             = FeatureCode("waituntilreadyfastfail")
	WaitUntilReadyAuthFailFeature             = FeatureCode("waituntilreadyauthfail")
	WaitUntilReadyClusterFeature              = FeatureCode("waituntilreadycluster")
	QueryIndexFeature                         = FeatureCode("queryindex")
	CollectionsQueryFeature                   = FeatureCode("collectionsquery")
	CollectionsAnalyticsFeature               = FeatureCode("collectionsanalytics")
	BucketMgrDurabilityFeature                = FeatureCode("bucketmgrdura")
	AnalyticsIndexLinksFeature                = FeatureCode("analyticsindexlinks")
	AnalyticsIndexLinksScopesFeature          = FeatureCode("analyticsindexscopeslinks")
	EnhancedPreparedStatementsFeature         = FeatureCode("enhancedpreparedstatements")
	PreserveExpiryFeature                     = FeatureCode("preserveexpiry")
	EventingFunctionManagerFeature            = FeatureCode("eventingmanagement")
	RateLimitingFeature                       = FeatureCode("ratelimits")
	StorageBackendFeature                     = FeatureCode("storagebackend")
	HLCFeature                                = FeatureCode("hlc")
	CustomConflictResolutionFeature           = FeatureCode("customconflictresolution")
	QueryImprovedErrorsFeature                = FeatureCode("queryimprovederrors")
	UserManagerChangePasswordFeature          = FeatureCode("usermanagerchangepassword")
	TransactionsFeature                       = FeatureCode("transactions")
	TransactionsBulkFeature                   = FeatureCode("transactionsbulk")
	TransactionsQueryFeature                  = FeatureCode("transactionsquery")
	TransactionsRemoveLocationFeature         = FeatureCode("transactionsremovelocation")
	TransactionsSingleQueryExistsErrorFeature = FeatureCode("transactionssinglequeryexists")
	TransactionsCustomMetadataFeature         = FeatureCode("transactionscustommetadata")
	EventingFunctionManagerMB52649Feature     = FeatureCode("eventingmanagementmb52649")
	EventingFunctionManagerMB52572Feature     = FeatureCode("eventingmanagementmb52572")
	RangeScanFeature                          = FeatureCode("rangescan")
	GetExpiryUsingLookupInFeature             = FeatureCode("getexpiryusinglookupin")
	KeyValueBulkFeature                       = FeatureCode("keyvaluebulk")
	KeyValueProjectionsFeature                = FeatureCode("keyvalueprojections")
	NodesMetadataFeature                      = FeatureCode("nodesmetadata")
	SubdocReplicaReadsFeature                 = FeatureCode("subdocreplicas")
	HistoryRetentionFeature                   = FeatureCode("historyretention")
	QueryMB57673Feature                       = FeatureCode("mb57673")
	FlushBucketFeature                        = FeatureCode("flushbucket")
	MemcachedBucketFeature                    = FeatureCode("memcachedbucket")
	UnlockMissingDocFailFeature               = FeatureCode("unlockmissingdocfail")
	NotLockedFeature                          = FeatureCode("notlocked")
	CollectionMaxExpiryNoExpiryFeature        = FeatureCode("collectionmaxexpirynoexpiry")
	CollectionUpdateMaxExpiryFeature          = FeatureCode("collectionupdatemaxexpiry")
	ScopeSearchIndexFeature                   = FeatureCode("scopesearchindex")
	ScopeSearchFeature                        = FeatureCode("scopesearch")
	VectorSearchFeature                       = FeatureCode("vectorsearch")
	ScopeEventingFunctionManagerFeature       = FeatureCode("scopeeventingmanagement")
)

type TestFeatureFlag struct {
	Enabled bool
	Feature FeatureCode
}

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
	Mock    *cavescli.Client
	RunID   string
	Version *NodeVersion

	FeatureFlags []TestFeatureFlag
}

func (c *testCluster) isMock() bool {
	return c.Mock != nil
}

func (c *testCluster) waitUntilReadyTimeout() time.Duration {
	if c.Version.Equal(srvVer750) {
		return 30 * time.Second
	} else {
		return 7 * time.Second
	}
}

func (c *testCluster) txnCleanupTimeout() time.Duration {
	if c.Version.Equal(srvVer750) {
		return 60 * time.Second
	} else {
		return 10 * time.Second
	}
}

func (c *testCluster) IsProtostellar() bool {
	return c.Version.Edition == ProtostellarNodeEdition
}

func (c *testCluster) SupportsFeature(feature FeatureCode) bool {
	featureFlagValue := 0
	for _, featureFlag := range c.FeatureFlags {
		if featureFlag.Feature == feature || featureFlag.Feature == "*" {
			if featureFlag.Enabled {
				featureFlagValue = +1
			} else {
				featureFlagValue = -1
			}
		}
	}
	if featureFlagValue == -1 {
		return false
	} else if featureFlagValue == +1 {
		return true
	}

	supported := false
	if c.Version.IsMock {
		supported = true

		switch feature {
		case SearchIndexFeature:
			supported = false
		case AnalyticsFeature:
			supported = false
		case QueryFeature:
			supported = false
		case ClusterLevelQueryFeature:
			supported = false
		case SearchFeature:
			supported = false
		case UserGroupFeature:
			supported = false
		case AnalyticsIndexFeature:
			supported = false
		case BucketMgrFeature:
			supported = false
		case SearchAnalyzeFeature:
			supported = false
		case AnalyticsIndexPendingMutationsFeature:
			supported = false
		case QueryIndexFeature:
			supported = false
		case CollectionsQueryFeature:
			supported = false
		case CollectionsAnalyticsFeature:
			supported = false
		case BucketMgrDurabilityFeature:
			supported = false
		case AnalyticsIndexLinksFeature:
			supported = false
		case AnalyticsIndexLinksScopesFeature:
			supported = false
		case EnhancedPreparedStatementsFeature:
			supported = false
		case PreserveExpiryFeature:
			supported = false
		case EventingFunctionManagerFeature:
			supported = false
		case RateLimitingFeature:
			supported = false
		case StorageBackendFeature:
			supported = false
		case TransactionsBulkFeature:
			supported = false
		case CustomConflictResolutionFeature:
			supported = false
		case QueryImprovedErrorsFeature:
			supported = false
		case TransactionsQueryFeature:
			supported = false
		case UserManagerChangePasswordFeature:
			supported = false
		case TransactionsRemoveLocationFeature:
			supported = false
		case TransactionsSingleQueryExistsErrorFeature:
			supported = false
		case TransactionsCustomMetadataFeature:
			supported = false
		case RangeScanFeature:
			supported = false
		case SubdocReplicaReadsFeature:
			supported = false
		case HistoryRetentionFeature:
			supported = false
		case FlushBucketFeature:
			supported = false
		case MemcachedBucketFeature:
			supported = false
		case NotLockedFeature:
			supported = false
		case CollectionUpdateMaxExpiryFeature:
			supported = false
		case CollectionMaxExpiryNoExpiryFeature:
			supported = false
		case ScopeSearchIndexFeature:
			supported = false
		case ScopeSearchFeature:
			supported = false
		case VectorSearchFeature:
			supported = false
		case ScopeEventingFunctionManagerFeature:
			supported = false
		}
	} else {
		switch feature {
		case KeyValueFeature:
			supported = !c.Version.Lower(srvVer180)
		case ViewFeature:
			supported = !c.Version.Lower(srvVer200) && !c.Version.Equal(srvVer650DP) && !c.Version.Equal(srvVer750) && !c.Version.Equal(protostellarVer)
		case QueryFeature:
			supported = !c.Version.Lower(srvVer400) && !c.Version.Equal(srvVer650DP)
		case ClusterLevelQueryFeature:
			supported = !c.Version.Lower(srvVer400) && !c.Version.Equal(srvVer650DP) && !c.Version.Equal(srvVer750)
		case SubdocFeature:
			supported = !c.Version.Lower(srvVer450)
		case XattrFeature:
			supported = !c.Version.Lower(srvVer450)
		case SearchFeature:
			supported = !c.Version.Lower(srvVer500) && !c.Version.Equal(srvVer650DP)
		case SearchIndexFeature:
			supported = !c.Version.Lower(srvVer500) && !c.Version.Equal(srvVer650DP) && !c.Version.Equal(srvVer750)
		case AnalyticsFeature:
			supported = !c.Version.Lower(srvVer600) && !c.Version.Equal(srvVer650DP) && !c.Version.Equal(srvVer750) && !c.Version.Equal(protostellarVer)
		case CollectionsFeature:
			supported = c.Version.Equal(srvVer650DP) || !c.Version.Lower(srvVer700)
		case ExpandMacrosFeature:
			supported = !c.Version.Lower(srvVer450) && !c.Version.Equal(protostellarVer)
		case AdjoinFeature:
			supported = !c.Version.Equal(srvVer551) && !c.Version.Equal(srvVer552) && !c.Version.Equal(srvVer553)
		case DurabilityFeature:
			supported = !c.Version.Lower(srvVer650)
		case UserGroupFeature:
			supported = !c.Version.Lower(srvVer650) && !c.Version.Equal(srvVer750) && !c.Version.Equal(protostellarVer)
		case UserManagerFeature:
			supported = !c.Version.Lower(srvVer500) && !c.Version.Equal(srvVer750) && !c.Version.Equal(protostellarVer)
		case AnalyticsIndexFeature:
			supported = !c.Version.Lower(srvVer600) && !c.Version.Equal(srvVer650DP) && !c.Version.Equal(srvVer750) && !c.Version.Equal(protostellarVer)
		case BucketMgrFeature:
			supported = !c.Version.Equal(srvVer750)
		case SearchAnalyzeFeature:
			supported = !c.Version.Lower(srvVer650) && !c.Version.Equal(srvVer650DP)
		case AnalyticsIndexPendingMutationsFeature:
			supported = !c.Version.Lower(srvVer650) && !c.Version.Equal(srvVer650DP) && !c.Version.Equal(protostellarVer)
		case GetMetaFeature:
			supported = true
		case PingFeature:
			supported = !c.Version.Equal(srvVer750) && !c.Version.Equal(protostellarVer)
		case ViewIndexUpsertBugFeature:
			supported = !c.Version.Equal(srvVer650)
		case PingAnalyticsFeature:
			supported = !c.Version.Lower(srvVer600) && !c.Version.Equal(protostellarVer)
		case WaitUntilReadyFeature:
			supported = true
		case WaitUntilReadyFastFailFeature:
			supported = !c.Version.Equal(protostellarVer)
		case WaitUntilReadyAuthFailFeature:
			supported = !c.Version.Equal(protostellarVer)
		case WaitUntilReadyClusterFeature:
			supported = !c.Version.Lower(srvVer650) && !c.Version.Equal(srvVer750) && !c.Version.Equal(protostellarVer)
		case ReplicasFeature:
			supported = true
		case QueryIndexFeature:
			supported = !c.Version.Equal(srvVer650DP)
		case CollectionsQueryFeature:
			supported = !c.Version.Lower(srvVer700)
		case CollectionsAnalyticsFeature:
			supported = !c.Version.Lower(srvVer700) && !c.Version.Equal(srvVer750) && !c.Version.Equal(protostellarVer)
		case CollectionsManagerFeature:
			supported = !c.Version.Lower(srvVer700)
		case CollectionsManagerMaxCollectionsFeature:
			supported = false
		case BucketMgrDurabilityFeature:
			supported = !c.Version.Lower(srvVer660)
		case AnalyticsIndexLinksFeature:
			supported = !c.Version.Lower(srvVer660) && !c.Version.Equal(protostellarVer)
		case AnalyticsIndexLinksScopesFeature:
			supported = !c.Version.Lower(srvVer700) && !c.Version.Equal(protostellarVer)
		case EnhancedPreparedStatementsFeature:
			supported = !c.Version.Lower(srvVer650)
		case PreserveExpiryFeature:
			supported = !c.Version.Lower(srvVer700)
		case EventingFunctionManagerFeature:
			supported = !c.Version.Lower(srvVer700) && !c.Version.Equal(srvVer750) && !c.Version.Equal(protostellarVer)
		case StorageBackendFeature:
			supported = !c.Version.Lower(srvVer710) && (c.Version.Edition != CommunityNodeEdition)
		case HLCFeature:
			supported = !c.Version.Lower(srvVer660)
		case TransactionsFeature:
			supported = !c.Version.Lower(srvVer700) && !c.Version.Equal(protostellarVer)
		case TransactionsQueryFeature:
			supported = !c.Version.Lower(srvVer700) && !c.Version.Equal(protostellarVer)
		case TransactionsBulkFeature:
			supported = !c.Version.Lower(srvVer700) && !c.Version.Equal(protostellarVer)
		case CustomConflictResolutionFeature:
			supported = c.Version.Equal(srvVer710DP) && !c.Version.Equal(protostellarVer)
		case QueryImprovedErrorsFeature:
			supported = !c.Version.Lower(srvVer710) && !c.Version.Equal(protostellarVer)
		case UserManagerChangePasswordFeature:
			supported = !c.Version.Lower(srvVer600) && !c.Version.Equal(protostellarVer)
		case TransactionsRemoveLocationFeature:
			supported = !c.Version.Lower(srvVer700) && !c.Version.Equal(protostellarVer)
		case TransactionsSingleQueryExistsErrorFeature:
			supported = !c.Version.Lower(srvVer710) && !c.Version.Equal(protostellarVer)
		case EventingFunctionManagerMB52649Feature:
			supported = !c.Version.Equal(srvVer711) && !c.Version.Equal(protostellarVer)
		case EventingFunctionManagerMB52572Feature:
			supported = !c.Version.Equal(srvVer711) && !c.Version.Equal(protostellarVer)
		case RangeScanFeature:
			supported = !c.Version.Lower(srvVer750) && !c.Version.Equal(protostellarVer)
		case GetExpiryUsingLookupInFeature:
			supported = !c.Version.Equal(protostellarVer)
		case KeyValueBulkFeature:
			supported = true
		case KeyValueProjectionsFeature:
			supported = true
		case NodesMetadataFeature:
			supported = !c.Version.Equal(protostellarVer)
		case SubdocReplicaReadsFeature:
			supported = !c.Version.Lower(srvVer750) && !c.Version.Equal(protostellarVer)
		case HistoryRetentionFeature:
			supported = !c.Version.Lower(srvVer720) && !c.Version.Equal(protostellarVer)
		case QueryMB57673Feature:
			supported = !c.Version.Equal(srvVer720)
		case FlushBucketFeature:
			supported = !c.Version.Equal(protostellarVer)
		case MemcachedBucketFeature:
			supported = !c.Version.Equal(protostellarVer)
		case TransactionsCustomMetadataFeature:
			supported = !c.Version.Lower(srvVer700) && !c.Version.Equal(protostellarVer) && !c.Version.Equal(srvVer750)
		case UnlockMissingDocFailFeature:
			supported = !c.Version.Equal(srvVer750)
		case NotLockedFeature:
			supported = !c.Version.Lower(srvVer760)
		case CollectionMaxExpiryNoExpiryFeature:
			supported = !c.Version.Lower(srvVer760)
		case CollectionUpdateMaxExpiryFeature:
			supported = !c.Version.Lower(srvVer760)
		case ScopeSearchIndexFeature:
			supported = !c.Version.Lower(srvVer760)
		case ScopeSearchFeature:
			supported = !c.Version.Lower(srvVer760) && !c.Version.Equal(protostellarVer)
		case VectorSearchFeature:
			supported = !c.Version.Lower(srvVer760) && !c.Version.Equal(protostellarVer)
		case ScopeEventingFunctionManagerFeature:
			supported = !c.Version.Lower(srvVer710) && !c.Version.Equal(protostellarVer)
		}
	}

	return supported
}

func (c *testCluster) NotSupportsFeature(feature FeatureCode) bool {
	return !c.SupportsFeature(feature)
}

func (c *testCluster) TimeTravel(waitDura time.Duration) {
	if c.Mock != nil {
		c.Mock.TimeTravelRun(c.RunID, waitDura)
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
