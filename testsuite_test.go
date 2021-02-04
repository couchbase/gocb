package gocb

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	gojcbmock "github.com/couchbase/gocbcore/v9/jcbmock"
	"github.com/stretchr/testify/suite"
)

const (
	defaultServerVersion = "5.1.0"
)

var globalBucket *Bucket
var globalCollection *Collection
var globalScope *Scope
var globalCluster *testCluster

type IntegrationTestSuite struct {
	suite.Suite

	tracer *testTracer
}

func (suite *IntegrationTestSuite) BeforeTest(suiteName, testName string) {
	suite.tracer.Reset()
}

func (suite *IntegrationTestSuite) SetupSuite() {
	var err error
	var connStr string
	var mock *gojcbmock.Mock
	var auth PasswordAuthenticator
	if globalConfig.Server == "" {
		if globalConfig.Version != "" {
			panic("version cannot be specified with mock")
		}

		mpath, err := gojcbmock.GetMockPath()
		if err != nil {
			panic(err.Error())
		}

		globalConfig.Bucket = "default"
		mock, err = gojcbmock.NewMock(mpath, 4, 1, 64, []gojcbmock.BucketSpec{
			{Name: "default", Type: gojcbmock.BCouchbase},
		}...)
		if err != nil {
			panic(err.Error())
		}

		mock.Control(gojcbmock.NewCommand(gojcbmock.CSetCCCP,
			map[string]interface{}{"enabled": "true"}))
		mock.Control(gojcbmock.NewCommand(gojcbmock.CSetSASLMechanisms,
			map[string]interface{}{"mechs": []string{"SCRAM-SHA512"}}))

		globalConfig.Version = mock.Version()

		var addrs []string
		for _, mcport := range mock.MemcachedPorts() {
			addrs = append(addrs, fmt.Sprintf("127.0.0.1:%d", mcport))
		}
		connStr = fmt.Sprintf("couchbase://%s", strings.Join(addrs, ","))
		globalConfig.Server = connStr
		auth = PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		}
	} else {
		connStr = globalConfig.Server

		auth = PasswordAuthenticator{
			Username: globalConfig.User,
			Password: globalConfig.Password,
		}

		if globalConfig.Version == "" {
			globalConfig.Version = defaultServerVersion
		}
	}

	suite.tracer = newTestTracer()

	cluster, err := Connect(connStr, ClusterOptions{
		Authenticator: auth,
		Tracer:        suite.tracer,
	})
	if err != nil {
		panic(err.Error())
	}

	globalConfig.connstr = connStr
	globalConfig.auth = auth

	nodeVersion, err := newNodeVersion(globalConfig.Version, mock != nil)
	if err != nil {
		panic(err.Error())
	}

	globalCluster = &testCluster{
		Cluster:      cluster,
		Mock:         mock,
		Version:      nodeVersion,
		FeatureFlags: globalConfig.FeatureFlags,
	}

	globalBucket = globalCluster.Bucket(globalConfig.Bucket)

	if globalConfig.Scope != "" {
		globalScope = globalBucket.Scope(globalConfig.Scope)
	} else {
		globalScope = globalBucket.DefaultScope()
	}

	if globalConfig.Collection != "" {
		globalCollection = globalScope.Collection(globalConfig.Collection)
	} else {
		globalCollection = globalScope.Collection("_default")
	}

	if globalCluster.SupportsFeature(ReplicasFeature) {
		if globalCluster.SupportsFeature(DurabilityFeature) {
			suite.ensureReplicasUpEnhDura()
		} else {
			suite.ensureReplicasUpLegacyDura()
		}
	}
}

func (suite *IntegrationTestSuite) TearDownSuite() {
	err := globalCluster.Close(nil)
	suite.Require().Nil(err, err)
}

func (suite *IntegrationTestSuite) ensureReplicasUpEnhDura() {
	success := suite.tryUntil(time.Now().Add(30*time.Second), 50*time.Millisecond, func() bool {
		_, err := globalCollection.Upsert("ensurereplicasup", "test", &UpsertOptions{
			DurabilityLevel: DurabilityLevelPersistToMajority,
		})
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			return false
		}

		_, err = globalCollection.GetAnyReplica("ensurereplicasup", &GetAnyReplicaOptions{})
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			return false
		}

		return true
	})

	if !success {
		panic("Ensuring that replicas are up did not succeed in time")
	}
}

func (suite *IntegrationTestSuite) ensureReplicasUpLegacyDura() {
	success := suite.tryUntil(time.Now().Add(30*time.Second), 50*time.Millisecond, func() bool {
		_, err := globalCollection.Upsert("ensurereplicasup", "test", &UpsertOptions{
			PersistTo: 1,
		})
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			return false
		}

		_, err = globalCollection.GetAnyReplica("ensurereplicasup", &GetAnyReplicaOptions{})
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			return false
		}

		return true
	})

	if !success {
		panic("Ensuring that replicas are up did not succeed in time")
	}
}

func (suite *IntegrationTestSuite) createBreweryDataset(datasetName, service, scope, collection string) (int, error) {
	var dataset []testBreweryDocument
	err := loadJSONTestDataset(datasetName, &dataset)
	if err != nil {
		return 0, err
	}

	if scope == "" {
		scope = "_default"
	}
	if collection == "" {
		collection = "_default"
	}

	scp := globalBucket.Scope(scope)
	col := scp.Collection(collection)

	for i, doc := range dataset {
		doc.Service = service

		_, err := col.Upsert(fmt.Sprintf("%s%d", service, i), doc, nil)
		if err != nil {
			return 0, err
		}
	}

	return len(dataset), nil
}

func (suite *IntegrationTestSuite) tryUntil(deadline time.Time, interval time.Duration, fn func() bool) bool {
	for {
		success := fn()
		if success {
			return true
		}

		sleepDeadline := time.Now().Add(interval)
		if sleepDeadline.After(deadline) {
			return false
		}
		time.Sleep(sleepDeadline.Sub(time.Now()))
	}
}

func (suite *IntegrationTestSuite) skipIfUnsupported(code FeatureCode) {
	if globalCluster.NotSupportsFeature(code) {
		suite.T().Skipf("Skipping test because feature %s unsupported or disabled", code)
	}
}

type UnitTestSuite struct {
	suite.Suite
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		return
	}

	suite.Run(t, new(IntegrationTestSuite))
}

func TestUnit(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}

func (suite *UnitTestSuite) defaultTimeoutConfig() TimeoutsConfig {
	return TimeoutsConfig{
		KVTimeout:         1000 * time.Second,
		KVDurableTimeout:  1000 * time.Second,
		AnalyticsTimeout:  1000 * time.Second,
		QueryTimeout:      1000 * time.Second,
		SearchTimeout:     1000 * time.Second,
		ManagementTimeout: 1000 * time.Second,
		ViewTimeout:       1000 * time.Second,
	}
}

func (suite *UnitTestSuite) bucket(name string, timeouts TimeoutsConfig, cli *mockConnectionManager) *Bucket {
	b := &Bucket{
		bucketName: name,
		timeoutsConfig: TimeoutsConfig{
			KVTimeout:         timeouts.KVTimeout,
			KVDurableTimeout:  timeouts.KVDurableTimeout,
			AnalyticsTimeout:  timeouts.AnalyticsTimeout,
			QueryTimeout:      timeouts.QueryTimeout,
			SearchTimeout:     timeouts.SearchTimeout,
			ManagementTimeout: timeouts.ManagementTimeout,
			ViewTimeout:       timeouts.ViewTimeout,
		},
		transcoder:           NewJSONTranscoder(),
		retryStrategyWrapper: newRetryStrategyWrapper(NewBestEffortRetryStrategy(nil)),
		tracer:               &NoopTracer{},
		useServerDurations:   true,
		useMutationTokens:    true,

		connectionManager: cli,
	}

	return b
}

func (suite *UnitTestSuite) newCluster(cli connectionManager) *Cluster {
	cluster := clusterFromOptions(ClusterOptions{
		Tracer: &NoopTracer{},
	})
	cluster.connectionManager = cli

	return cluster
}

func (suite *UnitTestSuite) newScope(b *Bucket, name string) *Scope {
	return newScope(b, name)
}

func (suite *UnitTestSuite) mustConvertToBytes(val interface{}) []byte {
	b, err := json.Marshal(val)
	suite.Require().Nil(err)

	return b
}

func (suite *UnitTestSuite) kvProvider(provider kvProvider, err error) func() (kvProvider, error) {
	return func() (kvProvider, error) {
		return provider, err
	}
}

func (suite *UnitTestSuite) kvCapabilityProvider(provider kvCapabilityVerifier, err error) func() (kvCapabilityVerifier, error) {
	return func() (kvCapabilityVerifier, error) {
		return provider, err
	}
}

func (suite *UnitTestSuite) collection(bucket, scope, collection string, provider kvProvider) *Collection {
	return &Collection{
		bucket: &Bucket{bucketName: bucket},

		collectionName: collection,
		scope:          scope,

		getKvProvider: suite.kvProvider(provider, nil),
		timeoutsConfig: kvTimeoutsConfig{
			KVTimeout: 2500 * time.Millisecond,
		},
		transcoder:           NewJSONTranscoder(),
		tracer:               &NoopTracer{},
		retryStrategyWrapper: newRetryStrategyWrapper(NewBestEffortRetryStrategy(nil)),
	}
}
