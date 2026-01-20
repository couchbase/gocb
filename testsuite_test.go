package gocb

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10"

	"github.com/stretchr/testify/suite"
)

const (
	defaultServerVersion = "5.1.0"
)

var transactionsTestKeys map[string][]string

type IntegrationTestSuite struct {
	suite.Suite
}

func (suite *IntegrationTestSuite) BeforeTest(suiteName, testName string) {
	globalTracer.Reset()
	globalMeter.Reset()
	if globalCluster.SupportsFeature(TransactionsBulkFeature) {
		suite.generateTransactionsKeys()
	}
}

func (suite *IntegrationTestSuite) SetupSuite() {
	if !globalCluster.IsProtostellar() {
		router, err := globalBucket.Internal().IORouter()
		suite.Require().NoError(err)

		snapCh := make(chan *gocbcore.ConfigSnapshot, 1)
		_, err = router.WaitForConfigSnapshot(time.Now().Add(15*time.Second), gocbcore.WaitForConfigSnapshotOptions{}, func(result *gocbcore.WaitForConfigSnapshotResult, err error) {
			if err != nil {
				log.Fatalf("Error geting config snapshot: %v", err)
			}

			snapCh <- result.Snapshot
		})
		suite.Require().NoError(err)

		snap := <-snapCh
		replicas, err := snap.NumReplicas()
		suite.Require().NoError(err)

		if replicas == 0 {
			suite.T().Logf("Unless explicitly set disabling replicas, durability, and transactions as no replicas on cluster")
			var replicas bool
			var dura bool
			var transactions bool
			for _, featureFlag := range globalCluster.FeatureFlags {
				if featureFlag.Feature == ReplicasFeature {
					replicas = true
					featureFlag.Enabled = false
				} else if featureFlag.Feature == DurabilityFeature {
					dura = true
					featureFlag.Enabled = false
				} else if featureFlag.Feature == TransactionsFeature {
					transactions = true
					featureFlag.Enabled = false
				}
			}
			if !replicas {
				globalCluster.FeatureFlags = append(globalCluster.FeatureFlags, TestFeatureFlag{Enabled: false, Feature: ReplicasFeature})
			}
			if !dura {
				globalCluster.FeatureFlags = append(globalCluster.FeatureFlags, TestFeatureFlag{Enabled: false, Feature: DurabilityFeature})
			}
			if !transactions {
				globalCluster.FeatureFlags = append(globalCluster.FeatureFlags, TestFeatureFlag{Enabled: false, Feature: TransactionsFeature})
			}
			return
		}
	}

	if globalCluster.SupportsFeature(ReplicasFeature) {
		if globalCluster.SupportsFeature(DurabilityFeature) {
			suite.ensureReplicasUpEnhDura()
		} else {
			suite.ensureReplicasUpLegacyDura()
		}
	}

	suite.setClusterName()
}

func (suite *IntegrationTestSuite) generateTransactionsKeys() {
	transactionsTestKeys = make(map[string][]string)

	k00tok19 := make([]string, 20)
	for i := range k00tok19 {
		k00tok19[i] = fmt.Sprintf("k%02d", i)
	}

	k19tok00 := make([]string, 20)
	for i := range k00tok19 {
		k19tok00[i] = fmt.Sprintf("k%02d", 49-i)
	}

	k20tok39 := make([]string, 20)
	for i := range k20tok39 {
		k20tok39[i] = fmt.Sprintf("k%02d", 50+i)
	}

	k000tok099 := make([]string, 100)
	for i := range k000tok099 {
		k000tok099[i] = fmt.Sprintf("k%02d", i)
	}

	k100tok199 := make([]string, 100)
	for i := range k100tok199 {
		k100tok199[i] = fmt.Sprintf("k%02d", 100+i)
	}

	k000tok499 := make([]string, 500)
	for i := range k000tok499 {
		k000tok499[i] = fmt.Sprintf("k%02d", i)
	}

	kCON := []string{"kCON"}

	kCandk00tok19 := append(append([]string{}, kCON...), k00tok19...)
	k00tok19andkC := append(append([]string{}, k00tok19...), kCON...)
	kCandk20tok39 := append(append([]string{}, kCON...), k20tok39...)
	k20tok39andkC := append(append([]string{}, k20tok39...), kCON...)

	kCandk000tok099 := append(append([]string{}, kCON...), k000tok099...)
	k000tok099andkC := append(append([]string{}, k000tok099...), kCON...)
	kCandk100tok199 := append(append([]string{}, kCON...), k100tok199...)
	k100tok199andkC := append(append([]string{}, k100tok199...), kCON...)

	transactionsTestKeys["k00tok19"] = k00tok19
	transactionsTestKeys["k19tok00"] = k19tok00
	transactionsTestKeys["k20tok39"] = k20tok39
	transactionsTestKeys["k000tok099"] = k000tok099
	transactionsTestKeys["k100tok199"] = k100tok199
	transactionsTestKeys["k000tok499"] = k000tok499
	transactionsTestKeys["kCON"] = kCON
	transactionsTestKeys["kCandk00tok19"] = kCandk00tok19
	transactionsTestKeys["k00tok19andkC"] = k00tok19andkC
	transactionsTestKeys["kCandk20tok39"] = kCandk20tok39
	transactionsTestKeys["k20tok39andkC"] = k20tok39andkC
	transactionsTestKeys["kCandk000tok099"] = kCandk000tok099
	transactionsTestKeys["k000tok099andkC"] = k000tok099andkC
	transactionsTestKeys["kCandk100tok199"] = kCandk100tok199
	transactionsTestKeys["k100tok199andkC"] = k100tok199andkC
}

func (suite *IntegrationTestSuite) setClusterName() {
	if globalCluster.IsProtostellar() {
		return
	}
	if globalCluster.NotSupportsFeature(ClusterLabelsFeature) {
		return
	}
	router, err := globalBucket.Internal().IORouter()
	suite.Require().NoError(err)

	newClusterName := "test-cluster"

	req := &gocbcore.HTTPRequest{
		UniqueID:    "setClusterName",
		Method:      "POST",
		Path:        "/pools/default",
		Service:     gocbcore.MgmtService,
		ContentType: "application/x-www-form-urlencoded",
		Body:        []byte(url.Values{"clusterName": []string{newClusterName}}.Encode()),
		Deadline:    time.Now().Add(10 * time.Second),
	}
	ch := make(chan struct{})
	_, err = router.DoHTTPRequest(req, func(resp *gocbcore.HTTPResponse, err error) {
		suite.Require().NoError(err)
		suite.Require().Equal(200, resp.StatusCode)
		suite.Require().NoError(resp.Body.Close())
		close(ch)
	})
	<-ch
	suite.Require().NoError(err)

	// ensureMgmtResource fires off concurrent requests, so we need to protect rev variable
	var revLock sync.Mutex
	var rev int64
	suite.ensureMgmtResource(time.Now().Add(20*time.Second), "/pools/default/b/"+globalBucket.Name(), func(reader io.ReadCloser) bool {
		var respBody map[string]interface{}
		err := json.NewDecoder(reader).Decode(&respBody)
		if err != nil {
			return false
		}
		clusterName, ok := respBody["clusterName"]
		if !ok {
			return false
		}

		thisRev := int64(int(respBody["rev"].(float64)))

		revLock.Lock()
		if thisRev > rev {
			rev = thisRev
		}
		revLock.Unlock()

		return clusterName == newClusterName
	})

	suite.tryUntil(time.Now().Add(5*time.Second), 100*time.Millisecond, func() bool {
		routerRevCh := make(chan int64, 1)
		_, err := router.WaitForConfigSnapshot(time.Now().Add(1*time.Second), gocbcore.WaitForConfigSnapshotOptions{}, func(result *gocbcore.WaitForConfigSnapshotResult, err error) {
			if err != nil {
				return
			}

			routerRevCh <- result.Snapshot.RevID()
		})
		if err != nil {
			return false
		}

		routerRevID := <-routerRevCh
		revLock.Lock()
		isGood := routerRevID >= rev
		revLock.Unlock()

		return isGood
	})
}

func (suite *IntegrationTestSuite) AssertKVMetrics(metricName, op string, length int, atLeastLen bool) {
	suite.AssertMetrics(makeMetricsKeyFromCmd(metricName, "kv", op), length, atLeastLen)
}

func makeMetricsKeyFromCmd(metricName, service, op string) string {
	return makeMetricsKey(metricName, service, op)
}

func makeMetricsKey(metricName, service, op string) string {
	key := metricName + ":" + service
	if op != "" {
		key = key + ":" + op
	}

	return key
}

func (suite *IntegrationTestSuite) AssertMetrics(key string, length int, atLeastLen bool) {
	globalMeter.lock.Lock()
	defer globalMeter.lock.Unlock()
	recorders := globalMeter.recorders
	if suite.Assert().Contains(recorders, key) {
		if atLeastLen {
			suite.Assert().GreaterOrEqual(len(recorders[key].values), length)
		} else {
			suite.Assert().Len(recorders[key].values, length)
		}
		for _, val := range recorders[key].values {
			suite.Assert().NotZero(val)
		}
	}
}

func (suite *IntegrationTestSuite) ensureReplicasUpEnhDura() {
	success := suite.tryUntil(time.Now().Add(30*time.Second), 50*time.Millisecond, func() bool {
		_, err := globalCollection.Upsert("ensurereplicasup", "test", &UpsertOptions{
			DurabilityLevel: DurabilityLevelPersistToMajority,
		})
		if err != nil {
			suite.T().Logf("Failed to upsert: %s", err)
			time.Sleep(50 * time.Millisecond)
			return false
		}

		_, err = globalCollection.GetAnyReplica("ensurereplicasup", &GetAnyReplicaOptions{})
		if err != nil {
			suite.T().Logf("Failed to get any replica: %s", err)
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

func (suite *IntegrationTestSuite) tryTimes(times int, interval time.Duration, fn func() bool) bool {
	for i := 0; i < times; i++ {
		success := fn()
		if success {
			return true
		}

		sleepDeadline := time.Now().Add(interval)
		time.Sleep(sleepDeadline.Sub(time.Now()))
	}

	return false
}

func (suite *IntegrationTestSuite) skipIfUnsupported(code FeatureCode) {
	if globalCluster.NotSupportsFeature(code) {
		suite.T().Skipf("Skipping test because feature %s unsupported or disabled", code)
	}
}

func (suite *IntegrationTestSuite) skipIfSupported(code FeatureCode) {
	if globalCluster.SupportsFeature(code) {
		suite.T().Skipf("Skipping test because feature %s is supported", code)
	}
}

func (suite *IntegrationTestSuite) dropAllIndexes() {
	mgr := globalCluster.QueryIndexes()

	var indexes []QueryIndex
	// Due to various eventual consistencies issues around dropping scopes query can sometimes
	// return us errors here.
	success := suite.tryUntil(time.Now().Add(30*time.Second), 100*time.Millisecond, func() bool {
		var err error
		indexes, err = mgr.GetAllIndexes(globalBucket.Name(), nil)
		if err != nil {
			suite.T().Logf("Failed to get all indexes: %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "Failed to get all indexes in time")

	for _, index := range indexes {
		if index.IsPrimary {
			mgr.DropPrimaryIndex(globalBucket.Name(), &DropPrimaryQueryIndexOptions{
				CollectionName: index.CollectionName,
				ScopeName:      index.ScopeName,
			})
		} else {
			mgr.DropIndex(globalBucket.Name(), index.Name, &DropQueryIndexOptions{
				CollectionName: index.CollectionName,
				ScopeName:      index.ScopeName,
			})
		}
	}

	globalMeter.Reset()
	globalTracer.Reset()
}

func (suite *IntegrationTestSuite) dropAllIndexesAtCollectionLevel() {
	mgr := globalCollection.QueryIndexes()

	var indexes []QueryIndex
	// Due to various eventual consistencies issues around dropping scopes query can sometimes
	// return us errors here.
	success := suite.tryUntil(time.Now().Add(30*time.Second), 100*time.Millisecond, func() bool {
		var err error
		indexes, err = mgr.GetAllIndexes(nil)
		if err != nil {
			suite.T().Logf("Failed to get all indexes: %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "Failed to get all indexes in time")

	for _, index := range indexes {
		if index.IsPrimary {
			mgr.DropPrimaryIndex(nil)
		} else {
			mgr.DropIndex(index.Name, nil)
		}
	}

	globalMeter.Reset()
	globalTracer.Reset()
}

// AssertExpiry will use the expiry value and whether the server supports HLC to calculate whether the
// document's expiry value is correct.
func (suite *IntegrationTestSuite) AssertExpiry(key string, startedAt time.Time, expiry time.Duration,
	epsilonLower time.Duration, epsilonUpper time.Duration) {
	// Gocaves does some weird things with absolute expirations.
	if globalCluster.isMock() {
		suite.AssertHLCRelativeExpiry(key, expiry, epsilonLower, epsilonUpper)
		return
	}

	hlcSupported := globalCluster.SupportsFeature(HLCFeature)
	if expiry < 30*24*time.Hour {
		if hlcSupported {
			suite.AssertHLCRelativeExpiry(key, expiry, epsilonLower, epsilonUpper)
		} else {
			suite.AssertNonHLCRelativeExpiry(key, expiry, epsilonUpper)
		}
	} else {
		suite.AssertAbsoluteExpiry(key, startedAt, expiry, epsilonLower, epsilonUpper)
	}
}

// AssertAbsoluteExpiry verifies that the specified expiry has been written to the document metadata.
func (suite *IntegrationTestSuite) AssertAbsoluteExpiry(key string, startedAt time.Time, expiry time.Duration,
	epsilonLower time.Duration, epsilonUpper time.Duration) {
	spec := []LookupInSpec{
		GetSpec("$document", &GetSpecOptions{IsXattr: true}),
	}
	lookupRes, err := globalCollection.LookupIn(
		key,
		spec,
		nil,
	)
	suite.Require().NoError(err, err)

	exp := suite.parseExpiryOutOfResult(lookupRes, 0)

	actualExpirySecs := time.Unix(exp, 0).Sub(startedAt)

	suite.Assert().Greaterf(
		expiry+epsilonUpper,
		actualExpirySecs,
		"Expected expiry to be less than %f but was %f", expiry.Seconds(),
		actualExpirySecs.Seconds())
	suite.Assert().Greaterf(
		actualExpirySecs,
		expiry-epsilonLower,
		"Expected expiry to be greater than %f but was %f", expiry.Seconds(),
		actualExpirySecs.Seconds())
}

// AssertNonHLCRelativeExpiry checks that the document exists immediately and then does not exist after the
// expiry duration. Care should be taken not to use long expirations with this test as it will sleep for the
// expiry duration.
func (suite *IntegrationTestSuite) AssertNonHLCRelativeExpiry(key string, expiry time.Duration, epsilon time.Duration) {
	_, err := globalCollection.Get(key, nil)
	suite.Require().NoError(err, err)

	time.Sleep(expiry + epsilon)

	_, err = globalCollection.Get(key, nil)
	suite.Require().ErrorIs(err, ErrDocumentNotFound)
}

// AssertHLCRelativeExpiry uses the HLC value from the server to verify that the specified expiry has been
// written to the document metadata.
func (suite *IntegrationTestSuite) AssertHLCRelativeExpiry(key string, expiry time.Duration,
	epsilonLower time.Duration, epsilonUpper time.Duration) {
	spec := []LookupInSpec{
		GetSpec("$document", &GetSpecOptions{IsXattr: true}),
		GetSpec("$vbucket.HLC", &GetSpecOptions{IsXattr: true}),
	}
	lookupRes, err := globalCollection.LookupIn(
		key,
		spec,
		nil,
	)
	suite.Require().NoError(err, err)

	exp := suite.parseExpiryOutOfResult(lookupRes, 0)
	hlc := suite.parseHLCOutOfResult(lookupRes, 1)

	actualExpirySecs := float64(exp - int64(hlc))
	expectedExpiryUpperSecs := (expiry + epsilonUpper).Seconds()
	expectedExpiryLowerSecs := (expiry - epsilonLower).Seconds()

	suite.Assert().LessOrEqual(
		actualExpirySecs,
		expectedExpiryUpperSecs,
		"Expected expiry to be less than %f but was %f", expectedExpiryUpperSecs,
		actualExpirySecs)
	suite.Assert().Greaterf(
		actualExpirySecs,
		expectedExpiryLowerSecs,
		"Expected expiry to be greater than %f but was %f", expectedExpiryLowerSecs,
		actualExpirySecs)
}

func (suite *IntegrationTestSuite) parseExpiryOutOfResult(res *LookupInResult, index uint) int64 {
	exp := struct {
		Expiration int64 `json:"exptime"`
	}{}
	err := res.ContentAt(index, &exp)
	suite.Require().NoError(err, err)

	return exp.Expiration
}

func (suite *IntegrationTestSuite) parseHLCOutOfResult(res *LookupInResult, index uint) int {
	hlcStr := struct {
		Now string `json:"now"`
	}{}
	err := res.ContentAt(index, &hlcStr)
	suite.Require().NoError(err, err)

	hlc, err := strconv.Atoi(hlcStr.Now)
	suite.Require().NoError(err, err)

	return hlc
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

func (suite *UnitTestSuite) bucket(name string, timeouts TimeoutsConfig, cli connectionManager) *Bucket {
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
		retryStrategyWrapper: newCoreRetryStrategyWrapper(NewBestEffortRetryStrategy(nil)),
		useServerDurations:   true,
		useMutationTokens:    true,

		connectionManager: cli,
	}

	return b
}

func (suite *UnitTestSuite) newCluster(cli connectionManager) *Cluster {
	cluster := clusterFromOptions(ClusterOptions{
		Tracer: &NoopTracer{},
		Meter:  &NoopMeter{},
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
		timeoutsConfig: TimeoutsConfig{
			KVTimeout:     2500 * time.Millisecond,
			KVScanTimeout: 75000 * time.Millisecond,
		},
		transcoder:           NewJSONTranscoder(),
		retryStrategyWrapper: newCoreRetryStrategyWrapper(NewBestEffortRetryStrategy(nil)),

		opController: mockOpController{},
	}
}

type mockOpController struct{}

func (m mockOpController) MarkOpBeginning() {}

func (m mockOpController) MarkOpCompleted() {}
