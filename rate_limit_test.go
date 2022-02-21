package gocb

import (
	"context"
	"errors"
	"fmt"
	"github.com/couchbase/gocb/v2/search"
	"github.com/google/uuid"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func (suite *IntegrationTestSuite) TestRateLimits() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RateLimitingFeature)

	username := "ratelimit"
	suite.enforceRateLimits()
	userMgr := globalCluster.Users()
	suite.createRateLimitUser(username, rateLimits{
		kvLimits: &kvRateLimits{
			NumConnections:   10,
			NumOpsPerMin:     10,
			IngressMibPerMin: 10,
			EgressMibPerMin:  10,
		},
	})
	defer userMgr.DropUser(username, nil)

	c, err := Connect(globalConfig.Server, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: username,
		Password: "password",
	}})
	defer c.Close(nil)
	suite.Require().Nil(err, err)

	b := c.Bucket(globalBucket.Name())
	err = b.WaitUntilReady(7*time.Second, nil)
	suite.Require().Nil(err, err)

	collection := b.Scope(globalScope.Name()).Collection(globalCollection.Name())

	var foundErr error
	success := suite.tryUntil(time.Now().Add(5*time.Second), 10*time.Millisecond, func() bool {
		_, err = collection.Upsert("ratelimit", "test", &UpsertOptions{})
		if err != nil {
			foundErr = err
			return true
		}

		return false
	})
	suite.Require().True(success, "Request was not rate limited in time")

	if !errors.Is(foundErr, ErrRateLimitedFailure) {
		suite.T().Fatalf("Expected rate limiting error but was %s", err)
	}
}

func (suite *IntegrationTestSuite) TestRateLimitsIngress() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RateLimitingFeature)

	username := "ratelimitingress"
	suite.enforceRateLimits()
	userMgr := globalCluster.Users()
	suite.createRateLimitUser(username, rateLimits{
		kvLimits: &kvRateLimits{
			NumConnections:   10,
			NumOpsPerMin:     100,
			IngressMibPerMin: 1,
			EgressMibPerMin:  10,
		},
	})
	defer userMgr.DropUser(username, nil)

	c, err := Connect(globalConfig.Server, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: username,
		Password: "password",
	}})
	defer c.Close(nil)
	suite.Require().Nil(err, err)

	b := c.Bucket(globalBucket.Name())
	err = b.WaitUntilReady(7*time.Second, nil)
	suite.Require().Nil(err, err)

	collection := b.Scope(globalScope.Name()).Collection(globalCollection.Name())

	doc := randomDoc(1024 * 512)
	_, err = collection.Upsert("ratelimitingress", doc, &UpsertOptions{})
	suite.Require().Nil(err, err)
	_, err = collection.Upsert("ratelimitingress", doc, &UpsertOptions{})
	if !errors.Is(err, ErrRateLimitedFailure) {
		suite.T().Fatalf("Expected rate limiting error but was %s", err)
	}
}

func (suite *IntegrationTestSuite) TestRateLimitsEgress() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RateLimitingFeature)

	username := "ratelimitegress"
	suite.enforceRateLimits()
	userMgr := globalCluster.Users()
	suite.createRateLimitUser(username, rateLimits{
		kvLimits: &kvRateLimits{
			NumConnections:   10,
			NumOpsPerMin:     100,
			IngressMibPerMin: 10,
			EgressMibPerMin:  1,
		},
	})
	defer userMgr.DropUser(username, nil)

	c, err := Connect(globalConfig.Server, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: username,
		Password: "password",
	}})
	defer c.Close(nil)
	suite.Require().Nil(err, err)

	b := c.Bucket(globalBucket.Name())
	err = b.WaitUntilReady(7*time.Second, nil)
	suite.Require().Nil(err, err)

	collection := b.Scope(globalScope.Name()).Collection(globalCollection.Name())

	_, err = collection.Upsert("ratelimitegress", randomDoc(1024*512), &UpsertOptions{})
	suite.Require().Nil(err, err)

	_, err = collection.Get("ratelimitegress", &GetOptions{
		Timeout: 10 * time.Second,
	})
	suite.Require().Nil(err, err)
	_, err = collection.Get("ratelimitegress", &GetOptions{
		Timeout: 10 * time.Second,
	})
	suite.Require().Nil(err, err)
	_, err = collection.Get("ratelimitegress", &GetOptions{
		Timeout: 10 * time.Second,
	})
	if !errors.Is(err, ErrRateLimitedFailure) {
		suite.T().Fatalf("Expected rate limiting error but was %s", err)
	}
}

func (suite *IntegrationTestSuite) TestRateLimitsMaxConnections() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RateLimitingFeature)

	username := "ratelimitmaxconns"
	suite.enforceRateLimits()
	userMgr := globalCluster.Users()
	suite.createRateLimitUser(username, rateLimits{
		kvLimits: &kvRateLimits{
			NumConnections:   1,
			NumOpsPerMin:     100,
			IngressMibPerMin: 10,
			EgressMibPerMin:  10,
		},
	})
	defer userMgr.DropUser(username, nil)

	c, err := Connect(globalConfig.Server, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: username,
		Password: "password",
	}})
	defer c.Close(nil)
	suite.Require().Nil(err, err)

	b := c.Bucket(globalBucket.Name())
	err = b.WaitUntilReady(7*time.Second, nil)
	suite.Require().Nil(err, err)

	c, err = Connect(globalConfig.Server, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: username,
		Password: "password",
	}})
	defer c.Close(nil)
	suite.Require().Nil(err, err)

	b = c.Bucket(globalBucket.Name())
	err = b.WaitUntilReady(7*time.Second, &WaitUntilReadyOptions{
		RetryStrategy: newFailFastRetryStrategy(),
	})
	if !errors.Is(err, ErrRateLimitedFailure) {
		suite.T().Fatalf("Expected rate limiting error but was %s", err)
	}
}

func (suite *IntegrationTestSuite) TestRateLimitsQuery() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RateLimitingFeature)

	username := "ratelimitquery"
	suite.enforceRateLimits()
	userMgr := globalCluster.Users()
	suite.createRateLimitUser(username, rateLimits{
		queryLimits: &queryRateLimits{
			IngressMibPerMin:      10,
			EgressMibPerMin:       10,
			NumConcurrentRequests: 10,
			NumQueriesPerMin:      1,
		},
	})
	defer userMgr.DropUser(username, nil)

	qMgr := globalCluster.QueryIndexes()
	err := qMgr.CreatePrimaryIndex(globalBucket.Name(), &CreatePrimaryQueryIndexOptions{
		IgnoreIfExists: true,
	})
	suite.Require().Nil(err, err)

	c, err := Connect(globalConfig.Server, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: username,
		Password: "password",
	}})
	defer c.Close(nil)
	suite.Require().Nil(err, err)

	err = c.WaitUntilReady(7*time.Second, nil)
	suite.Require().Nil(err, err)

	var foundErr error
	success := suite.tryUntil(time.Now().Add(3*time.Second), 50*time.Millisecond, func() bool {
		var resp *QueryResult
		resp, foundErr = c.Query(fmt.Sprintf("SELECT * from %s LIMIT 1", globalBucket.Name()), nil)
		if foundErr != nil {
			return true
		}
		err := resp.Close()
		suite.Require().Nil(err, err)

		return false
	})
	suite.Require().True(success, "Query was never rate limited")

	if !errors.Is(foundErr, ErrRateLimitedFailure) {
		suite.T().Fatalf("Expected rate limiting error but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestRateLimitsSearch() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RateLimitingFeature)

	username := uuid.New().String()
	suite.enforceRateLimits()
	userMgr := globalCluster.Users()
	suite.createRateLimitUser(username, rateLimits{
		searchLimits: &searchRateLimits{
			IngressMibPerMin:      10,
			EgressMibPerMin:       10,
			NumConcurrentRequests: 10,
			NumQueriesPerMin:      1,
		},
	})
	defer userMgr.DropUser(username, nil)

	c, err := Connect(globalConfig.Server, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: username,
		Password: "password",
	}})
	defer c.Close(nil)
	suite.Require().Nil(err, err)

	err = c.WaitUntilReady(7*time.Second, nil)
	suite.Require().Nil(err, err)

	mgr := globalCluster.SearchIndexes()
	err = mgr.UpsertIndex(SearchIndex{
		Name:       "ratelimits",
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
	}, nil)
	suite.Require().Nil(err, err)
	defer mgr.DropIndex("ratelimits", nil)

	query := search.NewTermQuery("search").Field("service")
	success := suite.tryUntil(time.Now().Add(5*time.Second), 100*time.Millisecond, func() bool {
		result, err := globalCluster.SearchQuery("ratelimits", query, &SearchOptions{
			Timeout: 1 * time.Second,
		})
		if err != nil {
			suite.T().Logf("Search query returned error: %v", err)
			return false
		}

		for result.Next() {
		}
		err = result.Err()
		if err != nil {
			suite.T().Logf("Search query returned error: %v", err)
			return false
		}
		return true
	})
	suite.Require().True(success)

	result, err := c.SearchQuery("ratelimits", query, &SearchOptions{})
	suite.Require().Nil(err, err)
	err = result.Close()
	suite.Require().Nil(err, err)

	result, err = c.SearchQuery("ratelimits", query, &SearchOptions{})
	if !errors.Is(err, ErrRateLimitedFailure) {
		suite.T().Fatalf("Expected rate limiting error but was %v", err)
	}
}

func (suite *IntegrationTestSuite) TestRateLimitsKVScopesDataSize() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RateLimitingFeature)
	suite.skipIfUnsupported(CollectionsFeature)

	scopename := "ratelimitDataSize"
	suite.enforceRateLimits()
	colMgr := globalBucket.Collections()
	suite.createRateLimitScope(scopename, globalBucket.Name(), scopeRateLimits{
		kv: &kvScopeRateLimit{
			DataSize: 1024 * 1024,
		},
	})
	defer colMgr.DropScope(scopename, nil)

	err := colMgr.CreateCollection(CollectionSpec{
		Name:      scopename,
		ScopeName: scopename,
	}, nil)
	suite.Require().Nil(err, err)

	suite.mustWaitForCollections(scopename, []string{scopename})

	collection := globalBucket.Scope(scopename).Collection(scopename)

	doc := randomDoc(512)
	_, err = collection.Upsert("ratelimitkvscope", doc, &UpsertOptions{})
	suite.Require().Nil(err, err)
	doc = randomDoc(2048)
	_, err = collection.Upsert("ratelimitkvscope2", doc, &UpsertOptions{})
	if !errors.Is(err, ErrQuotaLimitedFailure) {
		suite.T().Fatalf("Expected rate limiting error but was %s", err)
	}
}

func (suite *IntegrationTestSuite) TestRateLimitsFTSScopes() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RateLimitingFeature)
	suite.skipIfUnsupported(CollectionsFeature)

	scopename := "ratelimitftsscope"
	colname := "ratelimitftscollection"
	suite.enforceRateLimits()
	colMgr := globalBucket.Collections()
	suite.createRateLimitScope(scopename, globalBucket.Name(), scopeRateLimits{
		fts: &searchScopeRateLimit{
			NumFTSIndexes: 1,
		},
	})
	defer colMgr.DropScope(scopename, nil)
	err := colMgr.CreateCollection(CollectionSpec{
		Name:      colname,
		ScopeName: scopename,
	}, nil)
	suite.Require().Nil(err, err)

	suite.mustWaitForCollections(scopename, []string{colname})

	mgr := globalCluster.SearchIndexes()

	success := suite.tryUntil(time.Now().Add(10*time.Second), 500*time.Millisecond, func() bool {
		err = mgr.UpsertIndex(SearchIndex{
			Name:       "ratelimitsscope",
			Type:       "fulltext-index",
			SourceType: "gocbcore",
			SourceName: globalBucket.Name(),
			Params: map[string]interface{}{
				"mapping": map[string]interface{}{
					"types": map[string]interface{}{
						scopename + "." + colname: map[string]interface{}{
							"enabled": true,
							"dynamic": true,
						},
					},
					"default_mapping": map[string]interface{}{
						"enabled": false,
					},
					"default_type":     "_default",
					"default_analyzer": "standard",
					"default_field":    "_all",
				},
				"doc_config": map[string]interface{}{
					"mode":       "scope.collection.type_field",
					"type_field": "type",
				},
			},
		}, nil)
		if err != nil {
			suite.T().Logf("UpsertIndex failed: %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "UpsertIndex never succeeded")

	err = mgr.UpsertIndex(SearchIndex{
		Name:       "ratelimitsscope2",
		Type:       "fulltext-index",
		SourceType: "couchbase",
		SourceName: globalBucket.Name(),
		Params: map[string]interface{}{
			"mapping": map[string]interface{}{
				"types": map[string]interface{}{
					scopename + "." + colname: map[string]interface{}{
						"enabled": true,
						"dynamic": true,
					},
				},
				"default_mapping": map[string]interface{}{
					"enabled": false,
				},
				"default_type":     "_default",
				"default_analyzer": "standard",
				"default_field":    "_all",
			},
			"doc_config": map[string]interface{}{
				"mode": "scope.collection.type_field",
			},
		},
	}, nil)
	if !errors.Is(err, ErrQuotaLimitedFailure) {
		suite.T().Fatalf("Expected rate limiting error but was %s", err)
	}
}

func (suite *IntegrationTestSuite) TestRateLimitsIndexScopes() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RateLimitingFeature)
	suite.skipIfUnsupported(CollectionsFeature)

	scopename := "ratelimitindexscope"
	colname := "ratelimitindexcollection"
	suite.enforceRateLimits()
	colMgr := globalBucket.Collections()
	suite.createRateLimitScope(scopename, globalBucket.Name(), scopeRateLimits{
		index: &indexScopeRateLimit{
			NumIndexes: 1,
		},
	})
	defer colMgr.DropScope(scopename, nil)
	err := colMgr.CreateCollection(CollectionSpec{
		Name:      colname,
		ScopeName: scopename,
	}, nil)
	suite.Require().Nil(err, err)
	suite.mustWaitForCollections(scopename, []string{colname})

	scope := globalBucket.Scope(scopename)

	success := suite.tryUntil(time.Now().Add(2*time.Second), 100*time.Millisecond, func() bool {
		result, err := scope.Query(fmt.Sprintf("CREATE PRIMARY INDEX ON `%s`", colname), nil)
		if err != nil {
			suite.T().Logf("Query returned error: %v", err)
			return false
		}

		err = result.Close()
		if err != nil {
			suite.T().Logf("Query returned error: %v", err)
			return false
		}

		return true
	})
	suite.Require().True(success, "Query did not successfully create index in time")

	_, err = scope.Query(fmt.Sprintf("CREATE INDEX ratelimit ON `%s`(somefield)", colname), nil)
	if !errors.Is(err, ErrQuotaLimitedFailure) {
		suite.T().Fatalf("Expected rate limiting error but was %s", err)
	}
}

func (suite *IntegrationTestSuite) TestRateLimitsScopesCollectionsLimit() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RateLimitingFeature)
	suite.skipIfUnsupported(CollectionsFeature)

	scopename := "ratelimitscopemaxcols"
	colname := "ratelimitcollectionmaxcols"
	suite.enforceRateLimits()
	colMgr := globalBucket.Collections()
	suite.createRateLimitScope(scopename, globalBucket.Name(), scopeRateLimits{
		clusterManager: &clusterManagerScopeRateLimit{
			NumCollections: 1,
		},
	})
	defer colMgr.DropScope(scopename, nil)
	err := colMgr.CreateCollection(CollectionSpec{
		Name:      colname,
		ScopeName: scopename,
	}, nil)
	suite.Require().Nil(err, err)

	err = colMgr.CreateCollection(CollectionSpec{
		Name:      "hihohihohiho",
		ScopeName: scopename,
	}, nil)
	if !errors.Is(err, ErrQuotaLimitedFailure) {
		suite.T().Fatalf("Expected rate limiting error but was %s", err)
	}
}

func (suite *IntegrationTestSuite) TestRateLimitsClusterManagerConcurrency() {
	suite.skipIfUnsupported(KeyValueFeature)
	suite.skipIfUnsupported(RateLimitingFeature)

	username := "ratelimitingressclustermanagerconcurrency"
	suite.enforceRateLimits()
	userMgr := globalCluster.Users()
	suite.createRateLimitUser(username, rateLimits{
		clusterManagerLimits: &clusterManagerRateLimits{
			IngressMibPerMin:      10,
			EgressMibPerMin:       10,
			NumConcurrentRequests: 1,
		},
	})
	defer userMgr.DropUser(username, nil)

	c, err := Connect(globalConfig.Server, ClusterOptions{Authenticator: PasswordAuthenticator{
		Username: username,
		Password: "password",
	}})
	defer c.Close(nil)
	suite.Require().Nil(err, err)

	b := c.Bucket(globalBucket.Name())
	err = b.WaitUntilReady(7*time.Second, nil)
	suite.Require().Nil(err, err)

	mgr := b.Collections()

	var wg sync.WaitGroup
	numScopes := 5
	wg.Add(numScopes)
	foundRateLimitErr := uint32(0)
	for i := 0; i < numScopes; i++ {
		go func(i int) {
			err := mgr.CreateScope(fmt.Sprintf("ratelimitingressclustermanagerconcurrency%d", i), nil)
			if err == nil {
				err = mgr.DropScope(fmt.Sprintf("ratelimitingressclustermanagerconcurrency%d", i), nil)
				if err != nil {
					if errors.Is(err, ErrRateLimitedFailure) {
						atomic.StoreUint32(&foundRateLimitErr, 1)
					}
				}
			} else {
				if errors.Is(err, ErrRateLimitedFailure) {
					atomic.StoreUint32(&foundRateLimitErr, 1)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	if foundRateLimitErr != 1 {
		suite.T().Fatalf("Expected rate limiting failure but didn't happen")
	}
}

func randomDoc(size int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, size)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[:size]
}

type kvRateLimits struct {
	NumConnections   int
	NumOpsPerMin     int
	IngressMibPerMin int
	EgressMibPerMin  int
}

type queryRateLimits struct {
	IngressMibPerMin      int
	EgressMibPerMin       int
	NumConcurrentRequests int
	NumQueriesPerMin      int
}

type searchRateLimits struct {
	IngressMibPerMin      int
	EgressMibPerMin       int
	NumConcurrentRequests int
	NumQueriesPerMin      int
}

type clusterManagerRateLimits struct {
	IngressMibPerMin      int
	EgressMibPerMin       int
	NumConcurrentRequests int
}

type rateLimits struct {
	kvLimits             *kvRateLimits
	queryLimits          *queryRateLimits
	searchLimits         *searchRateLimits
	clusterManagerLimits *clusterManagerRateLimits
}

func (suite *IntegrationTestSuite) createRateLimitUser(username string, limits rateLimits) {
	var formlimits []string
	if limits.kvLimits != nil {
		formlimits = append(formlimits,
			fmt.Sprintf("\"kv\":{\"num_connections\": %d, \"num_ops_per_min\": %d, \"ingress_mib_per_min\": %d, \"egress_mib_per_min\": %d}",
				limits.kvLimits.NumConnections,
				limits.kvLimits.NumOpsPerMin,
				limits.kvLimits.IngressMibPerMin,
				limits.kvLimits.EgressMibPerMin,
			),
		)
	}
	if limits.queryLimits != nil {
		formlimits = append(formlimits,
			fmt.Sprintf("\"query\":{\"num_queries_per_min\": %d, \"num_concurrent_requests\": %d,\"ingress_mib_per_min\": %d, \"egress_mib_per_min\": %d}",
				limits.queryLimits.NumQueriesPerMin,
				limits.queryLimits.NumConcurrentRequests,
				limits.queryLimits.IngressMibPerMin,
				limits.queryLimits.EgressMibPerMin,
			),
		)
	}
	if limits.searchLimits != nil {
		formlimits = append(formlimits,
			fmt.Sprintf("\"fts\":{\"num_queries_per_min\": %d, \"num_concurrent_requests\": %d,\"ingress_mib_per_min\": %d, \"egress_mib_per_min\": %d}",
				limits.searchLimits.NumQueriesPerMin,
				limits.searchLimits.NumConcurrentRequests,
				limits.searchLimits.IngressMibPerMin,
				limits.searchLimits.EgressMibPerMin,
			),
		)
	}
	if limits.clusterManagerLimits != nil {
		formlimits = append(formlimits,
			fmt.Sprintf("\"clusterManager\":{\"num_concurrent_requests\": %d,\"ingress_mib_per_min\": %d, \"egress_mib_per_min\": %d}",
				limits.clusterManagerLimits.NumConcurrentRequests,
				limits.clusterManagerLimits.IngressMibPerMin,
				limits.clusterManagerLimits.EgressMibPerMin,
			),
		)
	}
	reqForm := make(url.Values)
	reqForm.Add("password", "password")
	reqForm.Add("roles", "admin")

	if len(formlimits) > 0 {
		reqForm.Add("limits", fmt.Sprintf("{%s}", strings.Join(formlimits, ",")))
	}

	resp, err := globalCluster.executeMgmtRequest(context.Background(), mgmtRequest{
		Method:      "PUT",
		Path:        "/settings/rbac/users/local/" + username,
		Service:     ServiceTypeManagement,
		Body:        []byte(reqForm.Encode()),
		ContentType: "application/x-www-form-urlencoded",
	})
	suite.Require().Nil(err, err)
	suite.Require().Equal(200, int(resp.StatusCode))
	resp.Body.Close()
}

func (suite *IntegrationTestSuite) createRateLimitScope(name, bucket string, limits scopeRateLimits) {
	var formlimits []string
	if limits.kv != nil {
		formlimits = append(formlimits,
			fmt.Sprintf("\"kv\":{\"data_size\": %d}",
				limits.kv.DataSize,
			),
		)
	}
	if limits.fts != nil {
		formlimits = append(formlimits,
			fmt.Sprintf("\"fts\":{\"num_fts_indexes\": %d}",
				limits.fts.NumFTSIndexes,
			),
		)
	}
	if limits.index != nil {
		formlimits = append(formlimits,
			fmt.Sprintf("\"index\":{\"num_indexes\": %d}",
				limits.index.NumIndexes,
			),
		)
	}
	if limits.clusterManager != nil {
		formlimits = append(formlimits,
			fmt.Sprintf("\"clusterManager\":{\"num_collections\": %d}",
				limits.clusterManager.NumCollections,
			),
		)
	}
	reqForm := make(url.Values)
	reqForm.Add("name", name)

	if len(formlimits) > 0 {
		reqForm.Add("limits", fmt.Sprintf("{%s}", strings.Join(formlimits, ",")))
	}

	resp, err := globalCluster.executeMgmtRequest(context.Background(), mgmtRequest{
		Method:      "POST",
		Path:        fmt.Sprintf("/pools/default/buckets/%s/scopes", bucket),
		Service:     ServiceTypeManagement,
		Body:        []byte(reqForm.Encode()),
		ContentType: "application/x-www-form-urlencoded",
	})
	suite.Require().Nil(err, err)
	suite.Require().Equal(200, int(resp.StatusCode))
	resp.Body.Close()
}

type kvScopeRateLimit struct {
	DataSize int
}

type searchScopeRateLimit struct {
	NumFTSIndexes int
}

type indexScopeRateLimit struct {
	NumIndexes int
}

type clusterManagerScopeRateLimit struct {
	NumCollections int
}

type scopeRateLimits struct {
	kv             *kvScopeRateLimit
	fts            *searchScopeRateLimit
	index          *indexScopeRateLimit
	clusterManager *clusterManagerScopeRateLimit
}

func (suite *IntegrationTestSuite) enforceRateLimits() {
	resp, err := globalCluster.executeMgmtRequest(context.Background(), mgmtRequest{
		Method:  "POST",
		Path:    "/internalSettings",
		Service: ServiceTypeManagement,
		Body:    []byte("enforceLimits=true"),
	})
	suite.Require().Nil(err, err)
	suite.Require().Equal(200, int(resp.StatusCode))
	resp.Body.Close()
}
