package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"runtime/debug"
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v10"

	"github.com/google/uuid"
)

func (suite *IntegrationTestSuite) EnsureUserGroupOnAllNodes(deadline time.Time, name string,
	predicate func(group jsonGroup) bool) {
	if globalCluster.IsProtostellar() {
		return
	}

	path := fmt.Sprintf("/settings/rbac/groups/%s", url.PathEscape(name))

	suite.ensureMgmtResource(deadline, path, func(reader io.ReadCloser) bool {
		if predicate == nil {
			return true
		}
		var groupData jsonGroup
		jsonDec := json.NewDecoder(reader)
		_ = jsonDec.Decode(&groupData)

		success := predicate(groupData)
		if !success {
			suite.T().Logf("Predicate failed for bucket")
			return false
		}

		return true
	})
}

func (suite *IntegrationTestSuite) EnsureUserOnAllNodes(deadline time.Time, name string,
	predicate func(group jsonUserMetadata) bool) {
	if globalCluster.IsProtostellar() {
		return
	}

	path := fmt.Sprintf("/settings/rbac/users/%s/%s", LocalDomain, url.PathEscape(name))

	suite.ensureMgmtResource(deadline, path, func(reader io.ReadCloser) bool {
		if predicate == nil {
			return true
		}
		var groupData jsonUserMetadata
		jsonDec := json.NewDecoder(reader)
		_ = jsonDec.Decode(&groupData)

		success := predicate(groupData)
		if !success {
			suite.T().Logf("Predicate failed for bucket")
			return false
		}

		return true
	})
}

type queryRow struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

func (suite *IntegrationTestSuite) EnsureIndexOnAllNodes(deadline time.Time, name, bucket, scope, collection string, predicate func(row queryRow) bool) {
	if globalCluster.IsProtostellar() {
		return
	}

	where, params := buildGetAllIndexesWhereClause(nil, bucket, scope, collection)
	payload := map[string]interface{}{
		"statement": "SELECT `idx`.* FROM system:indexes AS idx WHERE " + where + " AND `using` = \"gsi\" ",
	}
	for name, param := range params {
		payload["$"+name] = param
	}
	b, _ := json.Marshal(payload)

	type queryResult struct {
		Results []queryRow `json:"results"`
	}

	suite.ensureQueryResource(deadline, b, func(reader io.ReadCloser) bool {
		var result queryResult
		jsonDec := json.NewDecoder(reader)
		_ = jsonDec.Decode(&result)

		for _, row := range result.Results {
			if row.Name == name {
				if predicate == nil {
					return true
				} else {
					success := predicate(row)
					if success {
						return true
					} else {
						suite.T().Log("Predicate returned false")
						return false
					}
				}
			}
		}

		suite.T().Logf("Index was not found")

		return false
	})
}

func (suite *IntegrationTestSuite) EnsureCollectionOnAllIndexesAndNodes(deadline time.Time, bucket, scope,
	collection string) {
	if globalCluster.IsProtostellar() {
		return
	}

	payload := map[string]interface{}{
		"statement":   "SELECT COUNT(*) as count FROM system:keyspaces where `bucket`=$bucket and `scope`=$scope and `name`=$collection",
		"$bucket":     bucket,
		"$scope":      scope,
		"$collection": collection,
	}
	b, _ := json.Marshal(payload)

	type queryResult struct {
		Results []struct {
			Count int `json:"count"`
		} `json:"results"`
	}

	suite.ensureQueryResource(deadline, b, func(reader io.ReadCloser) bool {
		var result queryResult
		jsonDec := json.NewDecoder(reader)
		_ = jsonDec.Decode(&result)

		if len(result.Results) != 1 {
			suite.T().Logf("Unexpected number of results: %d", len(result.Results))
			return false
		}

		if result.Results[0].Count > 0 {
			return true
		}

		suite.T().Logf("Collection keyspace was not found")

		return false
	})
}

func (suite *IntegrationTestSuite) EnsureBucketOnAllNodes(deadline time.Time, name string,
	predicate func(bucket *BucketSettings) bool) {
	if globalCluster.IsProtostellar() {
		return
	}

	path := fmt.Sprintf("/pools/default/buckets/%s", name)
	suite.ensureMgmtResource(deadline, path, func(reader io.ReadCloser) bool {
		if predicate == nil {
			return true
		}
		bucket := parseBucket(reader)
		success := predicate(bucket)
		if !success {
			suite.T().Logf("Predicate failed for bucket")
			return false
		}

		return true
	})
}

func (suite *IntegrationTestSuite) EnsureCollectionsOnAllNodes(scopeName string, collections []string) {
	if globalCluster.IsProtostellar() {
		return
	}

	path := fmt.Sprintf("/pools/default/buckets/%s/scopes", url.PathEscape(globalBucket.Name()))
	suite.ensureMgmtResource(time.Now().Add(30*time.Second), path, func(reader io.ReadCloser) bool {
		var mfest gocbcore.Manifest
		jsonDec := json.NewDecoder(reader)
		jsonDec.Decode(&mfest)

		var found int
		for _, scope := range mfest.Scopes {
			if scope.Name != scopeName {
				continue
			}
			for _, col := range scope.Collections {
				for _, n := range collections {
					if col.Name == n {
						found++
					}
				}
			}
		}
		if found == len(collections) {
			return true
		}
		suite.T().Logf("Found %d required collections, will retry", found)

		return false
	})
}

func (suite *IntegrationTestSuite) ensureQueryResource(deadline time.Time, payload []byte, handleBody func(closer io.ReadCloser) bool) {
	router, err := globalBucket.Internal().IORouter()
	suite.Require().NoError(err, "Failed to get IO router")

	endpoints := router.N1qlEps()

	suite.ensureResource(deadline, ServiceTypeQuery, "POST", "/query/service", payload, endpoints, handleBody)
}

func (suite *IntegrationTestSuite) ensureMgmtResource(deadline time.Time, path string, handleBody func(closer io.ReadCloser) bool) {
	router, err := globalBucket.Internal().IORouter()
	suite.Require().NoError(err, "Failed to get IO router")

	endpoints := router.MgmtEps()

	suite.ensureResource(deadline, ServiceTypeManagement, "GET", path, nil, endpoints, handleBody)
}

func (suite *IntegrationTestSuite) ensureResource(deadline time.Time, service ServiceType, method, path string, body []byte, endpoints []string,
	handleBody func(closer io.ReadCloser) bool) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	suite.T().Logf("Ensuring resource against %s", strings.Join(endpoints, ","))

	wait := make(chan struct{}, len(endpoints))
	for _, ep := range endpoints {
		go func(ep string) {
			for {
				var success bool
				req := mgmtRequest{
					Service:      service,
					Path:         path,
					Method:       method,
					IsIdempotent: true,
					UniqueID:     uuid.New().String(),
					Timeout:      time.Until(deadline),
					Endpoint:     ep,
					Body:         body,
				}
				resp, err := globalCluster.executeMgmtRequest(ctx, req)
				if err != nil {
					suite.T().Logf("Failed to execute mgmt request against %s, err: %s", ep, err)
					return
				}

				if resp.StatusCode == 200 {
					success = handleBody(resp.Body)
					resp.Body.Close()
				} else {
					body, _ := io.ReadAll(resp.Body)
					suite.T().Logf("Execute mgmt request non-200 response against %s, status: %d, body: %s", ep, resp.StatusCode, body)
				}

				if success {
					wait <- struct{}{}
					return
				}

				select {
				case <-time.After(100 * time.Millisecond):
				case <-ctx.Done():
					return
				}
			}
		}(ep)
	}

	var i int
	for {
		select {
		case <-wait:
			i++
			if i == len(endpoints) {
				return
			}
		case <-ctx.Done():
			debug.PrintStack()
			suite.T().Fatal("Failed to ensure resource online on all nodes within specified time")
		}
	}
}

func parseBucket(body io.ReadCloser) *BucketSettings {
	var bucketData jsonBucketSettings
	jsonDec := json.NewDecoder(body)
	_ = jsonDec.Decode(&bucketData)

	var settings BucketSettings
	_ = settings.fromData(bucketData)

	return &settings
}
