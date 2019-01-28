package gocb

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"gopkg.in/couchbaselabs/gojcbmock.v1"
)

const (
	defaultServerVersion = "5.1.0"
)

var globalBucket *Bucket
var globalTravelBucket *Bucket
var globalCollection *Collection
var globalCluster *testCluster

func TestMain(m *testing.M) {
	// SetLogger(VerboseStdioLogger())

	server := flag.String("server", "", "The connection string to connect to for a real server")
	user := flag.String("user", "", "The username to use to authenticate when using a real server")
	password := flag.String("pass", "", "The password to use to authenticate when using a real server")
	bucketName := flag.String("bucket", "default", "The bucket to use to test against")
	// bucketPassword := flag.String("bucket-pass", "", "The bucket password to use when connecting to the bucket")
	version := flag.String("version", "", "The server or mock version being tested against (major.minor.patch.build_edition)")
	collectionName := flag.String("collection-name", "", "The name of the collection to use")
	flag.Parse()

	var err error
	var connStr string
	var mock *gojcbmock.Mock
	if *server == "" {
		if *version != "" {
			panic("version cannot be specified with mock")
		}

		mpath, err := gojcbmock.GetMockPath()
		if err != nil {
			panic(err.Error())
		}

		mock, err = gojcbmock.NewMock(mpath, 4, 1, 64, []gojcbmock.BucketSpec{
			{Name: "default", Type: gojcbmock.BCouchbase},
			{Name: "memd", Type: gojcbmock.BCouchbase},
		}...)

		if err != nil {
			panic(err.Error())
		}

		*version = mock.Version()

		connStr = fmt.Sprintf("http://127.0.0.1:%d", mock.EntryPort)
	} else {
		connStr = *server

		if *version == "" {
			*version = defaultServerVersion
		}
	}

	cluster, err := NewCluster(connStr, ClusterOptions{Authenticator: PasswordAuthenticator{Username: *user, Password: *password}})

	if err != nil {
		panic(err.Error())
	}

	nodeVersion, err := newNodeVersion(*version, mock != nil)
	if err != nil {
		panic(err.Error())
	}

	globalCluster = &testCluster{Cluster: cluster, Mock: mock, Version: nodeVersion}

	globalBucket = globalCluster.Bucket(*bucketName, &BucketOptions{UseMutationTokens: true})

	if *collectionName != "" {
		globalCollection = globalBucket.Collection("_default", *collectionName, nil)
	} else {
		globalCollection = globalBucket.DefaultCollection(nil)
	}

	globalTravelBucket = globalCluster.Bucket("travel-sample", &BucketOptions{UseMutationTokens: true})
	_, err = globalTravelBucket.DefaultCollection(nil).Get("invalid", nil)
	if !(err == nil || IsKeyNotFoundError(err)) {
		globalTravelBucket = nil
	}

	os.Exit(m.Run())
}

func loadTestDataset(dataset string, valuePtr interface{}) error {
	bytes, err := ioutil.ReadFile("testdata/" + dataset + ".json")
	if err != nil {
		return err
	}

	err = json.Unmarshal(bytes, &valuePtr)
	if err != nil {
		return err
	}

	return nil
}

type mockAuthenticator struct {
	username string
	password string
}

func newMockAuthenticator(username string, password string) *mockAuthenticator {
	return &mockAuthenticator{
		username: username,
		password: password,
	}
}

func (ma *mockAuthenticator) Credentials(req AuthCredsRequest) ([]UserPassPair, error) {
	return []UserPassPair{{
		Username: ma.username,
		Password: ma.password,
	}}, nil
}

type roundTripFunc func(req *http.Request) *http.Response

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}
