package gocb

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/couchbaselabs/gojcbmock"
)

const (
	defaultServerVersion = "5.1.0"
)

var globalBucket *Bucket
var globalCollection *Collection
var globalCluster *testCluster

func TestMain(m *testing.M) {
	server := flag.String("server", "", "The connection string to connect to for a real server")
	user := flag.String("user", "", "The username to use to authenticate when using a real server")
	password := flag.String("pass", "", "The password to use to authenticate when using a real server")
	bucketName := flag.String("bucket", "default", "The bucket to use to test against")
	// bucketPassword := flag.String("bucket-pass", "", "The bucket password to use when connecting to the bucket")
	version := flag.String("version", "", "The server or mock version being tested against (major.minor.patch.build_edition)")
	collectionName := flag.String("collection-name", "", "The name of the collection to use")
	disableLogger := flag.Bool("disable-logger", false, "Whether to disable the logger")
	flag.Parse()

	if !*disableLogger {
		SetLogger(VerboseStdioLogger())
	}

	var err error
	var connStr string
	var mock *gojcbmock.Mock
	var auth PasswordAuthenticator
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
		}...)

		mock.Control(gojcbmock.NewCommand(gojcbmock.CSetCCCP,
			map[string]interface{}{"enabled": "true"}))
		mock.Control(gojcbmock.NewCommand(gojcbmock.CSetSASLMechanisms,
			map[string]interface{}{"mechs": []string{"SCRAM-SHA512"}}))

		if err != nil {
			panic(err.Error())
		}

		*version = mock.Version()

		var addrs []string
		for _, mcport := range mock.MemcachedPorts() {
			addrs = append(addrs, fmt.Sprintf("127.0.0.1:%d", mcport))
		}
		connStr = fmt.Sprintf("couchbase://%s", strings.Join(addrs, ","))
		auth = PasswordAuthenticator{
			Username: "default",
			Password: "",
		}
	} else {
		connStr = *server

		auth = PasswordAuthenticator{
			Username: *user,
			Password: *password,
		}

		if *version == "" {
			*version = defaultServerVersion
		}
	}

	cluster, err := Connect(connStr, ClusterOptions{Authenticator: auth})

	time.Sleep(1000)

	if err != nil {
		panic(err.Error())
	}

	nodeVersion, err := newNodeVersion(*version, mock != nil)
	if err != nil {
		panic(err.Error())
	}

	globalCluster = &testCluster{Cluster: cluster, Mock: mock, Version: nodeVersion}

	globalBucket = globalCluster.Bucket(*bucketName, nil)

	if *collectionName != "" {
		globalCollection = globalBucket.Collection(*collectionName)
	} else {
		globalCollection = globalBucket.DefaultCollection()
	}

	os.Exit(m.Run())
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
