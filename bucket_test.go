package gocb

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"gopkg.in/couchbaselabs/gojcbmock.v1"
)

var globalBucket *Bucket
var globalMock *gojcbmock.Mock
var globalCluster *Cluster

func TestMain(m *testing.M) {
	SetLogger(VerboseStdioLogger())

	server := flag.String("server", "", "The connection string to connect to for a real server")
	user := flag.String("user", "", "The username to use to authenticate when using a real server")
	password := flag.String("pass", "", "The password to use to authenticate when using a real server")
	bucketName := flag.String("bucket", "default", "The bucket to use to test against")
	bucketPassword := flag.String("bucket-pass", "", "The bucket password to use when connecting to the bucket")
	flag.Parse()

	var err error
	var connStr string
	if *server == "" {
		mpath, err := gojcbmock.GetMockPath()
		if err != nil {
			panic(err.Error())
		}

		globalMock, err = gojcbmock.NewMock(mpath, 4, 1, 64, []gojcbmock.BucketSpec{
			{Name: "default", Type: gojcbmock.BCouchbase},
			{Name: "memd", Type: gojcbmock.BMemcached},
		}...)

		if err != nil {
			panic(err.Error())
		}

		connStr = fmt.Sprintf("http://127.0.0.1:%d", globalMock.EntryPort)
	} else {
		connStr = *server
	}

	globalCluster, err = Connect(connStr)

	if err != nil {
		panic(err.Error())
	}

	if *user != "" {
		globalCluster.Authenticate(PasswordAuthenticator{Username: *user, Password: *password})
	}

	globalBucket, err = globalCluster.OpenBucket(*bucketName, *bucketPassword)

	if err != nil {
		panic(err.Error())
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

func IsMockServer() bool {
	return globalMock != nil
}
