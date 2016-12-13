package gocb

import (
	"flag"
	"fmt"
	"gopkg.in/couchbaselabs/gojcbmock.v1"
	"os"
	"testing"
)

var globalBucket *Bucket
var globalMock *gojcbmock.Mock

func TestMain(m *testing.M) {
	flag.Parse()
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

	connStr := fmt.Sprintf("http://127.0.0.1:%d", globalMock.EntryPort)

	cluster, err := Connect(connStr)

	if err != nil {
		panic(err.Error())
	}

	globalBucket, err = cluster.OpenBucket("default", "")

	if err != nil {
		panic(err.Error())
	}

	os.Exit(m.Run())
}
