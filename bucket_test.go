package gocb

import (
	"flag"
	"fmt"
	"github.com/couchbase/gocb/gocbcore/javamock"
	"os"
	"testing"
)

var globalBucket *Bucket
var globalMock *gocbmock.Mock

func TestMain(m *testing.M) {
	flag.Parse()
	mpath, err := gocbmock.GetMockPath()
	if err != nil {
		panic(err.Error())
	}

	globalMock, err = gocbmock.NewMock(mpath, 4, 1, 64, []gocbmock.BucketSpec{
		{Name: "default", Type: gocbmock.BCouchbase},
		{Name: "memd", Type: gocbmock.BMemcached},
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
