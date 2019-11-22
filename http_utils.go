package gocb

import (
	"io/ioutil"

	"github.com/couchbase/gocbcore/v8"
)

func tryReadHTTPBody(resp *gocbcore.HTTPResponse) string {
	bytes, _ := ioutil.ReadAll(resp.Body)
	return string(bytes)
}
