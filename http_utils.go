package gocb

import (
	"io/ioutil"

	"github.com/couchbase/gocbcore/v8"
)

func tryReadHTTPBody(resp *gocbcore.HTTPResponse) string {
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logDebugf("failed to read http body: %s", err)
	}
	return string(bytes)
}
