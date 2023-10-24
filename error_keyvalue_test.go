package gocb

import (
	"encoding/json"
	"github.com/couchbase/gocbcore/v10/memd"
)

func (suite *UnitTestSuite) TestKeyValueError() {
	aErr := &KeyValueError{
		InnerError:         ErrPathNotFound,
		StatusCode:         memd.StatusBusy,
		DocumentID:         "key",
		BucketName:         "bucket",
		ScopeName:          "scope",
		CollectionName:     "collection",
		CollectionID:       9,
		ErrorName:          "barry",
		ErrorDescription:   "sheen",
		Opaque:             0xa1,
		RetryReasons:       []RetryReason{CircuitBreakerOpenRetryReason},
		RetryAttempts:      3,
		LastDispatchedTo:   "10.112.210.101",
		LastDispatchedFrom: "10.112.210.1",
		LastConnectionID:   "123456",
	}

	b, err := json.Marshal(aErr)
	suite.Require().Nil(err)

	suite.Assert().Equal(
		[]byte("{\"msg\":\"path not found\",\"status_code\":133,\"document_id\":\"key\",\"bucket\":\"bucket\",\"scope\":\"scope\",\"collection\":\"collection\",\"collection_id\":9,\"error_name\":\"barry\",\"error_description\":\"sheen\",\"opaque\":161,\"retry_reasons\":[\"CIRCUIT_BREAKER_OPEN\"],\"retry_attempts\":3,\"last_dispatched_to\":\"10.112.210.101\",\"last_dispatched_from\":\"10.112.210.1\",\"last_connection_id\":\"123456\"}"),
		b,
	)
	suite.Assert().Equal(
		"path not found | {\"status_code\":133,\"document_id\":\"key\",\"bucket\":\"bucket\",\"scope\":\"scope\",\"collection\":\"collection\",\"collection_id\":9,\"error_name\":\"barry\",\"error_description\":\"sheen\",\"opaque\":161,\"retry_reasons\":[\"CIRCUIT_BREAKER_OPEN\"],\"retry_attempts\":3,\"last_dispatched_to\":\"10.112.210.101\",\"last_dispatched_from\":\"10.112.210.1\",\"last_connection_id\":\"123456\"}",
		aErr.Error(),
	)
}
