package gocb

import (
	"encoding/json"
)

func (suite *UnitTestSuite) TestAnalyticsError() {
	aErr := AnalyticsError{
		InnerError:      ErrDatasetNotFound,
		Statement:       "select * from dataset",
		ClientContextID: "12345",
		Errors: []AnalyticsErrorDesc{{
			Code:    1000,
			Message: "error 1000",
		}},
		Endpoint:      "http://127.0.0.1:8095",
		RetryReasons:  []RetryReason{AnalyticsTemporaryFailureRetryReason},
		RetryAttempts: 3,
	}

	b, err := json.Marshal(aErr)
	suite.Require().Nil(err)

	suite.Assert().Equal(
		[]byte("{\"msg\":\"dataset not found\",\"statement\":\"select * from dataset\",\"client_context_id\":\"12345\",\"errors\":[{\"Code\":1000,\"Message\":\"error 1000\"}],\"endpoint\":\"http://127.0.0.1:8095\",\"retry_reasons\":[\"ANALYTICS_TEMPORARY_FAILURE\"],\"retry_attempts\":3}"),
		b,
	)
	suite.Assert().Equal(
		"dataset not found | {\"statement\":\"select * from dataset\",\"client_context_id\":\"12345\",\"errors\":[{\"Code\":1000,\"Message\":\"error 1000\"}],\"endpoint\":\"http://127.0.0.1:8095\",\"retry_reasons\":[\"ANALYTICS_TEMPORARY_FAILURE\"],\"retry_attempts\":3}",
		aErr.Error(),
	)
}
