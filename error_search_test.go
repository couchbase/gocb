package gocb

import (
	"encoding/json"
	"github.com/couchbase/gocb/v2/search"
)

func (suite *UnitTestSuite) TestSearchError() {
	aErr := SearchError{
		InnerError:    ErrIndexFailure,
		Query:         search.NewMatchAllQuery(),
		Endpoint:      "http://127.0.0.1:8094",
		RetryReasons:  []RetryReason{SearchTooManyRequestsRetryReason},
		RetryAttempts: 3,
		ErrorText:     "error text",
		IndexName:     "barry",
	}

	b, err := json.Marshal(aErr)
	suite.Require().Nil(err)

	suite.Assert().Equal(
		[]byte("{\"msg\":\"index failure\",\"index_name\":\"barry\",\"query\":{\"match_all\":null},\"error_text\":\"error text\",\"endpoint\":\"http://127.0.0.1:8094\",\"retry_reasons\":[\"SEARCH_TOO_MANY_REQUESTS\"],\"retry_attempts\":3}"),
		b,
	)
	suite.Assert().Equal(
		"index failure | {\"index_name\":\"barry\",\"query\":{\"match_all\":null},\"error_text\":\"error text\",\"endpoint\":\"http://127.0.0.1:8094\",\"retry_reasons\":[\"SEARCH_TOO_MANY_REQUESTS\"],\"retry_attempts\":3}",
		aErr.Error(),
	)
}
