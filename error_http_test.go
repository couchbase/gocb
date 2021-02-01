package gocb

import (
	"encoding/json"
	"errors"
	"fmt"
)

func (suite *UnitTestSuite) TestHTTPError() {
	aErr := HTTPError{
		InnerError:    errors.New("uh oh"),
		Endpoint:      "http://127.0.0.1:8091",
		UniqueID:      "123445",
		RetryReasons:  nil,
		RetryAttempts: 0,
	}

	b, err := json.Marshal(aErr)
	suite.Require().Nil(err)

	fmt.Println(string(b))
	suite.Assert().Equal(
		[]byte("{\"msg\":\"uh oh\",\"unique_id\":\"123445\",\"endpoint\":\"http://127.0.0.1:8091\"}"),
		b,
	)
	suite.Assert().Equal(
		"uh oh | {\"unique_id\":\"123445\",\"endpoint\":\"http://127.0.0.1:8091\"}",
		aErr.Error(),
	)
}
