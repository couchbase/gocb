package gocb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

func (suite *UnitTestSuite) TestHTTPError() {
	aErr := &HTTPError{
		InnerError:    errors.New("uh oh"),
		Endpoint:      "http://127.0.0.1:8091",
		UniqueID:      "123445",
		RetryReasons:  nil,
		RetryAttempts: 0,
	}

	b, err := json.Marshal(aErr)
	suite.Require().NoError(err)

	suite.Assert().Equal(
		[]byte("{\"msg\":\"uh oh\",\"unique_id\":\"123445\",\"endpoint\":\"http://127.0.0.1:8091\"}"),
		b,
	)
	suite.Assert().Equal(
		"uh oh | {\"unique_id\":\"123445\",\"endpoint\":\"http://127.0.0.1:8091\"}",
		aErr.Error(),
	)
}

func (suite *UnitTestSuite) TestMgmtErrorParsersHTTP400() {
	parsers := map[string]func(*mgmtRequest, *mgmtResponse) error{
		"bucket":          (&bucketManagementProviderCore{}).tryParseErrorMessage,
		"bucket flush":    (&bucketManagementProviderCore{}).tryParseFlushErrorMessage,
		"collections":     (&collectionsManagementProviderCore{}).tryParseErrorMessage,
		"users":           (&userManagerProviderCore{}).tryParseErrorMessage,
		"views":           (&viewIndexProviderCore{}).tryParseErrorMessage,
		"search indexes":  (&searchIndexProviderCore{}).tryParseErrorMessage,
		"analytics links": (&analyticsProviderCore{}).tryParseLinkErrorMessage,
		"eventing":        (&eventingManagementProviderCore{}).tryParseErrorMessage,
	}

	// Doesn't match any parser's specific errors, so each one reaches its fallback.
	newResp := func(statusCode uint32) *mgmtResponse {
		return &mgmtResponse{StatusCode: statusCode, Body: io.NopCloser(bytes.NewReader([]byte(`{"_":"some error"}`)))}
	}

	for name, parse := range parsers {
		suite.Run(name, func() {
			suite.Assert().ErrorIs(parse(&mgmtRequest{}, newResp(400)), ErrInvalidArgument)
			suite.Assert().NotErrorIs(parse(&mgmtRequest{}, newResp(500)), ErrInvalidArgument)
		})
	}
}

func (suite *UnitTestSuite) TestMgmtErrorParsersHTTP400SpecificMapping() {
	resp := &mgmtResponse{
		StatusCode: 400,
		Body:       io.NopCloser(bytes.NewReader([]byte(`{"errors":{"name":"Bucket with given name already exists"}}`))),
	}

	err := (&bucketManagementProviderCore{}).tryParseErrorMessage(&mgmtRequest{}, resp)
	suite.Assert().ErrorIs(err, ErrBucketExists)
	suite.Assert().NotErrorIs(err, ErrInvalidArgument)
}

func (suite *UnitTestSuite) TestMakeMgmtBadStatusErrorHTTP400() {
	resp := &mgmtResponse{
		StatusCode: 400,
		Body:       io.NopCloser(bytes.NewReader([]byte(`bad request`))),
	}

	err := makeMgmtBadStatusError("failed to do thing", &mgmtRequest{}, resp)
	suite.Assert().ErrorIs(err, ErrInvalidArgument)
	suite.Assert().ErrorContains(err, "failed to do thing")

	resp = &mgmtResponse{
		StatusCode: 500,
		Body:       io.NopCloser(bytes.NewReader([]byte(`server error`))),
	}

	err = makeMgmtBadStatusError("failed to do thing", &mgmtRequest{}, resp)
	suite.Assert().NotErrorIs(err, ErrInvalidArgument)
	suite.Assert().ErrorContains(err, "failed to do thing")
}
