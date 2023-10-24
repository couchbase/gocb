package gocb

import (
	"encoding/json"
)

func (suite *UnitTestSuite) TestViewError() {
	aErr := &ViewError{
		InnerError:         ErrViewNotFound,
		DesignDocumentName: "designdoc",
		ViewName:           "viewname",
		Errors: []ViewErrorDesc{
			{
				SourceNode: "http://127.0.0.1:8092",
				Message:    "error message",
			},
		},
		Endpoint: "http://127.0.0.1:8092",
	}

	b, err := json.Marshal(aErr)
	suite.Require().Nil(err)

	suite.Assert().Equal(
		[]byte("{\"msg\":\"view not found\",\"design_document_name\":\"designdoc\",\"view_name\":\"viewname\",\"errors\":[{\"SourceNode\":\"http://127.0.0.1:8092\",\"Message\":\"error message\"}],\"endpoint\":\"http://127.0.0.1:8092\"}"),
		b,
	)
	suite.Assert().Equal(
		"view not found | {\"design_document_name\":\"designdoc\",\"view_name\":\"viewname\",\"errors\":[{\"SourceNode\":\"http://127.0.0.1:8092\",\"Message\":\"error message\"}],\"endpoint\":\"http://127.0.0.1:8092\"}",
		aErr.Error(),
	)
}
