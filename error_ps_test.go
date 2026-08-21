package gocb

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
)

func (suite *UnitTestSuite) psStatus(code codes.Code, msg string, details ...protoadapt.MessageV1) *status.Status {
	st := status.New(code, msg)
	if len(details) > 0 {
		var err error
		st, err = st.WithDetails(details...)
		suite.Require().NoError(err)
	}

	return st
}

func (suite *UnitTestSuite) TestProtostellarErrorConversion() {
	type testCase struct {
		name        string
		grpcStatus  *status.Status
		readOnly    bool
		expectedErr error
	}

	testCases := []testCase{
		{
			name: "NotFoundResourceInfoBucket",
			grpcStatus: suite.psStatus(codes.NotFound, "bucket does not exist", &errdetails.ResourceInfo{
				ResourceType: "bucket",
				ResourceName: "test-bucket",
			}),
			expectedErr: ErrBucketNotFound,
		},
		{
			name: "NotFoundResourceInfoBucketNotFirstDetailBlock",
			grpcStatus: suite.psStatus(codes.NotFound, "bucket does not exist",
				&errdetails.ErrorInfo{
					Reason: "something",
				},
				&errdetails.ResourceInfo{
					ResourceType: "bucket",
					ResourceName: "test-bucket",
				}),
			expectedErr: ErrBucketNotFound,
		},
		{
			name: "AbortedCasMismatch",
			grpcStatus: suite.psStatus(codes.Aborted, "cas mismatch", &errdetails.ErrorInfo{
				Reason: "CAS_MISMATCH",
			}),
			expectedErr: ErrCasMismatch,
		},
		{
			name: "FailedPreconditionDocTooDeep",
			grpcStatus: suite.psStatus(codes.FailedPrecondition, "doc too deep", &errdetails.PreconditionFailure{
				Violations: []*errdetails.PreconditionFailure_Violation{
					{Type: "DOC_TOO_DEEP"},
				},
			}),
			expectedErr: ErrDocumentTooDeep,
		},
		{
			name: "FailedPreconditionNotLocked",
			grpcStatus: suite.psStatus(codes.FailedPrecondition, "doc not locked", &errdetails.PreconditionFailure{
				Violations: []*errdetails.PreconditionFailure_Violation{
					{Type: "NOT_LOCKED"},
				},
			}),
			expectedErr: ErrDocumentNotLocked,
		},
		{
			name: "AlreadyExistsResourceInfoDocument",
			grpcStatus: suite.psStatus(codes.AlreadyExists, "document exists", &errdetails.ResourceInfo{
				ResourceType: "document",
				ResourceName: "test-doc",
			}),
			expectedErr: ErrDocumentExists,
		},
		{
			name:        "DeadlineExceededReadOnly",
			grpcStatus:  suite.psStatus(codes.DeadlineExceeded, "deadline exceeded"),
			readOnly:    true,
			expectedErr: ErrUnambiguousTimeout,
		},
		{
			name:        "DeadlineExceededNotReadOnly",
			grpcStatus:  suite.psStatus(codes.DeadlineExceeded, "deadline exceeded"),
			expectedErr: ErrAmbiguousTimeout,
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			err := mapPsErrorStatusToGocbError(tc.grpcStatus, tc.readOnly)
			suite.Require().NotNil(err)
			suite.Assert().ErrorIs(err.InnerError, tc.expectedErr)
		})
	}
}

func (suite *UnitTestSuite) TestProtostellarErrorConversionNoApplicableDetail() {
	type testCase struct {
		name       string
		grpcStatus *status.Status
	}

	testCases := []testCase{
		{
			name:       "NotFoundNoDetails",
			grpcStatus: suite.psStatus(codes.NotFound, "not found"),
		},
		{
			name: "NotFoundNoResourceInfo",
			grpcStatus: suite.psStatus(codes.NotFound, "not found", &errdetails.ErrorInfo{
				Reason: "something",
			}),
		},
		{
			name: "NotFoundUnknownResourceType",
			grpcStatus: suite.psStatus(codes.NotFound, "not found", &errdetails.ResourceInfo{
				ResourceType: "something",
			}),
		},
		{
			name: "AlreadyExistsNoResourceInfo",
			grpcStatus: suite.psStatus(codes.AlreadyExists, "already exists", &errdetails.ErrorInfo{
				Reason: "something",
			}),
		},
		{
			name: "FailedPreconditionNoPreconditionFailure",
			grpcStatus: suite.psStatus(codes.FailedPrecondition, "failed precondition", &errdetails.ResourceInfo{
				ResourceType: "document",
			}),
		},
		{
			name: "FailedPreconditionUnknownViolation",
			grpcStatus: suite.psStatus(codes.FailedPrecondition, "failed precondition", &errdetails.PreconditionFailure{
				Violations: []*errdetails.PreconditionFailure_Violation{
					{Type: "SOMETHING"},
				},
			}),
		},
		{
			name: "AbortedNoErrorInfo",
			grpcStatus: suite.psStatus(codes.Aborted, "aborted", &errdetails.ResourceInfo{
				ResourceType: "document",
			}),
		},
		{
			name: "AbortedUnknownReason",
			grpcStatus: suite.psStatus(codes.Aborted, "aborted", &errdetails.ErrorInfo{
				Reason: "something",
			}),
		},
		{
			name:       "UnmappedStatusCode",
			grpcStatus: suite.psStatus(codes.ResourceExhausted, "resource exhausted"),
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			err := mapPsErrorStatusToGocbError(tc.grpcStatus, false)
			suite.Require().NotNil(err)

			// We could not map the status to a specific error, so the gRPC status error
			// should be used as the inner error.
			suite.Require().NotNil(err.InnerError)
			suite.Assert().Equal(tc.grpcStatus.Err().Error(), err.InnerError.Error())
		})
	}
}

func (suite *UnitTestSuite) TestProtostellarErrorDetailsExtraction() {
	st := suite.psStatus(codes.NotFound, "not found",
		&errdetails.ErrorInfo{Reason: "FIRST_REASON"},
		&errdetails.ResourceInfo{ResourceType: "document", ResourceName: "first-doc"},
		&errdetails.PreconditionFailure{
			Violations: []*errdetails.PreconditionFailure_Violation{
				{Type: "FIRST_VIOLATION"},
			},
		},
		&errdetails.ErrorInfo{Reason: "SECOND_REASON"},
		&errdetails.ResourceInfo{ResourceType: "bucket", ResourceName: "second-bucket"},
		&errdetails.PreconditionFailure{
			Violations: []*errdetails.PreconditionFailure_Violation{
				{Type: "SECOND_VIOLATION"},
			},
		},
	)

	details := extractPsErrorDetails(st)
	suite.Assert().Equal("FIRST_REASON", details.errorInfoReason)
	suite.Assert().Equal("document", details.resourceInfoType)
	suite.Assert().Equal("first-doc", details.resourceInfoName)
	suite.Assert().Equal("FIRST_VIOLATION", details.preconditionFailureViolation)
}

func (suite *UnitTestSuite) TestProtostellarErrorDetailsExtractionNoDetails() {
	details := extractPsErrorDetails(suite.psStatus(codes.NotFound, "not found"))
	suite.Assert().Equal(psErrorDetails{}, details)
}
