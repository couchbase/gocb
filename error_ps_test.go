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
		{
			name:        "PermissionDeniedNoDetailBlock",
			grpcStatus:  suite.psStatus(codes.PermissionDenied, "permission denied"),
			expectedErr: ErrAuthenticationFailure,
		},
		{
			name: "PermissionDeniedResourceTypeUser",
			grpcStatus: suite.psStatus(codes.PermissionDenied, "permission denied", &errdetails.ResourceInfo{
				ResourceType: resourceTypeUser,
				ResourceName: "user1",
			}),
			expectedErr: ErrAuthenticationFailure,
		},
		{
			name: "PermissionDeniedResourceTypeBucket",
			grpcStatus: suite.psStatus(codes.PermissionDenied, "permission denied", &errdetails.ResourceInfo{
				ResourceType: resourceTypeBucket,
				ResourceName: "default",
			}),
			expectedErr: ErrAuthorizationFailure,
		},
		{
			name: "PermissionDeniedResourceInfoNotFirstDetail",
			grpcStatus: suite.psStatus(codes.PermissionDenied, "permission denied",
				&errdetails.ErrorInfo{Reason: "something"},
				&errdetails.ResourceInfo{ResourceType: resourceTypeCollection, ResourceName: "test-collection"},
			),
			expectedErr: ErrAuthorizationFailure,
		},
		{
			name: "Unauthenticated",
			grpcStatus: suite.psStatus(codes.Unauthenticated, "unauthenticated", &errdetails.ResourceInfo{
				ResourceType: resourceTypeBucket,
				ResourceName: "default",
			}),
			expectedErr: ErrAuthenticationFailure,
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
		&errdetails.DebugInfo{Detail: "FIRST_DETAIL", StackEntries: []string{"frame1", "frame2"}},
		&errdetails.RequestInfo{RequestId: "FIRST_REQUEST_ID", ServingData: "FIRST_SERVING_DATA"},
		&errdetails.DebugInfo{Detail: "SECOND_DETAIL", StackEntries: []string{"frame3"}},
		&errdetails.RequestInfo{RequestId: "SECOND_REQUEST_ID", ServingData: "SECOND_SERVING_DATA"},
	)

	details := extractPsErrorDetails(st)
	suite.Assert().EqualExportedValues(&errdetails.ErrorInfo{
		Reason: "FIRST_REASON",
	}, details.errorInfo)
	suite.Assert().EqualExportedValues(&errdetails.ResourceInfo{
		ResourceType: "document",
		ResourceName: "first-doc",
	}, details.resourceInfo)
	suite.Assert().EqualExportedValues(&errdetails.PreconditionFailure{
		Violations: []*errdetails.PreconditionFailure_Violation{
			{Type: "FIRST_VIOLATION"},
		},
	}, details.preconditionFailure)
	suite.Assert().EqualExportedValues(&errdetails.DebugInfo{
		Detail:       "FIRST_DETAIL",
		StackEntries: []string{"frame1", "frame2"},
	}, details.debugInfo)
	suite.Assert().EqualExportedValues(&errdetails.RequestInfo{
		RequestId:   "FIRST_REQUEST_ID",
		ServingData: "FIRST_SERVING_DATA",
	}, details.requestInfo)
}

func (suite *UnitTestSuite) TestProtostellarErrorDetailsExtractionNoDetails() {
	details := extractPsErrorDetails(suite.psStatus(codes.NotFound, "not found"))
	suite.Assert().Zero(details)
}

func (suite *UnitTestSuite) TestPsErrorDetailsAddToContext() {
	type testCase struct {
		name     string
		details  psErrorDetails
		expected map[string]interface{}
	}

	testCases := []testCase{
		{
			name:     "Empty",
			details:  psErrorDetails{},
			expected: map[string]interface{}{},
		},
		{
			name: "PreconditionViolation",
			details: psErrorDetails{
				preconditionFailure: &errdetails.PreconditionFailure{
					Violations: []*errdetails.PreconditionFailure_Violation{
						{Type: "LOCKED"},
					},
				},
			},
			expected: map[string]interface{}{
				"precondition_violation": "LOCKED",
			},
		},
		{
			name: "ResourceTypeOnly",
			details: psErrorDetails{
				resourceInfo: &errdetails.ResourceInfo{ResourceType: "document"},
			},
			expected: map[string]interface{}{
				"resource_type": "document",
			},
		},
		{
			name: "ResourceNameOnly",
			details: psErrorDetails{
				resourceInfo: &errdetails.ResourceInfo{ResourceName: "my-doc"},
			},
			expected: map[string]interface{}{
				"resource_name": "my-doc",
			},
		},
		{
			name: "ResourceTypeAndName",
			details: psErrorDetails{
				resourceInfo: &errdetails.ResourceInfo{ResourceType: "document", ResourceName: "my-doc"},
			},
			expected: map[string]interface{}{
				"resource_type": "document",
				"resource_name": "my-doc",
			},
		},
		{
			name: "Reason",
			details: psErrorDetails{
				errorInfo: &errdetails.ErrorInfo{Reason: "CAS_MISMATCH"},
			},
			expected: map[string]interface{}{
				"reason": "CAS_MISMATCH",
			},
		},
		{
			name: "DebugInfo",
			details: psErrorDetails{
				debugInfo: &errdetails.DebugInfo{Detail: "some detail", StackEntries: []string{"frame1", "frame2"}},
			},
			expected: map[string]interface{}{
				"debug_info": map[string]interface{}{
					"detail":      "some detail",
					"stack_trace": []string{"frame1", "frame2"},
				},
			},
		},
		{
			name: "DebugInfoDetailOnly",
			details: psErrorDetails{
				debugInfo: &errdetails.DebugInfo{Detail: "some detail"},
			},
			expected: map[string]interface{}{
				"debug_info": map[string]interface{}{
					"detail":      "some detail",
					"stack_trace": []string(nil),
				},
			},
		},
		{
			name: "DebugInfoStackTraceOnly",
			details: psErrorDetails{
				debugInfo: &errdetails.DebugInfo{StackEntries: []string{"frame1"}},
			},
			expected: map[string]interface{}{
				"debug_info": map[string]interface{}{
					"detail":      "",
					"stack_trace": []string{"frame1"},
				},
			},
		},
		{
			name: "DebugInfoPresentButEmpty",
			details: psErrorDetails{
				debugInfo: &errdetails.DebugInfo{},
			},
			expected: map[string]interface{}{
				"debug_info": map[string]interface{}{
					"detail":      "",
					"stack_trace": []string(nil),
				},
			},
		},
		{
			name: "RequestInfo",
			details: psErrorDetails{
				requestInfo: &errdetails.RequestInfo{RequestId: "req-1", ServingData: "serving"},
			},
			expected: map[string]interface{}{
				"request_info": map[string]interface{}{
					"request_id":   "req-1",
					"serving_data": "serving",
				},
			},
		},
		{
			name: "RequestInfoRequestIDOnly",
			details: psErrorDetails{
				requestInfo: &errdetails.RequestInfo{RequestId: "req-1"},
			},
			expected: map[string]interface{}{
				"request_info": map[string]interface{}{
					"request_id":   "req-1",
					"serving_data": "",
				},
			},
		},
		{
			name: "RequestInfoPresentButEmpty",
			details: psErrorDetails{
				requestInfo: &errdetails.RequestInfo{},
			},
			expected: map[string]interface{}{
				"request_info": map[string]interface{}{
					"request_id":   "",
					"serving_data": "",
				},
			},
		},
		{
			name: "AllFields",
			details: psErrorDetails{
				resourceInfo: &errdetails.ResourceInfo{ResourceType: "document", ResourceName: "my-doc"},
				errorInfo:    &errdetails.ErrorInfo{Reason: "CAS_MISMATCH"},
				preconditionFailure: &errdetails.PreconditionFailure{
					Violations: []*errdetails.PreconditionFailure_Violation{
						{Type: "LOCKED"},
					},
				},
				debugInfo:   &errdetails.DebugInfo{Detail: "some detail", StackEntries: []string{"frame1"}},
				requestInfo: &errdetails.RequestInfo{RequestId: "req-1", ServingData: "serving"},
			},
			expected: map[string]interface{}{
				"resource_type":          "document",
				"resource_name":          "my-doc",
				"reason":                 "CAS_MISMATCH",
				"precondition_violation": "LOCKED",
				"debug_info": map[string]interface{}{
					"detail":      "some detail",
					"stack_trace": []string{"frame1"},
				},
				"request_info": map[string]interface{}{
					"request_id":   "req-1",
					"serving_data": "serving",
				},
			},
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			context := map[string]interface{}{}
			tc.details.addToContext(context)
			suite.Assert().Equal(tc.expected, context)
		})
	}
}
