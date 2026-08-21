package gocb

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	preconditionLocked              = "LOCKED"
	preconditionNotLocked           = "NOT_LOCKED"
	preconditionPathMismatch        = "PATH_MISMATCH"
	preconditionDocNotJSON          = "DOC_NOT_JSON"
	preconditionDocTooDeep          = "DOC_TOO_DEEP"
	preconditionValueTooLarge       = "VALUE_TOO_LARGE"
	preconditionValueOutOfRange     = "VALUE_OUT_OF_RANGE"
	preconditionPathValueOutOfRange = "PATH_VALUE_OUT_OF_RANGE"
)

const (
	resourceTypeDocument    = "document"
	resourceTypeIndex       = "queryindex"
	resourceTypeBucket      = "bucket"
	resourceTypeScope       = "scope"
	resourceTypeCollection  = "collection"
	resourceTypePath        = "path"
	resourceTypeSearchIndex = "searchindex"
	resourceTypeUser        = "user"
)

const (
	reasonCasMismatch = "CAS_MISMATCH"
)

func mapPsErrorToGocbError(err error, readOnly bool) *GenericError {
	st, ok := status.FromError(err)
	if !ok {
		return makeGenericError(err, nil)
	}

	return mapPsErrorStatusToGocbError(st, readOnly)
}

type psErrorDetails struct {
	preconditionFailure *errdetails.PreconditionFailure
	errorInfo           *errdetails.ErrorInfo
	resourceInfo        *errdetails.ResourceInfo
	debugInfo           *errdetails.DebugInfo
	requestInfo         *errdetails.RequestInfo
}

func (d psErrorDetails) preconditionViolation() string {
	if d.preconditionFailure == nil || len(d.preconditionFailure.Violations) == 0 {
		return ""
	}
	return d.preconditionFailure.Violations[0].Type
}

func (d psErrorDetails) addToContext(context map[string]interface{}) {
	if d.resourceInfo != nil {
		if d.resourceInfo.ResourceType != "" {
			context["resource_type"] = d.resourceInfo.ResourceType
		}
		if d.resourceInfo.ResourceName != "" {
			context["resource_name"] = d.resourceInfo.ResourceName
		}
	}
	if d.errorInfo != nil && d.errorInfo.Reason != "" {
		context["reason"] = d.errorInfo.Reason
	}
	if violation := d.preconditionViolation(); violation != "" {
		context["precondition_violation"] = violation
	}
	if d.debugInfo != nil {
		context["debug_info"] = map[string]interface{}{
			"detail":      d.debugInfo.Detail,
			"stack_trace": d.debugInfo.StackEntries,
		}
	}
	if d.requestInfo != nil {
		context["request_info"] = map[string]interface{}{
			"request_id":   d.requestInfo.RequestId,
			"serving_data": d.requestInfo.ServingData,
		}
	}
}

func extractPsErrorDetails(st *status.Status) psErrorDetails {
	var details psErrorDetails

	for _, detail := range st.Details() {
		if details.preconditionFailure != nil && details.errorInfo != nil && details.resourceInfo != nil &&
			details.debugInfo != nil && details.requestInfo != nil {
			// No more detail blocks we could be interested in
			break
		}
		switch d := detail.(type) {
		case *errdetails.PreconditionFailure:
			if details.preconditionFailure == nil {
				details.preconditionFailure = d
			}
		case *errdetails.ErrorInfo:
			if details.errorInfo == nil {
				details.errorInfo = d
			}
		case *errdetails.ResourceInfo:
			if details.resourceInfo == nil {
				details.resourceInfo = d
			}
		case *errdetails.DebugInfo:
			if details.debugInfo == nil {
				details.debugInfo = d
			}
		case *errdetails.RequestInfo:
			if details.requestInfo == nil {
				details.requestInfo = d
			}
		}
	}

	return details
}

func mapPsErrorStatusToGocbError(st *status.Status, readOnly bool) *GenericError {
	context := map[string]interface{}{
		"server":  st.Message(),
		"details": len(st.Details()),
	}

	details := extractPsErrorDetails(st)
	details.addToContext(context)

	var baseErr error
	switch st.Code() {
	case codes.Canceled:
		baseErr = ErrRequestCanceled
	case codes.Internal:
		baseErr = ErrInternalServerFailure
	case codes.InvalidArgument:
		baseErr = ErrInvalidArgument
	case codes.DeadlineExceeded:
		if readOnly {
			baseErr = ErrUnambiguousTimeout
		} else {
			baseErr = ErrAmbiguousTimeout
		}
	case codes.Unauthenticated:
		baseErr = wrapError(ErrAuthenticationFailure, "no credentials were provided - likely an SDK bug")
	case codes.PermissionDenied:
		if details.resourceInfo != nil && details.resourceInfo.ResourceType != "" {
			switch details.resourceInfo.ResourceType {
			case resourceTypeUser:
				baseErr = wrapError(ErrAuthenticationFailure, "the server has rejected the provided credentials")
			default:
				baseErr = wrapError(ErrAuthorizationFailure, "the user does not have permission to access or modify this resource")
			}
		} else {
			baseErr = wrapError(ErrAuthenticationFailure, "the server has rejected the provided credentials")
		}
	case codes.Unimplemented:
		baseErr = wrapError(ErrFeatureNotAvailable, st.Message())
	case codes.Unavailable:
		baseErr = ErrServiceNotAvailable
	case codes.FailedPrecondition:
		if violation := details.preconditionViolation(); violation != "" {
			switch violation {
			case preconditionLocked:
				baseErr = ErrDocumentLocked
			case preconditionNotLocked:
				baseErr = ErrDocumentNotLocked
			case preconditionPathMismatch:
				baseErr = ErrPathMismatch
			case preconditionDocNotJSON:
				baseErr = ErrDocumentNotJSON
			case preconditionDocTooDeep:
				baseErr = ErrDocumentTooDeep
			case preconditionValueTooLarge:
				baseErr = ErrValueTooLarge
			case preconditionValueOutOfRange:
				baseErr = ErrValueInvalid
			case preconditionPathValueOutOfRange:
				baseErr = ErrNumberTooBig
			}
		}
	case codes.NotFound:
		if details.resourceInfo != nil && details.resourceInfo.ResourceType != "" {
			switch details.resourceInfo.ResourceType {
			case resourceTypeDocument:
				baseErr = ErrDocumentNotFound
			case resourceTypeIndex:
				baseErr = ErrIndexNotFound
			case resourceTypeSearchIndex:
				baseErr = ErrIndexNotFound
			case resourceTypeBucket:
				baseErr = ErrBucketNotFound
			case resourceTypeScope:
				baseErr = ErrScopeNotFound
			case resourceTypeCollection:
				baseErr = ErrCollectionNotFound
			case resourceTypePath:
				baseErr = ErrPathNotFound
			}
		}
	case codes.AlreadyExists:
		if details.resourceInfo != nil && details.resourceInfo.ResourceType != "" {
			switch details.resourceInfo.ResourceType {
			case resourceTypeDocument:
				baseErr = ErrDocumentExists
			case resourceTypeIndex:
				baseErr = ErrIndexExists
			case resourceTypeSearchIndex:
				baseErr = ErrIndexExists
			case resourceTypeBucket:
				baseErr = ErrBucketExists
			case resourceTypeScope:
				baseErr = ErrScopeExists
			case resourceTypeCollection:
				baseErr = ErrCollectionExists
			case resourceTypePath:
				baseErr = ErrPathExists
			}
		}
	case codes.Aborted:
		if details.errorInfo != nil && details.errorInfo.Reason != "" {
			switch details.errorInfo.Reason {
			case reasonCasMismatch:
				baseErr = ErrCasMismatch
			}
		}
	}

	if baseErr == nil {
		// Either the status code is not one that we map to a specific error, or the
		// status did not contain a detail block applicable to the status code.
		baseErr = st.Err()
	}

	return makeGenericError(baseErr, context)
}
