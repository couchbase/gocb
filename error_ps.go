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
	resourceInfoType             string
	resourceInfoName             string
	errorInfoReason              string
	preconditionFailureViolation string
}

func extractPsErrorDetails(st *status.Status) psErrorDetails {
	var preconditionFailure *errdetails.PreconditionFailure
	var errorInfo *errdetails.ErrorInfo
	var resourceInfo *errdetails.ResourceInfo

	for _, detail := range st.Details() {
		if preconditionFailure != nil && errorInfo != nil && resourceInfo != nil {
			// No more detail blocks we could be interested in
			break
		}
		switch d := detail.(type) {
		case *errdetails.PreconditionFailure:
			if preconditionFailure == nil {
				preconditionFailure = d
			}
		case *errdetails.ErrorInfo:
			if errorInfo == nil {
				errorInfo = d
			}
		case *errdetails.ResourceInfo:
			if resourceInfo == nil {
				resourceInfo = d
			}
		}
	}

	details := psErrorDetails{}
	if preconditionFailure != nil && len(preconditionFailure.Violations) > 0 {
		details.preconditionFailureViolation = preconditionFailure.Violations[0].Type
	}
	if resourceInfo != nil {
		details.resourceInfoType = resourceInfo.ResourceType
		details.resourceInfoName = resourceInfo.ResourceName
	}
	if errorInfo != nil {
		details.errorInfoReason = errorInfo.Reason
	}
	return details
}

func mapPsErrorStatusToGocbError(st *status.Status, readOnly bool) *GenericError {
	context := map[string]interface{}{
		"server":  st.Message(),
		"details": len(st.Details()),
	}

	details := extractPsErrorDetails(st)

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
		baseErr = wrapError(ErrAuthenticationFailure, "server reported that permission to the resource was denied")
	case codes.PermissionDenied:
		baseErr = wrapError(ErrAuthenticationFailure, "server reported that permission to the resource was denied")
	case codes.Unimplemented:
		baseErr = wrapError(ErrFeatureNotAvailable, st.Message())
	case codes.Unavailable:
		baseErr = ErrServiceNotAvailable
	case codes.FailedPrecondition:
		if details.preconditionFailureViolation != "" {
			context["precondition_violation"] = details.preconditionFailureViolation

			switch details.preconditionFailureViolation {
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
		if details.resourceInfoType != "" {
			context["resource_type"] = details.resourceInfoType
			if details.resourceInfoName != "" {
				context["resource_name"] = details.resourceInfoName
			}
			switch details.resourceInfoType {
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
		if details.resourceInfoType != "" {
			context["resource_type"] = details.resourceInfoType
			if details.resourceInfoName != "" {
				context["resource_name"] = details.resourceInfoName
			}
			switch details.resourceInfoType {
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
		if details.errorInfoReason != "" {
			context["reason"] = details.errorInfoReason
			switch details.errorInfoReason {
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
