package helpers

import (
	"errors"

	"github.com/couchbase/gocb/v2"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
)

func MapErrorToProto(err error) *shared.Exception {
	cbErr := mapCbErrorToProto(err)
	if cbErr != nil {
		// In other SDKs, this is the name of the error class. This is not how Go errors work, but let's provide
		// the error message without the error context fields if possible, or just the full error message otherwise.
		// This isn't used in test assertions, but it is used by the situational result UI to provide the error summary.
		var name string
		if wrappedErr := errors.Unwrap(err); wrappedErr != nil {
			name = wrappedErr.Error()
		} else {
			name = err.Error()
		}

		return &shared.Exception{
			Exception: &shared.Exception_Couchbase{
				Couchbase: &shared.CouchbaseExceptionEx{
					Name:       name,
					Type:       *cbErr,
					Serialized: err.Error(),
				},
			},
		}
	}

	return &shared.Exception{
		Exception: &shared.Exception_Other{
			Other: &shared.ExceptionOther{
				Serialized: err.Error(),
			},
		},
	}
}

func mapCbErrorToProto(err error) *shared.CouchbaseExceptionType {
	if errType := errorIs(err, gocb.ErrRequestCanceled, shared.CouchbaseExceptionType_SDK_REQUEST_CANCELLED_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrInvalidArgument, shared.CouchbaseExceptionType_SDK_INVALID_ARGUMENT_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrServiceNotAvailable, shared.CouchbaseExceptionType_SDK_SERVICE_NOT_AVAILABLE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrInternalServerFailure, shared.CouchbaseExceptionType_SDK_INTERNAL_SERVER_FAILURE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrAuthenticationFailure, shared.CouchbaseExceptionType_SDK_AUTHENTICATION_FAILURE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrTemporaryFailure, shared.CouchbaseExceptionType_SDK_TEMPORARY_FAILURE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrParsingFailure, shared.CouchbaseExceptionType_SDK_PARSING_FAILURE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrCasMismatch, shared.CouchbaseExceptionType_SDK_CAS_MISMATCH_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrBucketNotFound, shared.CouchbaseExceptionType_SDK_BUCKET_NOT_FOUND_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrCollectionNotFound, shared.CouchbaseExceptionType_SDK_COLLECTION_NOT_FOUND_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrUnsupportedOperation, shared.CouchbaseExceptionType_SDK_UNSUPPORTED_OPERATION_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrAmbiguousTimeout, shared.CouchbaseExceptionType_SDK_AMBIGUOUS_TIMEOUT_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrUnambiguousTimeout, shared.CouchbaseExceptionType_SDK_UNAMBIGUOUS_TIMEOUT_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrFeatureNotAvailable, shared.CouchbaseExceptionType_SDK_FEATURE_NOT_AVAILABLE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrScopeNotFound, shared.CouchbaseExceptionType_SDK_SCOPE_NOT_FOUND_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrIndexNotFound, shared.CouchbaseExceptionType_SDK_INDEX_NOT_FOUND_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrIndexExists, shared.CouchbaseExceptionType_SDK_INDEX_EXISTS_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrEncodingFailure, shared.CouchbaseExceptionType_SDK_ENCODING_FAILURE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDecodingFailure, shared.CouchbaseExceptionType_SDK_DECODING_FAILURE_EXCEPTION); errType != nil {
		return errType
	}

	if errType := errorIs(err, gocb.ErrRateLimitedFailure, shared.CouchbaseExceptionType_SDK_RATE_LIMITED_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrQuotaLimitedFailure, shared.CouchbaseExceptionType_SDK_QUOTA_LIMITED_EXCEPTION); errType != nil {
		return errType
	}

	if errType := errorIs(err, gocb.ErrDocumentNotFound, shared.CouchbaseExceptionType_SDK_DOCUMENT_NOT_FOUND_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDocumentUnretrievable, shared.CouchbaseExceptionType_SDK_DOCUMENT_UNRETRIEVABLE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDocumentLocked, shared.CouchbaseExceptionType_SDK_DOCUMENT_LOCKED_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrValueTooLarge, shared.CouchbaseExceptionType_SDK_VALUE_TOO_LARGE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDocumentExists, shared.CouchbaseExceptionType_SDK_DOCUMENT_EXISTS_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDurabilityLevelNotAvailable, shared.CouchbaseExceptionType_SDK_DURABILITY_LEVEL_NOT_AVAILABLE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDurabilityImpossible, shared.CouchbaseExceptionType_SDK_DURABILITY_IMPOSSIBLE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDurabilityAmbiguous, shared.CouchbaseExceptionType_SDK_DURABILITY_AMBIGUOUS_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDurableWriteInProgress, shared.CouchbaseExceptionType_SDK_DURABLE_WRITE_IN_PROGRESS_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDurableWriteReCommitInProgress, shared.CouchbaseExceptionType_SDK_DURABLE_WRITE_RECOMMIT_IN_PROGRESS_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrPathNotFound, shared.CouchbaseExceptionType_SDK_PATH_NOT_FOUND_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrPathMismatch, shared.CouchbaseExceptionType_SDK_PATH_MISMATCH_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrPathInvalid, shared.CouchbaseExceptionType_SDK_PATH_INVALID_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrPathTooBig, shared.CouchbaseExceptionType_SDK_PATH_TOO_BIG_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrPathTooDeep, shared.CouchbaseExceptionType_SDK_PATH_TOO_DEEP_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrValueTooDeep, shared.CouchbaseExceptionType_SDK_VALUE_TOO_DEEP_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrValueInvalid, shared.CouchbaseExceptionType_SDK_VALUE_INVALID_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrValueNotJSON, shared.CouchbaseExceptionType_SDK_DOCUMENT_NOT_JSON_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrNumberTooBig, shared.CouchbaseExceptionType_SDK_NUMBER_TOO_BIG_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDeltaInvalid, shared.CouchbaseExceptionType_SDK_DELTA_INVALID_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrPathExists, shared.CouchbaseExceptionType_SDK_PATH_EXISTS_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrXattrUnknownMacro, shared.CouchbaseExceptionType_SDK_XATTR_UNKNOWN_MACRO_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrXattrInvalidKeyCombo, shared.CouchbaseExceptionType_SDK_XATTR_INVALID_KEY_COMBO_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrXattrUnknownVirtualAttribute, shared.CouchbaseExceptionType_SDK_XATTR_UNKNOWN_VIRTUAL_ATTRIBUTE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrXattrCannotModifyVirtualAttribute, shared.CouchbaseExceptionType_SDK_XATTR_CANNOT_MODIFY_VIRTUAL_ATTRIBUTE_EXCEPTION); errType != nil {
		return errType
	}
	// if errType := errorIs(err, gocb.ErrXattrNoAccess, shared.CouchbaseExceptionType_SDK_XATTR_NO_ACCESS_EXCEPTION); errType != nil {
	// 	return errType
	// }
	if errType := errorIs(err, gocb.ErrPlanningFailure, shared.CouchbaseExceptionType_SDK_PLANNING_FAILURE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrIndexFailure, shared.CouchbaseExceptionType_SDK_INDEX_FAILURE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrPreparedStatementFailure, shared.CouchbaseExceptionType_SDK_PREPARED_STATEMENT_FAILURE_EXCEPTION); errType != nil {
		return errType
	}
	// if errType := errorIs(err, gocb.ErrDmlFailure, shared.CouchbaseExceptionType_SDK_DML_FAILURE_EXCEPTION); errType != nil {
	// 	return errType
	// }
	if errType := errorIs(err, gocb.ErrCompilationFailure, shared.CouchbaseExceptionType_SDK_COMPILATION_FAILURE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrJobQueueFull, shared.CouchbaseExceptionType_SDK_JOB_QUEUE_FULL_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDatasetNotFound, shared.CouchbaseExceptionType_SDK_DATASET_NOT_FOUND_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDataverseNotFound, shared.CouchbaseExceptionType_SDK_DATAVERSE_NOT_FOUND_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDatasetExists, shared.CouchbaseExceptionType_SDK_DATASET_EXISTS_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDataverseExists, shared.CouchbaseExceptionType_SDK_DATAVERSE_EXISTS_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrLinkNotFound, shared.CouchbaseExceptionType_SDK_LINK_NOT_FOUND_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrViewNotFound, shared.CouchbaseExceptionType_SDK_VIEW_NOT_FOUND_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrDesignDocumentNotFound, shared.CouchbaseExceptionType_SDK_DESIGN_DOCUMENT_NOT_FOUND_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrCollectionExists, shared.CouchbaseExceptionType_SDK_COLLECTION_EXISTS_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrScopeExists, shared.CouchbaseExceptionType_SDK_SCOPE_EXISTS_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrUserNotFound, shared.CouchbaseExceptionType_SDK_USER_NOT_FOUND_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrGroupNotFound, shared.CouchbaseExceptionType_SDK_GROUP_NOT_FOUND_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrBucketExists, shared.CouchbaseExceptionType_SDK_BUCKET_EXISTS_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrUserExists, shared.CouchbaseExceptionType_SDK_USER_EXISTS_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrBucketNotFlushable, shared.CouchbaseExceptionType_SDK_BUCKET_NOT_FLUSHABLE_EXCEPTION); errType != nil {
		return errType
	}
	if errType := errorIs(err, gocb.ErrTimeout, shared.CouchbaseExceptionType_SDK_TIMEOUT_EXCEPTION); errType != nil {
		return errType
	}

	if errType := errorIs(err, gocb.ErrDocumentNotLocked, shared.CouchbaseExceptionType_SDK_DOCUMENT_NOT_LOCKED_EXCEPTION); errType != nil {
		return errType
	}

	if errType := errorIs(err, gocb.ErrDocumentTooDeep, shared.CouchbaseExceptionType_SDK_DOCUMENT_TOO_DEEP_EXCEPTION); errType != nil {
		return errType
	}

	var queryErr *gocb.QueryError
	if errors.As(err, &queryErr) {
		// This is a query error but doesn't have an underlying error that we recognise.
		e := shared.CouchbaseExceptionType_SDK_COUCHBASE_EXCEPTION
		return &e
	}

	var searchErr *gocb.SearchError
	if errors.As(err, &searchErr) {
		// This is a search error but doesn't have an underlying error that we recognise.
		e := shared.CouchbaseExceptionType_SDK_COUCHBASE_EXCEPTION
		return &e
	}

	var httpErr *gocb.HTTPError
	if errors.As(err, &httpErr) {
		e := shared.CouchbaseExceptionType_SDK_COUCHBASE_EXCEPTION
		return &e
	}

	return nil
}

func errorIs(err error, target error, targetType shared.CouchbaseExceptionType) *shared.CouchbaseExceptionType {
	if errors.Is(err, target) {
		res := targetType
		return &res
	}

	return nil
}
