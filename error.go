package gocb

import (
	"errors"
	"fmt"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

func newCliInternalError(message string) error {
	return errors.New(message)
}

type wrappedError struct {
	Message    string
	InnerError error
}

func (e wrappedError) Error() string {
	return fmt.Sprintf("%s: %s", e.Message, e.InnerError.Error())
}

func (e wrappedError) Unwrap() error {
	return e.InnerError
}

func wrapError(err error, message string) error {
	return wrappedError{
		Message:    message,
		InnerError: err,
	}
}

type invalidArgumentsError struct {
	message string
}

func (e invalidArgumentsError) Error() string {
	return fmt.Sprintf("invalid arguments: %s", e.message)
}

func (e invalidArgumentsError) Unwrap() error {
	return ErrInvalidArgument
}

func makeInvalidArgumentsError(message string) error {
	return invalidArgumentsError{
		message: message,
	}
}

// Shared Error Definitions RFC#58@15
var (
	// ErrTimeout occurs when an operation does not receive a response in a timely manner.
	ErrTimeout = gocbcore.ErrTimeout

	ErrRequestCanceled       = gocbcore.ErrRequestCanceled
	ErrInvalidArgument       = gocbcore.ErrInvalidArgument
	ErrServiceNotAvailable   = gocbcore.ErrServiceNotAvailable
	ErrInternalServerFailure = gocbcore.ErrInternalServerFailure
	ErrAuthenticationFailure = gocbcore.ErrAuthenticationFailure
	ErrTemporaryFailure      = gocbcore.ErrTemporaryFailure
	ErrParsingFailure        = gocbcore.ErrParsingFailure

	ErrCasMismatch          = gocbcore.ErrCasMismatch
	ErrBucketNotFound       = gocbcore.ErrBucketNotFound
	ErrCollectionNotFound   = gocbcore.ErrCollectionNotFound
	ErrEncodingFailure      = gocbcore.ErrEncodingFailure
	ErrDecodingFailure      = gocbcore.ErrDecodingFailure
	ErrUnsupportedOperation = gocbcore.ErrUnsupportedOperation
	ErrAmbiguousTimeout     = gocbcore.ErrAmbiguousTimeout

	ErrUnambiguousTimeout = gocbcore.ErrUnambiguousTimeout

	// ErrFeatureNotAvailable occurs when an operation is performed on a bucket which does not support it.
	ErrFeatureNotAvailable = gocbcore.ErrFeatureNotAvailable
	ErrScopeNotFound       = gocbcore.ErrScopeNotFound
	ErrIndexNotFound       = gocbcore.ErrIndexNotFound

	ErrIndexExists = gocbcore.ErrIndexExists
)

// Key Value Error Definitions RFC#58@15
var (
	ErrDocumentNotFound                  = gocbcore.ErrDocumentNotFound
	ErrDocumentUnretrievable             = gocbcore.ErrDocumentUnretrievable
	ErrDocumentLocked                    = gocbcore.ErrDocumentLocked
	ErrValueTooLarge                     = gocbcore.ErrValueTooLarge
	ErrDocumentExists                    = gocbcore.ErrDocumentExists
	ErrValueNotJSON                      = gocbcore.ErrValueNotJSON
	ErrDurabilityLevelNotAvailable       = gocbcore.ErrDurabilityLevelNotAvailable
	ErrDurabilityImpossible              = gocbcore.ErrDurabilityImpossible
	ErrDurabilityAmbiguous               = gocbcore.ErrDurabilityAmbiguous
	ErrDurableWriteInProgress            = gocbcore.ErrDurableWriteInProgress
	ErrDurableWriteReCommitInProgress    = gocbcore.ErrDurableWriteReCommitInProgress
	ErrMutationLost                      = gocbcore.ErrMutationLost
	ErrPathNotFound                      = gocbcore.ErrPathNotFound
	ErrPathMismatch                      = gocbcore.ErrPathMismatch
	ErrPathInvalid                       = gocbcore.ErrPathInvalid
	ErrPathTooBig                        = gocbcore.ErrPathTooBig
	ErrPathTooDeep                       = gocbcore.ErrPathTooDeep
	ErrValueTooDeep                      = gocbcore.ErrValueTooDeep
	ErrValueInvalid                      = gocbcore.ErrValueInvalid
	ErrDocumentNotJSON                   = gocbcore.ErrDocumentNotJSON
	ErrNumberTooBig                      = gocbcore.ErrNumberTooBig
	ErrDeltaInvalid                      = gocbcore.ErrDeltaInvalid
	ErrPathExists                        = gocbcore.ErrPathExists
	ErrXattrUnknownMacro                 = gocbcore.ErrXattrUnknownMacro
	ErrXattrInvalidFlagCombo             = gocbcore.ErrXattrInvalidFlagCombo
	ErrXattrInvalidKeyCombo              = gocbcore.ErrXattrInvalidKeyCombo
	ErrXattrUnknownVirtualAttribute      = gocbcore.ErrXattrUnknownVirtualAttribute
	ErrXattrCannotModifyVirtualAttribute = gocbcore.ErrXattrCannotModifyVirtualAttribute
	ErrXattrInvalidOrder                 = gocbcore.ErrXattrInvalidOrder
)

// Query Error Definitions RFC#58@15
var (
	ErrPlanningFailure = gocbcore.ErrPlanningFailure

	ErrIndexFailure = gocbcore.ErrIndexFailure

	ErrPreparedStatementFailure = gocbcore.ErrPreparedStatementFailure
)

// Analytics Error Definitions RFC#58@15
var (
	ErrCompilationFailure = gocbcore.ErrCompilationFailure

	ErrJobQueueFull = gocbcore.ErrJobQueueFull

	ErrDatasetNotFound = gocbcore.ErrDatasetNotFound

	ErrDataverseNotFound = gocbcore.ErrDataverseNotFound

	ErrDatasetExists = gocbcore.ErrDatasetExists

	ErrDataverseExists = gocbcore.ErrDataverseExists

	ErrLinkNotFound = gocbcore.ErrLinkNotFound
)

// Search Error Definitions RFC#58@15
var ()

// View Error Definitions RFC#58@15
var (
	ErrViewNotFound = gocbcore.ErrViewNotFound

	ErrDesignDocumentNotFound = gocbcore.ErrDesignDocumentNotFound
)

// Management Error Definitions RFC#58@15
var (
	ErrCollectionExists   = gocbcore.ErrCollectionExists
	ErrScopeExists        = gocbcore.ErrScopeExists
	ErrUserNotFound       = gocbcore.ErrUserNotFound
	ErrGroupNotFound      = gocbcore.ErrGroupNotFound
	ErrBucketExists       = gocbcore.ErrBucketExists
	ErrUserExists         = gocbcore.ErrUserExists
	ErrBucketNotFlushable = gocbcore.ErrBucketNotFlushable
)

// SDK specific error definitions
var (
	ErrOverload = gocbcore.ErrOverload

	ErrNoResult = errors.New("no result was available")
)
