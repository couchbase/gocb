package gocb

import (
	"fmt"
	"strings"

	gocbcore "github.com/couchbase/gocbcore/v8"
	"github.com/pkg/errors"
)

type retryAbleError interface {
	retryable() bool
}

// KeyValueError represents an error that occurred while
// executing a K/V operation. Assumes that the service has returned a response.
type KeyValueError interface {
	error
	ID() string
	StatusCode() int // ?
	Opaque() uint32
	KeyValueError() bool
}

// AuthenticationError represents an error caused by an authentication issue.
type AuthenticationError interface {
	AuthenticationError() bool
}

// TemporaryFailureError represents an error that is temporary.
type TemporaryFailureError interface {
	TemporaryFailureError() bool
}

// ServiceNotAvailableError represents that the service used for an operation is not available.
type ServiceNotAvailableError interface {
	ServiceNotAvailableError() bool
}

type kvError struct {
	id          string
	status      gocbcore.StatusCode
	description string
	opaque      uint32
	context     string
	ref         string
	name        string
	isInsertOp  bool
}

func (err kvError) Error() string {
	if err.context != "" && err.ref != "" {
		return fmt.Sprintf("%s (%s, context: %s, ref: %s)", err.description, err.name, err.context, err.ref)
	} else if err.context != "" {
		return fmt.Sprintf("%s (%s, context: %s)", err.description, err.name, err.context)
	} else if err.ref != "" {
		return fmt.Sprintf("%s (%s, ref: %s)", err.description, err.name, err.ref)
	} else if err.name != "" && err.description != "" {
		return fmt.Sprintf("%s (%s)", err.description, err.name)
	} else if err.description != "" {
		return err.description
	}

	return fmt.Sprintf("an unknown error occurred (%d)", err.status)
}

// StatusCode returns the memcached response status.
func (err kvError) StatusCode() int {
	return int(err.status)
}

// ID returns the ID of the document used for the operation that yielded the error.
func (err kvError) ID() string {
	return err.id
}

// Opaque is the unique identifier for the operation that yielded the error.
func (err kvError) Opaque() uint32 {
	return err.opaque
}

// KeyValueError specifies whether or not this is a kvError.
func (err kvError) KeyValueError() bool {
	return true
}

// AuthenticationError specifies whether or not this is an authentication error.
func (err kvError) AuthenticationError() bool {
	return err.StatusCode() == int(gocbcore.StatusAuthError) ||
		err.StatusCode() == int(gocbcore.StatusAccessError)
}

// TemporaryFailureError specifies whether or not this is a temporary error.
func (err kvError) TemporaryFailureError() bool {
	return err.StatusCode() == int(gocbcore.StatusTmpFail) ||
		err.StatusCode() == int(gocbcore.StatusOutOfMemory) ||
		err.StatusCode() == int(gocbcore.StatusBusy)
}

// DurabilityError specifies whether or not this is a durability related error.
func (err kvError) DurabilityError() bool {
	return err.StatusCode() == int(gocbcore.StatusSyncWriteAmbiguous) ||
		err.StatusCode() == int(gocbcore.StatusSyncWriteInProgress) ||
		err.StatusCode() == int(gocbcore.StatusDurabilityImpossible) ||
		err.StatusCode() == int(gocbcore.StatusDurabilityInvalidLevel)
}

// DurabilityError occurs when an error occurs during performing durability operations.
type DurabilityError interface {
	DurabilityError() bool
}

type durabilityError struct {
	reason string
}

func (err durabilityError) Error() string {
	return err.reason
}

func (err durabilityError) DurabilityError() bool {
	return true
}

// TimeoutError occurs when an operation times out.
type TimeoutError interface {
	Timeout() bool
}

type timeoutError struct {
}

func (err timeoutError) Error() string {
	return "operation timed out"
}

func (err timeoutError) Timeout() bool {
	return true
}

type serviceNotAvailableError struct {
	message string
}

func (e serviceNotAvailableError) Error() string {
	return e.message
}

// ServiceNotAvailableError returns whether or not the error is a service not available error.
func (e serviceNotAvailableError) ServiceNotAvailableError() bool {
	return true
}

// General Errors

// IsTemporaryFailureError indicates whether the passed error is a
// key-value "temporary failure, try again later" error.
func IsTemporaryFailureError(err error) bool {
	cause := errors.Cause(err)
	if tempErr, ok := cause.(TemporaryFailureError); ok && tempErr.TemporaryFailureError() {
		return true
	}

	return false
}

// IsAuthenticationError verifies whether or not the cause for an error is an authentication error.
func IsAuthenticationError(err error) bool {
	cause := errors.Cause(err)
	if authErr, ok := cause.(AuthenticationError); ok && authErr.AuthenticationError() {
		return true
	}

	return false
}

// IsServiceNotAvailableError indicates whether the passed error occurred due to
// the requested service not being available.
func IsServiceNotAvailableError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case ServiceNotAvailableError:
		return errType.ServiceNotAvailableError()
	default:
		return false
	}
}

// IsTimeoutError verifies whether or not the cause for an error is a timeout.
func IsTimeoutError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case TimeoutError:
		return errType.Timeout()
	default:
		return false
	}
}

// KV Specific Errors

// IsKeyValueError verifies whether or not the cause for an error is a KeyValueError.
func IsKeyValueError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return true
	}

	return false
}

// IsScopeMissingError verifies whether or not the cause for an error is scope unknown.
func IsScopeMissingError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusScopeUnknown)
	}

	return false
}

// IsCollectionMissingError verifies whether or not the cause for an error is scope unknown.
func IsCollectionMissingError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusCollectionUnknown)
	}

	return false

}

// IsKeyExistsError indicates whether the passed error is a
// key-value "Key Already Exists" error.
func IsKeyExistsError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusKeyExists)
	}

	return false
}

// IsKeyNotFoundError indicates whether the passed error is a
// key-value "Key Not Found" error.
func IsKeyNotFoundError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusKeyNotFound)
	}

	return false
}

// IsValueTooLargeError indicates whether the passed error is a
// key-value "document value was too large" error.
func IsValueTooLargeError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusTooBig)
	}

	return false
}

// IsKeyLockedError indicates whether the passed error is a
// key-value operation failed due to the document being locked.
func IsKeyLockedError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusLocked)
	}

	return false
}

// IsInvalidArgumentsError indicates whether the passed error occurred due to
// // invalid arguments being passed to an operation.
func IsInvalidArgumentsError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusInvalidArgs)
	}

	return false
}

// IsBucketMissingError verifies whether or not the cause for an error is a bucket missing error.
func IsBucketMissingError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusNoBucket)
	}

	return false
}

// IsConfigurationError verifies whether or not the cause for an error is a configuration error.
func IsConfigurationError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case ConfigurationError:
		return errType.ConfigurationError()
	default:
		return false
	}
}

// IsCasMismatchError verifies whether or not the cause for an error is a cas mismatch.
func IsCasMismatchError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(kvError); ok && kvErr.KeyValueError() {
		return kvErr.status == gocbcore.StatusKeyExists && kvErr.isInsertOp
	}

	return false
}

// KV Subdoc Specific Errors

// IsPathNotFoundError indicates whether the passed error is a
// key-value "sub-document path does not exist" error.
func IsPathNotFoundError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSubDocPathNotFound)
	}

	return false
}

// IsPathMismatchError indicates whether the passed error occurred because
// the path component does not match the type of the element requested.
func IsPathMismatchError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSubDocPathMismatch)
	}

	return false
}

// IsPathInvalidError indicates whether the passed error occurred because
// the path provided is invalid. For operations requiring an array index,
// this is returned if the last component of that path isn't an array.
// Similarly for operations requiring a dictionary, if the last component
// isn't a dictionary but eg. an array index.
func IsPathInvalidError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSubDocPathInvalid)
	}

	return false
}

// IsPathTooDeepError indicates whether the passed error occurred because
// the path is too large (ie. the string is too long) or too deep
// (more than 32 components).
func IsPathTooDeepError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSubDocPathTooBig)
	}

	return false
}

// IsDocumentTooDeepError indicates whether the passed error occurred because
// the target document's level of JSON nesting is too deep to be processed
// by the subdoc service.
func IsDocumentTooDeepError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSubDocDocTooDeep)
	}

	return false
}

// IsCannotInsertValueError indicates whether the passed error occurred because
// the target document is not flagged or recognized as JSON.
func IsCannotInsertValueError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSubDocCantInsert)
	}

	return false
}

// IsDocumentNotJsonEerror indicates whether the passed error occurred because
// the existing document is not valid JSON.
func IsDocumentNotJsonEerror(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSubDocNotJson)
	}

	return false
}

// IsNumRangeError indicates whether the passed error occurred because
// for arithmetic subdoc operations, the existing number is out of the valid range.
func IsNumRangeError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSubDocBadRange)
	}

	return false
}

// IsDeltaRangeError indicates whether the passed error occurred because
// for arithmetic subdoc operations, the operation will make the value out of valid range.
func IsDeltaRangeError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSubDocBadDelta)
	}

	return false
}

// IsPathExistsError indicates whether the passed error occurred because
// the last component of the path already exist despite the mutation operation
// expecting it not to exist (the mutation was expecting to create only the last part
// of the path and store the fragment there).
func IsPathExistsError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSubDocPathExists)
	}

	return false
}

// IsSubDocInvalidArgumentsError indicates whether the passed error occurred because
// in a multi-specification, an invalid combination of commands were specified,
// including the case where too many paths were specified.
func IsSubDocInvalidArgumentsError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSubDocBadCombo)
	}

	return false
}

// IsXattrUnknownMacroError indicates whether the passed error occurred because
// the server has no knowledge of the requested macro.
func IsXattrUnknownMacroError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSubDocXattrUnknownMacro)
	}

	return false
}

// IsSubdocPathNotFoundError verifies whether or not the cause for an error is due to a subdoc operation path not found.
func IsSubdocPathNotFoundError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSubDocPathNotFound)
	}

	return false
}

// IsSubdocPathExistsError verifies whether or not the cause for an error is due to a subdoc operation path exists
func IsSubdocPathExistsError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSubDocPathExists)
	}

	return false
}

// Durability Specific Errors

// IsDurabilityError verifies whether or not the cause for an error is due to a durability error.
func IsDurabilityError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case DurabilityError:
		return errType.DurabilityError()
	default:
		return false
	}
}

// IsDurabilityLevelInvalidError verifies whether or not the cause for an error is because
// the requested durability level is invalid.
func IsDurabilityLevelInvalidError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusDurabilityInvalidLevel)
	}

	return false
}

// IsDurabilityImpossibleError verifies whether or not the cause for an error is because
// the requested durability level is impossible given the cluster topology due to insufficient replica servers.
func IsDurabilityImpossibleError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusDurabilityImpossible)
	}

	return false
}

// IsSyncWriteInProgressError verifies whether or not the cause for an error is because of an
// attempt to mutate a key which has a SyncWrite pending. Client should retry, possibly with backoff.
func IsSyncWriteInProgressError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSyncWriteInProgress)
	}

	return false
}

// IsSyncWriteAmbiguousError verifies whether or not the cause for an error is because
// the client could not locate a replica within the cluster map or replica read. The bucket
// may not be configured to have replicas, which should be checked to ensure replica reads.
func IsSyncWriteAmbiguousError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSyncWriteAmbiguous)
	}

	return false
}

// IsNoReplicasError verifies whether or not the cause for an error is because of an
// the client could not locate a replica within the cluster map or replica read. The Bucket may not be configured
// to have replicas, which should be checked to ensure replica reads.
func IsNoReplicasError(err error) bool {
	cause := errors.Cause(err)
	if cause == gocbcore.ErrNoReplicas {
		return true
	}

	return false
}

// Service Specific Errors

// IsNoResultsError verifies whether or not the cause for an error is due no results being available to a query.
func IsNoResultsError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case NoResultsError:
		return errType.NoResultsError()
	default:
		return false
	}
}

type clientError struct {
	message string
}

func (e clientError) Error() string {
	return e.message
}

// ProjectionErrors is a collection of one or more KeyValueError that occurs during a Get with projections operation.
type ProjectionErrors interface {
	error
	Errors() []KeyValueError
	ProjectionErrors() bool
}

type projectionErrors struct {
	errors []KeyValueError
}

func (e projectionErrors) ProjectionErrors() bool {
	return true
}

func (e projectionErrors) Error() string {
	var errs []string
	for _, err := range e.errors {
		errs = append(errs, err.Error())
	}
	return strings.Join(errs, ", ")
}

// ViewQueryError is the error type for an error that occurs during view query execution.
type ViewQueryError interface {
	error
	Reason() string
	Message() string
}

type viewError struct {
	ErrorMessage string `json:"message"`
	ErrorReason  string `json:"reason"`
}

func (e viewError) Error() string {
	return e.ErrorMessage + " - " + e.ErrorReason
}

// Reason is the reason for the error occurring.
func (e viewError) Reason() string {
	return e.ErrorReason
}

// Message contains any message from the server for this error.
func (e viewError) Message() string {
	return e.ErrorMessage
}

// ViewQueryErrors is a collection of one or more ViewQueryError that occurs for errors created by Couchbase Server
// during View query execution.
type ViewQueryErrors interface {
	error
	Errors() []ViewQueryError
	HTTPStatus() int
	Endpoint() string
}

type viewMultiError struct {
	errors     []ViewQueryError
	httpStatus int
	endpoint   string
	partial    bool
}

func (e viewMultiError) Error() string {
	var errs []string
	for _, err := range e.errors {
		errs = append(errs, err.Error())
	}
	return strings.Join(errs, ", ")
}

// HTTPStatus returns the HTTP status code for the operation.
func (e viewMultiError) HTTPStatus() int {
	return e.httpStatus
}

// Endpoint returns the endpoint that was used for the operation.
func (e viewMultiError) Endpoint() string {
	return e.endpoint
}

// Errors returns the list of ViewQueryErrors.
func (e viewMultiError) Errors() []ViewQueryError {
	return e.errors
}

// AnalyticsQueryError occurs for errors created by Couchbase Server during Analytics query execution.
type AnalyticsQueryError interface {
	error
	Code() uint32
	Message() string
	HTTPStatus() int
	Endpoint() string
	ContextID() string
}

type analyticsQueryError struct {
	ErrorCode    uint32 `json:"code"`
	ErrorMessage string `json:"msg"`
	httpStatus   int
	endpoint     string
	contextID    string
}

func (e analyticsQueryError) Error() string {
	return fmt.Sprintf("[%d] %s", e.ErrorCode, e.ErrorMessage)
}

// Code returns the error code for this error.
func (e analyticsQueryError) Code() uint32 {
	return e.ErrorCode
}

// Message returns any message from the server for this error.
func (e analyticsQueryError) Message() string {
	return e.ErrorMessage
}

func (e analyticsQueryError) retryable() bool {
	if e.Code() == 21002 || e.Code() == 23000 || e.Code() == 23003 || e.Code() == 23007 {
		return true
	}

	return false
}

// Timeout indicates whether or not this error is a timeout.
func (e analyticsQueryError) Timeout() bool {
	if e.ErrorCode == 21002 {
		return true
	}
	return false
}

// HTTPStatus returns the HTTP status code for the operation.
func (e analyticsQueryError) HTTPStatus() int {
	return e.httpStatus
}

// Endpoint returns the endpoint that was used for the operation.
func (e analyticsQueryError) Endpoint() string {
	return e.endpoint
}

// ContextID returns the context ID that was used for the operation.
func (e analyticsQueryError) ContextID() string {
	return e.contextID
}

// QueryError occurs for errors created by Couchbase Server during N1ql query execution.
type QueryError interface {
	error
	Code() uint32
	Message() string
	HTTPStatus() int
	Endpoint() string
	ContextID() string
}

type queryError struct {
	ErrorCode    uint32 `json:"code"`
	ErrorMessage string `json:"msg"`
	httpStatus   int
	endpoint     string
	contextID    string
}

func (e queryError) Error() string {
	return fmt.Sprintf("[%d] %s", e.ErrorCode, e.ErrorMessage)
}

// Code returns the error code for this error.
func (e queryError) Code() uint32 {
	return e.ErrorCode
}

// Message returns any message from the server for this error.
func (e queryError) Message() string {
	return e.ErrorMessage
}

func (e queryError) retryable() bool {
	if e.ErrorCode == 4050 || e.ErrorCode == 4070 || e.ErrorCode == 5000 {
		return true
	}

	return false
}

// Timeout indicates whether or not this error is a timeout.
func (e queryError) Timeout() bool {
	if e.ErrorCode == 1080 {
		return true
	}
	return false
}

// HTTPStatus returns the HTTP status code for the operation.
func (e queryError) HTTPStatus() int {
	return e.httpStatus
}

// Endpoint returns the endpoint that was used for the operation.
func (e queryError) Endpoint() string {
	return e.endpoint
}

// ContextID returns the context ID that was used for the operation.
func (e queryError) ContextID() string {
	return e.contextID
}

// SearchError occurs for errors created by Couchbase Server during Search query execution.
type SearchError interface {
	error
	Message() string
}

type searchError struct {
	message string
}

func (e searchError) Error() string {
	return e.message
}

// Message returns any message from the server for this error.
func (e searchError) Message() string {
	return e.message
}

// SearchErrors is a collection of one or more SearchError that occurs for errors created by Couchbase Server
// during Search query execution.
type SearchErrors interface {
	error
	Errors() []SearchError
	HTTPStatus() int
	Endpoint() string
	ContextID() string
}

type searchMultiError struct {
	errors     []SearchError
	httpStatus int
	endpoint   string
	contextID  string
}

func (e searchMultiError) Error() string {
	var errs []string
	for _, err := range e.errors {
		errs = append(errs, err.Error())
	}
	return strings.Join(errs, ", ")
}

// HTTPStatus returns the HTTP status code for the operation.
func (e searchMultiError) HTTPStatus() int {
	return e.httpStatus
}

// Endpoint returns the endpoint that was used for the operation.
func (e searchMultiError) Endpoint() string {
	return e.endpoint
}

// ContextID returns the context ID that was used for the operation.
func (e searchMultiError) ContextID() string {
	return e.contextID
}

// Errors returns the list of SearchErrors.
func (e searchMultiError) Errors() []SearchError {
	return e.errors
}

// PartialResults indicates whether or not the operation also yielded results.
func (e searchMultiError) retryable() bool {
	return e.httpStatus == 419
}

// ConfigurationError occurs when the client is configured incorrectly.
type ConfigurationError interface {
	error
	ConfigurationError() bool
}

type configurationError struct {
	message string
}

func (e configurationError) Error() string {
	return e.message
}

// ConfigurationError indicates whether or not this error is a ConfigurationError
func (e configurationError) ConfigurationError() bool {
	return true
}

// ErrorCause returns the underlying cause of an error.
func ErrorCause(err error) error {
	return errors.Cause(err)
}

// NoResultsError occurs when when no results are available to a query.
type NoResultsError interface {
	error
	NoResultsError() bool
}

type noResultsError struct {
}

func (e noResultsError) Error() string {
	return "No results returned."
}

// NoResultsError indicates whether or not this error is a NoResultsError
func (e noResultsError) NoResultsError() bool {
	return true
}

func isRetryableError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case retryAbleError:
		return errType.retryable()
	default:
		return false
	}
}

func maybeEnhanceKVErr(err error, key string, isInsertOp bool) error {
	cause := errors.Cause(err)

	switch errType := cause.(type) {
	case *gocbcore.KvError:
		return kvError{
			id:          key,
			status:      errType.Code,
			description: errType.Description,
			opaque:      errType.Opaque,
			context:     errType.Context,
			ref:         errType.Ref,
			name:        errType.Name,
			isInsertOp:  isInsertOp,
		}
	default:
	}

	if cause == gocbcore.ErrNoCapiService {
		return serviceNotAvailableError{}
	}

	return err
}

var (
	// ErrNotEnoughReplicas occurs when not enough replicas exist to match the specified durability requirements.
	// ErrNotEnoughReplicas = errors.New("Not enough replicas to match durability requirements.")
	// ErrDurabilityTimeout occurs when the server took too long to meet the specified durability requirements.
	// ErrDurabilityTimeout = errors.New("Failed to meet durability requirements in time.")

	// ErrNoOpenBuckets occurs when a cluster-level operation is performed before any buckets are opened.
	ErrNoOpenBuckets = errors.New("You must open a bucket before you can perform cluster level operations.")
	// ErrIndexInvalidName occurs when an invalid name was specified for an index.
	ErrIndexInvalidName = errors.New("An invalid index name was specified.")
	// ErrIndexNoFields occurs when an index with no fields is created.
	ErrIndexNoFields = errors.New("You must specify at least one field to index.")
	// ErrIndexNotFound occurs when an operation expects an index but it was not found.
	ErrIndexNotFound = errors.New("The index specified does not exist.")
	// ErrIndexAlreadyExists occurs when an operation expects an index not to exist, but it was found.
	ErrIndexAlreadyExists = errors.New("The index specified already exists.")
	// ErrFacetNoRanges occurs when a range-based facet is specified but no ranges were indicated.
	ErrFacetNoRanges = errors.New("At least one range must be specified on a facet.")

	// ErrSearchIndexInvalidName occurs when an invalid name was specified for a search index.
	ErrSearchIndexInvalidName = errors.New("An invalid search index name was specified.")
	// ErrSearchIndexMissingType occurs when no type was specified for a search index.
	ErrSearchIndexMissingType = errors.New("No search index type was specified.")
	// ErrSearchIndexInvalidSourceType occurs when an invalid source type was specific for a search index.
	ErrSearchIndexInvalidSourceType = errors.New("An invalid search index source type was specified.")
	// ErrSearchIndexInvalidSourceName occurs when an invalid source name was specific for a search index.
	ErrSearchIndexInvalidSourceName = errors.New("An invalid search index source name was specified.")
	// ErrSearchIndexAlreadyExists occurs when an invalid source name was specific for a search index.
	ErrSearchIndexAlreadyExists = errors.New("The search index specified already exists.")
	// ErrSearchIndexInvalidIngestControlOp occurs when an invalid ingest control op was specific for a search index.
	ErrSearchIndexInvalidIngestControlOp = errors.New("An invalid search index ingest control op was specified.")
	// ErrSearchIndexInvalidQueryControlOp occurs when an invalid query control op was specific for a search index.
	ErrSearchIndexInvalidQueryControlOp = errors.New("An invalid search index query control op was specified.")
	// ErrSearchIndexInvalidPlanFreezeControlOp occurs when an invalid plan freeze control op was specific for a search index.
	ErrSearchIndexInvalidPlanFreezeControlOp = errors.New("An invalid search index plan freeze control op was specified.")

	// ErrDispatchFail occurs when we failed to execute an operation due to internal routing issues.
	ErrDispatchFail = gocbcore.ErrDispatchFail
	// ErrBadHosts occurs when an invalid list of hosts is specified for bootstrapping.
	ErrBadHosts = gocbcore.ErrBadHosts
	// ErrProtocol occurs when an invalid protocol is specified for bootstrapping.
	ErrProtocol = gocbcore.ErrProtocol
	// ErrInvalidServer occurs when a specified server index is invalid.
	ErrInvalidServer = gocbcore.ErrInvalidServer
	// ErrInvalidVBucket occurs when a specified vbucket index is invalid.
	ErrInvalidVBucket = gocbcore.ErrInvalidVBucket
	// ErrInvalidReplica occurs when a specified replica index is invalid.
	ErrInvalidReplica = gocbcore.ErrInvalidReplica
	// ErrInvalidCert occurs when the specified certificate is not valid.
	ErrInvalidCert = gocbcore.ErrInvalidCert
	// ErrInvalidCredentials is returned when an invalid set of credentials is provided for a service.
	ErrInvalidCredentials = gocbcore.ErrInvalidCredentials

	// ErrShutdown occurs when an operation is performed on a bucket that has been closed.
	ErrShutdown = gocbcore.ErrShutdown
	// ErrOverload occurs when more operations were dispatched than the client is capable of writing.
	ErrOverload = gocbcore.ErrOverload
)
