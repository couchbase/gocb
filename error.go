package gocb

import (
	"fmt"
	"strings"
	"time"

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

func (err kvError) retryable() bool {
	return err.TemporaryFailureError()
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

// TimeoutErrorWithDetail occurs when an operation times out.
// This error type contains extra details about why the operation
// timed out.
type TimeoutErrorWithDetail interface {
	Timeout() bool
	OperationID() string
	RetryAttempts() uint32
	RetryReasons() []RetryReason
	LocalAddress() string
	RemoteAddress() string
	Elapsed() time.Duration
	Operation() string
}

// TimeoutError occurs when an operation times out.
type TimeoutError interface {
	Timeout() bool
}

type timeoutError struct {
	operationID   string
	retryReasons  []gocbcore.RetryReason
	retryAttempts uint32
	operation     string
	local         string
	remote        string
	elapsed       time.Duration
	connectionID  string
}

func (err timeoutError) Error() string {
	base := "operation timed out"
	if err.operationID != "" {
		base = fmt.Sprintf("%s, lastOperationID: %s", base, err.operationID)
	}
	if err.retryAttempts > 0 {
		base = fmt.Sprintf("%s, retried: %d", base, err.retryAttempts)
	}
	if len(err.retryReasons) > 0 {
		var reasons []string
		for _, reason := range err.retryReasons {
			reasons = append(reasons, reason.Description())
		}
		base = fmt.Sprintf("%s, retryReasons: [%s]", base, strings.Join(reasons, ","))
	}
	if err.local != "" {
		base = fmt.Sprintf("%s, lastDispatchedFrom: %s", base, err.local)
	}
	if err.remote != "" {
		base = fmt.Sprintf("%s, lastDispatchedTo: %s", base, err.remote)
	}
	if err.connectionID != "" {
		base = fmt.Sprintf("%s, lastConnectionID: %s", base, err.connectionID)
	}
	if err.elapsed != 0 {
		base = fmt.Sprintf("%s, totalMicros: %d", base, err.elapsed/time.Microsecond)
	}
	if err.operation != "" {
		if err.elapsed != 0 {
			base = fmt.Sprintf("%s, operation: %s", base, err.operation)
		}
	}

	return base
}

func (err timeoutError) Timeout() bool {
	return true
}

func (err timeoutError) OperationID() string {
	return err.operationID
}

func (err timeoutError) RetryAttempts() uint32 {
	return err.retryAttempts
}

func (err timeoutError) RetryReasons() []RetryReason {
	var reasons []RetryReason
	for _, reason := range err.retryReasons {
		reasons = append(reasons, RetryReason(reason))
	}
	return reasons
}

func (err timeoutError) LocalAddress() string {
	return err.local
}

func (err timeoutError) RemoteAddress() string {
	return err.remote
}

func (err timeoutError) Elapsed() time.Duration {
	return err.elapsed
}

func (err timeoutError) Operation() string {
	return err.operation
}

type serviceNotAvailableError struct {
	message string
}

func (e serviceNotAvailableError) Error() string {
	return e.message
}

// InvalidIndexError occurs when an invalid index is specified on a LookupInResult.
type InvalidIndexError interface {
	InvalidIndex() bool
}

type invalidIndexError struct {
}

func (err invalidIndexError) InvalidIndex() bool {
	return true
}

func (err invalidIndexError) Error() string {
	return "an invalid index was specified"
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

// IsRetryableError indicates that the operation should be retried.
func IsRetryableError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case retryAbleError:
		return errType.retryable()
	default:
		return false
	}
}

// IsInvalidArgumentsError indicates whether the passed error occurred due to
// invalid arguments being passed to an operation.
func IsInvalidArgumentsError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case KeyValueError:
		return errType.KeyValueError() && errType.StatusCode() == int(gocbcore.StatusInvalidArgs)
	case InvalidArgumentsError:
		return errType.InvalidArgumentsError()
	}

	return false
}

// IsScopeNotFoundError verifies whether or not the cause for an error is scope unknown.
func IsScopeNotFoundError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case KeyValueError:
		return errType.KeyValueError() && errType.StatusCode() == int(gocbcore.StatusScopeUnknown)
	case CollectionManagerError:
		return errType.ScopeNotFoundError()
	}

	return false
}

// IsCollectionNotFoundError verifies whether or not the cause for an error is scope unknown.
func IsCollectionNotFoundError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case KeyValueError:
		return errType.KeyValueError() && errType.StatusCode() == int(gocbcore.StatusCollectionUnknown)
	case CollectionManagerError:
		return errType.CollectionNotFoundError()
	}

	return false
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
		return kvErr.status == gocbcore.StatusKeyExists && !kvErr.isInsertOp
	}

	return false
}

// IsQueueOverloadError verifies that the cause for an error is that more operations were dispatched than the client
// is capable of writing.
func IsQueueOverloadError(err error) bool {
	return errors.Cause(err) == gocbcore.ErrOverload
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

// IsInvalidIndexError verifies whether or not the cause for an error is due to an invalid index being specified on
// a LookupInResult
func IsInvalidIndexError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case InvalidIndexError:
		return errType.InvalidIndex()
	default:
		return false
	}
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

// IsSyncWriteReCommitInProgressError verifies whether or not the cause for an error is because of an
// attempt to mutate a key which has a SyncWrite recommit pending. Client should retry, possibly with backoff.
func IsSyncWriteReCommitInProgressError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(KeyValueError); ok && kvErr.KeyValueError() {
		return kvErr.StatusCode() == int(gocbcore.StatusSyncWriteReCommitInProgress)
	}

	return false
}

// IsSyncWriteInProgressError verifies whether or not the cause for an error is because of an
// attempt to mutate a document which has a SyncWrite pending. Client should retry, possibly with backoff.
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

// CollectionNotFoundError returns whether or not an error occurred because a collection could not be found.
type CollectionNotFoundError interface {
	CollectionNotFound() bool
}

// IsNoResultsError verifies whether or not the cause for an error is due no results being available to a query.
func IsNoResultsError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case NoResultsError:
		return errType.NoResultsError()
	default:
		return false
	}
}

// IsDesignDocumentNotFoundError occurs when a specific design document could not be found.
func IsDesignDocumentNotFoundError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case ViewIndexesError:
		return errType.DesignDocumentNotFoundError()
	default:
		return false
	}
}

// IsDesignDocumentExistsError occurs when a specific design document already exists.
func IsDesignDocumentExistsError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case ViewIndexesError:
		return errType.DesignDocumentExistsError()
	default:
		return false
	}
}

// IsDesignDocumentPublishDropFailError occurs when dropping a design document already exists.
func IsDesignDocumentPublishDropFailError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case ViewIndexesError:
		return errType.DesignDocumentPublishDropFailError()
	default:
		return false
	}
}

// IsBucketNotFoundError occurs when a specific bucket could not be found.
func IsBucketNotFoundError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case BucketManagerError:
		return errType.BucketNotFoundError()
	default:
		return false
	}
}

// IsBucketExistsError occurs when a specific bucket already exists.
func IsBucketExistsError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case BucketManagerError:
		return errType.BucketExistsError()
	default:
		return false
	}
}

// IsQueryIndexAlreadyExistsError verifies that an index already exists.
func IsQueryIndexAlreadyExistsError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case QueryIndexesError:
		return errType.QueryIndexExistsError()
	default:
		return false
	}
}

// IsQueryIndexNotFoundError verifies that an index could not be found.
func IsQueryIndexNotFoundError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case QueryIndexesError:
		return errType.QueryIndexNotFoundError()
	default:
		return false
	}
}

// IsAnalyticsIndexAlreadyExistsError verifies that an analytics index already exists.
func IsAnalyticsIndexAlreadyExistsError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case AnalyticsIndexesError:
		return errType.AnalyticsIndexExistsError()
	default:
		return false
	}
}

// IsAnalyticsIndexNotFoundError verifies that an analytics index could not be found.
func IsAnalyticsIndexNotFoundError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case AnalyticsIndexesError:
		return errType.AnalyticsIndexNotFoundError()
	default:
		return false
	}
}

// IsAnalyticsDatasetAlreadyExistsError verifies that an analytics dataset already exists.
func IsAnalyticsDatasetAlreadyExistsError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case AnalyticsIndexesError:
		return errType.AnalyticsDatasetExistsError()
	default:
		return false
	}
}

// IsAnalyticsDatasetNotFoundError verifies that an analytics dataset could not be found.
func IsAnalyticsDatasetNotFoundError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case AnalyticsIndexesError:
		return errType.AnalyticsDatasetNotFoundError()
	default:
		return false
	}
}

// IsAnalyticsDataverseAlreadyExistsError verifies that an analytics dataverse already exists.
func IsAnalyticsDataverseAlreadyExistsError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case AnalyticsIndexesError:
		return errType.AnalyticsDataverseExistsError()
	default:
		return false
	}
}

// IsAnalyticsDataverseNotFoundError verifies that an analytics dataverse could not be found.
func IsAnalyticsDataverseNotFoundError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case AnalyticsIndexesError:
		return errType.AnalyticsDataverseNotFoundError()
	default:
		return false
	}
}

// IsAnalyticsLinkNotFoundError verifies that an analytics link could not be found.
func IsAnalyticsLinkNotFoundError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case AnalyticsIndexesError:
		return errType.AnalyticsLinkNotFoundError()
	default:
		return false
	}
}

// IsCollectionExistsError occurs when a specific collection already exists.
func IsCollectionExistsError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case CollectionManagerError:
		return errType.CollectionExistsError()
	default:
		return false
	}
}

// IsScopeExistsError occurs when a specific scope already exists.
func IsScopeExistsError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case CollectionManagerError:
		return errType.ScopeExistsError()
	default:
		return false
	}
}

// IsUserNotFoundError verifies that a user could not be found.
func IsUserNotFoundError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case UserManagerError:
		return errType.UserNotFoundError()
	default:
		return false
	}
}

// IsGroupNotFoundError verifies that a group could not be found.
func IsGroupNotFoundError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case UserManagerError:
		return errType.GroupNotFoundError()
	default:
		return false
	}
}

// IsSearchIndexNotFoundError verifies that an index could not be found.
func IsSearchIndexNotFoundError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case SearchIndexesError:
		return errType.SearchIndexNotFoundError()
	default:
		return false
	}
}

// IsServiceNotConfiguredError verifies that no nodes could be found for the specified service.
func IsServiceNotConfiguredError(err error) bool {
	switch errors.Cause(err) {
	case gocbcore.ErrNoN1qlService:
		return true
	case gocbcore.ErrNoCbasService:
		return true
	case gocbcore.ErrNoCapiService:
		return true
	case gocbcore.ErrNoFtsService:
		return true
	case gocbcore.ErrNoMgmtService:
		return true
	default:
		return false
	}
}

// IsFeatureNotFoundError verifies that an error occurred because the requested feature is not supported by the server.
func IsFeatureNotFoundError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case FeatureNotFoundError:
		return errType.FeatureNotFoundError()
	case HTTPError:
		return errType.HTTPStatus() == 404 && errType.Error() == "Not Found."
	default:
		return false
	}
}

// HTTPError indicates that an error occurred with a valid HTTP response for an operation.
type HTTPError interface {
	error
	HTTPStatus() int
}

// FeatureNotFoundError indicates that an error occurred because the requested feature is not supported by the server.
type FeatureNotFoundError interface {
	error
	FeatureNotFoundError() bool
}

// InvalidArgumentsError indicates that invalid arguments were provided to an operation.
type InvalidArgumentsError interface {
	error
	InvalidArgumentsError() bool
}

type invalidArgumentsError struct {
	message string
}

func (e invalidArgumentsError) Error() string {
	return e.message
}

// InvalidArgumentsError indicates that invalid arguments were provided to an operation.
func (e invalidArgumentsError) InvalidArgumentsError() bool {
	return true
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
	ErrorCode             uint32 `json:"code"`
	ErrorMessage          string `json:"msg"`
	httpStatus            int
	endpoint              string
	contextID             string
	enhancedStmtSupported bool
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
	if e.enhancedStmtSupported {
		if e.ErrorCode == 4040 {
			return true
		}

		return false
	}
	if e.ErrorCode == 4040 || e.ErrorCode == 4050 || e.ErrorCode == 4070 {
		return true
	}
	if e.ErrorCode == 5000 && strings.Contains(e.Message(), "queryport.indexNotFound") {
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
	return e.httpStatus == 429
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

// ViewIndexesError occurs for errors created By Couchbase Server when performing index management.
type ViewIndexesError interface {
	error
	HTTPStatus() int
	DesignDocumentNotFoundError() bool
	DesignDocumentExistsError() bool
	DesignDocumentPublishDropFailError() bool
}

type viewIndexError struct {
	statusCode      int
	message         string
	indexMissing    bool
	indexExists     bool
	publishDropFail bool
}

func (e viewIndexError) Error() string {
	return e.message
}

// HTTPStatus returns the HTTP status code for the operation.
func (e viewIndexError) HTTPStatus() int {
	return e.statusCode
}

// DesignDocumentNotFoundError indicates that a design document could not be found.
func (e viewIndexError) DesignDocumentNotFoundError() bool {
	return e.indexMissing
}

// DesignDocumentExistsError indicates that a design document already exists.
func (e viewIndexError) DesignDocumentExistsError() bool {
	return e.indexExists
}

// DesignDocumentPublishDropFailError indicates dropping a development view failed during publish.
func (e viewIndexError) DesignDocumentPublishDropFailError() bool {
	return e.publishDropFail
}

func (e viewIndexError) FeatureNotFoundError() bool {
	return e.statusCode == 404 && e.message == "Not Found."
}

// BucketManagerError occurs for errors created By Couchbase Server when performing bucket management.
type BucketManagerError interface {
	error
	HTTPStatus() int
	BucketNotFoundError() bool
	BucketExistsError() bool
}

type bucketManagerError struct {
	statusCode int
	message    string
}

func (e bucketManagerError) Error() string {
	return e.message
}

// HTTPStatus returns the HTTP status code for the operation.
func (e bucketManagerError) HTTPStatus() int {
	return e.statusCode
}

// BucketNotFoundError indicates that a bucket could not be found.
func (e bucketManagerError) BucketNotFoundError() bool {
	return e.statusCode == 404 && strings.Contains(e.message, "Requested resource not found")
}

// BucketExistsError indicates that a bucket already exists.
func (e bucketManagerError) BucketExistsError() bool {
	return e.statusCode == 404 && strings.Contains(e.message, "Bucket with given name already exists")
}

func (e bucketManagerError) FeatureNotFoundError() bool {
	return e.statusCode == 404 && e.message == "Not Found."
}

// QueryIndexesError occurs for errors created By Couchbase Server when performing query index management.
type QueryIndexesError interface {
	error
	HTTPStatus() int
	QueryIndexNotFoundError() bool
	QueryIndexExistsError() bool
	BucketNotFoundError() bool
}

type queryIndexError struct {
	statusCode   int
	message      string
	indexMissing bool
}

func (e queryIndexError) Error() string {
	return e.message
}

// HTTPStatus returns the HTTP status code for the operation.
func (e queryIndexError) HTTPStatus() int {
	return e.statusCode
}

// Code returns the analytics error for the error.
func (e queryIndexError) Code() int {
	return e.statusCode
}

// QueryIndexNotFoundError indicates that an index could not be found.
func (e queryIndexError) QueryIndexNotFoundError() bool {
	return e.indexMissing
}

// QueryIndexExistsError indicates that an index already exists.
func (e queryIndexError) QueryIndexExistsError() bool {
	return e.statusCode == 409 && strings.Contains(strings.ToLower(e.message), "already exists")
}

// BucketNotFoundError indicates that a bucket with a given name could not be found.
func (e queryIndexError) BucketNotFoundError() bool {
	return e.statusCode == 500 && strings.Contains(strings.ToLower(e.message), "no bucket named")
}

func (e queryIndexError) FeatureNotFoundError() bool {
	return e.statusCode == 404 && e.message == "Not Found."
}

// UserManagerError occurs for errors created By Couchbase Server when performing user management.
type UserManagerError interface {
	error
	HTTPStatus() int
	UserNotFoundError() bool
	GroupNotFoundError() bool
}

type userManagerError struct {
	statusCode int
	message    string
}

func (e userManagerError) Error() string {
	return e.message
}

func (e userManagerError) HTTPStatus() int {
	return e.statusCode
}

// UserNotFoundError indicates that a specified user could not be found.
func (e userManagerError) UserNotFoundError() bool {
	if strings.Contains(strings.ToLower(e.message), "unknown user.") {
		return true
	}

	return false
}

// GroupNotFoundError indicates that a specified group could not be found.
func (e userManagerError) GroupNotFoundError() bool {
	if strings.Contains(strings.ToLower(e.message), "unknown group.") {
		return true
	}

	return false
}

func (e userManagerError) FeatureNotFoundError() bool {
	return e.statusCode == 404 && e.message == "Not Found."
}

// AnalyticsIndexesError occurs for errors created By Couchbase Server when performing analytics index management.
type AnalyticsIndexesError interface {
	error
	HTTPStatus() int
	AnalyticsIndexNotFoundError() bool
	AnalyticsIndexExistsError() bool
	AnalyticsDatasetNotFoundError() bool
	AnalyticsDatasetExistsError() bool
	AnalyticsDataverseExistsError() bool
	AnalyticsDataverseNotFoundError() bool
	AnalyticsLinkNotFoundError() bool
}

type analyticsIndexesError struct {
	statusCode    int
	analyticsCode uint32
	message       string
}

func (e analyticsIndexesError) Error() string {
	return e.message
}

// HTTPStatus returns the HTTP status code for the operation.
func (e analyticsIndexesError) HTTPStatus() int {
	return e.statusCode
}

// AnalyticsIndexNotFoundError indicates that a specified analytics index could not be found.
func (e analyticsIndexesError) AnalyticsIndexNotFoundError() bool {
	if strings.Contains(strings.ToLower(e.message), "cannot find index") {
		return true
	}

	return false
}

// AnalyticsIndexExistsError indicates that a specified analytics index already exists.
func (e analyticsIndexesError) AnalyticsIndexExistsError() bool {
	if e.analyticsCode == 24048 {
		return true
	}

	return false
}

// AnalyticsDatasetNotFoundError indicates that a specified analytics dataset could not be found.
func (e analyticsIndexesError) AnalyticsDatasetNotFoundError() bool {
	if strings.Contains(strings.ToLower(e.message), "cannot find dataset") {
		return true
	}

	return false
}

// AnalyticsDatasetExistsError indicates that a specified analytics dataset already exists.
func (e analyticsIndexesError) AnalyticsDatasetExistsError() bool {
	if e.analyticsCode == 24040 {
		return true
	}

	return false
}

// AnalyticsDataverseExistsError indicates that a specified analytics dataverse already exists.
func (e analyticsIndexesError) AnalyticsDataverseExistsError() bool {
	if e.analyticsCode == 24039 {
		return true
	}

	return false
}

// AnalyticsDataverseNotFoundError indicates that a specified analytics dataverse could not be found.
func (e analyticsIndexesError) AnalyticsDataverseNotFoundError() bool {
	if e.analyticsCode == 24034 {
		return true
	}

	return false
}

// AnalyticsLinkNotFoundError indicates that a specified analytics link could not be found.
func (e analyticsIndexesError) AnalyticsLinkNotFoundError() bool {
	if strings.Contains(strings.ToLower(e.message), "not exist") {
		if strings.Contains(strings.ToLower(e.message), "link") {
			return true
		}
	}

	return false
}

func (e analyticsIndexesError) FeatureNotFoundError() bool {
	return e.statusCode == 404 && e.message == "Not Found."
}

// SearchIndexesError occurs for errors created By Couchbase Server when performing search index management.
type SearchIndexesError interface {
	error
	HTTPStatus() int
	SearchIndexNotFoundError() bool
}

type searchIndexError struct {
	statusCode int
	message    string
}

func (e searchIndexError) Error() string {
	return e.message
}

// HTTPStatus returns the HTTP status code for the operation.
func (e searchIndexError) HTTPStatus() int {
	return e.statusCode
}

// Code returns the analytics error for the error.
func (e searchIndexError) Code() int {
	return e.statusCode
}

// SearchIndexNotFoundError indicates that an index could not be found.
func (e searchIndexError) SearchIndexNotFoundError() bool {
	if strings.Contains(strings.ToLower(e.message), "index not found") {
		return true
	}

	return false
}

func (e searchIndexError) FeatureNotFoundError() bool {
	return e.statusCode == 404 && e.message == "Not Found."
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

// CollectionManagerError occurs for errors created By Couchbase Server when performing collection management.
type CollectionManagerError interface {
	error
	HTTPStatus() int
	CollectionNotFoundError() bool
	CollectionExistsError() bool
	ScopeNotFoundError() bool
	ScopeExistsError() bool
}

type collectionMgrError struct {
	statusCode int
	message    string
}

func (e collectionMgrError) Error() string {
	return e.message
}

func (e collectionMgrError) HTTPStatus() int {
	return e.statusCode
}

// CollectionNotFoundError indicates that a given collection could not be found.
func (e collectionMgrError) CollectionNotFoundError() bool {
	if e.statusCode == 404 {
		if strings.Contains(e.message, "not found") {
			if strings.Contains(strings.ToLower(e.message), "collection") {
				return true
			}
		}
	}

	return false
}

// CollectionExistsError indicates that a given collection already exists.
func (e collectionMgrError) CollectionExistsError() bool {
	if strings.Contains(e.message, "already exist") {
		if strings.Contains(strings.ToLower(e.message), "collection") {
			return true
		}
	}

	return false
}

// ScopeNotFoundError indicates that a given scope could not be found.
func (e collectionMgrError) ScopeNotFoundError() bool {
	if e.statusCode == 404 {
		if strings.Contains(e.message, "not found") {
			if strings.Contains(strings.ToLower(e.message), "scope") {
				return true
			}
		}
	}

	return false
}

// CollectionExistsError indicates that a given scope already exists.
func (e collectionMgrError) ScopeExistsError() bool {
	if strings.Contains(e.message, "already exist") {
		if strings.Contains(strings.ToLower(e.message), "scope") {
			return true
		}
	}

	return false
}
