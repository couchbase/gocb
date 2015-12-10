package gocbcore

import (
    "errors"
    "fmt"
)

var (
    TimeoutError  = errors.New("The operation has timed out.")
    DispatchError = errors.New("Failed to dispatch operation.")
    NetworkError  = errors.New("Network error.")
    OverloadError = errors.New("Queue overflow.")
    ShutdownError = errors.New("Connection shut down.")
)

var (
    SuccessError        = &memdError{code: StatusSuccess, message: "Success."}
    KeyNotFoundError    = &memdError{code: StatusKeyNotFound, message: "Key not found."}
    KeyExistsError      = &memdError{code: StatusKeyExists, message: "Key already exists."}
    TooBigError         = &memdError{code: StatusTooBig, message: "Document value was too large."}
    NotStoredError      = &memdError{code: StatusNotStored, message: "The document could not be stored."}
    BadDeltaError       = &memdError{code: StatusBadDelta, message: "An invalid delta was passed."}
    NotMyVBucketError   = &memdError{code: StatusNotMyVBucket, message: "Operation sent to incorrect server."}
    NoBucketError       = &memdError{code: StatusNoBucket, message: "Not connected to a bucket."}
    AuthStaleError      = &memdError{code: StatusAuthStale, message: "The authenication context is stale. Try re-authenticating."}
    AuthError           = &memdError{code: StatusAuthError, message: "Authentication Error."}
    AuthContinueError   = &memdError{code: StatusAuthContinue, message: "Auth Continue."}
    RangeError          = &memdError{code: StatusRangeError, message: "Requested value is outside range."}
    AccessError         = &memdError{code: StatusAccessError, message: "No access."}
    NotInitializedError = &memdError{code: StatusNotInitialized, message: "The cluster is being initialized. Requests are blocked."}
    RollbackError       = &memdError{code: StatusRollback, message: "A rollback is required."}
    UnknownCommandError = &memdError{code: StatusUnknownCommand, message: "An unknown command was received."}
    OutOfMemoryError    = &memdError{code: StatusOutOfMemory, message: "The server is out of memory."}
    NotSupportedError   = &memdError{code: StatusNotSupported, message: "The server does not support this command."}
    InternalError       = &memdError{code: StatusInternalError, message: "Internal server error."}
    BusyError           = &memdError{code: StatusBusy, message: "The server is busy. Try again later."}
    TmpFailError        = &memdError{code: StatusTmpFail, message: "A temporary failure occurred.  Try again later."}
)

var (
    StreamEndOKError           = &streamEndError{code: StreamEndOK, message: "Success."}
    StreamEndClosedError       = &streamEndError{code: StreamEndClosed, message: "Stream closed."}
    StreamEndStateChangedError = &streamEndError{code: StreamEndStateChanged, message: "State changed."}
    StreamEndDisconnectedError = &streamEndError{code: StreamEndDisconnected, message: "Disconnected."}
    StreamEndTooSlowError      = &streamEndError{code: StreamEndTooSlow, message: "Too slow."}
)

type memdError struct {
    code    StatusCode
    message string
}

func getMemdError(code StatusCode) *memdError {
    switch code {
    case StatusSuccess:
        return SuccessError
    case StatusKeyNotFound:
        return KeyNotFoundError
    case StatusKeyExists:
        return KeyExistsError
    case StatusTooBig:
        return TooBigError
    case StatusNotStored:
        return NotStoredError
    case StatusBadDelta:
        return BadDeltaError
    case StatusNotMyVBucket:
        return NotMyVBucketError
    case StatusNoBucket:
        return NoBucketError
    case StatusAuthStale:
        return AuthStaleError
    case StatusAuthError:
        return AuthError
    case StatusAuthContinue:
        return AuthContinueError
    case StatusRangeError:
        return RangeError
    case StatusAccessError:
        return AccessError
    case StatusNotInitialized:
        return NotInitializedError
    case StatusRollback:
        return RollbackError
    case StatusUnknownCommand:
        return UnknownCommandError
    case StatusOutOfMemory:
        return OutOfMemoryError
    case StatusNotSupported:
        return NotSupportedError
    case StatusInternalError:
        return InternalError
    case StatusBusy:
        return BusyError
    case StatusTmpFail:
        return TmpFailError
    default:
        return &memdError{code: code, message: fmt.Sprintf("An unknown error occurred (%d).", code)}
    }
}

func (e memdError) Error() string {
    return e.message
}

type agentError struct {
    message string
}

func (e agentError) Error() string {
    return e.message
}

type streamEndError struct {
    code    StreamEndStatus
    message string
}

func (e streamEndError) Error() string {
    return e.message
}
func getStreamEndError(code StreamEndStatus) *streamEndError {
    switch code {
    case StreamEndOK:
        return StreamEndOKError
    case StreamEndClosed:
        return StreamEndClosedError
    case StreamEndStateChanged:
        return StreamEndStateChangedError
    case StreamEndDisconnected:
        return StreamEndDisconnectedError
    case StreamEndTooSlow:
        return StreamEndDisconnectedError
    default:
        return &streamEndError{code: code, message: fmt.Sprintf("Stream closed for unknown reason: (%d).", code)}
    }
}
