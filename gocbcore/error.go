package gocbcore

import (
	"fmt"
)

type generalError struct {
	message string
}

func (e generalError) Error() string {
	return e.message
}

type timeoutError struct {
}

func (e timeoutError) Error() string {
	return "The operation has timed out."
}
func (e timeoutError) Timeout() bool {
	return true
}


type networkError struct {
}

func (e networkError) Error() string {
	return "Network error."
}

func (e networkError) NetworkError() bool {
	return true
}

type overloadError struct {
}

func (e overloadError) Error() string {
	return "Queue overflow."
}
func (e overloadError) Overload() bool {
	return true
}

type shutdownError struct {
}

func (e shutdownError) Error() string {
	return "Connection shut down."
}
func (e shutdownError) ShutdownError() bool {
	return true
}


type memdError struct {
	code StatusCode
}

func (e memdError) Error() string {
	switch e.code {
	case StatusSuccess:
		return "Success."
	case StatusKeyNotFound:
		return "Key not found."
	case StatusKeyExists:
		return "Key already exists."
	case StatusTooBig:
		return "Document value was too large."
	case StatusNotStored:
		return "The document could not be stored."
	case StatusBadDelta:
		return "An invalid delta was passed."
	case StatusNotMyVBucket:
		return "Operation sent to incorrect server."
	case StatusNoBucket:
		return "Not connected to a bucket."
	case StatusAuthStale:
		return "The authenication context is stale. Try re-authenticating."
	case StatusAuthError:
		return "Authentication Error."
	case StatusAuthContinue:
		return "Auth Continue."
	case StatusRangeError:
		return "Requested value is outside range."
	case StatusAccessError:
		return "No access."
	case StatusNotInitialized:
		return "The cluster is being initialized. Requests are blocked."
	case StatusRollback:
		return "A rollback is required."
	case StatusUnknownCommand:
		return "An unknown command was received."
	case StatusOutOfMemory:
		return "The server is out of memory."
	case StatusNotSupported:
		return "The server does not support this command."
	case StatusInternalError:
		return "Internal server error."
	case StatusBusy:
		return "The server is busy. Try again later."
	case StatusTmpFail:
		return "A temporary failure occurred.  Try again later."
	default:
		return fmt.Sprintf("An unknown error occurred (%d).", e.code)
	}
}
func (e memdError) KeyNotFound() bool {
	return e.code == StatusKeyNotFound
}
func (e memdError) KeyExists() bool {
	return e.code == StatusKeyExists
}
func (e memdError) Temporary() bool {
	return e.code == StatusOutOfMemory || e.code == StatusTmpFail
}
func (e memdError) AuthError() bool {
	return e.code == StatusAuthError
}
func (e memdError) ValueTooBig() bool {
	return e.code == StatusTooBig
}
func (e memdError) NotStored() bool {
	return e.code == StatusNotStored
}
func (e memdError) BadDelta() bool {
	return e.code == StatusBadDelta
}

type agentError struct {
	message string
}

func (e agentError) Error() string {
	return e.message
}

type streamEndError struct {
	code StreamEndStatus
}

func (e streamEndError) Error() string {
	switch e.code {
	case StreamEndOK:
		return "Success."
	case StreamEndClosed:
		return "Stream closed."
	case StreamEndStateChanged:
		return "State changed."
	case StreamEndDisconnected:
		return "Disconnected."
	case StreamEndTooSlow:
		return "Too slow."
	default:
		return fmt.Sprintf("Stream closed for unknown reason: (%d).", e.code)
	}
}

func (e streamEndError) Success() bool {
	return e.code == StreamEndOK
}
func (e streamEndError) Closed() bool {
	return e.code == StreamEndClosed
}
func (e streamEndError) StateChanged() bool {
	return e.code == StreamEndStateChanged
}
func (e streamEndError) Disconnected() bool {
	return e.code == StreamEndDisconnected
}
func (e streamEndError) TooSlow() bool {
	return e.code == StreamEndTooSlow
}