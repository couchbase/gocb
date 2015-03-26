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

func (e generalError) GeneralError() bool {
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

func (e overloadError) OverloadError() bool {
	return true
}

type memdError struct {
	code StatusCode
}

type agentError struct {
	message string
}

func (e agentError) Error() string {
	return e.message
}

func (e agentError) AgentError() bool {
	return true
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
	case StatusAuthError:
		return "Authentication Error."
	case StatusAuthContinue:
		return "Auth Continue."
	case StatusUnknownCommand:
		return "An unknown command was received."
	case StatusOutOfMemory:
		return "The server is out of memory."
	case StatusTmpFail:
		return "A temporary failure occurred.  Try again later."
	default:
		return fmt.Sprintf("An unknown error occurred (%d).", e.code)
	}
}

func (e memdError) Success() bool {
	return e.code == StatusSuccess
}

func (e memdError) KeyNotFound() bool {
	return e.code == StatusKeyNotFound
}

func (e memdError) KeyExists() bool {
	return e.code == StatusKeyExists
}

func (e memdError) TooBig() bool {
	return e.code == StatusTooBig
}

func (e memdError) NotStored() bool {
	return e.code == StatusNotStored
}

func (e memdError) BadDelta() bool {
	return e.code == StatusBadDelta
}

func (e memdError) NotMyVBucket() bool {
	return e.code == StatusNotMyVBucket
}

func (e memdError) Temporary() bool {
	return e.code == StatusOutOfMemory || e.code == StatusTmpFail
}

func (e memdError) AuthError() bool {
	return e.code == StatusAuthError
}

func (e memdError) AuthContinue() bool {
	return e.code == StatusAuthContinue
}

func (e memdError) UnknownCommand() bool {
	return e.code == StatusUnknownCommand
}

func (e memdError) UnknownError() bool {
	return e.code != StatusSuccess &&
		e.code != StatusKeyNotFound &&
		e.code != StatusKeyExists &&
		e.code != StatusTooBig &&
		e.code != StatusNotStored &&
		e.code != StatusBadDelta &&
		e.code != StatusNotMyVBucket &&
		e.code != StatusAuthError &&
		e.code != StatusAuthContinue &&
		e.code != StatusUnknownCommand &&
		e.code != StatusOutOfMemory &&
		e.code != StatusTmpFail
}
