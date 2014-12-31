package couchbase

import "fmt"

type Error interface {
	KeyNotFound() bool
	KeyExists() bool
	Timeout() bool
	Temporary() bool
}

type clientError struct {
	message string
}

func (e clientError) Error() string {
	return e.message
}
func (e clientError) KeyNotFound() bool {
	return false
}
func (e clientError) KeyExists() bool {
	return false
}
func (e clientError) Timeout() bool {
	return false
}
func (e clientError) Temporary() bool {
	return false
}

type timeoutError struct {
}

func (e timeoutError) Error() string {
	return "The operation has timed out."
}
func (e timeoutError) KeyNotFound() bool {
	return false
}
func (e timeoutError) KeyExists() bool {
	return false
}
func (e timeoutError) Timeout() bool {
	return true
}
func (e timeoutError) Temporary() bool {
	return false
}

type memdError struct {
	code statusCode
}

func (e memdError) Error() string {
	switch e.code {
	case success:
		return "Success."
	case keyNotFound:
		return "Key not found."
	case keyExists:
		return "Key already exists."
	case tooBig:
		return "Document value was too large."
	case notStored:
		return "The document could not be stored."
	case badDelta:
		return "An invalid delta was passed."
	case notMyVBucket:
		return "Operation sent to incorrect server."
	case unknownCommand:
		return "An unknown command was received."
	case outOfMemory:
		return "The server is out of memory."
	case tmpFail:
		return "A temporary failure occurred.  Try again later."
	default:
		return fmt.Sprintf("An unknown error occurred (%d).", e.code)
	}
}
func (e memdError) KeyNotFound() bool {
	return e.code == keyNotFound
}
func (e memdError) KeyExists() bool {
	return e.code == keyExists
}
func (e memdError) Timeout() bool {
	return false
}
func (e memdError) Temporary() bool {
	return e.code == outOfMemory || e.code == tmpFail
}
