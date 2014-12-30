package couchbase

import "fmt"

type Error interface {
	KeyNotFound() bool
	KeyExists() bool
	Timeout() bool
	Temporary() bool
}

type ClientError struct {
	message string
}

func (e ClientError) Error() string {
	return e.message
}
func (e ClientError) KeyNotFound() bool {
	return false
}
func (e ClientError) KeyExists() bool {
	return false
}
func (e ClientError) Timeout() bool {
	return false
}
func (e ClientError) Temporary() bool {
	return false
}

type TimeoutError struct {
}

func (e TimeoutError) Error() string {
	return "The operation has timed out."
}
func (e TimeoutError) KeyNotFound() bool {
	return false
}
func (e TimeoutError) KeyExists() bool {
	return false
}
func (e TimeoutError) Timeout() bool {
	return true
}
func (e TimeoutError) Temporary() bool {
	return false
}

type MemdError struct {
	code statusCode
}

func (e MemdError) Error() string {
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
func (e MemdError) KeyNotFound() bool {
	return e.code == keyNotFound
}
func (e MemdError) KeyExists() bool {
	return e.code == keyExists
}
func (e MemdError) Timeout() bool {
	return false
}
func (e MemdError) Temporary() bool {
	return e.code == outOfMemory || e.code == tmpFail
}
