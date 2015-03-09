package gocbcore

import (
	"errors"
	"fmt"
)

var (
	ErrTimeout        = errors.New("Operation timed out")
	ErrFailedDispatch = errors.New("Failed to dispatch operation")
	ErrNetworkError   = errors.New("Network error")
	ErrQueueOverflow  = errors.New("Queue overflow")

	ErrAuthInvalidReturn = errors.New("Failed to parse returned value")
	ErrAuthFailAllSpecHosts = errors.New("Failed to connect to all specified hosts")

	ErrSuccess        = errors.New("Success")
	ErrKeyNotFound    = errors.New("Key not found")
	ErrKeyExists      = errors.New("Key already exists")
	ErrTooBig         = errors.New("Document value was too large")
	ErrNotStored      = errors.New("The document could not be stored")
	ErrBadDelta       = errors.New("An invalid delta was passed")
	ErrNotMyVBucket   = errors.New("Operation sent to incorrect server")
	ErrAuthError      = errors.New("Authentication Error")
	ErrAuthContinue   = errors.New("Auth Continue")
	ErrUnknownCommand = errors.New("An unknown command was received")
	ErrOutOfMemory    = errors.New("The server is out of memory")
	ErrTmpFail        = errors.New("A temporary failure occurred, try again later")

	status2error = map[StatusCode]error{
		StatusSuccess:        ErrSuccess,
		StatusKeyNotFound:    ErrKeyNotFound,
		StatusKeyExists:      ErrKeyExists,
		StatusTooBig:         ErrTooBig,
		StatusNotStored:      ErrNotStored,
		StatusBadDelta:       ErrBadDelta,
		StatusNotMyVBucket:   ErrNotMyVBucket,
		StatusAuthError:      ErrAuthError,
		StatusAuthContinue:   ErrAuthContinue,
		StatusUnknownCommand: ErrUnknownCommand,
		StatusOutOfMemory:    ErrOutOfMemory,
		StatusTmpFail:        ErrTmpFail,
	}
)

func newMemdError(code StatusCode) error {
	if err, ok := status2error[code]; ok {
		return err
	} else {
		return errors.New(fmt.Sprintf("Unknown error (%d)", code))
	}
}
