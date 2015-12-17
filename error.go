package gocb

import (
	"errors"
	"github.com/couchbase/gocb/gocbcore"
)

type clientError struct {
	message string
}

func (e clientError) Error() string {
	return e.message
}

var (
	ErrNotEnoughReplicas = errors.New("Not enough replicas to match durability requirements.")
	ErrDurabilityTimeout = errors.New("Failed to meet durability requirements in time.")
	ErrNoResults         = errors.New("No results returned.")

	ErrDispatchFail   = gocbcore.ErrDispatchFail
	ErrBadHosts       = gocbcore.ErrBadHosts
	ErrProtocol       = gocbcore.ErrProtocol
	ErrNoReplicas     = gocbcore.ErrNoReplicas
	ErrInvalidServer  = gocbcore.ErrInvalidServer
	ErrInvalidVBucket = gocbcore.ErrInvalidVBucket
	ErrInvalidReplica = gocbcore.ErrInvalidReplica

	ErrShutdown = gocbcore.ErrShutdown
	ErrOverload = gocbcore.ErrOverload
	ErrNetwork  = gocbcore.ErrNetwork
	ErrTimeout  = gocbcore.ErrTimeout

	ErrStreamClosed       = gocbcore.ErrStreamClosed
	ErrStreamStateChanged = gocbcore.ErrStreamStateChanged
	ErrStreamDisconnected = gocbcore.ErrStreamDisconnected
	ErrStreamTooSlow      = gocbcore.ErrStreamTooSlow

	ErrKeyNotFound    = gocbcore.ErrKeyNotFound
	ErrKeyExists      = gocbcore.ErrKeyExists
	ErrTooBig         = gocbcore.ErrTooBig
	ErrInvalidArgs    = gocbcore.ErrInvalidArgs
	ErrNotStored      = gocbcore.ErrNotStored
	ErrBadDelta       = gocbcore.ErrBadDelta
	ErrNotMyVBucket   = gocbcore.ErrNotMyVBucket
	ErrNoBucket       = gocbcore.ErrNoBucket
	ErrAuthStale      = gocbcore.ErrAuthStale
	ErrAuthError      = gocbcore.ErrAuthError
	ErrAuthContinue   = gocbcore.ErrAuthContinue
	ErrRangeError     = gocbcore.ErrRangeError
	ErrRollback       = gocbcore.ErrRollback
	ErrAccessError    = gocbcore.ErrAccessError
	ErrNotInitialized = gocbcore.ErrNotInitialized
	ErrUnknownCommand = gocbcore.ErrUnknownCommand
	ErrOutOfMemory    = gocbcore.ErrOutOfMemory
	ErrNotSupported   = gocbcore.ErrNotSupported
	ErrInternalError  = gocbcore.ErrInternalError
	ErrBusy           = gocbcore.ErrBusy
	ErrTmpFail        = gocbcore.ErrTmpFail
)
