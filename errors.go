package gocb

import (
	"errors"
)

var (
	ErrTimeout                     = errors.New("Operation timed out")
	ErrUnexpectedLegacyFlagsValue  = errors.New("Unexpected legacy flags value")
	ErrUnexpectedValueCompression  = errors.New("Unexpected value compression")
	ErrUnexpectedFlagsValue        = errors.New("Unexpected flags value")
	ErrInvalidDeltaValue           = errors.New("Delta must be a non-zero value")
	ErrNoResultsReturned           = errors.New("No results returned")
	ErrDesignDocumentAlreadyExists = errors.New("Design document already exists")
)
