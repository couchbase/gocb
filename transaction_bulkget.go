package gocb

import (
	"github.com/couchbase/gocbcore/v10"
)

// TransactionBulkGetSpec represents a request to fetch an individual document, as part of a TransactionAttemptContext.BulkGet operation.
type TransactionBulkGetSpec struct {
	Collection *Collection
	ID         string
}

// TransactionBulkGetMode specifies the level of effort to spend on minimizing read skew for a TransactionAttemptContext.BulkGet operation.
type TransactionBulkGetMode uint8

const (
	// TransactionBulkGetModeUnset specifies that the default mode should be used.
	TransactionBulkGetModeUnset TransactionBulkGetMode = TransactionBulkGetMode(gocbcore.TransactionGetMultiModeUnset)

	// TransactionBulkGetModePrioritiseLatency specifies that some time-bounded effort will be made to detect and avoid read skew.
	TransactionBulkGetModePrioritiseLatency = TransactionBulkGetMode(gocbcore.TransactionGetMultiModePrioritiseLatency)

	// TransactionBulkGetModeDisableReadSkewDetection specifies that no read skew detection should be attempted. Once the documents
	// are fetched, they will be returned immediately.
	TransactionBulkGetModeDisableReadSkewDetection = TransactionBulkGetMode(gocbcore.TransactionGetMultiModeDisableReadSkewDetection)

	// TransactionBulkGetModePrioritiseReadSkewDetection specifies that great effort will be made to detect and avoid read skew.
	TransactionBulkGetModePrioritiseReadSkewDetection = TransactionBulkGetMode(gocbcore.TransactionGetMultiModePrioritiseReadSkewDetection)
)

// TransactionBulkGetOptions provides options for a TransactionAttemptContext.BulkGet operation.
type TransactionBulkGetOptions struct {
	Mode TransactionBulkGetMode
}

// TransactionBulkGetResult represents the result of a TransactionAttemptContext.BulkGet operation.
type TransactionBulkGetResult struct {
	transcoder Transcoder
	flags      uint32

	specCount uint
	coreRes   *gocbcore.TransactionGetMultiResult
}

// ContentAt provides access to the contents of a document that was fetched, given the index of the corresponding TransactionBulkGetSpec.
func (bgr *TransactionBulkGetResult) ContentAt(idx uint, valuePtr interface{}) error {
	if idx >= bgr.specCount {
		return makeInvalidArgumentsError("invalid index")
	}
	value, ok := bgr.coreRes.Values[int(idx)]
	if !ok {
		return ErrDocumentNotFound
	}
	return bgr.transcoder.Decode(value, bgr.flags, valuePtr)
}

// Exists returns whether a document exists, given the index of the corresponding TransactionBulkGetSpec.
func (bgr *TransactionBulkGetResult) Exists(idx uint) bool {
	_, ok := bgr.coreRes.Values[int(idx)]
	return ok
}

// TransactionBulkGetReplicaFromPreferredServerGroupSpec represents a request to fetch an individual document, as part of a
// TransactionAttemptContext.BulkGetReplicaFromPreferredServerGroup operation.
type TransactionBulkGetReplicaFromPreferredServerGroupSpec struct {
	Collection *Collection
	ID         string
}

// TransactionBulkGetReplicaFromPreferredServerGroupMode specifies the level of effort to spend on minimizing read skew for a
// TransactionAttemptContext.BulkGetReplicaFromPreferredServerGroup operation.
type TransactionBulkGetReplicaFromPreferredServerGroupMode uint8

const (
	// TransactionBulkGetReplicaFromPreferredServerGroupModeUnset specifies that the default mode should be used.
	TransactionBulkGetReplicaFromPreferredServerGroupModeUnset = TransactionBulkGetReplicaFromPreferredServerGroupMode(gocbcore.TransactionGetMultiModeUnset)

	// TransactionBulkGetReplicaFromPreferredServerGroupModePrioritiseLatency specifies that some time-bounded effort will be made to detect and avoid read skew.
	TransactionBulkGetReplicaFromPreferredServerGroupModePrioritiseLatency = TransactionBulkGetReplicaFromPreferredServerGroupMode(gocbcore.TransactionGetMultiModePrioritiseLatency)

	// TransactionBulkGetReplicaFromPreferredServerGroupModeDisableReadSkewDetection specifies that no read skew detection should be attempted. Once the documents
	// are fetched, they will be returned immediately.
	TransactionBulkGetReplicaFromPreferredServerGroupModeDisableReadSkewDetection = TransactionBulkGetReplicaFromPreferredServerGroupMode(gocbcore.TransactionGetMultiModeDisableReadSkewDetection)

	// TransactionBulkGetReplicaFromPreferredServerGroupModePrioritiseReadSkewDetection specifies that great effort will be made to detect and avoid read skew.
	TransactionBulkGetReplicaFromPreferredServerGroupModePrioritiseReadSkewDetection = TransactionBulkGetReplicaFromPreferredServerGroupMode(gocbcore.TransactionGetMultiModePrioritiseReadSkewDetection)
)

// TransactionBulkGetReplicaFromPreferredServerGroupOptions provides options for a TransactionAttemptContext.BulkGetReplicaFromPreferredServerGroup operation.
type TransactionBulkGetReplicaFromPreferredServerGroupOptions struct {
	Mode TransactionBulkGetReplicaFromPreferredServerGroupMode
}

// TransactionBulkGetReplicaFromPreferredServerGroupResult represents the result of a TransactionAttemptContext.BulkGetReplicaFromPreferredServerGroup operation.
type TransactionBulkGetReplicaFromPreferredServerGroupResult struct {
	transcoder Transcoder
	flags      uint32

	specCount uint
	coreRes   *gocbcore.TransactionGetMultiResult
}

// ContentAt provides access to the contents of a document that was fetched, given the index of the corresponding TransactionBulkGetReplicaFromPreferredServerGroupSpec.
func (bgr *TransactionBulkGetReplicaFromPreferredServerGroupResult) ContentAt(idx uint, valuePtr interface{}) error {
	if idx >= bgr.specCount {
		return makeInvalidArgumentsError("invalid index")
	}
	value, ok := bgr.coreRes.Values[int(idx)]
	if !ok {
		return ErrDocumentNotFound
	}
	return bgr.transcoder.Decode(value, bgr.flags, valuePtr)
}

// Exists returns whether a document exists, given the index of the corresponding TransactionBulkGetReplicaFromPreferredServerGroupSpec.
func (bgr *TransactionBulkGetReplicaFromPreferredServerGroupResult) Exists(idx uint) bool {
	_, ok := bgr.coreRes.Values[int(idx)]
	return ok
}
