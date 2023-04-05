package gocb

import (
	"time"

	"github.com/couchbase/gocbcore/v10"
)

// kvProviderCoreProvider provides us with a way to unit test what the kvProviderCore layer is doing.
type kvProviderCoreProvider interface {
	Add(opts gocbcore.AddOptions, cb gocbcore.StoreCallback) (gocbcore.PendingOp, error)
	Set(opts gocbcore.SetOptions, cb gocbcore.StoreCallback) (gocbcore.PendingOp, error)
	Replace(opts gocbcore.ReplaceOptions, cb gocbcore.StoreCallback) (gocbcore.PendingOp, error)
	Get(opts gocbcore.GetOptions, cb gocbcore.GetCallback) (gocbcore.PendingOp, error)
	GetOneReplica(opts gocbcore.GetOneReplicaOptions, cb gocbcore.GetReplicaCallback) (gocbcore.PendingOp, error)
	Observe(opts gocbcore.ObserveOptions, cb gocbcore.ObserveCallback) (gocbcore.PendingOp, error)
	ObserveVb(opts gocbcore.ObserveVbOptions, cb gocbcore.ObserveVbCallback) (gocbcore.PendingOp, error)
	GetMeta(opts gocbcore.GetMetaOptions, cb gocbcore.GetMetaCallback) (gocbcore.PendingOp, error)
	Delete(opts gocbcore.DeleteOptions, cb gocbcore.DeleteCallback) (gocbcore.PendingOp, error)
	LookupIn(opts gocbcore.LookupInOptions, cb gocbcore.LookupInCallback) (gocbcore.PendingOp, error)
	MutateIn(opts gocbcore.MutateInOptions, cb gocbcore.MutateInCallback) (gocbcore.PendingOp, error)
	GetAndTouch(opts gocbcore.GetAndTouchOptions, cb gocbcore.GetAndTouchCallback) (gocbcore.PendingOp, error)
	GetAndLock(opts gocbcore.GetAndLockOptions, cb gocbcore.GetAndLockCallback) (gocbcore.PendingOp, error)
	Unlock(opts gocbcore.UnlockOptions, cb gocbcore.UnlockCallback) (gocbcore.PendingOp, error)
	Touch(opts gocbcore.TouchOptions, cb gocbcore.TouchCallback) (gocbcore.PendingOp, error)
	Increment(opts gocbcore.CounterOptions, cb gocbcore.CounterCallback) (gocbcore.PendingOp, error)
	Decrement(opts gocbcore.CounterOptions, cb gocbcore.CounterCallback) (gocbcore.PendingOp, error)
	Append(opts gocbcore.AdjoinOptions, cb gocbcore.AdjoinCallback) (gocbcore.PendingOp, error)
	Prepend(opts gocbcore.AdjoinOptions, cb gocbcore.AdjoinCallback) (gocbcore.PendingOp, error)
	WaitForConfigSnapshot(deadline time.Time, opts gocbcore.WaitForConfigSnapshotOptions, cb gocbcore.WaitForConfigSnapshotCallback) (gocbcore.PendingOp, error)
	RangeScanCreate(vbID uint16, opts gocbcore.RangeScanCreateOptions, cb gocbcore.RangeScanCreateCallback) (gocbcore.PendingOp, error)
	RangeScanContinue(scanUUID []byte, vbID uint16, opts gocbcore.RangeScanContinueOptions, dataCb gocbcore.RangeScanContinueDataCallback,
		actionCb gocbcore.RangeScanContinueActionCallback) (gocbcore.PendingOp, error)
	RangeScanCancel(scanUUID []byte, vbID uint16, opts gocbcore.RangeScanCancelOptions, cb gocbcore.RangeScanCancelCallback) (gocbcore.PendingOp, error)
}
