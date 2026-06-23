package hooks

import "github.com/couchbase/gocb/v2"

type attemptCtxFn func(ctx gocb.TransactionAttemptContext) error
type attemptCtxWithIDFn func(ctx gocb.TransactionAttemptContext, docID string) error

type TransactionHookMock struct {
	beforeATRCommit                        attemptCtxFn
	afterATRCommit                         attemptCtxFn
	beforeDocCommitted                     attemptCtxWithIDFn
	beforeRemovingDocDuringStagedInsert    attemptCtxWithIDFn
	beforeRollbackDeleteInserted           attemptCtxWithIDFn
	afterDocCommittedBeforeSavingCAS       attemptCtxWithIDFn
	afterDocCommitted                      attemptCtxWithIDFn
	beforeStagedInsert                     attemptCtxWithIDFn
	beforeStagedRemove                     attemptCtxWithIDFn
	beforeStagedReplace                    attemptCtxWithIDFn
	beforeDocRemoved                       attemptCtxWithIDFn
	beforeDocRolledBack                    attemptCtxWithIDFn
	afterDocRemovedPreRetry                attemptCtxWithIDFn
	afterDocRemovedPostRetry               attemptCtxWithIDFn
	afterGetComplete                       attemptCtxWithIDFn
	afterStagedReplaceComplete             attemptCtxWithIDFn
	afterStagedRemoveComplete              attemptCtxWithIDFn
	afterStagedInsertComplete              attemptCtxWithIDFn
	afterRollbackReplaceOrRemove           attemptCtxWithIDFn
	afterRollbackDeleteInserted            attemptCtxWithIDFn
	beforeCheckATREntryForBlockingDoc      attemptCtxWithIDFn
	beforeDocGet                           attemptCtxWithIDFn
	beforeGetDocInExistsDuringStagedInsert attemptCtxWithIDFn
	beforeRemoveStagedInsert               attemptCtxWithIDFn
	afterRemoveStagedInsert                attemptCtxWithIDFn
	afterDocsCommitted                     attemptCtxFn
	afterDocsRemoved                       attemptCtxFn
	afterATRPending                        attemptCtxFn
	beforeATRPending                       attemptCtxFn
	beforeATRComplete                      attemptCtxFn
	beforeATRRolledBack                    attemptCtxFn
	afterATRComplete                       attemptCtxFn
	beforeATRAborted                       attemptCtxFn
	afterATRAborted                        attemptCtxFn
	afterATRRolledBack                     attemptCtxFn
	beforeATRCommitAmbiguityResolution     attemptCtxFn
	beforeQuery                            func(ctx gocb.TransactionAttemptContext, statement string) error
	afterQuery                             func(ctx gocb.TransactionAttemptContext, statement string) error
	randomATRIDForVbucket                  func(ctx gocb.TransactionAttemptContext) (string, error)
	hasExpiredClientSideHook               func(ctx gocb.TransactionAttemptContext, stage string, docID string) (bool, error)
}

func execOrNil(ctx gocb.TransactionAttemptContext, fn func(ctx gocb.TransactionAttemptContext) error) error {
	if fn == nil {
		return nil
	}

	return fn(ctx)
}

func execOrNilDocID(ctx gocb.TransactionAttemptContext, docID string,
	fn func(ctx gocb.TransactionAttemptContext, docID string) error) error {
	if fn == nil {
		return nil
	}

	return fn(ctx, docID)
}

func (thm *TransactionHookMock) BeforeATRCommit(ctx gocb.TransactionAttemptContext) error {
	return execOrNil(ctx, thm.beforeATRCommit)
}

func (thm *TransactionHookMock) AfterATRCommit(ctx gocb.TransactionAttemptContext) error {
	return execOrNil(ctx, thm.afterATRCommit)
}

func (thm *TransactionHookMock) BeforeDocCommitted(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.beforeDocCommitted)
}

func (thm *TransactionHookMock) BeforeRemovingDocDuringStagedInsert(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.beforeRemovingDocDuringStagedInsert)
}

func (thm *TransactionHookMock) BeforeRollbackDeleteInserted(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.beforeRollbackDeleteInserted)
}

func (thm *TransactionHookMock) AfterDocCommittedBeforeSavingCAS(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.afterDocCommittedBeforeSavingCAS)
}

func (thm *TransactionHookMock) AfterDocCommitted(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.afterDocCommitted)
}

func (thm *TransactionHookMock) BeforeStagedInsert(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.beforeStagedInsert)
}

func (thm *TransactionHookMock) BeforeStagedRemove(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.beforeStagedRemove)
}

func (thm *TransactionHookMock) BeforeStagedReplace(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.beforeStagedReplace)
}

func (thm *TransactionHookMock) BeforeDocRemoved(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.beforeDocRemoved)
}

func (thm *TransactionHookMock) BeforeDocRolledBack(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.beforeDocRolledBack)
}

func (thm *TransactionHookMock) AfterDocRemovedPreRetry(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.afterDocRemovedPreRetry)
}

func (thm *TransactionHookMock) AfterDocRemovedPostRetry(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.afterDocRemovedPostRetry)
}

func (thm *TransactionHookMock) AfterGetComplete(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.afterGetComplete)
}

func (thm *TransactionHookMock) AfterStagedReplaceComplete(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.afterStagedReplaceComplete)
}

func (thm *TransactionHookMock) AfterStagedRemoveComplete(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.afterStagedRemoveComplete)
}

func (thm *TransactionHookMock) AfterStagedInsertComplete(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.afterStagedInsertComplete)
}

func (thm *TransactionHookMock) AfterRollbackReplaceOrRemove(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.afterRollbackReplaceOrRemove)
}

func (thm *TransactionHookMock) AfterRollbackDeleteInserted(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.afterRollbackDeleteInserted)
}

func (thm *TransactionHookMock) BeforeCheckATREntryForBlockingDoc(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.beforeCheckATREntryForBlockingDoc)
}

func (thm *TransactionHookMock) BeforeDocGet(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.beforeDocGet)
}

func (thm *TransactionHookMock) BeforeGetDocInExistsDuringStagedInsert(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.beforeGetDocInExistsDuringStagedInsert)
}

func (thm *TransactionHookMock) BeforeRemoveStagedInsert(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.beforeRemoveStagedInsert)
}

func (thm *TransactionHookMock) AfterRemoveStagedInsert(ctx gocb.TransactionAttemptContext, docID string) error {
	return execOrNilDocID(ctx, docID, thm.afterRemoveStagedInsert)
}

func (thm *TransactionHookMock) AfterDocsCommitted(ctx gocb.TransactionAttemptContext) error {
	return execOrNil(ctx, thm.afterDocsCommitted)
}

func (thm *TransactionHookMock) AfterDocsRemoved(ctx gocb.TransactionAttemptContext) error {
	return execOrNil(ctx, thm.afterDocsRemoved)
}

func (thm *TransactionHookMock) AfterATRPending(ctx gocb.TransactionAttemptContext) error {
	return execOrNil(ctx, thm.afterATRPending)
}

func (thm *TransactionHookMock) BeforeATRPending(ctx gocb.TransactionAttemptContext) error {
	return execOrNil(ctx, thm.beforeATRPending)
}

func (thm *TransactionHookMock) BeforeATRComplete(ctx gocb.TransactionAttemptContext) error {
	return execOrNil(ctx, thm.beforeATRComplete)
}

func (thm *TransactionHookMock) BeforeATRRolledBack(ctx gocb.TransactionAttemptContext) error {
	return execOrNil(ctx, thm.beforeATRRolledBack)
}

func (thm *TransactionHookMock) AfterATRComplete(ctx gocb.TransactionAttemptContext) error {
	return execOrNil(ctx, thm.afterATRComplete)
}

func (thm *TransactionHookMock) BeforeATRAborted(ctx gocb.TransactionAttemptContext) error {
	return execOrNil(ctx, thm.beforeATRAborted)
}

func (thm *TransactionHookMock) AfterATRAborted(ctx gocb.TransactionAttemptContext) error {
	return execOrNil(ctx, thm.afterATRAborted)
}

func (thm *TransactionHookMock) AfterATRRolledBack(ctx gocb.TransactionAttemptContext) error {
	return execOrNil(ctx, thm.afterATRRolledBack)
}

func (thm *TransactionHookMock) BeforeATRCommitAmbiguityResolution(ctx gocb.TransactionAttemptContext) error {
	return execOrNil(ctx, thm.beforeATRCommitAmbiguityResolution)
}

func (thm *TransactionHookMock) RandomATRIDForVbucket(ctx gocb.TransactionAttemptContext) (string, error) {
	if thm.randomATRIDForVbucket == nil {
		return "", nil
	}
	return thm.randomATRIDForVbucket(ctx)
}

func (thm *TransactionHookMock) HasExpiredClientSideHook(ctx gocb.TransactionAttemptContext, stage string, docID string) (bool, error) {
	if thm.hasExpiredClientSideHook == nil {
		return false, nil
	}
	return thm.hasExpiredClientSideHook(ctx, stage, docID)
}

func (thm *TransactionHookMock) BeforeQuery(ctx gocb.TransactionAttemptContext, statement string) error {
	if thm.beforeQuery == nil {
		return nil
	}

	return thm.beforeQuery(ctx, statement)
}

func (thm *TransactionHookMock) AfterQuery(ctx gocb.TransactionAttemptContext, statement string) error {
	if thm.afterQuery == nil {
		return nil
	}

	return thm.afterQuery(ctx, statement)
}
