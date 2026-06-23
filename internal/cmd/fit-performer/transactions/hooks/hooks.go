package hooks

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/transactions/callcounts"

	"github.com/couchbase/gocb/v2"
	protoHooksTxns "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/hooks/transactions"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/cluster"
)

type Mock struct {
	transactionHooks  *TransactionHookMock
	cleanupHooks      *CleanupHookMock
	clientRecordHooks *ClientRecordHookMock
}

func NewHooksMock(txnHooks *TransactionHookMock, cleanHooks *CleanupHookMock, cliHooks *ClientRecordHookMock) *Mock {
	return &Mock{
		transactionHooks:  txnHooks,
		cleanupHooks:      cleanHooks,
		clientRecordHooks: cliHooks,
	}
}
func (mock *Mock) TransactionHooks() gocb.TransactionHooks {
	return mock.transactionHooks
}
func (mock *Mock) CleanUpHooks() gocb.TransactionCleanupHooks {
	return mock.cleanupHooks
}
func (mock *Mock) ClientRecordHooks() gocb.TransactionClientRecordHooks {
	return mock.clientRecordHooks
}

// Note that this is a bit weird, it will return nil, nil as a standard case. This is because we have to allow
// for the ability to return string, error in a single case and we may as well future proof.
func configureHook(ctx gocb.TransactionAttemptContext, hook *protoHooksTxns.Hook, counts *callcounts.CallCounts,
	cluster *cluster.Connection, docID string) func() (interface{}, error) {
	return func() (interface{}, error) {
		var action func() (interface{}, error)
		var out func() (interface{}, error)
		switch hook.HookAction {
		case protoHooksTxns.HookAction_FAIL_HARD:
			action = func() (interface{}, error) {
				return nil, gocb.ErrHard
			}
		case protoHooksTxns.HookAction_FAIL_OTHER:
			action = func() (interface{}, error) {
				return nil, gocb.ErrOther
			}
		case protoHooksTxns.HookAction_FAIL_TRANSIENT:
			action = func() (interface{}, error) {
				return nil, gocb.ErrTransient
			}
		case protoHooksTxns.HookAction_FAIL_AMBIGUOUS:
			action = func() (interface{}, error) {
				return nil, gocb.ErrAmbiguous
			}
		case protoHooksTxns.HookAction_FAIL_DOC_NOT_FOUND:
			action = func() (interface{}, error) {
				return nil, gocb.ErrDocumentNotFound
			}
		case protoHooksTxns.HookAction_FAIL_DOC_ALREADY_EXISTS:
			action = func() (interface{}, error) {
				return nil, gocb.ErrDocumentExists
			}
		case protoHooksTxns.HookAction_FAIL_PATH_ALREADY_EXISTS:
			action = func() (interface{}, error) {
				return nil, gocb.ErrPathExists
			}
		case protoHooksTxns.HookAction_FAIL_PATH_NOT_FOUND:
			action = func() (interface{}, error) {
				return nil, gocb.ErrPathNotFound
			}
		case protoHooksTxns.HookAction_FAIL_CAS_MISMATCH:
			action = func() (interface{}, error) {
				return nil, gocb.ErrCasMismatch
			}
		case protoHooksTxns.HookAction_FAIL_ATR_FULL:
			action = func() (interface{}, error) {
				return nil, gocb.ErrValueTooLarge
			}
		case protoHooksTxns.HookAction_MUTATE_DOC:
			// In format "bucket-name/collection-name/doc-id"
			action = func() (interface{}, error) {
				bucketName, collectionName, id := splitLocation(hook.HookActionParam1)
				content := hook.HookActionParam2
				col := cluster.Cluster().Bucket(bucketName).Collection(collectionName)
				_, err := col.Upsert(id, content, &gocb.UpsertOptions{
					Transcoder: gocb.NewRawJSONTranscoder(),
				})
				if err != nil {
					return nil, err
				}
				return nil, nil
			}
		case protoHooksTxns.HookAction_REMOVE_DOC:
			// In format "bucket-name/collection-name/doc-id"
			action = func() (interface{}, error) {
				bucketName, collectionName, id := splitLocation(hook.HookActionParam1)
				col := cluster.Cluster().Bucket(bucketName).Collection(collectionName)
				_, err := col.Remove(id, nil)
				if err != nil {
					return nil, err
				}
				return nil, nil
			}
		case protoHooksTxns.HookAction_RETURN_STRING:
			action = func() (interface{}, error) {
				return hook.HookActionParam1, nil
			}
		case protoHooksTxns.HookAction_BLOCK:
			action = func() (interface{}, error) {
				wait, err := strconv.Atoi(hook.HookActionParam1)
				if err != nil {
					return nil, err
				}

				time.Sleep(time.Duration(wait) * time.Millisecond)

				return nil, nil
			}
		default:
			return nil, fmt.Errorf("cannot handle hook action %s", hook.HookAction)
		}
		switch hook.HookCondition {
		case protoHooksTxns.HookCondition_ON_CALL:
			out = func() (interface{}, error) {
				desiredCalls := hook.HookConditionParam1
				actualCalls := counts.Count(hook.HookPoint)
				if desiredCalls == actualCalls {
					return action()
				}
				return nil, nil
			}
		case protoHooksTxns.HookCondition_ON_CALL_LE:
			out = func() (interface{}, error) {
				desiredCalls := hook.HookConditionParam1
				actualCalls := counts.Count(hook.HookPoint)
				if actualCalls <= desiredCalls {
					return action()
				}
				return nil, nil
			}
		case protoHooksTxns.HookCondition_ON_CALL_GE:
			out = func() (interface{}, error) {
				desiredCalls := hook.HookConditionParam1
				actualCalls := counts.Count(hook.HookPoint)
				if actualCalls >= desiredCalls {
					return action()
				}
				return nil, nil
			}
		case protoHooksTxns.HookCondition_ALWAYS:
			out = func() (interface{}, error) {
				// We do need to add logging at some point so this does need to be a function.
				return action()
			}
		case protoHooksTxns.HookCondition_EQUALS:
			out = func() (interface{}, error) {
				desiredParam := hook.HookConditionParam2
				if docID == desiredParam {
					return action()
				}
				return nil, nil
			}
		case protoHooksTxns.HookCondition_ON_CALL_AND_EQUALS:
			out = func() (interface{}, error) {
				counts.IncrementWithParam(hook.HookPoint, docID)
				desiredCalls := hook.HookConditionParam1
				desiredParam := hook.HookConditionParam2
				actualCalls := counts.CountWithParam(hook.HookPoint, docID)
				if docID == desiredParam && desiredCalls == actualCalls {
					return action()
				}
				return nil, nil
			}
		case protoHooksTxns.HookCondition_WHILE_NOT_EXPIRED:
			out = func() (interface{}, error) {
				if !ctx.Internal().IsExpired() {
					return action()
				}
				return nil, nil
			}
		case protoHooksTxns.HookCondition_WHILE_EXPIRED:
			out = func() (interface{}, error) {
				if ctx.Internal().IsExpired() {
					return action()
				}
				return nil, nil
			}
		default:
			return nil, fmt.Errorf("cannot handle hook condition %s", hook.HookCondition)
		}
		counts.Increment(hook.HookPoint)
		return out()
	}
}
func NewTransactionHooks() *TransactionHookMock {
	return &TransactionHookMock{}
}
func (mock *TransactionHookMock) Configure(conn *cluster.Connection, hooks []*protoHooksTxns.Hook) error {
	counts := callcounts.NewCallCounts()
	var hasExpired bool
	if len(hooks) == 0 {
		// nothing to setup so just return the default handlers
		return nil
	}
	for i := 0; i < len(hooks); i++ {
		hook := hooks[i]
		switch hook.HookPoint {
		case protoHooksTxns.HookPoint_BEFORE_ATR_COMMIT:
			mock.beforeATRCommit = func(ctx gocb.TransactionAttemptContext) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_ATR_COMPLETE:
			mock.beforeATRComplete = func(ctx gocb.TransactionAttemptContext) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_ATR_COMMIT:
			mock.afterATRCommit = func(ctx gocb.TransactionAttemptContext) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_DOC_COMMITTED:
			mock.beforeDocCommitted = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_DOC_ROLLED_BACK:
			mock.beforeDocRolledBack = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_DOC_COMMITTED_BEFORE_SAVING_CAS:
			mock.afterDocCommittedBeforeSavingCAS = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_DOC_COMMITTED:
			mock.afterDocCommitted = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_DOC_REMOVED:
			mock.beforeDocRemoved = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_DOC_REMOVED_PRE_RETRY:
			mock.afterDocRemovedPreRetry = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_DOC_REMOVED_POST_RETRY:
			mock.afterDocRemovedPostRetry = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_DOCS_REMOVED:
			mock.afterDocsRemoved = func(ctx gocb.TransactionAttemptContext) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_ATR_PENDING:
			mock.beforeATRPending = func(ctx gocb.TransactionAttemptContext) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_ATR_PENDING:
			mock.afterATRPending = func(ctx gocb.TransactionAttemptContext) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_ATR_COMPLETE:
			mock.afterATRComplete = func(ctx gocb.TransactionAttemptContext) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_ATR_ROLLED_BACK:
			mock.beforeATRRolledBack = func(ctx gocb.TransactionAttemptContext) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_GET_COMPLETE:
			mock.afterGetComplete = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_ROLLBACK_DELETE_INSERTED:
			mock.beforeRollbackDeleteInserted = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_STAGED_REPLACE_COMPLETE:
			mock.afterStagedReplaceComplete = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_STAGED_REMOVE_COMPLETE:
			mock.afterStagedRemoveComplete = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_STAGED_INSERT:
			mock.beforeStagedInsert = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_STAGED_REMOVE:
			mock.beforeStagedRemove = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_STAGED_REPLACE:
			mock.beforeStagedReplace = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_STAGED_INSERT_COMPLETE:
			mock.afterStagedInsertComplete = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_ATR_ABORTED:
			mock.beforeATRAborted = func(ctx gocb.TransactionAttemptContext) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_ATR_ABORTED:
			mock.afterATRAborted = func(ctx gocb.TransactionAttemptContext) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_ATR_ROLLED_BACK:
			mock.afterATRRolledBack = func(ctx gocb.TransactionAttemptContext) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_ROLLBACK_REPLACE_OR_REMOVE:
			mock.afterRollbackReplaceOrRemove = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_ROLLBACK_DELETE_INSERTED:
			mock.afterRollbackDeleteInserted = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_REMOVING_DOC_DURING_STAGING_INSERT:
			mock.beforeRemovingDocDuringStagedInsert = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_CHECK_ATR_ENTRY_FOR_BLOCKING_DOC:
			mock.beforeCheckATREntryForBlockingDoc = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_DOC_GET:
			mock.beforeDocGet = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_ATR_COMMIT_AMBIGUITY_RESOLUTION:
			mock.beforeATRCommitAmbiguityResolution = func(ctx gocb.TransactionAttemptContext) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_QUERY:
			mock.beforeQuery = func(ctx gocb.TransactionAttemptContext, statement string) error {
				_, err := configureHook(ctx, hook, counts, conn, statement)()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_QUERY:
			mock.afterQuery = func(ctx gocb.TransactionAttemptContext, statement string) error {
				_, err := configureHook(ctx, hook, counts, conn, statement)()
				return err
			}
		case protoHooksTxns.HookPoint_HAS_EXPIRED:
			mock.hasExpiredClientSideHook = func(ctx gocb.TransactionAttemptContext, stage string, docID string) (bool, error) {
				if hasExpired {
					return true, nil
				}
				switch hook.HookCondition {
				case protoHooksTxns.HookCondition_ALWAYS:
					hasExpired = true
					return true, nil
				case protoHooksTxns.HookCondition_EQUALS_BOTH:
					if docID == "" {
						return false, nil
					}
					if stage == hook.HookConditionParam3 && docID == hook.HookConditionParam2 {
						hasExpired = true
						return true, nil
					}
					return false, nil
				case protoHooksTxns.HookCondition_EQUALS:
					if stage == hook.HookConditionParam2 {
						hasExpired = true
						return true, nil
					}
					return false, nil
				default:
					return false, fmt.Errorf("cannot handle hook condition %s", hook.HookCondition)
				}
			}
		case protoHooksTxns.HookPoint_ATR_ID_FOR_VBUCKET:
			mock.randomATRIDForVbucket = func(ctx gocb.TransactionAttemptContext) (string, error) {
				idFace, err := configureHook(ctx, hook, counts, conn, "")()
				if err != nil {
					return "", err
				}
				id, ok := idFace.(string)
				if !ok {
					return "", errors.New("couldn't convert ATR ID to string")
				}
				return id, nil
			}
		case protoHooksTxns.HookPoint_BEFORE_GET_DOC_IN_EXISTS_DURING_STAGED_INSERT:
			mock.beforeGetDocInExistsDuringStagedInsert = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_BEFORE_REMOVE_STAGED_INSERT:
			mock.beforeRemoveStagedInsert = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_AFTER_REMOVE_STAGED_INSERT:
			mock.afterRemoveStagedInsert = func(ctx gocb.TransactionAttemptContext, docID string) error {
				_, err := configureHook(ctx, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_CLEANUP_BEFORE_ATR_REMOVE:
		case protoHooksTxns.HookPoint_CLEANUP_BEFORE_COMMIT_DOC:
		case protoHooksTxns.HookPoint_CLEANUP_BEFORE_DOC_GET:
		case protoHooksTxns.HookPoint_CLEANUP_BEFORE_REMOVE_DOC:
		case protoHooksTxns.HookPoint_CLEANUP_BEFORE_REMOVE_DOC_LINKS:
		case protoHooksTxns.HookPoint_CLEANUP_BEFORE_REMOVE_DOC_STAGED_FOR_REMOVAL:
		// The CLEANUP hooks will be handled by the cleanup
		default:
			return fmt.Errorf("cannot handle hook point %s", hook.HookPoint)
		}
	}
	return nil
}
func NewCleanupHooks() *CleanupHookMock {
	return &CleanupHookMock{}
}

func (mock *CleanupHookMock) Configure(conn *cluster.Connection, hooks []*protoHooksTxns.Hook) error {
	counts := callcounts.NewCallCounts()
	if len(hooks) == 0 {
		// nothing to setup so just return the default handlers
		return nil
	}
	for i := 0; i < len(hooks); i++ {
		hook := hooks[i]
		switch hook.HookPoint {
		case protoHooksTxns.HookPoint_CLEANUP_BEFORE_ATR_REMOVE:
			mock.beforeATRRemove = func(docID string) error {
				_, err := configureHook(gocb.TransactionAttemptContext{}, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_CLEANUP_BEFORE_COMMIT_DOC:
			mock.beforeCommitDoc = func(docID string) error {
				_, err := configureHook(gocb.TransactionAttemptContext{}, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_CLEANUP_BEFORE_DOC_GET:
			mock.beforeDocGet = func(docID string) error {
				_, err := configureHook(gocb.TransactionAttemptContext{}, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_CLEANUP_BEFORE_REMOVE_DOC:
			mock.beforeRemoveDoc = func(docID string) error {
				_, err := configureHook(gocb.TransactionAttemptContext{}, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_CLEANUP_BEFORE_REMOVE_DOC_LINKS:
			mock.beforeRemoveLinks = func(docID string) error {
				_, err := configureHook(gocb.TransactionAttemptContext{}, hook, counts, conn, docID)()
				return err
			}
		case protoHooksTxns.HookPoint_CLEANUP_BEFORE_REMOVE_DOC_STAGED_FOR_REMOVAL:
			mock.beforeRemoveDocStagedForRemoval = func(docID string) error {
				_, err := configureHook(gocb.TransactionAttemptContext{}, hook, counts, conn, docID)()
				return err
			}
		}
	}
	return nil
}
func NewClientRecordHooks() *ClientRecordHookMock {
	return &ClientRecordHookMock{}
}

func (mock *ClientRecordHookMock) Configure(conn *cluster.Connection, hooks []*protoHooksTxns.Hook) error {
	counts := callcounts.NewCallCounts()
	if len(hooks) == 0 {
		// nothing to setup so just return the default handlers
		return nil
	}
	for i := 0; i < len(hooks); i++ {
		hook := hooks[i]
		switch hook.HookPoint {
		case protoHooksTxns.HookPoint_CLIENT_RECORD_BEFORE_CREATE:
			mock.beforeCreateRecord = func() error {
				_, err := configureHook(gocb.TransactionAttemptContext{}, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_CLIENT_RECORD_BEFORE_GET:
			mock.beforeGetRecord = func() error {
				_, err := configureHook(gocb.TransactionAttemptContext{}, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_CLIENT_RECORD_BEFORE_REMOVE_CLIENT:
			mock.beforeRemoveClient = func() error {
				_, err := configureHook(gocb.TransactionAttemptContext{}, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_CLIENT_RECORD_BEFORE_UPDATE:
			mock.beforeUpdateRecord = func() error {
				_, err := configureHook(gocb.TransactionAttemptContext{}, hook, counts, conn, "")()
				return err
			}
		case protoHooksTxns.HookPoint_CLIENT_RECORD_BEFORE_UPDATE_CAS:
			mock.beforeUpdateCAS = func() error {
				_, err := configureHook(gocb.TransactionAttemptContext{}, hook, counts, conn, "")()
				return err
			}
		}
	}
	return nil
}

func splitLocation(location string) (string, string, string) {
	splits := strings.Split(location, "/")
	bucket := splits[0]
	collection := splits[1]
	id := splits[2]
	return bucket, collection, id
}
