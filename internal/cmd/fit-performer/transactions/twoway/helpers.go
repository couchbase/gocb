package twoway

import (
	"errors"

	"github.com/couchbase/gocb/v2"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/cluster"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"

	protoTransactions "github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/transactions"
)

func mapFinalErrorCause(err error) protoTransactions.ExternalException {
	if err == nil {
		return protoTransactions.ExternalException_NotSet
	}

	if e := mapSentinelToExternalException(err); e != protoTransactions.ExternalException_Unknown {
		return e
	}

	var cbError *gocb.QueryError
	if errors.As(err, &cbError) {
		return protoTransactions.ExternalException_CouchbaseException
	}

	var tfe *gocb.TransactionFailedError
	if errors.As(err, &tfe) {
		if tfe.Unwrap() == nil {
			return protoTransactions.ExternalException_NotSet
		}
	}

	return protoTransactions.ExternalException_Unknown
}

func mapSentinelToExternalException(err error) protoTransactions.ExternalException {
	if errors.Is(err, gocb.ErrAtrEntryNotFound) {
		return protoTransactions.ExternalException_ActiveTransactionRecordEntryNotFound
	} else if errors.Is(err, gocb.ErrAtrFull) {
		return protoTransactions.ExternalException_ActiveTransactionRecordFull
	} else if errors.Is(err, gocb.ErrAtrNotFound) {
		return protoTransactions.ExternalException_ActiveTransactionRecordNotFound
	} else if errors.Is(err, gocb.ErrDocAlreadyInTransaction) {
		return protoTransactions.ExternalException_DocumentAlreadyInTransaction
	} else if errors.Is(err, gocb.ErrDocumentExists) {
		return protoTransactions.ExternalException_DocumentExistsException
	} else if errors.Is(err, gocb.ErrDocumentNotFound) {
		return protoTransactions.ExternalException_DocumentNotFoundException
	} else if errors.Is(err, gocb.ErrFeatureNotAvailable) {
		return protoTransactions.ExternalException_FeatureNotAvailableException
	} else if errors.Is(err, gocb.ErrPreviousOperationFailed) {
		return protoTransactions.ExternalException_PreviousOperationFailed
	} else if errors.Is(err, gocb.ErrForwardCompatibilityFailure) {
		return protoTransactions.ExternalException_ForwardCompatibilityFailure
	} else if errors.Is(err, gocb.ErrParsingFailure) {
		return protoTransactions.ExternalException_ParsingFailure
	} else if errors.Is(err, gocb.ErrIllegalState) {
		return protoTransactions.ExternalException_IllegalStateException
	} else if errors.Is(err, gocb.ErrServiceNotAvailable) {
		return protoTransactions.ExternalException_ServiceNotAvailableException
	} else if errors.Is(err, gocb.ErrUnambiguousTimeout) {
		return protoTransactions.ExternalException_UnambiguousTimeoutException
	} else if errors.Is(err, gocb.ErrAmbiguousTimeout) {
		return protoTransactions.ExternalException_AmbiguousTimeoutException
	} else if errors.Is(err, gocb.ErrAuthenticationFailure) {
		return protoTransactions.ExternalException_AuthenticationFailureException
	} else if errors.Is(err, gocb.ErrDocumentUnretrievable) {
		return protoTransactions.ExternalException_DocumentUnretrievableException

	} else if errors.Is(err, gocb.ErrCommitNotPermitted) {
		return protoTransactions.ExternalException_CommitNotPermitted
	} else if errors.Is(err, gocb.ErrRollbackNotPermitted) {
		return protoTransactions.ExternalException_RollbackNotPermitted
	} else if errors.Is(err, gocb.ErrConcurrentOperationsDetectedOnSameDocument) {
		return protoTransactions.ExternalException_ConcurrentOperationsDetectedOnSameDocument

	}

	return protoTransactions.ExternalException_Unknown
}

func mapTOFCause(err *gocb.TransactionOperationFailedError) protoTransactions.ExternalException {
	if err == nil {
		return protoTransactions.ExternalException_NotSet
	}
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return protoTransactions.ExternalException_DocumentNotFoundException
	}

	wrapped := err.InternalUnwrap()
	if e := mapSentinelToExternalException(wrapped); e != protoTransactions.ExternalException_Unknown {
		return e
	}

	var cbError *gocb.QueryError
	if errors.As(err, &cbError) {
		return protoTransactions.ExternalException_CouchbaseException
	}

	var tfe *gocb.TransactionFailedError
	if errors.As(err, &tfe) {
		if tfe.Unwrap() == nil {
			return protoTransactions.ExternalException_NotSet
		}
	}

	return protoTransactions.ExternalException_Unknown
}

func txnRaiseToProtocolRaise(raise gocb.TransactionErrorReason) protoTransactions.TransactionException {
	switch raise {
	case gocb.TransactionErrorReasonTransactionFailed:
		return protoTransactions.TransactionException_EXCEPTION_FAILED
	case gocb.TransactionErrorReasonTransactionExpired:
		return protoTransactions.TransactionException_EXCEPTION_EXPIRED
	case gocb.TransactionErrorReasonTransactionCommitAmbiguous:
		return protoTransactions.TransactionException_EXCEPTION_COMMIT_AMBIGUOUS
	case gocb.TransactionErrorReasonTransactionFailedPostCommit:
		return protoTransactions.TransactionException_EXCEPTION_FAILED_POST_COMMIT
	default:
		return protoTransactions.TransactionException_EXCEPTION_UNKNOWN
	}
}

func toGocbTxnBulkGetSpecs(conn *cluster.Connection, cmd *protoTransactions.CommandGetMulti) ([]gocb.TransactionBulkGetSpec, error) {
	var out []gocb.TransactionBulkGetSpec
	for _, protoSpec := range cmd.Specs {
		location, err := helpers.Location(protoSpec.Location, nil)
		if err != nil {
			return nil, err
		}
		spec := gocb.TransactionBulkGetSpec{
			Collection: conn.Collection(location.Bucket(), location.Scope(), location.Collection()),
			ID:         location.ID(),
		}

		if protoSpec.GetTranscoder() != nil {
			transcoder, err := helpers.Transcoder(protoSpec.GetTranscoder())
			if err != nil {
				return nil, err
			}
			spec.Transcoder = transcoder
		}

		out = append(out, spec)
	}
	return out, nil
}

func toGocbTxnBulkGetReplicaSpecs(conn *cluster.Connection, cmd *protoTransactions.CommandGetMulti) ([]gocb.TransactionBulkGetReplicaFromPreferredServerGroupSpec, error) {
	var out []gocb.TransactionBulkGetReplicaFromPreferredServerGroupSpec
	for _, protoSpec := range cmd.Specs {
		location, err := helpers.Location(protoSpec.Location, nil)
		if err != nil {
			return nil, err
		}
		spec := gocb.TransactionBulkGetReplicaFromPreferredServerGroupSpec{
			Collection: conn.Collection(location.Bucket(), location.Scope(), location.Collection()),
			ID:         location.ID(),
		}

		if protoSpec.GetTranscoder() != nil {
			transcoder, err := helpers.Transcoder(protoSpec.GetTranscoder())
			if err != nil {
				return nil, err
			}
			spec.Transcoder = transcoder
		}

		out = append(out, spec)
	}
	return out, nil
}

func toGocbTxnBulkGetOptions(cmd *protoTransactions.CommandGetMulti) *gocb.TransactionBulkGetOptions {
	if cmd.GetOptions() == nil {
		return nil
	}

	var opts gocb.TransactionBulkGetOptions

	switch cmd.GetOptions().GetMode() {
	case protoTransactions.TransactionGetMultiOptions_PRIORITISE_LATENCY:
		opts.Mode = gocb.TransactionBulkGetModePrioritiseLatency
	case protoTransactions.TransactionGetMultiOptions_PRIORITISE_READ_SKEW_DETECTION:
		opts.Mode = gocb.TransactionBulkGetModePrioritiseReadSkewDetection
	case protoTransactions.TransactionGetMultiOptions_DISABLE_READ_SKEW_DETECTION:
		opts.Mode = gocb.TransactionBulkGetModeDisableReadSkewDetection
	}

	return &opts
}

func toGocbTxnBulkGetReplicaOptions(cmd *protoTransactions.CommandGetMulti) *gocb.TransactionBulkGetReplicaFromPreferredServerGroupOptions {
	if cmd.GetOptions() == nil {
		return nil
	}

	var opts gocb.TransactionBulkGetReplicaFromPreferredServerGroupOptions

	switch cmd.GetOptions().GetMode() {
	case protoTransactions.TransactionGetMultiOptions_PRIORITISE_LATENCY:
		opts.Mode = gocb.TransactionBulkGetReplicaFromPreferredServerGroupModePrioritiseLatency
	case protoTransactions.TransactionGetMultiOptions_PRIORITISE_READ_SKEW_DETECTION:
		opts.Mode = gocb.TransactionBulkGetReplicaFromPreferredServerGroupModePrioritiseReadSkewDetection
	case protoTransactions.TransactionGetMultiOptions_DISABLE_READ_SKEW_DETECTION:
		opts.Mode = gocb.TransactionBulkGetReplicaFromPreferredServerGroupModeDisableReadSkewDetection
	}

	return &opts
}

func toGocbTxnReplaceOptions(cmd *protoTransactions.CommandReplace) (*gocb.TransactionReplaceOptions, error) {
	if cmd.GetOptions() == nil {
		return nil, nil
	}

	opts := &gocb.TransactionReplaceOptions{}
	if cmd.GetOptions().GetTranscoder() != nil {
		transcoder, err := helpers.Transcoder(cmd.GetOptions().GetTranscoder())
		if err != nil {
			return nil, err
		}
		opts.Transcoder = transcoder
	}

	return opts, nil
}

func toGocbTxnInsertOptions(cmd *protoTransactions.CommandInsert) (*gocb.TransactionInsertOptions, error) {
	if cmd.GetOptions() == nil {
		return nil, nil
	}

	opts := &gocb.TransactionInsertOptions{}
	if cmd.GetOptions().GetTranscoder() != nil {
		transcoder, err := helpers.Transcoder(cmd.GetOptions().GetTranscoder())
		if err != nil {
			return nil, err
		}
		opts.Transcoder = transcoder
	}

	return opts, nil
}
