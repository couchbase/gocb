package gocb

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
)

func (p *kvProviderCore) LookupIn(c *Collection, id string, ops []LookupInSpec, opts *LookupInOptions) (docOut *LookupInResult, errOut error) {
	opm := newKvOpManagerCore(c, "lookup_in", opts.ParentSpan, p)
	defer opm.Finish(opts.noMetrics)

	opm.SetDocumentID(id)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	return p.internalLookupIn(opm, ops, memd.SubdocDocFlag(opts.Internal.DocFlags))
}

func (p *kvProviderCore) internalLookupIn(
	opm *kvOpManagerCore,
	ops []LookupInSpec,
	flags memd.SubdocDocFlag,
) (*LookupInResult, error) {
	var subdocs []gocbcore.SubDocOp
	for _, op := range ops {
		if op.op == memd.SubDocOpGet && op.path == "" {
			if op.isXattr {
				return nil, errors.New("invalid xattr fetch with no path")
			}

			subdocs = append(subdocs, gocbcore.SubDocOp{
				Op:    memd.SubDocOpGetDoc,
				Flags: memd.SubdocFlag(SubdocFlagNone),
			})
			continue
		} else if op.op == memd.SubDocOpDictSet && op.path == "" {
			if op.isXattr {
				return nil, errors.New("invalid xattr set with no path")
			}

			subdocs = append(subdocs, gocbcore.SubDocOp{
				Op:    memd.SubDocOpSetDoc,
				Flags: memd.SubdocFlag(SubdocFlagNone),
			})
			continue
		}

		flags := memd.SubdocFlagNone
		if op.isXattr {
			flags |= memd.SubdocFlagXattrPath
		}

		subdocs = append(subdocs, gocbcore.SubDocOp{
			Op:    op.op,
			Path:  op.path,
			Flags: flags,
		})
	}

	var docOut *LookupInResult
	var errOut error
	err := opm.Wait(p.agent.LookupIn(gocbcore.LookupInOptions{
		Key:            opm.DocumentID(),
		Ops:            subdocs,
		CollectionName: opm.CollectionName(),
		ScopeName:      opm.ScopeName(),
		RetryStrategy:  opm.RetryStrategy(),
		TraceContext:   opm.TraceSpanContext(),
		Deadline:       opm.Deadline(),
		Flags:          flags,
		User:           opm.Impersonate(),
	}, func(res *gocbcore.LookupInResult, err error) {
		if err != nil && res == nil {
			errOut = opm.EnhanceErr(err)
		}

		if res != nil {
			docOut = &LookupInResult{}
			docOut.cas = Cas(res.Cas)
			docOut.contents = make([]lookupInPartial, len(subdocs))
			for i, opRes := range res.Ops {
				docOut.contents[i].err = opm.EnhanceErr(opRes.Err)
				docOut.contents[i].data = opRes.Value
			}
		}

		if err == nil {
			opm.Resolve(nil)
		} else {
			opm.Reject()
		}
	}))
	if err != nil {
		errOut = err
	}
	return docOut, errOut
}

func (p *kvProviderCore) MutateIn(c *Collection, id string, ops []MutateInSpec, opts *MutateInOptions) (mutOut *MutateInResult, errOut error) {
	opm := newKvOpManagerCore(c, "mutate_in", opts.ParentSpan, p)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)
	opm.SetPreserveExpiry(opts.PreserveExpiry)
	opm.SetDuraOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	return p.internalMutateIn(opm, opts.StoreSemantic, opts.Expiry, opts.Cas, ops, memd.SubdocDocFlag(opts.Internal.DocFlags))
}

func (p *kvProviderCore) internalMutateIn(
	opm *kvOpManagerCore,
	action StoreSemantics,
	expiry time.Duration,
	cas Cas,
	ops []MutateInSpec,
	docFlags memd.SubdocDocFlag,
) (*MutateInResult, error) {
	preserveTTL := opm.PreserveExpiry()
	if action == StoreSemanticsReplace {
		// this is the default behaviour
		if expiry > 0 && preserveTTL {
			return nil, makeInvalidArgumentsError("cannot use preserve ttl with expiry for replace store semantics")
		}
	} else if action == StoreSemanticsUpsert {
		docFlags |= memd.SubdocDocFlagMkDoc
	} else if action == StoreSemanticsInsert {
		if preserveTTL {
			return nil, makeInvalidArgumentsError("cannot use preserve ttl with insert store semantics")
		}
		docFlags |= memd.SubdocDocFlagAddDoc
	} else {
		return nil, makeInvalidArgumentsError("invalid StoreSemantics value provided")
	}

	var subdocs []gocbcore.SubDocOp
	for _, op := range ops {
		if op.path == "" {
			switch op.op {
			case memd.SubDocOpDictAdd:
				return nil, makeInvalidArgumentsError("cannot specify a blank path with InsertSpec")
			case memd.SubDocOpDictSet:
				return nil, makeInvalidArgumentsError("cannot specify a blank path with UpsertSpec")
			case memd.SubDocOpDelete:
				op.op = memd.SubDocOpDeleteDoc
			case memd.SubDocOpReplace:
				op.op = memd.SubDocOpSetDoc
			default:
			}
		}

		etrace := opm.parent.startKvOpTrace("request_encoding", opm.TraceSpanContext(), true)
		bytes, flags, err := jsonMarshalMutateSpec(op)
		etrace.End()
		if err != nil {
			return nil, err
		}

		if op.createPath {
			flags |= memd.SubdocFlagMkDirP
		}

		if op.isXattr {
			flags |= memd.SubdocFlagXattrPath
		}

		subdocs = append(subdocs, gocbcore.SubDocOp{
			Op:    op.op,
			Flags: flags,
			Path:  op.path,
			Value: bytes,
		})
	}

	var mutOut *MutateInResult
	var errOut error
	err := opm.Wait(p.agent.MutateIn(gocbcore.MutateInOptions{
		Key:                    opm.DocumentID(),
		Flags:                  docFlags,
		Cas:                    gocbcore.Cas(cas),
		Ops:                    subdocs,
		Expiry:                 durationToExpiry(expiry),
		CollectionName:         opm.CollectionName(),
		ScopeName:              opm.ScopeName(),
		DurabilityLevel:        opm.DurabilityLevel(),
		DurabilityLevelTimeout: opm.DurabilityTimeout(),
		RetryStrategy:          opm.RetryStrategy(),
		TraceContext:           opm.TraceSpanContext(),
		Deadline:               opm.Deadline(),
		User:                   opm.Impersonate(),
		PreserveExpiry:         preserveTTL,
	}, func(res *gocbcore.MutateInResult, err error) {
		if err != nil {
			// GOCBC-1019: Due to a previous bug in gocbcore we need to convert cas mismatch back to exists.
			if kvErr, ok := err.(*gocbcore.KeyValueError); ok {
				if errors.Is(kvErr.InnerError, ErrCasMismatch) {
					kvErr.InnerError = ErrDocumentExists
				}
			}
			errOut = opm.EnhanceErr(err)
			opm.Reject()
			return
		}

		mutOut = &MutateInResult{}
		mutOut.cas = Cas(res.Cas)
		mutOut.mt = opm.EnhanceMt(res.MutationToken)
		mutOut.contents = make([]mutateInPartial, len(res.Ops))
		for i, op := range res.Ops {
			mutOut.contents[i] = mutateInPartial{data: op.Value}
		}

		opm.Resolve(mutOut.mt)
	}))
	if err != nil {
		errOut = err
	}
	return mutOut, errOut
}

func jsonMarshalMultiArray(in interface{}) ([]byte, error) {
	out, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}

	// Assert first character is a '['
	if len(out) < 2 || out[0] != '[' {
		return nil, makeInvalidArgumentsError("not a JSON array")
	}

	out = out[1 : len(out)-1]
	return out, nil
}

func jsonMarshalMutateSpec(op MutateInSpec) ([]byte, memd.SubdocFlag, error) {
	if op.value == nil {
		// If the mutation is to write, then this is a json `null` value
		switch op.op {
		case memd.SubDocOpDictAdd,
			memd.SubDocOpDictSet,
			memd.SubDocOpReplace,
			memd.SubDocOpArrayPushLast,
			memd.SubDocOpArrayPushFirst,
			memd.SubDocOpArrayInsert,
			memd.SubDocOpArrayAddUnique,
			memd.SubDocOpSetDoc,
			memd.SubDocOpAddDoc:
			return []byte("null"), memd.SubdocFlagNone, nil
		}

		return nil, memd.SubdocFlagNone, nil
	}

	if macro, ok := op.value.(MutationMacro); ok {
		return []byte(macro), memd.SubdocFlagExpandMacros | memd.SubdocFlagXattrPath, nil
	}

	if op.multiValue {
		bytes, err := jsonMarshalMultiArray(op.value)
		return bytes, memd.SubdocFlagNone, err
	}

	bytes, err := json.Marshal(op.value)
	return bytes, memd.SubdocFlagNone, err
}
