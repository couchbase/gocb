package gocb

import (
	"context"
	"encoding/json"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

// LookupInOptions are the set of options available to LookupIn.
type LookupInOptions struct {
	Timeout       time.Duration
	RetryStrategy RetryStrategy
	ParentSpan    RequestSpan

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		DocFlags SubdocDocFlag
		User     string
	}

	noMetrics bool
}

// LookupIn performs a set of subdocument lookup operations on the document identified by id.
func (c *Collection) LookupIn(id string, ops []LookupInSpec, opts *LookupInOptions) (docOut *LookupInResult, errOut error) {
	if opts == nil {
		opts = &LookupInOptions{}
	}

	opm := newKvOpManager(c, "lookup_in", opts.ParentSpan)
	defer opm.Finish(opts.noMetrics)

	opm.SetDocumentID(id)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	return agent.LookupIn(opm, ops, opts.Internal.DocFlags)
}

// StoreSemantics is used to define the document level action to take during a MutateIn operation.
type StoreSemantics uint8

const (
	// StoreSemanticsReplace signifies to Replace the document, and fail if it does not exist.
	// This is the default action
	StoreSemanticsReplace StoreSemantics = iota

	// StoreSemanticsUpsert signifies to replace the document or create it if it doesn't exist.
	StoreSemanticsUpsert

	// StoreSemanticsInsert signifies to create the document, and fail if it exists.
	StoreSemanticsInsert
)

// MutateInOptions are the set of options available to MutateIn.
type MutateInOptions struct {
	Expiry          time.Duration
	Cas             Cas
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	StoreSemantic   StoreSemantics
	Timeout         time.Duration
	RetryStrategy   RetryStrategy
	ParentSpan      RequestSpan
	PreserveExpiry  bool

	// Using a deadlined Context alongside a Timeout will cause the shorter of the two to cause cancellation, this
	// also applies to global level timeouts.
	// UNCOMMITTED: This API may change in the future.
	Context context.Context

	// Internal: This should never be used and is not supported.
	Internal struct {
		DocFlags SubdocDocFlag
		User     string
	}
}

// MutateIn performs a set of subdocument mutations on the document specified by id.
func (c *Collection) MutateIn(id string, ops []MutateInSpec, opts *MutateInOptions) (mutOut *MutateInResult, errOut error) {
	if opts == nil {
		opts = &MutateInOptions{}
	}

	opm := newKvOpManager(c, "mutate_in", opts.ParentSpan)
	defer opm.Finish(false)

	opm.SetDocumentID(id)
	opm.SetRetryStrategy(opts.RetryStrategy)
	opm.SetTimeout(opts.Timeout)
	opm.SetImpersonate(opts.Internal.User)
	opm.SetContext(opts.Context)
	opm.SetPreserveExpiry(opts.PreserveExpiry)
	opm.SetDuraOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)

	opm.SetCas(opts.Cas)
	opm.SetExpiry(opts.Expiry)

	if err := opm.CheckReadyForOp(); err != nil {
		return nil, err
	}

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	return agent.MutateIn(opm, opts.StoreSemantic, ops, opts.Internal.DocFlags)
	// return c.internalMutateIn(opm, opts.StoreSemantic, opts.Expiry, opts.Cas, ops, memd.SubdocDocFlag(opts.Internal.DocFlags))
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
