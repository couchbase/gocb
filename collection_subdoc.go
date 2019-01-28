package gocb

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/opentracing/opentracing-go"
	"gopkg.in/couchbase/gocbcore.v8"
)

// LookupInSpec provides a way to create LookupInOps.
type LookupInSpec struct {
}

// LookupInOp is the representation of an operation available when calling LookupIn
type LookupInOp struct {
	op gocbcore.SubDocOp
}

// LookupInOptions are the set of options available to LookupIn.
type LookupInOptions struct {
	Context           context.Context
	Timeout           time.Duration
	ParentSpanContext opentracing.SpanContext
	WithExpiry        bool
}

// Get indicates a path to be retrieved from the document.  The value of the path
// can later be retrieved from the LookupResult.
// The path syntax follows N1QL's path syntax (e.g. `foo.bar.baz`).
func (spec LookupInSpec) Get(path string) LookupInOp {
	return spec.getWithFlags(path, SubdocFlagNone)
}

func (spec LookupInSpec) getWithFlags(path string, flags SubdocFlag) LookupInOp {
	var op gocbcore.SubDocOp
	if path == "" {
		op = gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpGetDoc,
			Flags: gocbcore.SubdocFlag(flags),
		}
	} else {
		op = gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpGet,
			Path:  path,
			Flags: gocbcore.SubdocFlag(flags),
		}
	}

	return LookupInOp{op: op}
}

// Exists is similar to Path(), but does not actually retrieve the value from the server.
// This may save bandwidth if you only need to check for the existence of a
// path (without caring for its content). You can check the status of this
// operation by using .ContentAt (and ignoring the value) or .Exists() on the LookupResult.
func (spec LookupInSpec) Exists(path string) LookupInOp {
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpExists,
		Path:  path,
		Flags: gocbcore.SubdocFlagNone,
	}

	return LookupInOp{op: op}
}

// Count allows you to retrieve the number of items in an array or keys within an
// dictionary within an element of a document.
func (spec LookupInSpec) Count(path string) LookupInOp {
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpGetCount,
		Path:  path,
		Flags: gocbcore.SubdocFlagNone,
	}

	return LookupInOp{op: op}
}

// XAttr indicates an extended attribute to be retrieved from the document.  The value of the path
// can later be retrieved from the LookupResult.
// The path syntax follows N1QL's path syntax (e.g. `foo.bar.baz`).
func (spec LookupInSpec) XAttr(path string) LookupInOp {
	return spec.getWithFlags(path, SubdocFlagXattr)
}

// LookupIn performs a set of subdocument lookup operations on the document identified by key.
func (c *Collection) LookupIn(key string, ops []LookupInOp, opts *LookupInOptions) (docOut *LookupInResult, errOut error) {
	if opts == nil {
		opts = &LookupInOptions{}
	}

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	d := c.deadline(ctx, time.Now(), opts.Timeout)
	if currentD, ok := ctx.Deadline(); ok {
		if d.Before(currentD) {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, d)
			defer cancel()
		}
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, d)
		defer cancel()
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "LookupIn")
	defer span.Finish()

	res, err := c.lookupIn(ctx, span.Context(), key, ops, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) lookupIn(ctx context.Context, traceCtx opentracing.SpanContext, key string, ops []LookupInOp, opts LookupInOptions) (docOut *LookupInResult, errOut error) {
	span := c.startKvOpTrace(traceCtx, "lookupIn")
	defer span.Finish()

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	var subdocs []gocbcore.SubDocOp
	for _, op := range ops {
		subdocs = append(subdocs, op.op)
	}

	// Prepend the expiry get if required, xattrs have to be at the front of the ops list.
	if opts.WithExpiry {
		op := gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpGet,
			Path:  "$document.exptime",
			Flags: gocbcore.SubdocFlag(SubdocFlagXattr),
		}

		subdocs = append([]gocbcore.SubDocOp{op}, subdocs...)
	}

	if len(ops) > 16 {
		return nil, errors.New("too many lookupIn ops specified, maximum 16")
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.LookupInEx(gocbcore.LookupInOptions{
		Key:            []byte(key),
		Ops:            subdocs,
		TraceContext:   traceCtx,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.LookupInResult, err error) {
		if err != nil && !gocbcore.IsErrorStatus(err, gocbcore.StatusSubDocBadMulti) {
			errOut = maybeEnhanceErr(err, key)
			ctrl.resolve()
			return
		}

		if res != nil {
			resSet := &LookupInResult{}
			resSet.cas = Cas(res.Cas)
			resSet.contents = make([]lookupInPartial, len(subdocs))

			for i, opRes := range res.Ops {
				// resSet.contents[i].path = opts.spec.ops[i].Path
				resSet.contents[i].err = maybeEnhanceErr(opRes.Err, key)
				if opRes.Value != nil {
					resSet.contents[i].data = append([]byte(nil), opRes.Value...)
				}
			}

			if opts.WithExpiry {
				// if expiry was requested then extract and remove it from the results
				resSet.withExpiration = true
				err = resSet.ContentAt(0, &resSet.expiration)
				if err != nil {
					errOut = err
					ctrl.resolve()
					return
				}
				resSet.contents = resSet.contents[1:]
			}

			docOut = resSet
		}

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// MutateInSpec provides a way to create MutateInOps.
type MutateInSpec struct {
}

// MutateInOp is the representation of an operation available when calling MutateIn
type MutateInOp struct {
	op gocbcore.SubDocOp
}

// MutateInOptions are the set of options available to MutateIn.
type MutateInOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	Expiration        uint32
	Cas               Cas
	PersistTo         uint
	ReplicateTo       uint
	DurabilityLevel   DurabilityLevel
	CreateDocument    bool
}

func (spec *MutateInSpec) marshalValue(value interface{}) ([]byte, error) {
	if val, ok := value.([]byte); ok {
		return val, nil
	}

	if val, ok := value.(*[]byte); ok {
		return *val, nil
	}

	bytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// MutateInSpecInsertOptions are the options available to subdocument Insert operations.
type MutateInSpecInsertOptions struct {
	CreateParents bool
	IsXattr       bool
}

// Insert inserts a value at the specified path within the document, optionally creating the document first.
func (spec MutateInSpec) Insert(path string, val interface{}, opts *MutateInSpecInsertOptions) (*MutateInOp, error) {
	if opts == nil {
		opts = &MutateInSpecInsertOptions{}
	}
	var flags SubdocFlag
	if opts.CreateParents {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	var op gocbcore.SubDocOp
	marshaled, err := spec.marshalValue(val)
	if err != nil {
		return nil, err
	}
	if path == "" {
		op = gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpAddDoc,
			Flags: gocbcore.SubdocFlag(flags),
			Value: marshaled,
		}
	} else {
		op = gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpDictAdd,
			Path:  path,
			Flags: gocbcore.SubdocFlag(flags),
			Value: marshaled,
		}
	}

	return &MutateInOp{op: op}, nil
}

// MutateInSpecUpsertOptions are the options available to subdocument Upsert operations.
type MutateInSpecUpsertOptions struct {
	CreateParents bool
	IsXattr       bool
}

// Upsert creates a new value at the specified path within the document if it does not exist, if it does exist then it
// updates it.
func (spec MutateInSpec) Upsert(path string, val interface{}, opts *MutateInSpecUpsertOptions) (*MutateInOp, error) {
	if opts == nil {
		opts = &MutateInSpecUpsertOptions{}
	}
	var flags SubdocFlag
	if opts.CreateParents {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	var op gocbcore.SubDocOp
	marshaled, err := spec.marshalValue(val)
	if err != nil {
		return nil, err
	}
	if path == "" {
		op = gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpSetDoc,
			Flags: gocbcore.SubdocFlag(flags),
			Value: marshaled,
		}
	} else {
		op = gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpDictSet,
			Path:  path,
			Flags: gocbcore.SubdocFlag(flags),
			Value: marshaled,
		}
	}

	return &MutateInOp{op: op}, nil
}

// MutateInSpecReplaceOptions are the options available to subdocument Replace operations.
type MutateInSpecReplaceOptions struct {
	IsXattr bool
}

// Replace replaces the value of the field at path.
func (spec MutateInSpec) Replace(path string, val interface{}, opts *MutateInSpecReplaceOptions) (*MutateInOp, error) {
	if opts == nil {
		opts = &MutateInSpecReplaceOptions{}
	}
	var flags SubdocFlag
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	marshaled, err := spec.marshalValue(val)
	if err != nil {
		return nil, err
	}
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpReplace,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: marshaled,
	}

	return &MutateInOp{op: op}, nil
}

// MutateInSpecRemoveOptions are the options available to subdocument Remove operations.
type MutateInSpecRemoveOptions struct {
	IsXattr bool
}

// Remove removes the field at path.
func (spec MutateInSpec) Remove(path string, opts *MutateInSpecRemoveOptions) (*MutateInOp, error) {
	if opts == nil {
		opts = &MutateInSpecRemoveOptions{}
	}
	var flags SubdocFlag
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	var op gocbcore.SubDocOp
	if path == "" {
		op = gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpDeleteDoc,
			Flags: gocbcore.SubdocFlag(flags),
		}
	} else {
		op = gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpDelete,
			Path:  path,
			Flags: gocbcore.SubdocFlag(flags),
		}
	}

	return &MutateInOp{op: op}, nil
}

// MutateInSpecArrayAppendOptions are the options available to subdocument ArrayAppend operations.
type MutateInSpecArrayAppendOptions struct {
	CreateParents bool
	IsXattr       bool
}

// ArrayAppend adds an element to the end (i.e. right) of an array
func (spec MutateInSpec) ArrayAppend(path string, bytes []byte, opts *MutateInSpecArrayAppendOptions) (*MutateInOp, error) {
	if opts == nil {
		opts = &MutateInSpecArrayAppendOptions{}
	}
	var flags SubdocFlag
	if opts.CreateParents {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpArrayPushLast,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: bytes,
	}

	return &MutateInOp{op: op}, nil
}

// MutateInSpecArrayPrependOptions are the options available to subdocument ArrayPrepend operations.
type MutateInSpecArrayPrependOptions struct {
	CreateParents bool
	IsXattr       bool
}

// ArrayPrepend adds an element to the beginning (i.e. left) of an array
func (spec MutateInSpec) ArrayPrepend(path string, bytes []byte, opts *MutateInSpecArrayPrependOptions) (*MutateInOp, error) {
	if opts == nil {
		opts = &MutateInSpecArrayPrependOptions{}
	}
	var flags SubdocFlag
	if opts.CreateParents {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpArrayPushFirst,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: bytes,
	}

	return &MutateInOp{op: op}, nil
}

// MutateInSpecArrayInsertOptions are the options available to subdocument ArrayInsert operations.
type MutateInSpecArrayInsertOptions struct {
	CreateParents bool
	IsXattr       bool
}

// ArrayInsert inserts an element at a given position within an array. The position should be
// specified as part of the path, e.g. path.to.array[3]
func (spec MutateInSpec) ArrayInsert(path string, bytes []byte, opts *MutateInSpecArrayInsertOptions) (*MutateInOp, error) {
	if opts == nil {
		opts = &MutateInSpecArrayInsertOptions{}
	}
	var flags SubdocFlag
	if opts.CreateParents {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpArrayInsert,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: bytes,
	}

	return &MutateInOp{op: op}, nil
}

// MutateInSpecArrayAddUniqueOptions are the options available to subdocument ArrayAddUnique operations.
type MutateInSpecArrayAddUniqueOptions struct {
	CreateParents bool
	IsXattr       bool
}

// ArrayAddUnique adds an dictionary add unique operation to this mutation operation set.
func (spec MutateInSpec) ArrayAddUnique(path string, bytes []byte, opts *MutateInSpecArrayAddUniqueOptions) (*MutateInOp, error) {
	if opts == nil {
		opts = &MutateInSpecArrayAddUniqueOptions{}
	}
	var flags SubdocFlag
	if opts.CreateParents {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpArrayAddUnique,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: bytes,
	}

	return &MutateInOp{op: op}, nil
}

// MutateInSpecCounterOptions are the options available to subdocument Increment and Decrement operations.
type MutateInSpecCounterOptions struct {
	CreateParents bool
	IsXattr       bool
}

// Increment adds an increment operation to this mutation operation set.
func (spec MutateInSpec) Increment(path string, delta int64, opts *MutateInSpecCounterOptions) (*MutateInOp, error) {
	if opts == nil {
		opts = &MutateInSpecCounterOptions{}
	}
	var flags SubdocFlag
	if opts.CreateParents {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	marshaled, err := spec.marshalValue(delta)
	if err != nil {
		return nil, err
	}
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpCounter,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: marshaled,
	}

	return &MutateInOp{op: op}, nil
}

// Decrement adds a decrement operation to this mutation operation set.
func (spec MutateInSpec) Decrement(path string, delta int64, opts *MutateInSpecCounterOptions) (*MutateInOp, error) {
	if opts == nil {
		opts = &MutateInSpecCounterOptions{}
	}
	var flags SubdocFlag
	if opts.CreateParents {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	marshaled, err := spec.marshalValue(-delta)
	if err != nil {
		return nil, err
	}
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpCounter,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: marshaled,
	}

	return &MutateInOp{op: op}, nil
}

// Mutate performs a set of subdocument mutations on the document specified by key.
func (c *Collection) Mutate(key string, ops []MutateInOp, opts *MutateInOptions) (mutOut *MutateInResult, errOut error) {
	if opts == nil {
		opts = &MutateInOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "MutateIn")
	defer span.Finish()

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	d := c.deadline(ctx, time.Now(), opts.Timeout)
	if currentD, ok := ctx.Deadline(); ok {
		if d.Before(currentD) {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, d)
			defer cancel()
		}
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, d)
		defer cancel()
	}

	res, err := c.mutate(ctx, span.Context(), key, ops, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(durabilitySettings{
		ctx:            opts.Context,
		tracectx:       span.Context(),
		key:            key,
		cas:            res.Cas(),
		mt:             res.MutationToken(),
		replicaTo:      opts.ReplicateTo,
		persistTo:      opts.PersistTo,
		forDelete:      false,
		scopeName:      c.scopeName(),
		collectionName: c.name(),
	})
}

func (c *Collection) mutate(ctx context.Context, traceCtx opentracing.SpanContext, key string, ops []MutateInOp, opts MutateInOptions) (mutOut *MutateInResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	var flags SubdocDocFlag
	if opts.CreateDocument {
		flags |= SubdocDocFlagMkDoc
	}

	var subdocs []gocbcore.SubDocOp
	for _, op := range ops {
		subdocs = append(subdocs, op.op)
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.MutateInEx(gocbcore.MutateInOptions{
		Key:            []byte(key),
		Flags:          gocbcore.SubdocDocFlag(flags),
		Cas:            gocbcore.Cas(opts.Cas),
		Ops:            subdocs,
		TraceContext:   traceCtx,
		Expiry:         opts.Expiration,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.MutateInResult, err error) {
		if err != nil {
			errOut = maybeEnhanceErr(err, key)
			ctrl.resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.sb.BucketName,
		}
		mutRes := &MutateInResult{
			MutationResult: MutationResult{
				mt: mutTok,
				Result: Result{
					cas: Cas(res.Cas),
				},
			},
			contents: make([]mutateInPartial, len(res.Ops)),
		}

		for i, op := range res.Ops {
			mutRes.contents[i] = mutateInPartial{data: op.Value}
		}

		mutOut = mutRes

		ctrl.resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}
