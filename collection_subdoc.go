package gocb

import (
	"context"
	"errors"
	"time"

	"github.com/couchbase/gocbcore/v8"
	"github.com/opentracing/opentracing-go"
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

// LookupInSpecGetOptions are the options available to LookupIn subdoc Get operations.
type LookupInSpecGetOptions struct {
	IsXattr bool
}

// Get indicates a path to be retrieved from the document.  The value of the path
// can later be retrieved from the LookupResult.
// The path syntax follows N1QL's path syntax (e.g. `foo.bar.baz`).
func (spec LookupInSpec) Get(path string, opts *LookupInSpecGetOptions) LookupInOp {
	if opts == nil {
		opts = &LookupInSpecGetOptions{}
	}
	return spec.getWithFlags(path, opts.IsXattr)
}

// LookupInSpecGetFullOptions are the options available to LookupIn subdoc GetFull operations.
// There are currently no options and this is left empty for future extensibility.
type LookupInSpecGetFullOptions struct {
}

// GetFull indicates that a full document should be retrieved. This command allows you
// to do things like combine with Get to fetch a document with certain Xattrs
func (spec LookupInSpec) GetFull(opts *LookupInSpecGetFullOptions) LookupInOp {
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpGetDoc,
		Flags: gocbcore.SubdocFlag(SubdocFlagNone),
	}

	return LookupInOp{op: op}
}

func (spec LookupInSpec) getWithFlags(path string, isXattr bool) LookupInOp {
	var flags gocbcore.SubdocFlag
	if isXattr {
		flags |= gocbcore.SubdocFlag(SubdocFlagXattr)
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpGet,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
	}

	return LookupInOp{op: op}
}

// LookupInSpecExistsOptions are the options available to LookupIn subdoc Exists operations.
type LookupInSpecExistsOptions struct {
	IsXattr bool
}

// Exists is similar to Path(), but does not actually retrieve the value from the server.
// This may save bandwidth if you only need to check for the existence of a
// path (without caring for its content). You can check the status of this
// operation by using .ContentAt (and ignoring the value) or .Exists() on the LookupResult.
func (spec LookupInSpec) Exists(path string, opts *LookupInSpecExistsOptions) LookupInOp {
	if opts == nil {
		opts = &LookupInSpecExistsOptions{}
	}

	var flags gocbcore.SubdocFlag
	if opts.IsXattr {
		flags |= gocbcore.SubdocFlag(SubdocFlagXattr)
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpExists,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
	}

	return LookupInOp{op: op}
}

// LookupInSpecCountOptions are the options available to LookupIn subdoc Count operations.
type LookupInSpecCountOptions struct {
	IsXattr bool
}

// Count allows you to retrieve the number of items in an array or keys within an
// dictionary within an element of a document.
func (spec LookupInSpec) Count(path string, opts *LookupInSpecCountOptions) LookupInOp {
	if opts == nil {
		opts = &LookupInSpecCountOptions{}
	}

	var flags gocbcore.SubdocFlag
	if opts.IsXattr {
		flags |= gocbcore.SubdocFlag(SubdocFlagXattr)
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpGetCount,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
	}

	return LookupInOp{op: op}
}

// LookupIn performs a set of subdocument lookup operations on the document identified by key.
func (c *Collection) LookupIn(key string, ops []LookupInOp, opts *LookupInOptions) (docOut *LookupInResult, errOut error) {
	if opts == nil {
		opts = &LookupInOptions{}
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
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
	op  gocbcore.SubDocOp
	err error
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
	InsertDocument    bool
	UpsertDocument    bool
	// Internal: This should never be used and is not supported.
	AccessDeleted bool
}

func (spec *MutateInSpec) encodeMultiArray(in interface{}, encoder Encode) ([]byte, error) {
	out, _, err := encoder(in)
	if err != nil {
		return nil, err
	}

	// Assert first character is a '['
	if len(out) < 2 || out[0] != '[' {
		return nil, errors.New("not a JSON array")
	}

	out = out[1 : len(out)-1]
	return out, nil
}

// MutateInSpecInsertOptions are the options available to subdocument Insert operations.
type MutateInSpecInsertOptions struct {
	CreatePath bool
	IsXattr    bool
	Encoder    Encode
}

// Insert inserts a value at the specified path within the document.
func (spec MutateInSpec) Insert(path string, val interface{}, opts *MutateInSpecInsertOptions) MutateInOp {
	if opts == nil {
		opts = &MutateInSpecInsertOptions{}
	}
	var flags SubdocFlag
	_, ok := val.(MutationMacro)
	if ok {
		flags |= SubdocFlagUseMacros
		opts.IsXattr = true
	}

	if opts.CreatePath {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	encoder := opts.Encoder
	if opts.Encoder == nil {
		encoder = JSONEncode
	}

	marshaled, _, err := encoder(val)
	if err != nil {
		return MutateInOp{err: err}
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpDictAdd,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: marshaled,
	}

	return MutateInOp{op: op}
}

// MutateInSpecUpsertOptions are the options available to subdocument Upsert operations.
type MutateInSpecUpsertOptions struct {
	CreatePath bool
	IsXattr    bool
	Encoder    Encode
}

// Upsert creates a new value at the specified path within the document if it does not exist, if it does exist then it
// updates it.
func (spec MutateInSpec) Upsert(path string, val interface{}, opts *MutateInSpecUpsertOptions) MutateInOp {
	if opts == nil {
		opts = &MutateInSpecUpsertOptions{}
	}
	var flags SubdocFlag
	_, ok := val.(MutationMacro)
	if ok {
		flags |= SubdocFlagUseMacros
		opts.IsXattr = true
	}

	if opts.CreatePath {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	encoder := opts.Encoder
	if opts.Encoder == nil {
		encoder = JSONEncode
	}

	marshaled, _, err := encoder(val)
	if err != nil {
		return MutateInOp{err: err}
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpDictSet,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: marshaled,
	}

	return MutateInOp{op: op}
}

// MutateInSpecUpsertFullOptions are the options available to subdocument UpsertFull operations.
type MutateInSpecUpsertFullOptions struct {
	Encoder Encode
}

// UpsertFull creates a new document if it does not exist, if it does exist then it
// updates it. This command allows you to do things like updating xattrs whilst upserting
// a document.
func (spec MutateInSpec) UpsertFull(val interface{}, opts *MutateInSpecUpsertFullOptions) MutateInOp {
	if opts == nil {
		opts = &MutateInSpecUpsertFullOptions{}
	}

	encoder := opts.Encoder
	if opts.Encoder == nil {
		encoder = JSONEncode
	}

	marshaled, _, err := encoder(val)
	if err != nil {
		return MutateInOp{err: err}
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpSetDoc,
		Flags: gocbcore.SubdocFlag(SubdocFlagNone),
		Value: marshaled,
	}

	return MutateInOp{op: op}
}

// MutateInSpecReplaceOptions are the options available to subdocument Replace operations.
type MutateInSpecReplaceOptions struct {
	IsXattr bool
	Encoder Encode
}

// Replace replaces the value of the field at path.
func (spec MutateInSpec) Replace(path string, val interface{}, opts *MutateInSpecReplaceOptions) MutateInOp {
	if opts == nil {
		opts = &MutateInSpecReplaceOptions{}
	}
	var flags SubdocFlag
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	encoder := opts.Encoder
	if opts.Encoder == nil {
		encoder = JSONEncode
	}

	marshaled, _, err := encoder(val)
	if err != nil {
		return MutateInOp{err: err}
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpReplace,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: marshaled,
	}

	return MutateInOp{op: op}
}

// MutateInSpecRemoveOptions are the options available to subdocument Remove operations.
type MutateInSpecRemoveOptions struct {
	IsXattr bool
}

// Remove removes the field at path.
func (spec MutateInSpec) Remove(path string, opts *MutateInSpecRemoveOptions) MutateInOp {
	if opts == nil {
		opts = &MutateInSpecRemoveOptions{}
	}
	var flags SubdocFlag
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpDelete,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
	}

	return MutateInOp{op: op}
}

// RemoveFull removes the full document, including metadata.
func (spec MutateInSpec) RemoveFull() (*MutateInOp, error) {
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpDeleteDoc,
		Flags: gocbcore.SubdocFlag(SubdocFlagNone),
	}

	return &MutateInOp{op: op}, nil
}

// MutateInSpecArrayAppendOptions are the options available to subdocument ArrayAppend operations.
type MutateInSpecArrayAppendOptions struct {
	CreatePath bool
	IsXattr    bool
	// HasMultiple adds multiple values as elements to an array.
	// When used `value` in the spec must be an array type
	// ArrayAppend("path", []int{1,2,3,4}, MutateInSpecArrayAppendOptions{HasMultiple:true}) =>
	//   "path" [..., 1,2,3,4]
	//
	// This is a more efficient version (at both the network and server levels)
	// of doing
	// spec.ArrayAppend("path", 1, nil)
	// spec.ArrayAppend("path", 2, nil)
	// spec.ArrayAppend("path", 3, nil)
	HasMultiple bool
	Encoder     Encode
}

// ArrayAppend adds an element(s) to the end (i.e. right) of an array
func (spec MutateInSpec) ArrayAppend(path string, val interface{}, opts *MutateInSpecArrayAppendOptions) MutateInOp {
	if opts == nil {
		opts = &MutateInSpecArrayAppendOptions{}
	}
	var flags SubdocFlag
	_, ok := val.(MutationMacro)
	if ok {
		flags |= SubdocFlagUseMacros
		opts.IsXattr = true
	}
	if opts.CreatePath {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	encoder := opts.Encoder
	if opts.Encoder == nil {
		encoder = JSONEncode
	}

	var marshaled []byte
	var err error
	if opts.HasMultiple {
		marshaled, err = spec.encodeMultiArray(val, encoder)
	} else {
		marshaled, _, err = encoder(val)
	}

	if err != nil {
		return MutateInOp{err: err}
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpArrayPushLast,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: marshaled,
	}

	return MutateInOp{op: op}
}

// MutateInSpecArrayPrependOptions are the options available to subdocument ArrayPrepend operations.
type MutateInSpecArrayPrependOptions struct {
	CreatePath bool
	IsXattr    bool
	// HasMultiple adds multiple values as elements to an array.
	// When used `value` in the spec must be an array type
	// ArrayPrepend("path", []int{1,2,3,4}, MutateInSpecArrayPrependOptions{HasMultiple:true}) =>
	//   "path" [1,2,3,4, ....]
	//
	// This is a more efficient version (at both the network and server levels)
	// of doing
	// spec.ArrayPrepend("path", 1, nil)
	// spec.ArrayPrepend("path", 2, nil)
	// spec.ArrayPrepend("path", 3, nil)
	HasMultiple bool
	Encoder     Encode
}

// ArrayPrepend adds an element to the beginning (i.e. left) of an array
func (spec MutateInSpec) ArrayPrepend(path string, val interface{}, opts *MutateInSpecArrayPrependOptions) MutateInOp {
	if opts == nil {
		opts = &MutateInSpecArrayPrependOptions{}
	}
	var flags SubdocFlag
	_, ok := val.(MutationMacro)
	if ok {
		flags |= SubdocFlagUseMacros
		opts.IsXattr = true
	}
	if opts.CreatePath {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	encoder := opts.Encoder
	if opts.Encoder == nil {
		encoder = JSONEncode
	}

	var marshaled []byte
	var err error
	if opts.HasMultiple {
		marshaled, err = spec.encodeMultiArray(val, encoder)
	} else {
		marshaled, _, err = encoder(val)
	}

	if err != nil {
		return MutateInOp{err: err}
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpArrayPushFirst,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: marshaled,
	}

	return MutateInOp{op: op}
}

// MutateInSpecArrayInsertOptions are the options available to subdocument ArrayInsert operations.
type MutateInSpecArrayInsertOptions struct {
	CreatePath bool
	IsXattr    bool
	// HasMultiple adds multiple values as elements to an array.
	// When used `value` in the spec must be an array type
	// ArrayInsert("path[1]", []int{1,2,3,4}, MutateInSpecArrayInsertOptions{HasMultiple:true}) =>
	//   "path" [..., 1,2,3,4]
	//
	// This is a more efficient version (at both the network and server levels)
	// of doing
	// spec.ArrayInsert("path[2]", 1, nil)
	// spec.ArrayInsert("path[3]", 2, nil)
	// spec.ArrayInsert("path[4]", 3, nil)
	HasMultiple bool
	Encoder     Encode
}

// ArrayInsert inserts an element at a given position within an array. The position should be
// specified as part of the path, e.g. path.to.array[3]
func (spec MutateInSpec) ArrayInsert(path string, val interface{}, opts *MutateInSpecArrayInsertOptions) MutateInOp {
	if opts == nil {
		opts = &MutateInSpecArrayInsertOptions{}
	}
	var flags SubdocFlag
	_, ok := val.(MutationMacro)
	if ok {
		flags |= SubdocFlagUseMacros
		opts.IsXattr = true
	}
	if opts.CreatePath {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	encoder := opts.Encoder
	if opts.Encoder == nil {
		encoder = JSONEncode
	}

	var marshaled []byte
	var err error
	if opts.HasMultiple {
		marshaled, err = spec.encodeMultiArray(val, encoder)
	} else {
		marshaled, _, err = encoder(val)
	}

	if err != nil {
		return MutateInOp{err: err}
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpArrayInsert,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: marshaled,
	}

	return MutateInOp{op: op}
}

// MutateInSpecArrayAddUniqueOptions are the options available to subdocument ArrayAddUnique operations.
type MutateInSpecArrayAddUniqueOptions struct {
	CreatePath bool
	IsXattr    bool
	Encoder    Encode
}

// ArrayAddUnique adds an dictionary add unique operation to this mutation operation set.
func (spec MutateInSpec) ArrayAddUnique(path string, val interface{}, opts *MutateInSpecArrayAddUniqueOptions) MutateInOp {
	if opts == nil {
		opts = &MutateInSpecArrayAddUniqueOptions{}
	}
	var flags SubdocFlag
	_, ok := val.(MutationMacro)
	if ok {
		flags |= SubdocFlagUseMacros
		opts.IsXattr = true
	}

	if opts.CreatePath {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	encoder := opts.Encoder
	if opts.Encoder == nil {
		encoder = JSONEncode
	}

	marshaled, _, err := encoder(val)
	if err != nil {
		return MutateInOp{err: err}
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpArrayAddUnique,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: marshaled,
	}

	return MutateInOp{op: op}
}

// MutateInSpecCounterOptions are the options available to subdocument Increment and Decrement operations.
type MutateInSpecCounterOptions struct {
	CreatePath bool
	IsXattr    bool
	Encoder    Encode
}

// Increment adds an increment operation to this mutation operation set.
func (spec MutateInSpec) Increment(path string, delta int64, opts *MutateInSpecCounterOptions) MutateInOp {
	if opts == nil {
		opts = &MutateInSpecCounterOptions{}
	}
	var flags SubdocFlag
	if opts.CreatePath {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	encoder := opts.Encoder
	if opts.Encoder == nil {
		encoder = JSONEncode
	}

	marshaled, _, err := encoder(delta)
	if err != nil {
		return MutateInOp{err: err}
	}
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpCounter,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: marshaled,
	}

	return MutateInOp{op: op}
}

// Decrement adds a decrement operation to this mutation operation set.
func (spec MutateInSpec) Decrement(path string, delta int64, opts *MutateInSpecCounterOptions) MutateInOp {
	if opts == nil {
		opts = &MutateInSpecCounterOptions{}
	}
	var flags SubdocFlag
	if opts.CreatePath {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	encoder := opts.Encoder
	if opts.Encoder == nil {
		encoder = JSONEncode
	}

	marshaled, _, err := encoder(-delta)
	if err != nil {
		return MutateInOp{err: err}
	}
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpCounter,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: marshaled,
	}

	return MutateInOp{op: op}
}

// MutateIn performs a set of subdocument mutations on the document specified by key.
func (c *Collection) MutateIn(key string, ops []MutateInOp, opts *MutateInOptions) (mutOut *MutateInResult, errOut error) {
	if opts == nil {
		opts = &MutateInOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "MutateIn")
	defer span.Finish()

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
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
	if opts.InsertDocument {
		flags |= SubdocDocFlagMkDoc
	}
	if opts.UpsertDocument {
		flags |= SubdocDocFlagReplaceDoc
	}
	if opts.AccessDeleted {
		flags |= SubdocDocFlagAccessDeleted
	}

	var subdocs []gocbcore.SubDocOp
	for _, op := range ops {
		if op.err != nil {
			return nil, err
		}
		subdocs = append(subdocs, op.op)
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.MutateInEx(gocbcore.MutateInOptions{
		Key:             []byte(key),
		Flags:           gocbcore.SubdocDocFlag(flags),
		Cas:             gocbcore.Cas(opts.Cas),
		Ops:             subdocs,
		TraceContext:    traceCtx,
		Expiry:          opts.Expiration,
		CollectionName:  c.name(),
		ScopeName:       c.scopeName(),
		DurabilityLevel: gocbcore.DurabilityLevel(opts.DurabilityLevel),
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
