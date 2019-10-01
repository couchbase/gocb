package gocb

import (
	"context"
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
)

// LookupInSpec is the representation of an operation available when calling LookupIn
type LookupInSpec struct {
	op gocbcore.SubDocOp
}

// LookupInOptions are the set of options available to LookupIn.
type LookupInOptions struct {
	Context    context.Context
	Timeout    time.Duration
	Serializer JSONSerializer
}

// GetSpecOptions are the options available to LookupIn subdoc Get operations.
type GetSpecOptions struct {
	IsXattr bool
}

// GetSpec indicates a path to be retrieved from the document.  The value of the path
// can later be retrieved from the LookupResult.
// The path syntax follows N1QL's path syntax (e.g. `foo.bar.baz`).
func GetSpec(path string, opts *GetSpecOptions) LookupInSpec {
	if opts == nil {
		opts = &GetSpecOptions{}
	}
	return getSpecWithFlags(path, opts.IsXattr)
}

func getSpecWithFlags(path string, isXattr bool) LookupInSpec {
	var flags gocbcore.SubdocFlag
	if isXattr {
		flags |= gocbcore.SubdocFlag(SubdocFlagXattr)
	}

	if path == "" {
		op := gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpGetDoc,
			Flags: gocbcore.SubdocFlag(SubdocFlagNone),
		}

		return LookupInSpec{op: op}
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpGet,
		Path:  path,
		Flags: flags,
	}

	return LookupInSpec{op: op}
}

// ExistsSpecOptions are the options available to LookupIn subdoc Exists operations.
type ExistsSpecOptions struct {
	IsXattr bool
}

// ExistsSpec is similar to Path(), but does not actually retrieve the value from the server.
// This may save bandwidth if you only need to check for the existence of a
// path (without caring for its content). You can check the status of this
// operation by using .ContentAt (and ignoring the value) or .Exists() on the LookupResult.
func ExistsSpec(path string, opts *ExistsSpecOptions) LookupInSpec {
	if opts == nil {
		opts = &ExistsSpecOptions{}
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

	return LookupInSpec{op: op}
}

// CountSpecOptions are the options available to LookupIn subdoc Count operations.
type CountSpecOptions struct {
	IsXattr bool
}

// CountSpec allows you to retrieve the number of items in an array or keys within an
// dictionary within an element of a document.
func CountSpec(path string, opts *CountSpecOptions) LookupInSpec {
	if opts == nil {
		opts = &CountSpecOptions{}
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

	return LookupInSpec{op: op}
}

// LookupIn performs a set of subdocument lookup operations on the document identified by id.
func (c *Collection) LookupIn(id string, ops []LookupInSpec, opts *LookupInOptions) (docOut *LookupInResult, errOut error) {
	if opts == nil {
		opts = &LookupInOptions{}
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	res, err := c.lookupIn(ctx, id, ops, *opts)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Collection) lookupIn(ctx context.Context, id string, ops []LookupInSpec, opts LookupInOptions) (docOut *LookupInResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	var subdocs []gocbcore.SubDocOp
	for _, op := range ops {
		subdocs = append(subdocs, op.op)
	}

	if len(ops) > 16 {
		return nil, invalidArgumentsError{message: "too many lookupIn ops specified, maximum 16"}
	}

	serializer := opts.Serializer
	if serializer == nil {
		serializer = &DefaultJSONSerializer{}
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.LookupInEx(gocbcore.LookupInOptions{
		Key:            []byte(id),
		Ops:            subdocs,
		CollectionName: c.name(),
		ScopeName:      c.scopeName(),
	}, func(res *gocbcore.LookupInResult, err error) {
		if err != nil && !gocbcore.IsErrorStatus(err, gocbcore.StatusSubDocBadMulti) {
			errOut = maybeEnhanceKVErr(err, id, false)
			ctrl.resolve()
			return
		}

		if res != nil {
			resSet := &LookupInResult{}
			resSet.serializer = serializer
			resSet.cas = Cas(res.Cas)
			resSet.contents = make([]lookupInPartial, len(subdocs))

			for i, opRes := range res.Ops {
				// resSet.contents[i].path = opts.spec.ops[i].Path
				resSet.contents[i].err = maybeEnhanceKVErr(opRes.Err, id, false)
				if opRes.Value != nil {
					resSet.contents[i].data = append([]byte(nil), opRes.Value...)
				}
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

type subDocOp struct {
	Op         gocbcore.SubDocOpType
	Flags      gocbcore.SubdocFlag
	Path       string
	Value      interface{}
	MultiValue bool
}

// StoreSemantics is used to define the document level action to take during a MutateIn operation.
type StoreSemantics uint8

const (
	// StoreSemanticsReplace signifies to Replace the document, and fail if it does not exist.
	// This is the default action
	StoreSemanticsReplace = StoreSemantics(0)

	// StoreSemanticsUpsert signifies to replace the document or create it if it doesn't exist.
	StoreSemanticsUpsert = StoreSemantics(1)

	// StoreSemanticsInsert signifies to create the document, and fail if it exists.
	StoreSemanticsInsert = StoreSemantics(2)
)

// MutateInSpec is the representation of an operation available when calling MutateIn
type MutateInSpec struct {
	op subDocOp
}

// MutateInOptions are the set of options available to MutateIn.
type MutateInOptions struct {
	Timeout         time.Duration
	Context         context.Context
	Expiry          uint32
	Cas             Cas
	PersistTo       uint
	ReplicateTo     uint
	DurabilityLevel DurabilityLevel
	StoreSemantic   StoreSemantics
	Serializer      JSONSerializer
	// Internal: This should never be used and is not supported.
	AccessDeleted bool
}

func (c *Collection) encodeMultiArray(in interface{}, serializer JSONSerializer) ([]byte, error) {
	out, err := serializer.Serialize(in)
	if err != nil {
		return nil, err
	}

	// Assert first character is a '['
	if len(out) < 2 || out[0] != '[' {
		return nil, invalidArgumentsError{message: "not a JSON array"}
	}

	out = out[1 : len(out)-1]
	return out, nil
}

// InsertSpecOptions are the options available to subdocument Insert operations.
type InsertSpecOptions struct {
	CreatePath bool
	IsXattr    bool
}

// InsertSpec inserts a value at the specified path within the document.
func InsertSpec(path string, val interface{}, opts *InsertSpecOptions) MutateInSpec {
	if opts == nil {
		opts = &InsertSpecOptions{}
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

	op := subDocOp{
		Op:    gocbcore.SubDocOpDictAdd,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: val,
	}

	return MutateInSpec{op: op}
}

// UpsertSpecOptions are the options available to subdocument Upsert operations.
type UpsertSpecOptions struct {
	CreatePath bool
	IsXattr    bool
}

// UpsertSpec creates a new value at the specified path within the document if it does not exist, if it does exist then it
// updates it.
func UpsertSpec(path string, val interface{}, opts *UpsertSpecOptions) MutateInSpec {
	if opts == nil {
		opts = &UpsertSpecOptions{}
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

	op := subDocOp{
		Op:    gocbcore.SubDocOpDictSet,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: val,
	}

	return MutateInSpec{op: op}
}

// ReplaceSpecOptions are the options available to subdocument Replace operations.
type ReplaceSpecOptions struct {
	IsXattr bool
}

// ReplaceSpec replaces the value of the field at path.
func ReplaceSpec(path string, val interface{}, opts *ReplaceSpecOptions) MutateInSpec {
	if opts == nil {
		opts = &ReplaceSpecOptions{}
	}
	var flags SubdocFlag
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	if path == "" {
		op := subDocOp{
			Op:    gocbcore.SubDocOpSetDoc,
			Flags: gocbcore.SubdocFlag(flags),
			Value: val,
		}

		return MutateInSpec{op: op}
	}

	op := subDocOp{
		Op:    gocbcore.SubDocOpReplace,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: val,
	}

	return MutateInSpec{op: op}
}

// RemoveSpecOptions are the options available to subdocument Remove operations.
type RemoveSpecOptions struct {
	IsXattr bool
}

// RemoveSpec removes the field at path.
func RemoveSpec(path string, opts *RemoveSpecOptions) MutateInSpec {
	if opts == nil {
		opts = &RemoveSpecOptions{}
	}
	var flags SubdocFlag
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	op := subDocOp{
		Op:    gocbcore.SubDocOpDelete,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
	}

	return MutateInSpec{op: op}
}

// ArrayAppendSpecOptions are the options available to subdocument ArrayAppend operations.
type ArrayAppendSpecOptions struct {
	CreatePath bool
	IsXattr    bool
	// HasMultiple adds multiple values as elements to an array.
	// When used `value` in the spec must be an array type
	// ArrayAppend("path", []int{1,2,3,4}, ArrayAppendSpecOptions{HasMultiple:true}) =>
	//   "path" [..., 1,2,3,4]
	//
	// This is a more efficient version (at both the network and server levels)
	// of doing
	// spec.ArrayAppend("path", 1, nil)
	// spec.ArrayAppend("path", 2, nil)
	// spec.ArrayAppend("path", 3, nil)
	HasMultiple bool
}

// ArrayAppendSpec adds an element(s) to the end (i.e. right) of an array
func ArrayAppendSpec(path string, val interface{}, opts *ArrayAppendSpecOptions) MutateInSpec {
	if opts == nil {
		opts = &ArrayAppendSpecOptions{}
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

	op := subDocOp{
		Op:    gocbcore.SubDocOpArrayPushLast,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: val,
	}

	if opts.HasMultiple {
		op.MultiValue = true
	}

	return MutateInSpec{op: op}
}

// ArrayPrependSpecOptions are the options available to subdocument ArrayPrepend operations.
type ArrayPrependSpecOptions struct {
	CreatePath bool
	IsXattr    bool
	// HasMultiple adds multiple values as elements to an array.
	// When used `value` in the spec must be an array type
	// ArrayPrepend("path", []int{1,2,3,4}, ArrayPrependSpecOptions{HasMultiple:true}) =>
	//   "path" [1,2,3,4, ....]
	//
	// This is a more efficient version (at both the network and server levels)
	// of doing
	// spec.ArrayPrepend("path", 1, nil)
	// spec.ArrayPrepend("path", 2, nil)
	// spec.ArrayPrepend("path", 3, nil)
	HasMultiple bool
}

// ArrayPrependSpec adds an element to the beginning (i.e. left) of an array
func ArrayPrependSpec(path string, val interface{}, opts *ArrayPrependSpecOptions) MutateInSpec {
	if opts == nil {
		opts = &ArrayPrependSpecOptions{}
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

	op := subDocOp{
		Op:    gocbcore.SubDocOpArrayPushFirst,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: val,
	}

	if opts.HasMultiple {
		op.MultiValue = true
	}

	return MutateInSpec{op: op}
}

// ArrayInsertSpecOptions are the options available to subdocument ArrayInsert operations.
type ArrayInsertSpecOptions struct {
	CreatePath bool
	IsXattr    bool
	// HasMultiple adds multiple values as elements to an array.
	// When used `value` in the spec must be an array type
	// ArrayInsert("path[1]", []int{1,2,3,4}, ArrayInsertSpecOptions{HasMultiple:true}) =>
	//   "path" [..., 1,2,3,4]
	//
	// This is a more efficient version (at both the network and server levels)
	// of doing
	// spec.ArrayInsert("path[2]", 1, nil)
	// spec.ArrayInsert("path[3]", 2, nil)
	// spec.ArrayInsert("path[4]", 3, nil)
	HasMultiple bool
}

// ArrayInsertSpec inserts an element at a given position within an array. The position should be
// specified as part of the path, e.g. path.to.array[3]
func ArrayInsertSpec(path string, val interface{}, opts *ArrayInsertSpecOptions) MutateInSpec {
	if opts == nil {
		opts = &ArrayInsertSpecOptions{}
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

	op := subDocOp{
		Op:    gocbcore.SubDocOpArrayInsert,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: val,
	}

	if opts.HasMultiple {
		op.MultiValue = true
	}

	return MutateInSpec{op: op}
}

// ArrayAddUniqueSpecOptions are the options available to subdocument ArrayAddUnique operations.
type ArrayAddUniqueSpecOptions struct {
	CreatePath bool
	IsXattr    bool
}

// ArrayAddUniqueSpec adds an dictionary add unique operation to this mutation operation set.
func ArrayAddUniqueSpec(path string, val interface{}, opts *ArrayAddUniqueSpecOptions) MutateInSpec {
	if opts == nil {
		opts = &ArrayAddUniqueSpecOptions{}
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

	op := subDocOp{
		Op:    gocbcore.SubDocOpArrayAddUnique,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: val,
	}

	return MutateInSpec{op: op}
}

// CounterSpecOptions are the options available to subdocument Increment and Decrement operations.
type CounterSpecOptions struct {
	CreatePath bool
	IsXattr    bool
}

// IncrementSpec adds an increment operation to this mutation operation set.
func IncrementSpec(path string, delta int64, opts *CounterSpecOptions) MutateInSpec {
	if opts == nil {
		opts = &CounterSpecOptions{}
	}
	var flags SubdocFlag
	if opts.CreatePath {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	op := subDocOp{
		Op:    gocbcore.SubDocOpCounter,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: delta,
	}

	return MutateInSpec{op: op}
}

// DecrementSpec adds a decrement operation to this mutation operation set.
func DecrementSpec(path string, delta int64, opts *CounterSpecOptions) MutateInSpec {
	if opts == nil {
		opts = &CounterSpecOptions{}
	}
	var flags SubdocFlag
	if opts.CreatePath {
		flags |= SubdocFlagCreatePath
	}
	if opts.IsXattr {
		flags |= SubdocFlagXattr
	}

	op := subDocOp{
		Op:    gocbcore.SubDocOpCounter,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: -delta,
	}

	return MutateInSpec{op: op}
}

// MutateIn performs a set of subdocument mutations on the document specified by id.
func (c *Collection) MutateIn(id string, ops []MutateInSpec, opts *MutateInOptions) (mutOut *MutateInResult, errOut error) {
	if opts == nil {
		opts = &MutateInOptions{}
	}

	// Only update ctx if necessary, this means that the original ctx.Done() signal will be triggered as expected
	ctx, cancel := c.context(opts.Context, opts.Timeout)
	if cancel != nil {
		defer cancel()
	}

	err := c.verifyObserveOptions(opts.PersistTo, opts.ReplicateTo, opts.DurabilityLevel)
	if err != nil {
		return nil, err
	}

	res, err := c.mutate(ctx, id, ops, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(durabilitySettings{
		ctx:            opts.Context,
		key:            id,
		cas:            res.Cas(),
		mt:             *res.MutationToken(),
		replicaTo:      opts.ReplicateTo,
		persistTo:      opts.PersistTo,
		forDelete:      false,
		scopeName:      c.scopeName(),
		collectionName: c.name(),
	})
}

func (c *Collection) mutate(ctx context.Context, id string, ops []MutateInSpec, opts MutateInOptions) (mutOut *MutateInResult, errOut error) {
	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	if (opts.PersistTo != 0 || opts.ReplicateTo != 0) && !c.sb.UseMutationTokens {
		return nil, invalidArgumentsError{"cannot use observe based durability without mutation tokens"}
	}

	var isInsertDocument bool
	var flags SubdocDocFlag
	action := opts.StoreSemantic
	if action == StoreSemanticsUpsert {
		flags |= SubdocDocFlagMkDoc
	}
	if action == StoreSemanticsInsert {
		isInsertDocument = true
		flags |= SubdocDocFlagAddDoc
	}
	if action > 2 {
		return nil, invalidArgumentsError{message: "invalid StoreSemantics value provided"}
	}

	if opts.AccessDeleted {
		flags |= SubdocDocFlagAccessDeleted
	}

	serializer := opts.Serializer
	if serializer == nil {
		serializer = &DefaultJSONSerializer{}
	}

	var subdocs []gocbcore.SubDocOp
	for _, op := range ops {
		if op.op.Path == "" {
			switch op.op.Op {
			case gocbcore.SubDocOpDictAdd:
				return nil, invalidArgumentsError{"cannot specify a blank path with InsertSpec"}
			case gocbcore.SubDocOpDictSet:
				return nil, invalidArgumentsError{"cannot specify a blank path with UpsertSpec"}
			case gocbcore.SubDocOpDelete:
				return nil, invalidArgumentsError{"cannot specify a blank path with DeleteSpec"}
			default:
			}
		}

		if op.op.Value == nil {
			subdocs = append(subdocs, gocbcore.SubDocOp{
				Op:    op.op.Op,
				Flags: op.op.Flags,
				Path:  op.op.Path,
			})

			continue
		}

		var marshaled []byte
		var err error
		if op.op.MultiValue {
			marshaled, err = c.encodeMultiArray(op.op.Value, serializer)
		} else {
			marshaled, err = serializer.Serialize(op.op.Value)
		}
		if err != nil {
			return nil, err
		}

		subdocs = append(subdocs, gocbcore.SubDocOp{
			Op:    op.op.Op,
			Flags: op.op.Flags,
			Path:  op.op.Path,
			Value: marshaled,
		})
	}

	coerced, durabilityTimeout := c.durabilityTimeout(ctx, opts.DurabilityLevel)
	if coerced {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(durabilityTimeout)*time.Millisecond)
		defer cancel()
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.wait(agent.MutateInEx(gocbcore.MutateInOptions{
		Key:                    []byte(id),
		Flags:                  gocbcore.SubdocDocFlag(flags),
		Cas:                    gocbcore.Cas(opts.Cas),
		Ops:                    subdocs,
		Expiry:                 opts.Expiry,
		CollectionName:         c.name(),
		ScopeName:              c.scopeName(),
		DurabilityLevel:        gocbcore.DurabilityLevel(opts.DurabilityLevel),
		DurabilityLevelTimeout: durabilityTimeout,
	}, func(res *gocbcore.MutateInResult, err error) {
		if err != nil {
			errOut = maybeEnhanceKVErr(err, id, isInsertDocument)
			ctrl.resolve()
			return
		}

		mutRes := &MutateInResult{
			MutationResult: MutationResult{
				Result: Result{
					cas: Cas(res.Cas),
				},
			},
			contents: make([]mutateInPartial, len(res.Ops)),
		}

		if res.MutationToken.VbUuid != 0 {
			mutTok := &MutationToken{
				token:      res.MutationToken,
				bucketName: c.sb.BucketName,
			}
			mutRes.mt = mutTok
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
