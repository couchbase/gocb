package executor

import (
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocb/v2"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/helpers"

	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/collection/mutatein"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/kv"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/kv/lookupin"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/sdk/kv/rangescan"
	"github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol/shared"
)

func (e *Executor) createScanOptions(opts *rangescan.ScanOptions) (*gocb.ScanOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.ScanOptions{
		Timeout:        time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
		IDsOnly:        opts.GetIdsOnly(),
		BatchItemLimit: opts.BatchItemLimit,
		BatchByteLimit: opts.BatchByteLimit,
	}

	if opts.Transcoder != nil {
		t, err := helpers.Transcoder(opts.Transcoder)
		if err != nil {
			return nil, err
		}

		gocbOpts.Transcoder = t
	}

	if opts.ConsistentWith != nil {
		consistentWith, err := helpers.ProtoMutationStateToGocb(opts.ConsistentWith)
		if err != nil {
			return nil, err
		}
		gocbOpts.ConsistentWith = consistentWith
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}
	if opts.Concurrency != nil {
		gocbOpts.Concurrency = uint16(*opts.Concurrency)
	}

	return gocbOpts, nil
}

func (e *Executor) createInsertOptions(opts *kv.InsertOptions) (*gocb.InsertOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.InsertOptions{
		Timeout: time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
	}

	if opts.Expiry != nil {
		expiry, err := helpers.Expiry(opts.Expiry)
		if err != nil {
			return nil, err
		}

		gocbOpts.Expiry = expiry
	}

	if opts.Durability != nil {
		level, err := durabilityLevel(opts.Durability)
		if err != nil {
			return nil, err
		}
		gocbOpts.PersistTo = level.PersistTo
		gocbOpts.ReplicateTo = level.ReplicateTo
		gocbOpts.DurabilityLevel = level.DurabilityLevel
	}

	if opts.Transcoder != nil {
		t, err := helpers.Transcoder(opts.Transcoder)
		if err != nil {
			return nil, err
		}

		gocbOpts.Transcoder = t
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createReplaceOptions(opts *kv.ReplaceOptions) (*gocb.ReplaceOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.ReplaceOptions{
		Timeout:        time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
		PreserveExpiry: opts.GetPreserveExpiry(),
		Cas:            gocb.Cas(opts.GetCas()),
	}

	if opts.Expiry != nil {
		expiry, err := helpers.Expiry(opts.Expiry)
		if err != nil {
			return nil, err
		}

		gocbOpts.Expiry = expiry
	}

	if opts.Durability != nil {
		level, err := durabilityLevel(opts.Durability)
		if err != nil {
			return nil, err
		}
		gocbOpts.PersistTo = level.PersistTo
		gocbOpts.ReplicateTo = level.ReplicateTo
		gocbOpts.DurabilityLevel = level.DurabilityLevel
	}

	if opts.Transcoder != nil {
		t, err := helpers.Transcoder(opts.Transcoder)
		if err != nil {
			return nil, err
		}

		gocbOpts.Transcoder = t
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createUpsertOptions(opts *kv.UpsertOptions) (*gocb.UpsertOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.UpsertOptions{
		Timeout:        time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
		PreserveExpiry: opts.GetPreserveExpiry(),
	}

	if opts.Expiry != nil {
		expiry, err := helpers.Expiry(opts.Expiry)
		if err != nil {
			return nil, err
		}

		gocbOpts.Expiry = expiry
	}

	if opts.Durability != nil {
		level, err := durabilityLevel(opts.Durability)
		if err != nil {
			return nil, err
		}
		gocbOpts.PersistTo = level.PersistTo
		gocbOpts.ReplicateTo = level.ReplicateTo
		gocbOpts.DurabilityLevel = level.DurabilityLevel
	}

	if opts.Transcoder != nil {
		t, err := helpers.Transcoder(opts.Transcoder)
		if err != nil {
			return nil, err
		}

		gocbOpts.Transcoder = t
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createRemoveOptions(opts *kv.RemoveOptions) (*gocb.RemoveOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.RemoveOptions{
		Timeout: time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
		Cas:     gocb.Cas(opts.GetCas()),
	}

	if opts.Durability != nil {
		level, err := durabilityLevel(opts.Durability)
		if err != nil {
			return nil, err
		}
		gocbOpts.PersistTo = level.PersistTo
		gocbOpts.ReplicateTo = level.ReplicateTo
		gocbOpts.DurabilityLevel = level.DurabilityLevel
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createGetOptions(opts *kv.GetOptions) (*gocb.GetOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.GetOptions{
		Timeout:    time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
		WithExpiry: opts.GetWithExpiry(),
		Project:    opts.Projection,
	}

	if opts.Transcoder != nil {
		t, err := helpers.Transcoder(opts.Transcoder)
		if err != nil {
			return nil, err
		}

		gocbOpts.Transcoder = t
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createGetAndTouchOptions(opts *kv.GetAndTouchOptions) (*gocb.GetAndTouchOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.GetAndTouchOptions{
		Timeout: time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
	}

	if opts.Transcoder != nil {
		t, err := helpers.Transcoder(opts.Transcoder)
		if err != nil {
			return nil, err
		}

		gocbOpts.Transcoder = t
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createMutateInOptions(opts *mutatein.MutateInOptions) (*gocb.MutateInOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.MutateInOptions{
		Timeout:        time.Duration(opts.GetTimeoutMillis()) * time.Millisecond,
		Cas:            gocb.Cas(opts.GetCas()),
		PreserveExpiry: opts.GetPreserveExpiry(),
	}

	if opts.StoreSemantics != nil {
		gocbOpts.StoreSemantic = toGocbStoreSemantic(opts.GetStoreSemantics())
	}

	if opts.Durability != nil {
		level, err := durabilityLevel(opts.Durability)
		if err != nil {
			return nil, err
		}
		gocbOpts.PersistTo = level.PersistTo
		gocbOpts.ReplicateTo = level.ReplicateTo
		gocbOpts.DurabilityLevel = level.DurabilityLevel
	}

	if opts.Expiry != nil {
		expiry, err := helpers.Expiry(opts.Expiry)
		if err != nil {
			return nil, err
		}

		gocbOpts.Expiry = expiry
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createGetAndLockOptions(opts *kv.GetAndLockOptions) (*gocb.GetAndLockOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.GetAndLockOptions{
		Timeout: time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
	}

	if opts.Transcoder != nil {
		t, err := helpers.Transcoder(opts.Transcoder)
		if err != nil {
			return nil, err
		}

		gocbOpts.Transcoder = t
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createTouchOptions(opts *kv.TouchOptions) (*gocb.TouchOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.TouchOptions{
		Timeout: time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createUnlockOptions(opts *kv.UnlockOptions) (*gocb.UnlockOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.UnlockOptions{
		Timeout: time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createExistsOptions(opts *kv.ExistsOptions) (*gocb.ExistsOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.ExistsOptions{
		Timeout: time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createIncrementOptions(opts *kv.IncrementOptions) (*gocb.IncrementOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.IncrementOptions{
		Timeout: time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
		Delta:   uint64(opts.GetDelta()),
		Initial: opts.GetInitial(),
	}

	if opts.Expiry != nil {
		expiry, err := helpers.Expiry(opts.Expiry)
		if err != nil {
			return nil, err
		}

		gocbOpts.Expiry = expiry
	}

	if opts.Durability != nil {
		level, err := durabilityLevel(opts.Durability)
		if err != nil {
			return nil, err
		}
		gocbOpts.PersistTo = level.PersistTo
		gocbOpts.ReplicateTo = level.ReplicateTo
		gocbOpts.DurabilityLevel = level.DurabilityLevel
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createDecrementOptions(opts *kv.DecrementOptions) (*gocb.DecrementOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.DecrementOptions{
		Timeout: time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
		Delta:   uint64(opts.GetDelta()),
		Initial: opts.GetInitial(),
	}

	if opts.Expiry != nil {
		expiry, err := helpers.Expiry(opts.Expiry)
		if err != nil {
			return nil, err
		}

		gocbOpts.Expiry = expiry
	}

	if opts.Durability != nil {
		level, err := durabilityLevel(opts.Durability)
		if err != nil {
			return nil, err
		}
		gocbOpts.PersistTo = level.PersistTo
		gocbOpts.ReplicateTo = level.ReplicateTo
		gocbOpts.DurabilityLevel = level.DurabilityLevel
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createAppendOptions(opts *kv.AppendOptions) (*gocb.AppendOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.AppendOptions{
		Timeout: time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
		Cas:     gocb.Cas(opts.GetCas()),
	}

	if opts.Durability != nil {
		level, err := durabilityLevel(opts.Durability)
		if err != nil {
			return nil, err
		}
		gocbOpts.PersistTo = level.PersistTo
		gocbOpts.ReplicateTo = level.ReplicateTo
		gocbOpts.DurabilityLevel = level.DurabilityLevel
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createPrependOptions(opts *kv.PrependOptions) (*gocb.PrependOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.PrependOptions{
		Timeout: time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
		Cas:     gocb.Cas(opts.GetCas()),
	}

	if opts.Durability != nil {
		level, err := durabilityLevel(opts.Durability)
		if err != nil {
			return nil, err
		}
		gocbOpts.PersistTo = level.PersistTo
		gocbOpts.ReplicateTo = level.ReplicateTo
		gocbOpts.DurabilityLevel = level.DurabilityLevel
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createGetAllReplicasOptions(opts *kv.GetAllReplicasOptions) (*gocb.GetAllReplicaOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.GetAllReplicaOptions{
		Timeout: time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
	}

	if opts.Transcoder != nil {
		t, err := helpers.Transcoder(opts.Transcoder)
		if err != nil {
			return nil, err
		}

		gocbOpts.Transcoder = t
	}

	if opts.ReadPreference != nil {
		switch opts.GetReadPreference() {
		case shared.ReadPreference_NO_PREFERENCE:
			gocbOpts.ReadPreference = gocb.ReadPreferenceNone
		case shared.ReadPreference_SELECTED_SERVER_GROUP:
			gocbOpts.ReadPreference = gocb.ReadPreferenceSelectedServerGroup
		default:
			return nil, fmt.Errorf("unknown read preference %s", opts.GetReadPreference())
		}
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createGetAnyReplicaOptions(opts *kv.GetAnyReplicaOptions) (*gocb.GetAnyReplicaOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.GetAnyReplicaOptions{
		Timeout: time.Duration(opts.GetTimeoutMsecs()) * time.Millisecond,
	}

	if opts.Transcoder != nil {
		t, err := helpers.Transcoder(opts.Transcoder)
		if err != nil {
			return nil, err
		}

		gocbOpts.Transcoder = t
	}

	if opts.ReadPreference != nil {
		switch opts.GetReadPreference() {
		case shared.ReadPreference_NO_PREFERENCE:
			gocbOpts.ReadPreference = gocb.ReadPreferenceNone
		case shared.ReadPreference_SELECTED_SERVER_GROUP:
			gocbOpts.ReadPreference = gocb.ReadPreferenceSelectedServerGroup
		default:
			return nil, fmt.Errorf("unknown read preference %s", opts.GetReadPreference())
		}
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createLookupInOptions(opts *lookupin.LookupInOptions) (*gocb.LookupInOptions, error) {
	if opts == nil {
		return nil, nil
	}
	var docFlags gocb.SubdocDocFlag
	if opts.GetAccessDeleted() {
		docFlags = docFlags | gocb.SubdocDocFlagAccessDeleted
	}

	gocbOpts := &gocb.LookupInOptions{
		Timeout: time.Duration(opts.GetTimeoutMillis()) * time.Millisecond,
		Internal: struct {
			DocFlags gocb.SubdocDocFlag
			User     string
		}{DocFlags: docFlags},
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	return gocbOpts, nil
}

func (e *Executor) createLookupInAnyReplicaOptions(opts *lookupin.LookupInAnyReplicaOptions) (*gocb.LookupInAnyReplicaOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.LookupInAnyReplicaOptions{
		Timeout: time.Duration(opts.GetTimeoutMillis()) * time.Millisecond,
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	if opts.ReadPreference != nil {
		switch opts.GetReadPreference() {
		case shared.ReadPreference_NO_PREFERENCE:
			gocbOpts.ReadPreference = gocb.ReadPreferenceNone
		case shared.ReadPreference_SELECTED_SERVER_GROUP:
			gocbOpts.ReadPreference = gocb.ReadPreferenceSelectedServerGroup
		default:
			return nil, fmt.Errorf("unknown read preference %s", opts.GetReadPreference())
		}
	}

	return gocbOpts, nil
}

func (e *Executor) createLookupInAllReplicasOptions(opts *lookupin.LookupInAllReplicasOptions) (*gocb.LookupInAllReplicaOptions, error) {
	if opts == nil {
		return nil, nil
	}

	gocbOpts := &gocb.LookupInAllReplicaOptions{
		Timeout: time.Duration(opts.GetTimeoutMillis()) * time.Millisecond,
	}

	if opts.ParentSpanId != nil {
		parent, ok := e.spanOwner.GetSpan(*opts.ParentSpanId)
		if !ok {
			return nil, fmt.Errorf("unknown parent span id: %s", *opts.ParentSpanId)
		}
		gocbOpts.ParentSpan = parent
	}

	if opts.ReadPreference != nil {
		switch opts.GetReadPreference() {
		case shared.ReadPreference_NO_PREFERENCE:
			gocbOpts.ReadPreference = gocb.ReadPreferenceNone
		case shared.ReadPreference_SELECTED_SERVER_GROUP:
			gocbOpts.ReadPreference = gocb.ReadPreferenceSelectedServerGroup
		default:
			return nil, fmt.Errorf("unknown read preference %s", opts.GetReadPreference())
		}
	}

	return gocbOpts, nil
}

func createLookupInSpecs(specs []*lookupin.LookupInSpec) ([]gocb.LookupInSpec, error) {
	var gocbSpecs []gocb.LookupInSpec
	for _, spec := range specs {
		switch s := spec.Operation.(type) {
		case *lookupin.LookupInSpec_Get:
			var opts *gocb.GetSpecOptions
			if s.Get.Xattr != nil {
				opts = &gocb.GetSpecOptions{
					IsXattr: s.Get.GetXattr(),
				}
			}
			gocbSpecs = append(gocbSpecs, gocb.GetSpec(s.Get.Path, opts))
		case *lookupin.LookupInSpec_Count:
			var opts *gocb.CountSpecOptions
			if s.Count.Xattr != nil {
				opts = &gocb.CountSpecOptions{
					IsXattr: s.Count.GetXattr(),
				}
			}
			gocbSpecs = append(gocbSpecs, gocb.CountSpec(s.Count.Path, opts))
		case *lookupin.LookupInSpec_Exists:
			var opts *gocb.ExistsSpecOptions
			if s.Exists.Xattr != nil {
				opts = &gocb.ExistsSpecOptions{
					IsXattr: s.Exists.GetXattr(),
				}
			}
			gocbSpecs = append(gocbSpecs, gocb.ExistsSpec(s.Exists.Path, opts))
		default:
			return nil, errors.New("unknown spec type")
		}
	}

	return gocbSpecs, nil
}

func createMutateInSpecs(specs []*mutatein.MutateInSpec) ([]gocb.MutateInSpec, error) {
	var mops []gocb.MutateInSpec

	for _, spec := range specs {
		convertedSpec, err := convertMutateInSpec(spec)
		if err != nil {
			return nil, err
		}
		mops = append(mops, convertedSpec)
	}

	return mops, nil
}

func convertMutateInSpec(spec *mutatein.MutateInSpec) (gocb.MutateInSpec, error) {
	switch spec.Operation.(type) {
	case *mutatein.MutateInSpec_Upsert:
		var contentConverted interface{}
		if spec.GetUpsert().GetContent() != nil {
			var err error
			contentConverted, err = helpers.ContentOrMacro(spec.GetUpsert().GetContent())
			if err != nil {
				return gocb.MutateInSpec{}, err
			}
		}
		return gocb.UpsertSpec(
			spec.GetUpsert().GetPath(),
			contentConverted,
			&gocb.UpsertSpecOptions{
				CreatePath: spec.GetUpsert().GetCreatePath(),
				IsXattr:    spec.GetUpsert().GetXattr()}), nil
	case *mutatein.MutateInSpec_Insert:
		var contentConverted interface{}
		if spec.GetInsert().GetContent() != nil {
			var err error
			contentConverted, err = helpers.ContentOrMacro(spec.GetInsert().GetContent())
			if err != nil {
				return gocb.MutateInSpec{}, err
			}
		}
		return gocb.InsertSpec(
			spec.GetInsert().GetPath(),
			contentConverted,
			&gocb.InsertSpecOptions{
				CreatePath: spec.GetInsert().GetCreatePath(),
				IsXattr:    spec.GetInsert().GetXattr()}), nil

	case *mutatein.MutateInSpec_Replace:
		var contentConverted interface{}
		if spec.GetReplace().GetContent() != nil {
			var err error
			contentConverted, err = helpers.ContentOrMacro(spec.GetReplace().GetContent())
			if err != nil {
				return gocb.MutateInSpec{}, err
			}
		}
		return gocb.ReplaceSpec(
			spec.GetReplace().GetPath(),
			contentConverted,
			&gocb.ReplaceSpecOptions{IsXattr: spec.GetReplace().GetXattr()}), nil
	case *mutatein.MutateInSpec_Remove:
		return gocb.RemoveSpec(
			spec.GetRemove().GetPath(),
			&gocb.RemoveSpecOptions{IsXattr: spec.GetRemove().GetXattr()}), nil
	case *mutatein.MutateInSpec_ArrayAppend:
		var convertedContents []interface{}
		if spec.GetArrayAppend().GetContent() != nil {
			for _, content := range spec.GetArrayAppend().GetContent() {
				val, err := helpers.ContentOrMacro(content)
				if err != nil {
					return gocb.MutateInSpec{}, err
				}
				convertedContents = append(convertedContents, val)
			}
		}
		var convertedContent interface{}
		if len(convertedContents) != 1 {
			convertedContent = convertedContents
		} else {
			convertedContent = convertedContents[0]
		}
		return gocb.ArrayAppendSpec(
			spec.GetArrayAppend().GetPath(),
			convertedContent,
			&gocb.ArrayAppendSpecOptions{
				CreatePath:  spec.GetArrayAppend().GetCreatePath(),
				IsXattr:     spec.GetArrayAppend().GetXattr(),
				HasMultiple: len(convertedContents) != 1}), nil
	case *mutatein.MutateInSpec_ArrayPrepend:
		var convertedContents []interface{}
		if spec.GetArrayPrepend().GetContent() != nil {
			for _, content := range spec.GetArrayPrepend().GetContent() {
				val, err := helpers.ContentOrMacro(content)
				if err != nil {
					return gocb.MutateInSpec{}, err
				}
				convertedContents = append(convertedContents, val)
			}
		}
		var convertedContent interface{}
		if len(convertedContents) != 1 {
			convertedContent = convertedContents
		} else {
			convertedContent = convertedContents[0]
		}
		return gocb.ArrayPrependSpec(
			spec.GetArrayPrepend().GetPath(),
			convertedContent,
			&gocb.ArrayPrependSpecOptions{
				CreatePath:  spec.GetArrayPrepend().GetCreatePath(),
				IsXattr:     spec.GetArrayPrepend().GetXattr(),
				HasMultiple: len(convertedContents) != 1}), nil
	case *mutatein.MutateInSpec_ArrayInsert:
		var convertedContents []interface{}
		if spec.GetArrayInsert().GetContent() != nil {
			for _, content := range spec.GetArrayInsert().GetContent() {
				val, err := helpers.ContentOrMacro(content)
				if err != nil {
					return gocb.MutateInSpec{}, err
				}
				convertedContents = append(convertedContents, val)
			}
		}
		var convertedContent interface{}
		if len(convertedContents) != 1 {
			convertedContent = convertedContents
		} else {
			convertedContent = convertedContents[0]
		}
		return gocb.ArrayInsertSpec(
			spec.GetArrayInsert().GetPath(),
			convertedContent,
			&gocb.ArrayInsertSpecOptions{
				CreatePath:  spec.GetArrayInsert().GetCreatePath(),
				IsXattr:     spec.GetArrayInsert().GetXattr(),
				HasMultiple: len(convertedContents) != 1}), nil
	case *mutatein.MutateInSpec_ArrayAddUnique:
		var contentConverted interface{}
		if spec.GetArrayAddUnique().GetContent() != nil {
			var err error
			contentConverted, err = helpers.ContentOrMacro(spec.GetArrayAddUnique().GetContent())
			if err != nil {
				return gocb.MutateInSpec{}, err
			}
		}
		return gocb.ArrayAddUniqueSpec(
			spec.GetArrayAddUnique().GetPath(),
			contentConverted,
			&gocb.ArrayAddUniqueSpecOptions{
				CreatePath: spec.GetArrayAddUnique().GetCreatePath(),
				IsXattr:    spec.GetArrayAddUnique().GetXattr()}), nil
	case *mutatein.MutateInSpec_Increment:
		return gocb.IncrementSpec(
			spec.GetIncrement().GetPath(),
			spec.GetIncrement().GetDelta(),
			&gocb.CounterSpecOptions{
				CreatePath: spec.GetIncrement().GetCreatePath(),
				IsXattr:    spec.GetIncrement().GetXattr()}), nil
	case *mutatein.MutateInSpec_Decrement:
		return gocb.DecrementSpec(
			spec.GetDecrement().GetPath(),
			spec.GetDecrement().GetDelta(),
			&gocb.CounterSpecOptions{
				CreatePath: spec.GetDecrement().GetCreatePath(),
				IsXattr:    spec.GetDecrement().GetXattr()}), nil
	}
	return gocb.MutateInSpec{}, errors.New("unsupported MutateInSpec operation")

}

type lookupInResult interface {
	ContentAt(uint, interface{}) error
	Exists(uint) bool
}

func (e *Executor) parseLookupinResults(specs []*lookupin.LookupInSpec, res lookupInResult) []*lookupin.LookupInSpecResult {
	results := make([]*lookupin.LookupInSpecResult, len(specs))
	for i, spec := range specs {
		result := &lookupin.LookupInSpecResult{
			ExistsResult: &lookupin.BooleanOrError{
				Result: &lookupin.BooleanOrError_Value{
					Value: res.Exists(uint(i)),
				},
			},
		}

		content, err := helpers.ParseContentAs(spec.ContentAs, func(content interface{}) error {
			return res.ContentAt(uint(i), content)
		})
		if err != nil {
			e.logger.Errorf("Failed to parse lookup in content %s", err)
			result.ContentAsResult = &shared.ContentOrError{
				Result: &shared.ContentOrError_Exception{
					Exception: helpers.MapErrorToProto(err),
				},
			}
			results[i] = result
			continue
		}

		result.ContentAsResult = &shared.ContentOrError{
			Result: &shared.ContentOrError_Content{
				Content: content,
			},
		}

		results[i] = result
	}
	return results
}
