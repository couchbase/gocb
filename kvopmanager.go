package gocb

import (
	"time"

	gocbcore "github.com/couchbase/gocbcore/v8"
	"github.com/pkg/errors"
)

type kvOpManager struct {
	parent *Collection
	signal chan struct{}

	err           error
	wasResolved   bool
	mutationToken *MutationToken

	span            requestSpan
	documentID      string
	transcoder      Transcoder
	deadline        time.Time
	bytes           []byte
	flags           uint32
	persistTo       uint
	replicateTo     uint
	durabilityLevel DurabilityLevel
	retryStrategy   *retryStrategyWrapper
	cancelCh        chan struct{}
}

func (m *kvOpManager) SetDocumentID(id string) {
	m.documentID = id
}

func (m *kvOpManager) SetCancelCh(cancelCh chan struct{}) {
	m.cancelCh = cancelCh
}

func (m *kvOpManager) SetTimeout(timeout time.Duration) {
	if timeout == 0 || timeout > m.parent.sb.KvTimeout {
		timeout = m.parent.sb.KvTimeout
	}
	m.deadline = time.Now().Add(timeout)
}

func (m *kvOpManager) SetTranscoder(transcoder Transcoder) {
	if transcoder == nil {
		transcoder = m.parent.sb.Transcoder
	}
	m.transcoder = transcoder
}

func (m *kvOpManager) SetValue(val interface{}) {
	if m.err != nil {
		return
	}
	if m.transcoder == nil {
		m.err = errors.New("Expected a transcoder to be specified first")
		return
	}

	espan := m.parent.startKvOpTrace("encode", m.span)
	defer espan.Finish()

	bytes, flags, err := m.transcoder.Encode(val)
	if err != nil {
		m.err = err
		return
	}

	m.bytes = bytes
	m.flags = flags
}

func (m *kvOpManager) SetDuraOptions(persistTo, replicateTo uint, level DurabilityLevel) {
	if persistTo != 0 || replicateTo != 0 {
		if !m.parent.sb.UseMutationTokens {
			m.err = makeInvalidArgumentsError("cannot use observe based durability without mutation tokens")
			return
		}

		if level > 0 {
			m.err = makeInvalidArgumentsError("cannot mix observe based durability and synchronous durability")
			return
		}
	}

	m.persistTo = persistTo
	m.replicateTo = replicateTo
	m.durabilityLevel = level
}

func (m *kvOpManager) SetRetryStrategy(retryStrategy RetryStrategy) {
	wrapper := m.parent.sb.RetryStrategyWrapper
	if retryStrategy != nil {
		wrapper = newRetryStrategyWrapper(retryStrategy)
	}
	m.retryStrategy = wrapper
}

func (m *kvOpManager) Finish() {
	m.span.Finish()
}

func (m *kvOpManager) TraceSpan() requestSpan {
	return m.span
}

func (m *kvOpManager) DocumentID() []byte {
	return []byte(m.documentID)
}

func (m *kvOpManager) CollectionName() string {
	return m.parent.name()
}

func (m *kvOpManager) ScopeName() string {
	return m.parent.scopeName()
}

func (m *kvOpManager) BucketName() string {
	return m.parent.sb.BucketName
}

func (m *kvOpManager) ValueBytes() []byte {
	return m.bytes
}

func (m *kvOpManager) ValueFlags() uint32 {
	return m.flags
}

func (m *kvOpManager) Transcoder() Transcoder {
	return m.transcoder
}

func (m *kvOpManager) DurabilityLevel() gocbcore.DurabilityLevel {
	return gocbcore.DurabilityLevel(m.durabilityLevel)
}

func (m *kvOpManager) DurabilityTimeout() uint16 {
	duraTimeout := m.deadline.Sub(time.Now()) * 10 / 9
	return uint16(duraTimeout / time.Millisecond)
}

func (m *kvOpManager) RetryStrategy() *retryStrategyWrapper {
	return m.retryStrategy
}

func (m *kvOpManager) CheckReadyForOp() error {
	if m.err != nil {
		return m.err
	}

	if m.deadline.IsZero() {
		return errors.New("op manager had no deadline specified")
	}

	return nil
}

func (m *kvOpManager) NeedsObserve() bool {
	return m.persistTo > 0 || m.replicateTo > 0
}

func (m *kvOpManager) EnhanceErr(err error) error {
	return maybeEnhanceCollKVErr(err, nil, m.parent, m.documentID)
}

func (m *kvOpManager) EnhanceMt(token gocbcore.MutationToken) *MutationToken {
	if token.VbUUID != 0 {
		return &MutationToken{
			token:      token,
			bucketName: m.BucketName(),
		}
	}

	return nil
}

func (m *kvOpManager) Reject() {
	m.signal <- struct{}{}
}

func (m *kvOpManager) Resolve(token *MutationToken) {
	m.wasResolved = true
	m.mutationToken = token
	m.signal <- struct{}{}
}

func (m *kvOpManager) Wait(op gocbcore.PendingOp, err error) error {
	if m.err != nil {
		op.Cancel(errors.New("performed operation with invalid data"))
	}

	// We do this to allow operations with no deadline to still proceed
	// without immediately timing out due to bad math on the sub time below.
	waitDeadline := m.deadline
	if waitDeadline.IsZero() {
		waitDeadline = time.Now().Add(24 * time.Hour)
	}

	select {
	case <-m.signal:
		// Good to go
	case <-m.cancelCh:
		op.Cancel(ErrRequestCanceled)
		<-m.signal
	case <-time.After(waitDeadline.Sub(time.Now())):
		// Ran out of time...
		op.Cancel(ErrAmbiguousTimeout)
		<-m.signal
	}

	if m.wasResolved && (m.persistTo > 0 || m.replicateTo > 0) {
		if m.mutationToken == nil {
			return errors.New("expected a mutation token")
		}

		return m.parent.waitForDurability(
			m.span,
			m.documentID,
			m.mutationToken.token,
			m.replicateTo,
			m.persistTo,
			m.deadline,
			m.cancelCh,
		)
	}

	return nil
}

func (c *Collection) newKvOpManager(opName string, tracectx requestSpanContext) *kvOpManager {
	span := c.startKvOpTrace(opName, tracectx)

	return &kvOpManager{
		parent: c,
		signal: make(chan struct{}, 1),
		span:   span,
	}
}

func durationToExpiry(dura time.Duration) uint32 {
	// If the duration is 0, that indicates never-expires
	if dura == 0 {
		return 0
	}

	// If the duration is less than one second, we must force the
	// value to 1 to avoid accidentally making it never expire.
	if dura < 1*time.Second {
		return 1
	}

	// Translate into a uint32 in seconds.
	return uint32(dura / time.Second)
}
