package gocbcore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	noManifestUid = uint64(0xFFFFFFFFFFFFFFFF)
	noScopeId     = uint32(0xFFFFFFFF)
	noStreamId    = uint16(0xFFFF)
)

// SnapshotState represents the state of a particular cluster snapshot.
type SnapshotState uint32

// HasInMemory returns whether this snapshot is available in memory.
func (s SnapshotState) HasInMemory() bool {
	return uint32(s)&1 != 0
}

// HasOnDisk returns whether this snapshot is available on disk.
func (s SnapshotState) HasOnDisk() bool {
	return uint32(s)&2 != 0
}

// FailoverEntry represents a single entry in the server fail-over log.
type FailoverEntry struct {
	VbUuid VbUuid
	SeqNo  SeqNo
}

// StreamObserver provides an interface to receive events from a running DCP stream.
type StreamObserver interface {
	SnapshotMarker(startSeqNo, endSeqNo uint64, vbId uint16, streamId uint16, snapshotType SnapshotState)
	Mutation(seqNo, revNo uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16, collectionId uint32, streamId uint16, key, value []byte)
	Deletion(seqNo, revNo, cas uint64, datatype uint8, vbId uint16, collectionId uint32, streamId uint16, key, value []byte)
	Expiration(seqNo, revNo, cas uint64, vbId uint16, collectionId uint32, streamId uint16, key []byte)
	End(vbId uint16, streamId uint16, err error)
	CreateCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32, collectionId uint32, ttl uint32, streamId uint16, key []byte)
	DeleteCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32, collectionId uint32, streamId uint16)
	FlushCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, collectionId uint32)
	CreateScope(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32, streamId uint16, key []byte)
	DeleteScope(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, scopeId uint32, streamId uint16)
	ModifyCollection(seqNo uint64, version uint8, vbId uint16, manifestUid uint64, collectionId uint32, ttl uint32, streamId uint16)
}

// NewStreamFilter returns a new StreamFilter.
func NewStreamFilter() *StreamFilter {
	return &StreamFilter{
		ManifestUid: noManifestUid,
		Scope:       noScopeId,
		StreamId:    noStreamId,
	}
}

// StreamFilter provides options for filtering a DCP stream.
type StreamFilter struct {
	ManifestUid uint64
	Collections []uint32
	Scope       uint32
	StreamId    uint16
}

type streamFilter struct {
	ManifestUid string   `json:"uid,omitempty"`
	Collections []string `json:"collections,omitempty"`
	Scope       string   `json:"scope,omitempty"`
	StreamId    uint16   `json:"sid,omitempty"`
}

// OpenStreamCallback is invoked with the results of `OpenStream` operations.
type OpenStreamCallback func([]FailoverEntry, error)

// CloseStreamCallback is invoked with the results of `CloseStream` operations.
type CloseStreamCallback func(error)

// GetFailoverLogCallback is invoked with the results of `GetFailoverLog` operations.
type GetFailoverLogCallback func([]FailoverEntry, error)

// VbSeqNoEntry represents a single GetVbucketSeqnos sequence number entry.
type VbSeqNoEntry struct {
	VbId  uint16
	SeqNo SeqNo
}

// GetVBucketSeqnosCallback is invoked with the results of `GetVBucketSeqnos` operations.
type GetVBucketSeqnosCallback func([]VbSeqNoEntry, error)

// OpenStream opens a DCP stream for a particular VBucket and, optionally, filter.
func (agent *Agent) OpenStream(vbId uint16, flags DcpStreamAddFlag, vbUuid VbUuid, startSeqNo,
	endSeqNo, snapStartSeqNo, snapEndSeqNo SeqNo, evtHandler StreamObserver, filter *StreamFilter, cb OpenStreamCallback) (PendingOp, error) {
	var req *memdQRequest
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if resp != nil && resp.Magic == resMagic {
			// This is the response to the open stream request.
			if err != nil {
				req.Cancel()

				// All client errors are handled by the StreamObserver
				cb(nil, err)
				return
			}

			numEntries := len(resp.Value) / 16
			entries := make([]FailoverEntry, numEntries)
			for i := 0; i < numEntries; i++ {
				entries[i] = FailoverEntry{
					VbUuid: VbUuid(binary.BigEndian.Uint64(resp.Value[i*16+0:])),
					SeqNo:  SeqNo(binary.BigEndian.Uint64(resp.Value[i*16+8:])),
				}
			}

			cb(entries, nil)
			return
		}

		if err != nil {
			req.Cancel()
			evtHandler.End(vbId, filter.StreamId, err)
			return
		}

		// This is one of the stream events
		switch resp.Opcode {
		case cmdDcpSnapshotMarker:
			vbId := uint16(resp.Vbucket)
			newStartSeqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			newEndSeqNo := binary.BigEndian.Uint64(resp.Extras[8:])
			snapshotType := binary.BigEndian.Uint32(resp.Extras[16:])
			var streamId uint16
			if resp.FrameExtras != nil && resp.FrameExtras.HasStreamId {
				streamId = resp.FrameExtras.StreamId
			}
			evtHandler.SnapshotMarker(newStartSeqNo, newEndSeqNo, vbId, streamId, SnapshotState(snapshotType))
		case cmdDcpMutation:
			vbId := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			flags := binary.BigEndian.Uint32(resp.Extras[16:])
			expiry := binary.BigEndian.Uint32(resp.Extras[20:])
			lockTime := binary.BigEndian.Uint32(resp.Extras[24:])
			var streamId uint16
			if resp.FrameExtras != nil && resp.FrameExtras.HasStreamId {
				streamId = resp.FrameExtras.StreamId
			}
			evtHandler.Mutation(seqNo, revNo, flags, expiry, lockTime, resp.Cas, resp.Datatype, vbId, resp.CollectionID, streamId, resp.Key, resp.Value)
		case cmdDcpDeletion:
			vbId := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			var streamId uint16
			if resp.FrameExtras != nil && resp.FrameExtras.HasStreamId {
				streamId = resp.FrameExtras.StreamId
			}
			evtHandler.Deletion(seqNo, revNo, resp.Cas, resp.Datatype, vbId, resp.CollectionID, streamId, resp.Key, resp.Value)
		case cmdDcpExpiration:
			vbId := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			var streamId uint16
			if resp.FrameExtras != nil && resp.FrameExtras.HasStreamId {
				streamId = resp.FrameExtras.StreamId
			}
			evtHandler.Expiration(seqNo, revNo, resp.Cas, vbId, resp.CollectionID, streamId, resp.Key)
		case cmdDcpEvent:
			vbId := uint16(resp.Vbucket)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			eventCode := StreamEventCode(binary.BigEndian.Uint32(resp.Extras[8:]))
			version := resp.Extras[12]
			var streamId uint16
			if resp.FrameExtras != nil && resp.FrameExtras.HasStreamId {
				streamId = resp.FrameExtras.StreamId
			}

			switch eventCode {
			case StreamEventCollectionCreate:
				manifestUid := binary.BigEndian.Uint64(resp.Value[0:])
				scopeId := binary.BigEndian.Uint32(resp.Value[8:])
				collectionId := binary.BigEndian.Uint32(resp.Value[12:])
				var ttl uint32
				if version == 1 {
					ttl = binary.BigEndian.Uint32(resp.Value[16:])
				}
				evtHandler.CreateCollection(seqNo, version, vbId, manifestUid, scopeId, collectionId, ttl, streamId, resp.Key)
			case StreamEventCollectionDelete:
				manifestUid := binary.BigEndian.Uint64(resp.Value[0:])
				scopeId := binary.BigEndian.Uint32(resp.Value[8:])
				collectionId := binary.BigEndian.Uint32(resp.Value[12:])
				evtHandler.DeleteCollection(seqNo, version, vbId, manifestUid, scopeId, collectionId, streamId)
			case StreamEventCollectionFlush:
				manifestUid := binary.BigEndian.Uint64(resp.Value[0:])
				collectionId := binary.BigEndian.Uint32(resp.Value[8:])
				evtHandler.FlushCollection(seqNo, version, vbId, manifestUid, collectionId)
			case StreamEventScopeCreate:
				manifestUid := binary.BigEndian.Uint64(resp.Value[0:])
				scopeId := binary.BigEndian.Uint32(resp.Value[8:])
				evtHandler.CreateScope(seqNo, version, vbId, manifestUid, scopeId, streamId, resp.Key)
			case StreamEventScopeDelete:
				manifestUid := binary.BigEndian.Uint64(resp.Value[0:])
				scopeId := binary.BigEndian.Uint32(resp.Value[8:])
				evtHandler.DeleteScope(seqNo, version, vbId, manifestUid, scopeId, streamId)
			case StreamEventCollectionChanged:
				manifestUid := binary.BigEndian.Uint64(resp.Value[0:])
				collectionId := binary.BigEndian.Uint32(resp.Value[8:])
				ttl := binary.BigEndian.Uint32(resp.Value[12:])
				evtHandler.ModifyCollection(seqNo, version, vbId, manifestUid, collectionId, ttl, streamId)
			}
		case cmdDcpStreamEnd:
			vbId := uint16(resp.Vbucket)
			code := streamEndStatus(binary.BigEndian.Uint32(resp.Extras[0:]))
			var streamId uint16
			if resp.FrameExtras != nil && resp.FrameExtras.HasStreamId {
				streamId = resp.FrameExtras.StreamId
			}
			evtHandler.End(vbId, streamId, getStreamEndError(code))
			req.Cancel()
		}
	}

	extraBuf := make([]byte, 48)
	binary.BigEndian.PutUint32(extraBuf[0:], uint32(flags))
	binary.BigEndian.PutUint32(extraBuf[4:], 0)
	binary.BigEndian.PutUint64(extraBuf[8:], uint64(startSeqNo))
	binary.BigEndian.PutUint64(extraBuf[16:], uint64(endSeqNo))
	binary.BigEndian.PutUint64(extraBuf[24:], uint64(vbUuid))
	binary.BigEndian.PutUint64(extraBuf[32:], uint64(snapStartSeqNo))
	binary.BigEndian.PutUint64(extraBuf[40:], uint64(snapEndSeqNo))

	var val []byte
	val = nil
	if filter != nil {
		convertedFilter := streamFilter{}
		for _, cid := range filter.Collections {
			convertedFilter.Collections = append(convertedFilter.Collections, fmt.Sprintf("%x", cid))
		}
		if filter.Scope != noScopeId {
			convertedFilter.Scope = fmt.Sprintf("%x", filter.Scope)
		}
		if filter.ManifestUid != noManifestUid {
			convertedFilter.ManifestUid = fmt.Sprintf("%x", filter.ManifestUid)
		}
		if filter.StreamId != noStreamId {
			convertedFilter.StreamId = filter.StreamId
		}
		var err error
		val, err = json.Marshal(convertedFilter)
		if err != nil {
			return nil, err
		}
	}

	req = &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdDcpStreamReq,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      nil,
			Value:    val,
			Vbucket:  vbId,
		},
		Callback:   handler,
		ReplicaIdx: 0,
		Persistent: true,
	}
	return agent.dispatchOp(req)
}

// CloseStreamWithId shuts down an open stream for the specified VBucket for the specified stream.
func (agent *Agent) CloseStreamWithId(vbId uint16, streamId uint16, cb CloseStreamCallback) (PendingOp, error) {
	handler := func(_ *memdQResponse, _ *memdQRequest, err error) {
		cb(err)
	}

	frameExtras := &memdFrameExtras{
		HasStreamId: true,
		StreamId:    streamId,
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:       altReqMagic,
			Opcode:      cmdDcpCloseStream,
			Datatype:    0,
			Cas:         0,
			Extras:      nil,
			Key:         nil,
			Value:       nil,
			Vbucket:     vbId,
			FrameExtras: frameExtras,
		},
		Callback:   handler,
		ReplicaIdx: 0,
		Persistent: false,
	}
	return agent.dispatchOp(req)
}

// CloseStream shuts down an open stream for the specified VBucket.
func (agent *Agent) CloseStream(vbId uint16, cb CloseStreamCallback) (PendingOp, error) {
	handler := func(_ *memdQResponse, _ *memdQRequest, err error) {
		cb(err)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdDcpCloseStream,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
			Vbucket:  vbId,
		},
		Callback:   handler,
		ReplicaIdx: 0,
		Persistent: false,
	}
	return agent.dispatchOp(req)
}

// GetFailoverLog retrieves the fail-over log for a particular VBucket.  This is used
// to resume an interrupted stream after a node fail-over has occurred.
func (agent *Agent) GetFailoverLog(vbId uint16, cb GetFailoverLogCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		numEntries := len(resp.Value) / 16
		entries := make([]FailoverEntry, numEntries)
		for i := 0; i < numEntries; i++ {
			entries[i] = FailoverEntry{
				VbUuid: VbUuid(binary.BigEndian.Uint64(resp.Value[i*16+0:])),
				SeqNo:  SeqNo(binary.BigEndian.Uint64(resp.Value[i*16+8:])),
			}
		}
		cb(entries, nil)
	}

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdDcpGetFailoverLog,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
			Vbucket:  vbId,
		},
		Callback:   handler,
		ReplicaIdx: 0,
		Persistent: false,
	}
	return agent.dispatchOp(req)
}

// GetVbucketSeqnosWithCollectionId returns the last checkpoint for a particular VBucket for a particular collection. This is useful
// for starting a DCP stream from wherever the server currently is.
func (agent *Agent) GetVbucketSeqnosWithCollectionId(serverIdx int, state VbucketState, collectionId uint32, cb GetVBucketSeqnosCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		var vbs []VbSeqNoEntry

		numVbs := len(resp.Value) / 10
		for i := 0; i < numVbs; i++ {
			vbs = append(vbs, VbSeqNoEntry{
				VbId:  binary.BigEndian.Uint16(resp.Value[i*10:]),
				SeqNo: SeqNo(binary.BigEndian.Uint64(resp.Value[i*10+2:])),
			})
		}

		cb(vbs, nil)
	}

	extraBuf := make([]byte, 8)
	binary.BigEndian.PutUint32(extraBuf[0:], uint32(state))
	binary.BigEndian.PutUint32(extraBuf[4:], collectionId)

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdGetAllVBSeqnos,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      nil,
			Value:    nil,
			Vbucket:  0,
		},
		Callback:   handler,
		ReplicaIdx: -serverIdx,
		Persistent: false,
	}

	return agent.dispatchOp(req)
}

// GetVbucketSeqnos returns the last checkpoint for a particular VBucket.  This is useful
// for starting a DCP stream from wherever the server currently is.
func (agent *Agent) GetVbucketSeqnos(serverIdx int, state VbucketState, cb GetVBucketSeqnosCallback) (PendingOp, error) {
	handler := func(resp *memdQResponse, _ *memdQRequest, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		var vbs []VbSeqNoEntry

		numVbs := len(resp.Value) / 10
		for i := 0; i < numVbs; i++ {
			vbs = append(vbs, VbSeqNoEntry{
				VbId:  binary.BigEndian.Uint16(resp.Value[i*10:]),
				SeqNo: SeqNo(binary.BigEndian.Uint64(resp.Value[i*10+2:])),
			})
		}

		cb(vbs, nil)
	}

	extraBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(extraBuf[0:], uint32(state))

	req := &memdQRequest{
		memdPacket: memdPacket{
			Magic:    reqMagic,
			Opcode:   cmdGetAllVBSeqnos,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      nil,
			Value:    nil,
			Vbucket:  0,
		},
		Callback:   handler,
		ReplicaIdx: -serverIdx,
		Persistent: false,
	}

	return agent.dispatchOp(req)
}
