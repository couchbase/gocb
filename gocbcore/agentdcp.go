package gocbcore

import (
	"encoding/binary"
)

type SnapshotState uint32

func (s SnapshotState) HasInMemory() bool {
	return uint32(s)&1 != 0
}
func (s SnapshotState) HasOnDisk() bool {
	return uint32(s)&2 != 0
}

type StreamObserver interface {
	SnapshotMarker(startSeqNo, endSeqNo uint64, snapshotType SnapshotState)
	Mutation(seqNo, revNo uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16, key, value []byte)
	Deletion(seqNo, revNo, cas uint64, vbId uint16, key []byte)
	Expiration(seqNo, revNo, cas uint64, vbId uint16, key []byte)
	Flush()
	End(err error)
}

type FailoverEntry struct {
	VbUuid VbUuid
	SeqNo SeqNo
}

type OpenStreamCallback func(error)
type CloseStreamCallback func(error)
type GetFailoverLogCallback func([]FailoverEntry, error)
type GetLastCheckpointCallback func(SeqNo, error)

func (c *Agent) OpenStream(vbId uint16, vbUuid VbUuid, startSeqNo, endSeqNo SeqNo, evtHandler StreamObserver, cb OpenStreamCallback) (PendingOp, error) {
	streamOpened := false

	handler := func(resp *memdResponse, err error) {
		if err != nil {
			if !streamOpened {
				cb(err)
				return
			}

			// We need to shutdown the stream here as well...
			evtHandler.End(err)
			return
		}

		if resp.Magic == ResMagic {
			// This is the response to the open stream request.
			streamOpened = true
			return
		}

		// This is one of the stream events
		switch resp.Opcode {
		case CmdDcpSnapshotMarker:
			newStartSeqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			newEndSeqNo := binary.BigEndian.Uint64(resp.Extras[8:])
			snapshotType := binary.BigEndian.Uint32(resp.Extras[16:])
			evtHandler.SnapshotMarker(newStartSeqNo, newEndSeqNo, SnapshotState(snapshotType))
		case CmdDcpMutation:
			vbId := uint16(resp.Status)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			flags := binary.BigEndian.Uint32(resp.Extras[16:])
			expiry := binary.BigEndian.Uint32(resp.Extras[20:])
			lockTime := binary.BigEndian.Uint32(resp.Extras[24:])
			evtHandler.Mutation(seqNo, revNo, flags, expiry, lockTime, resp.Cas, resp.Datatype, vbId, resp.Key, resp.Value)
		case CmdDcpDeletion:
			vbId := uint16(resp.Status)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			evtHandler.Deletion(seqNo, revNo, resp.Cas, vbId, resp.Key)
		case CmdDcpExpiration:
			vbId := uint16(resp.Status)
			seqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			revNo := binary.BigEndian.Uint64(resp.Extras[8:])
			evtHandler.Expiration(seqNo, revNo, resp.Cas, vbId, resp.Key)
		case CmdDcpFlush:
		case CmdDcpStreamEnd:
			code := StreamEndStatus(binary.BigEndian.Uint32(resp.Extras[0:]))
			evtHandler.End(streamEndError{code})
		}
	}

	extraBuf := make([]byte, 48)
	binary.BigEndian.PutUint32(extraBuf[0:], 0)
	binary.BigEndian.PutUint32(extraBuf[4:], 0)
	binary.BigEndian.PutUint64(extraBuf[8:], uint64(startSeqNo))
	binary.BigEndian.PutUint64(extraBuf[16:], uint64(endSeqNo))
	binary.BigEndian.PutUint64(extraBuf[24:], uint64(vbUuid))
	binary.BigEndian.PutUint64(extraBuf[32:], uint64(startSeqNo))
	binary.BigEndian.PutUint64(extraBuf[40:], uint64(endSeqNo))

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdDcpStreamReq,
			Datatype: 0,
			Cas:      0,
			Extras:   extraBuf,
			Key:      nil,
			Value:    nil,
			Vbucket:  vbId,
		},
		Callback:   handler,
		ReplicaIdx: 0,
		Persistent: true,
	}
	return c.dispatchOp(req)
}

func (c *Agent) CloseStream(vbId uint16, cb CloseStreamCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		cb(err)
	}

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdDcpCloseStream,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
			Vbucket:  vbId,
		},
		Callback:   handler,
		ReplicaIdx: 0,
		Persistent: true,
	}
	return c.dispatchOp(req)
}

func (c *Agent) GetFailoverLog(vbId uint16, cb GetFailoverLogCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(nil, err)
			return
		}

		numEntries := len(resp.Value) / 16
		entries := make([]FailoverEntry, numEntries)
		for i := 0; i < numEntries; i++ {
			entries[i] = FailoverEntry{
				VbUuid: VbUuid(binary.BigEndian.Uint64(resp.Value[i*16+0:])),
				SeqNo: SeqNo(binary.BigEndian.Uint64(resp.Value[i*16+8:])),
			}
		}
		cb(entries, nil)
	}

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdDcpGetFailoverLog,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
			Vbucket:  vbId,
		},
		Callback:   handler,
		ReplicaIdx: 0,
		Persistent: true,
	}
	return c.dispatchOp(req)
}

func (c *Agent) GetLastCheckpoint(vbId uint16, cb GetLastCheckpointCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			cb(0, err)
			return
		}

		seqNo := SeqNo(binary.BigEndian.Uint64(resp.Value[0:]))
		cb(seqNo, err)
	}

	req := &memdQRequest{
		memdRequest: memdRequest{
			Magic:    ReqMagic,
			Opcode:   CmdGetLastCheckpoint,
			Datatype: 0,
			Cas:      0,
			Extras:   nil,
			Key:      nil,
			Value:    nil,
			Vbucket:  vbId,
		},
		Callback:   handler,
		ReplicaIdx: 0,
		Persistent: true,
	}
	return c.dispatchOp(req)
}

