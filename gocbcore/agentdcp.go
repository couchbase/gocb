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
	SnapshotMarker(startSeqNo, endSeqNo uint64, vbId uint16, snapshotType SnapshotState)
	Mutation(seqNo, revNo uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbId uint16, key, value []byte)
	Deletion(seqNo, revNo, cas uint64, vbId uint16, key []byte)
	Expiration(seqNo, revNo, cas uint64, vbId uint16, key []byte)
	End(vbId uint16, err error)
}

type FailoverEntry struct {
	VbUuid VbUuid
	SeqNo SeqNo
}

type OpenStreamCallback func([]FailoverEntry, error)
type CloseStreamCallback func(error)
type GetFailoverLogCallback func([]FailoverEntry, error)
type GetLastCheckpointCallback func(SeqNo, error)

func (c *Agent) OpenStream(vbId uint16, vbUuid VbUuid, startSeqNo, endSeqNo, snapStartSeqNo, snapEndSeqNo SeqNo, evtHandler StreamObserver, cb OpenStreamCallback) (PendingOp, error) {
	var req *memdQRequest
	handler := func(resp *memdResponse, err error) {
		if err != nil {
			// All client errors are handled by the StreamObserver
			cb(nil, err)
			return
		}

		if resp.Magic == ResMagic {
			// This is the response to the open stream request.

			numEntries := len(resp.Value) / 16
			entries := make([]FailoverEntry, numEntries)
			for i := 0; i < numEntries; i++ {
				entries[i] = FailoverEntry{
					VbUuid: VbUuid(binary.BigEndian.Uint64(resp.Value[i*16+0:])),
					SeqNo: SeqNo(binary.BigEndian.Uint64(resp.Value[i*16+8:])),
				}
			}

			cb(entries, nil)
			return
		}

		// This is one of the stream events
		switch resp.Opcode {
		case CmdDcpSnapshotMarker:
			vbId := uint16(resp.Status)
			newStartSeqNo := binary.BigEndian.Uint64(resp.Extras[0:])
			newEndSeqNo := binary.BigEndian.Uint64(resp.Extras[8:])
			snapshotType := binary.BigEndian.Uint32(resp.Extras[16:])
			evtHandler.SnapshotMarker(newStartSeqNo, newEndSeqNo, vbId, SnapshotState(snapshotType))
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
		case CmdDcpStreamEnd:
			vbId := uint16(resp.Status)
			code := StreamEndStatus(binary.BigEndian.Uint32(resp.Extras[0:]))
			evtHandler.End(vbId, getStreamEndError(code))
			req.Cancel()
		}
	}

	extraBuf := make([]byte, 48)
	binary.BigEndian.PutUint32(extraBuf[0:], 0)
	binary.BigEndian.PutUint32(extraBuf[4:], 0)
	binary.BigEndian.PutUint64(extraBuf[8:], uint64(startSeqNo))
	binary.BigEndian.PutUint64(extraBuf[16:], uint64(endSeqNo))
	binary.BigEndian.PutUint64(extraBuf[24:], uint64(vbUuid))
	binary.BigEndian.PutUint64(extraBuf[32:], uint64(snapStartSeqNo))
	binary.BigEndian.PutUint64(extraBuf[40:], uint64(snapEndSeqNo))

	req = &memdQRequest{
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
		Persistent: false,
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
		Persistent: false,
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

