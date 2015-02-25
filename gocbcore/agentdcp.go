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
	Deletion(seqNo, revNo, cas uint64, datatype uint8, vbId uint16, key []byte)
	Expiration(seqNo, revNo, cas uint64, datatype uint8, vbId uint16, key []byte)
	Flush()
	End(err error)
}

type OpenStreamCallback func(error)
type CloseStreamCallback func(error)

func (c *Agent) OpenStream(vbId uint16, vbUuid, startSeqNo, endSeqNo uint64, evtHandler StreamObserver, cb OpenStreamCallback) (PendingOp, error) {
	handler := func(resp *memdResponse, err error) {
		if resp.Magic == ReqMagic {
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
			case CmdDcpExpiration:
			case CmdDcpFlush:
			case CmdDcpStreamEnd:
			}
		} else {
			// This is the response to the open stream request.
			cb(err)
		}
	}

	extraBuf := make([]byte, 48)
	binary.BigEndian.PutUint32(extraBuf[0:], 0)
	binary.BigEndian.PutUint32(extraBuf[4:], 0)
	binary.BigEndian.PutUint64(extraBuf[8:], startSeqNo)
	binary.BigEndian.PutUint64(extraBuf[16:], endSeqNo)
	binary.BigEndian.PutUint64(extraBuf[24:], vbUuid)
	binary.BigEndian.PutUint64(extraBuf[32:], startSeqNo)
	binary.BigEndian.PutUint64(extraBuf[40:], endSeqNo)

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
		ReplicaIdx: -1,
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
		ReplicaIdx: -1,
		Persistent: true,
	}
	return c.dispatchOp(req)
}
