package gocbcore

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"io"
	"math"
	"net"
	"time"
)

type memdFrameExtras struct {
	HasSrvDuration         bool
	SrvDuration            time.Duration
	HasStreamId            bool
	StreamId               uint16
	DurabilityLevel        DurabilityLevel
	DurabilityLevelTimeout uint16
}

type memdPacket struct {
	Magic        commandMagic
	Opcode       commandCode
	Datatype     uint8
	Status       StatusCode
	Vbucket      uint16
	Opaque       uint32
	Cas          uint64
	Key          []byte
	Extras       []byte
	Value        []byte
	CollectionID uint32

	FrameExtras *memdFrameExtras
}

type memdConn interface {
	LocalAddr() string
	RemoteAddr() string
	WritePacket(*memdPacket) error
	ReadPacket(*memdPacket) error
	Close() error
	EnableFramingExtras(bool)
	EnableCollections(bool)
}

type memdTcpConn struct {
	conn             io.ReadWriteCloser
	reader           *bufio.Reader
	headerBuf        []byte
	localAddr        string
	remoteAddr       string
	useFramingExtras bool
	useCollections   bool
}

func dialMemdConn(address string, tlsConfig *tls.Config, deadline time.Time) (memdConn, error) {
	d := net.Dialer{
		Deadline: deadline,
	}

	baseConn, err := d.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	tcpConn, isTcpConn := baseConn.(*net.TCPConn)
	if !isTcpConn || tcpConn == nil {
		return nil, ErrCliInternalError
	}

	err = tcpConn.SetNoDelay(false)
	if err != nil {
		logWarnf("Failed to disable TCP nodelay (%s)", err)
	}

	var conn io.ReadWriteCloser
	if tlsConfig == nil {
		conn = tcpConn
	} else {
		tlsConn := tls.Client(tcpConn, tlsConfig)
		err = tlsConn.Handshake()
		if err != nil {
			return nil, err
		}

		conn = tlsConn
	}

	return &memdTcpConn{
		conn:       conn,
		reader:     bufio.NewReader(conn),
		headerBuf:  make([]byte, 24),
		localAddr:  baseConn.LocalAddr().String(),
		remoteAddr: address,
	}, nil
}

func (s *memdTcpConn) LocalAddr() string {
	return s.localAddr
}

func (s *memdTcpConn) RemoteAddr() string {
	return s.remoteAddr
}

func (s *memdTcpConn) Close() error {
	return s.conn.Close()
}

func (s *memdTcpConn) WritePacket(req *memdPacket) error {
	encodedKey := req.Key
	if s.useCollections {
		if supported, ok := cidSupportedOps[req.Opcode]; ok && supported {
			encodedKey = leb128EncodeKey(req.Key, req.CollectionID)
		}

		// this is a special case where the key is actually in value
		if req.Opcode == cmdObserve {
			keyLen := int(binary.BigEndian.Uint16(req.Value[2:]))
			key := req.Value[4 : keyLen+4]
			observeEncodedKey := leb128EncodeKey(key, req.CollectionID)
			keyLen = len(observeEncodedKey)

			valueBuf := make([]byte, 2+2+keyLen)
			binary.BigEndian.PutUint16(valueBuf[0:], req.Vbucket)
			binary.BigEndian.PutUint16(valueBuf[2:], uint16(keyLen))
			copy(valueBuf[4:], observeEncodedKey)

			req.Value = valueBuf
		}
	} else {
		if req.CollectionID > 0 {
			return ErrCollectionsUnsupported
		}
	}

	extLen := len(req.Extras)
	keyLen := len(encodedKey)
	valLen := len(req.Value)
	var frameLen int
	if req.FrameExtras != nil {
		if req.FrameExtras.HasStreamId {
			frameLen += 3
		}

		if req.FrameExtras.DurabilityLevel > 0 {
			frameLen += 2

			if req.FrameExtras.DurabilityLevelTimeout > 0 {
				frameLen += 2
			}
		}
	}

	// Go appears to do some clever things in regards to writing data
	//   to the kernel for network dispatch.  Having a write buffer
	//   per-server that is re-used actually hinders performance...
	// For now, we will simply create a new buffer and let it be GC'd.
	buffer := make([]byte, 24+keyLen+extLen+valLen+frameLen)

	buffer[0] = uint8(req.Magic)
	buffer[1] = uint8(req.Opcode)
	if req.FrameExtras != nil {
		buffer[2] = uint8(frameLen)
		buffer[3] = uint8(keyLen)
	} else {
		binary.BigEndian.PutUint16(buffer[2:], uint16(keyLen))
	}
	buffer[4] = byte(extLen)
	buffer[5] = req.Datatype
	if req.Magic != resMagic {
		binary.BigEndian.PutUint16(buffer[6:], uint16(req.Vbucket))
	} else {
		binary.BigEndian.PutUint16(buffer[6:], uint16(req.Status))
	}
	binary.BigEndian.PutUint32(buffer[8:], uint32(len(buffer)-24))
	binary.BigEndian.PutUint32(buffer[12:], req.Opaque)
	binary.BigEndian.PutUint64(buffer[16:], req.Cas)

	extrasStart := 24
	if req.FrameExtras != nil {
		if req.FrameExtras.HasStreamId {
			buffer[extrasStart] = byte(((streamIdFrameExtra & 0xF) << 4) | (2 & 0xF))

			binary.BigEndian.PutUint16(buffer[extrasStart+1:], uint16(req.FrameExtras.StreamId))
			extrasStart = extrasStart + 3
		}

		if req.FrameExtras.DurabilityLevel > 0 {
			duraFrameLen := 1
			if req.FrameExtras.DurabilityLevelTimeout > 0 {
				duraFrameLen = 3
			}
			buffer[extrasStart] = byte(((int(enhancedDurabilityFrameExtra) & 0xF) << 4) | (duraFrameLen & 0xF))
			extrasStart = extrasStart + 1
			buffer[extrasStart] = byte(req.FrameExtras.DurabilityLevel)
			extrasStart = extrasStart + 1

			if req.FrameExtras.DurabilityLevelTimeout > 0 {
				binary.BigEndian.PutUint16(buffer[extrasStart:], uint16(req.FrameExtras.DurabilityLevelTimeout))
				extrasStart = extrasStart + 2
			}
		}
	}
	copy(buffer[extrasStart:], req.Extras)
	copy(buffer[extrasStart+extLen:], encodedKey)
	copy(buffer[extrasStart+extLen+keyLen:], req.Value)

	_, err := s.conn.Write(buffer)
	return err
}

func (s *memdTcpConn) readFullBuffer(buf []byte) error {
	for len(buf) > 0 {
		r, err := s.reader.Read(buf)
		if err != nil {
			return err
		}

		if r >= len(buf) {
			break
		}

		buf = buf[r:]
	}

	return nil
}

func (s *memdTcpConn) ReadPacket(resp *memdPacket) error {
	err := s.readFullBuffer(s.headerBuf)
	if err != nil {
		return err
	}

	bodyLen := int(binary.BigEndian.Uint32(s.headerBuf[8:]))

	bodyBuf := make([]byte, bodyLen)
	err = s.readFullBuffer(bodyBuf)
	if err != nil {
		return err
	}

	resp.Magic = commandMagic(s.headerBuf[0])
	resp.Opcode = commandCode(s.headerBuf[1])
	resp.Datatype = s.headerBuf[5]
	if resp.Magic == resMagic || resp.Magic == altResMagic {
		resp.Status = StatusCode(binary.BigEndian.Uint16(s.headerBuf[6:]))
	} else {
		resp.Vbucket = binary.BigEndian.Uint16(s.headerBuf[6:])
	}
	resp.Opaque = binary.BigEndian.Uint32(s.headerBuf[12:])
	resp.Cas = binary.BigEndian.Uint64(s.headerBuf[16:])

	var keyLen int
	var frameExtrasLen int
	if resp.Magic == altResMagic {
		resp.Magic = resMagic
		keyLen = int(s.headerBuf[3])
		frameExtrasLen = int(s.headerBuf[2])
	} else if resp.Magic == altReqMagic {
		keyLen = int(s.headerBuf[3])
		frameExtrasLen = int(s.headerBuf[2])
	} else {
		keyLen = int(binary.BigEndian.Uint16(s.headerBuf[2:]))
	}
	extLen := int(s.headerBuf[4])

	if frameExtrasLen > 0 {
		resp.FrameExtras = &memdFrameExtras{}

		frameExtras := bodyBuf[:frameExtrasLen]
		framePos := 0
		for framePos < len(frameExtras) {
			extraHeader := frameExtras[framePos]
			framePos++
			extraType := frameExtraType((extraHeader & 0xF0) >> 4)
			if extraType == 15 {
				extraType = 15 + frameExtraType(frameExtras[framePos])
				framePos++
			}
			extraLen := int(extraHeader & 0x0F)
			if extraLen == 15 {
				extraLen = int(15 + frameExtras[framePos])
				framePos++
			}

			extraBody := frameExtras[framePos : framePos+extraLen]
			framePos = framePos + extraLen

			if extraType == srvDurationFrameExtra {
				srvDurEncoded := binary.BigEndian.Uint16(extraBody)
				srvDurMicros := math.Pow(float64(srvDurEncoded), 1.74) / 2

				resp.FrameExtras.HasSrvDuration = true
				resp.FrameExtras.SrvDuration = time.Duration(srvDurMicros) * time.Microsecond
			} else if extraType == streamIdFrameExtra {
				resp.FrameExtras.HasStreamId = true
				resp.FrameExtras.StreamId = binary.BigEndian.Uint16(extraBody)
			}
		}
	}

	resp.Extras = bodyBuf[frameExtrasLen : frameExtrasLen+extLen]
	var collectionIdLen int
	if s.useCollections {
		var n uint8
		// Some operations do not encode the cid in the key
		switch resp.Opcode {
		case cmdDcpEvent:
			break
		default:
			resp.CollectionID, n = decodeleb128_32(bodyBuf[frameExtrasLen+extLen : frameExtrasLen+extLen+keyLen])
			collectionIdLen = int(n)
		}
	}
	resp.Key = bodyBuf[frameExtrasLen+extLen+collectionIdLen : frameExtrasLen+extLen+keyLen]
	resp.Value = bodyBuf[frameExtrasLen+extLen+keyLen:]
	return nil
}

func (s *memdTcpConn) EnableFramingExtras(use bool) {
	s.useFramingExtras = use
}

func (s *memdTcpConn) EnableCollections(use bool) {
	s.useCollections = use
}

var cidSupportedOps = map[commandCode]bool{
	cmdGet:                  true,
	cmdSet:                  true,
	cmdAdd:                  true,
	cmdReplace:              true,
	cmdDelete:               true,
	cmdIncrement:            true,
	cmdDecrement:            true,
	cmdAppend:               true,
	cmdPrepend:              true,
	cmdTouch:                true,
	cmdGAT:                  true,
	cmdGetReplica:           true,
	cmdGetLocked:            true,
	cmdUnlockKey:            true,
	cmdGetMeta:              true,
	cmdSetMeta:              true,
	cmdDelMeta:              true,
	cmdSubDocGet:            true,
	cmdSubDocExists:         true,
	cmdSubDocDictAdd:        true,
	cmdSubDocDictSet:        true,
	cmdSubDocDelete:         true,
	cmdSubDocReplace:        true,
	cmdSubDocArrayPushLast:  true,
	cmdSubDocArrayPushFirst: true,
	cmdSubDocArrayInsert:    true,
	cmdSubDocArrayAddUnique: true,
	cmdSubDocCounter:        true,
	cmdSubDocMultiLookup:    true,
	cmdSubDocMultiMutation:  true,
	cmdSubDocGetCount:       true,
}
