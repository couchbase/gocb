package gocbcore

type CommandMagic uint8

const (
	ReqMagic = CommandMagic(0x80)
	ResMagic = CommandMagic(0x81)
)

// CommandCode for memcached packets.
type CommandCode uint8

const (
	CmdGet                = CommandCode(0x00)
	CmdSet                = CommandCode(0x01)
	CmdAdd                = CommandCode(0x02)
	CmdReplace            = CommandCode(0x03)
	CmdDelete             = CommandCode(0x04)
	CmdIncrement          = CommandCode(0x05)
	CmdDecrement          = CommandCode(0x06)
	CmdAppend             = CommandCode(0x0e)
	CmdPrepend            = CommandCode(0x0f)
	CmdStat               = CommandCode(0x10)
	CmdTouch              = CommandCode(0x1c)
	CmdGAT                = CommandCode(0x1d)
	CmdHello              = CommandCode(0x1f)
	CmdSASLListMechs      = CommandCode(0x20)
	CmdSASLAuth           = CommandCode(0x21)
	CmdSASLStep           = CommandCode(0x22)
	CmdDcpOpenConnection  = CommandCode(0x50)
	CmdDcpAddStream       = CommandCode(0x51)
	CmdDcpCloseStream     = CommandCode(0x52)
	CmdDcpStreamReq       = CommandCode(0x53)
	CmdDcpGetFailoverLog  = CommandCode(0x54)
	CmdDcpStreamEnd       = CommandCode(0x55)
	CmdDcpSnapshotMarker  = CommandCode(0x56)
	CmdDcpMutation        = CommandCode(0x57)
	CmdDcpDeletion        = CommandCode(0x58)
	CmdDcpExpiration      = CommandCode(0x59)
	CmdDcpFlush           = CommandCode(0x5a)
	CmdDcpSetVbucketState = CommandCode(0x5b)
	CmdDcpNoop            = CommandCode(0x5c)
	CmdDcpBufferAck       = CommandCode(0x5d)
	CmdDcpControl         = CommandCode(0x5e)
	CmdGetReplica         = CommandCode(0x83)
	CmdSelectBucket       = CommandCode(0x89)
	CmdObserveSeqNo       = CommandCode(0x91)
	CmdObserve            = CommandCode(0x92)
	CmdGetLocked          = CommandCode(0x94)
	CmdUnlockKey          = CommandCode(0x95)
	CmdGetLastCheckpoint  = CommandCode(0x97)
	CmdGetClusterConfig   = CommandCode(0xb5)
	CmdGetRandom          = CommandCode(0xb6)
)

type HelloFeature uint16

const (
	FeatureDatatype = HelloFeature(0x01)
	FeatureSeqNo = HelloFeature(0x04)
)

// Status field for memcached response.
type StatusCode uint16

const (
	StatusSuccess        = StatusCode(0x00)
	StatusKeyNotFound    = StatusCode(0x01)
	StatusKeyExists      = StatusCode(0x02)
	StatusTooBig         = StatusCode(0x03)
	StatusInvalidArgs    = StatusCode(0x04)
	StatusNotStored      = StatusCode(0x05)
	StatusBadDelta       = StatusCode(0x06)
	StatusNotMyVBucket   = StatusCode(0x07)
	StatusAuthError      = StatusCode(0x20)
	StatusAuthContinue   = StatusCode(0x21)
	StatusUnknownCommand = StatusCode(0x81)
	StatusOutOfMemory    = StatusCode(0x82)
	StatusTmpFail        = StatusCode(0x86)
)

type KeyState uint8

const (
	KeyStateNotPersisted = KeyState(0x00)
	KeyStatePersisted    = KeyState(0x01)
	KeyStateNotFound     = KeyState(0x80)
	KeyStateDeleted      = KeyState(0x81)
)

type StreamEndStatus uint32

const (
	StreamEndOK           = StreamEndStatus(0x00)
	StreamEndClosed       = StreamEndStatus(0x01)
	StreamEndStateChanged = StreamEndStatus(0x02)
	StreamEndDisconnected = StreamEndStatus(0x03)
	StreamEndTooSlow      = StreamEndStatus(0x04)
)
