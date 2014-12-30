package couchbase

type commandMagic uint8

const (
	reqMagic = commandMagic(0x80)
	resMagic = commandMagic(0x81)
)

// commandCode for memcached packets.
type commandCode uint8

const (
	cmdGet              = commandCode(0x00)
	cmdSet              = commandCode(0x01)
	cmdAdd              = commandCode(0x02)
	cmdReplace          = commandCode(0x03)
	cmdDelete           = commandCode(0x04)
	cmdIncrement        = commandCode(0x05)
	cmdDecrement        = commandCode(0x06)
	cmdAppend           = commandCode(0x0e)
	cmdPrepend          = commandCode(0x0f)
	cmdStat             = commandCode(0x10)
	cmdTouch            = commandCode(0x1c)
	cmdGAT              = commandCode(0x1d)
	cmdSASLListMechs    = commandCode(0x20)
	cmdSASLAuth         = commandCode(0x21)
	cmdSASLStep         = commandCode(0x22)
	cmdGetReplica       = commandCode(0x83)
	cmdObserve          = commandCode(0x92)
	cmdGetLocked        = commandCode(0x94)
	cmdUnlockKey        = commandCode(0x95)
	cmdGetClusterConfig = commandCode(0xb5)
)

// Status field for memcached response.
type statusCode uint16

const (
	success        = statusCode(0x00)
	keyNotFound    = statusCode(0x01)
	keyExists      = statusCode(0x02)
	tooBig         = statusCode(0x03)
	invalidArgs    = statusCode(0x04)
	notStored      = statusCode(0x05)
	badDelta       = statusCode(0x06)
	notMyVBucket   = statusCode(0x07)
	unknownCommand = statusCode(0x81)
	outOfMemory    = statusCode(0x82)
	tmpFail        = statusCode(0x86)
)

const (
	// Legacy Flag Formats
	lfJson = 0

	// Common Flag Masks
	cfMask     = 0xFF000000
	cfFmtMask  = 0x0F000000
	cfCmprMask = 0xE0000000

	// Common Flag Formats
	cfFmtPrivate = 1 << 24
	cfFmtJson    = 2 << 24
	cfFmtBinary  = 3 << 24
	cfFmtString  = 4 << 24

	// Common Flag Compressions
	cfCmprNone = 0 << 29
)
