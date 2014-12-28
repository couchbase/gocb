package couchbase

type commandMagic uint8

const (
	REQ_MAGIC = commandMagic(0x80)
	RES_MAGIC = commandMagic(0x81)
)

// commandCode for memcached packets.
type commandCode uint8

const (
	GET        = commandCode(0x00)
	SET        = commandCode(0x01)
	ADD        = commandCode(0x02)
	REPLACE    = commandCode(0x03)
	DELETE     = commandCode(0x04)
	INCREMENT  = commandCode(0x05)
	DECREMENT  = commandCode(0x06)
	QUIT       = commandCode(0x07)
	FLUSH      = commandCode(0x08)
	GETQ       = commandCode(0x09)
	NOOP       = commandCode(0x0a)
	VERSION    = commandCode(0x0b)
	GETK       = commandCode(0x0c)
	GETKQ      = commandCode(0x0d)
	APPEND     = commandCode(0x0e)
	PREPEND    = commandCode(0x0f)
	STAT       = commandCode(0x10)
	SETQ       = commandCode(0x11)
	ADDQ       = commandCode(0x12)
	REPLACEQ   = commandCode(0x13)
	DELETEQ    = commandCode(0x14)
	INCREMENTQ = commandCode(0x15)
	DECREMENTQ = commandCode(0x16)
	QUITQ      = commandCode(0x17)
	FLUSHQ     = commandCode(0x18)
	APPENDQ    = commandCode(0x19)
	PREPENDQ   = commandCode(0x1a)
	VERBOSITY  = commandCode(0x1b)
	TOUCH      = commandCode(0x1c)
	GAT        = commandCode(0x1d)
	GATQ       = commandCode(0x1e)

	SASL_LIST_MECHS = commandCode(0x20)
	SASL_AUTH       = commandCode(0x21)
	SASL_STEP       = commandCode(0x22)

	RGET      = commandCode(0x30)
	RSET      = commandCode(0x31)
	RSETQ     = commandCode(0x32)
	RAPPEND   = commandCode(0x33)
	RAPPENDQ  = commandCode(0x34)
	RPREPEND  = commandCode(0x35)
	RPREPENDQ = commandCode(0x36)
	RDELETE   = commandCode(0x37)
	RDELETEQ  = commandCode(0x38)
	RINCR     = commandCode(0x39)
	RINCRQ    = commandCode(0x3a)
	RDECR     = commandCode(0x3b)
	RDECRQ    = commandCode(0x3c)

	TAP_CONNECT          = commandCode(0x40) // Client-sent request to initiate Tap feed
	TAP_MUTATION         = commandCode(0x41) // Notification of a SET/ADD/REPLACE/etc. on the server
	TAP_DELETE           = commandCode(0x42) // Notification of a DELETE on the server
	TAP_FLUSH            = commandCode(0x43) // Replicates a flush_all command
	TAP_OPAQUE           = commandCode(0x44) // Opaque control data from the engine
	TAP_VBUCKET_SET      = commandCode(0x45) // Sets state of vbucket in receiver (used in takeover)
	TAP_CHECKPOINT_START = commandCode(0x46) // Notifies start of new checkpoint
	TAP_CHECKPOINT_END   = commandCode(0x47) // Notifies end of checkpoint

	UPR_OPEN        = commandCode(0x50) // Open a UPR connection with a name
	UPR_ADDSTREAM   = commandCode(0x51) // Sent by ebucketMigrator to UPR Consumer
	UPR_CLOSESTREAM = commandCode(0x52) // Sent by eBucketMigrator to UPR Consumer
	UPR_FAILOVERLOG = commandCode(0x54) // Request failover logs
	UPR_STREAMREQ   = commandCode(0x53) // Stream request from consumer to producer
	UPR_STREAMEND   = commandCode(0x55) // Sent by producer when it has no more messages to stream
	UPR_SNAPSHOT    = commandCode(0x56) // Start of a new snapshot
	UPR_MUTATION    = commandCode(0x57) // Key mutation
	UPR_DELETION    = commandCode(0x58) // Key deletion
	UPR_EXPIRATION  = commandCode(0x59) // Key expiration
	UPR_FLUSH       = commandCode(0x5a) // Delete all the data for a vbucket
	UPR_NOOP        = commandCode(0x5c) // UPR NOOP
	UPR_BUFFERACK   = commandCode(0x5d) // UPR Buffer Acknowledgement
	UPR_CONTROL     = commandCode(0x5e) // Set flow control params

	GET_REPLICA = commandCode(0x83)

	SELECT_BUCKET = commandCode(0x89) // Select bucket

	OBSERVE = commandCode(0x92)

	GET_LOCKED = commandCode(0x94)
	UNLOCK_KEY = commandCode(0x95)

	GET_CLUSTER_CONFIG = commandCode(0xb5) // GET CCCP CONFIG
)

// Status field for memcached response.
type statusCode uint16

const (
	SUCCESS         = statusCode(0x00)
	KEY_ENOENT      = statusCode(0x01)
	KEY_EEXISTS     = statusCode(0x02)
	E2BIG           = statusCode(0x03)
	EINVAL          = statusCode(0x04)
	NOT_STORED      = statusCode(0x05)
	DELTA_BADVAL    = statusCode(0x06)
	NOT_MY_VBUCKET  = statusCode(0x07)
	ERANGE          = statusCode(0x22)
	ROLLBACK        = statusCode(0x23)
	UNKNOWN_COMMAND = statusCode(0x81)
	ENOMEM          = statusCode(0x82)
	TMPFAIL         = statusCode(0x86)
)

const (
	// Legacy Flag Formats
	LF_JSON = 0

	// Common Flag Masks
	CF_MASK      = 0xFF000000
	CF_FMT_MASK  = 0x0F000000
	CF_CMPR_MASK = 0xE0000000

	// Common Flag Formats
	CF_FMT_PRIVATE = 1 << 24
	CF_FMT_JSON    = 2 << 24
	CF_FMT_BINARY  = 3 << 24
	CF_FMT_STRING  = 4 << 24

	// Common Flag Compressions
	CF_CMPR_NONE = 0 << 29
)
