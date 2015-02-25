package gocb

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
