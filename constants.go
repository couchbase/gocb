package gocb

import (
	"gopkg.in/couchbase/gocbcore.v5"
)

const (
	// Legacy flag format for JSON data.
	lfJson = 0

	// Common flags mask
	cfMask = 0xFF000000
	// Common flags mask for data format
	cfFmtMask = 0x0F000000
	// Common flags mask for compression mode.
	cfCmprMask = 0xE0000000

	// Common flag format for sdk-private data.
	cfFmtPrivate = 1 << 24
	// Common flag format for JSON data.
	cfFmtJson = 2 << 24
	// Common flag format for binary data.
	cfFmtBinary = 3 << 24
	// Common flag format for string data.
	cfFmtString = 4 << 24

	// Common flags compression for disabled compression.
	cfCmprNone = 0 << 29
)

// IndexType provides information on the type of indexer used for an index.
type IndexType string

const (
	// IndexTypeN1ql indicates that GSI was used to build the index.
	IndexTypeN1ql = IndexType("gsi")

	// IndexTypeView indicates that views were used to build the index.
	IndexTypeView = IndexType("views")
)

// SubdocFlag provides special handling flags for sub-document operations
type SubdocFlag gocbcore.SubdocFlag

const (
	// SubdocFlagNone indicates no special behaviours
	SubdocFlagNone = SubdocFlag(gocbcore.SubdocFlagNone)

	// SubdocFlagCreateDoc indicates you wish to create the document if it does not exist.
	SubdocFlagCreateDoc = SubdocFlag(gocbcore.SubdocFlagMkDoc)

	// SubdocFlagCreatePath indicates you wish to recursively create the tree of paths
	// if it does not already exist within the document.
	SubdocFlagCreatePath = SubdocFlag(gocbcore.SubdocFlagMkDirP)

	// SubdocFlagXattr indicates your path refers to an extended attribute rather than the document.
	SubdocFlagXattr = SubdocFlag(gocbcore.SubdocFlagXattrPath)

	// SubdocFlagUseMacros indicates that you wish macro substitution to occur on the value
	SubdocFlagUseMacros = SubdocFlag(gocbcore.SubdocFlagExpandMacros)
)
