package gocb

import gocbcore "github.com/couchbase/gocbcore/v8"

const (
	goCbVersionStr = "v2.0.0-beta.1"

	persistenceTimeoutFloor = 1500
)

// QueryIndexType provides information on the type of indexer used for an index.
type QueryIndexType string

const (
	// QueryIndexTypeN1ql indicates that GSI was used to build the index.
	QueryIndexTypeN1ql = QueryIndexType("gsi")

	// QueryIndexTypeView indicates that views were used to build the index.
	QueryIndexTypeView = QueryIndexType("views")
)

// ServiceType specifies a particular Couchbase service type.
type ServiceType gocbcore.ServiceType

const (
	// MgmtService represents a management service (typically ns_server).
	MgmtService = ServiceType(gocbcore.MgmtService)

	// KeyValueService represents a memcached service.
	KeyValueService = ServiceType(gocbcore.MemdService)

	// CapiService represents a CouchAPI service (typically for views).
	CapiService = ServiceType(gocbcore.CapiService)

	// QueryService represents a N1QL service (typically for query).
	QueryService = ServiceType(gocbcore.N1qlService)

	// SearchService represents a full-text-search service.
	SearchService = ServiceType(gocbcore.FtsService)

	// AnalyticsService represents an analytics service.
	AnalyticsService = ServiceType(gocbcore.CbasService)
)

// QueryProfileType specifies the profiling mode to use during a query.
type QueryProfileType string

const (
	// QueryProfileNone disables query profiling
	QueryProfileNone = QueryProfileType("off")

	// QueryProfilePhases includes phase profiling information in the query response
	QueryProfilePhases = QueryProfileType("phases")

	// QueryProfileTimings includes timing profiling information in the query response
	QueryProfileTimings = QueryProfileType("timings")
)

// SubdocFlag provides special handling flags for sub-document operations
type SubdocFlag gocbcore.SubdocFlag

const (
	// SubdocFlagNone indicates no special behaviours
	SubdocFlagNone = SubdocFlag(gocbcore.SubdocFlagNone)

	// SubdocFlagCreatePath indicates you wish to recursively create the tree of paths
	// if it does not already exist within the document.
	SubdocFlagCreatePath = SubdocFlag(gocbcore.SubdocFlagMkDirP)

	// SubdocFlagXattr indicates your path refers to an extended attribute rather than the document.
	SubdocFlagXattr = SubdocFlag(gocbcore.SubdocFlagXattrPath)

	// SubdocFlagUseMacros indicates that you wish macro substitution to occur on the value
	SubdocFlagUseMacros = SubdocFlag(gocbcore.SubdocFlagExpandMacros)
)

// SubdocDocFlag specifies document-level flags for a sub-document operation.
type SubdocDocFlag gocbcore.SubdocDocFlag

const (
	// SubdocDocFlagNone indicates no special behaviours
	SubdocDocFlagNone = SubdocDocFlag(gocbcore.SubdocDocFlagNone)

	// SubdocDocFlagMkDoc indicates that the document should be created if it does not already exist.
	SubdocDocFlagMkDoc = SubdocDocFlag(gocbcore.SubdocDocFlagMkDoc)

	// SubdocDocFlagAddDoc indices that the document should be created only if it does not already exist.
	SubdocDocFlagAddDoc = SubdocDocFlag(gocbcore.SubdocDocFlagAddDoc)

	// SubdocDocFlagAccessDeleted indicates that you wish to receive soft-deleted documents.
	SubdocDocFlagAccessDeleted = SubdocDocFlag(gocbcore.SubdocDocFlagAccessDeleted)
)

// DurabilityLevel specifies the level of synchronous replication to use.
type DurabilityLevel uint8

const (
	// DurabilityLevelMajority specifies that a mutation must be replicated (held in memory) to a majority of nodes.
	DurabilityLevelMajority = DurabilityLevel(1)

	// DurabilityLevelMajorityAndPersistOnMaster specifies that a mutation must be replicated (held in memory) to a
	// majority of nodes and also persisted (written to disk) on the active node.
	DurabilityLevelMajorityAndPersistOnMaster = DurabilityLevel(2)

	// DurabilityLevelPersistToMajority specifies that a mutation must be persisted (written to disk) to a majority
	// of nodes.
	DurabilityLevelPersistToMajority = DurabilityLevel(3)
)

// MutationMacro can be supplied to MutateIn operations to perform ExpandMacros operations.
type MutationMacro string

const (
	// MutationMacroCAS can be used to tell the server to use the CAS macro.
	MutationMacroCAS = MutationMacro("${Mutation.CAS}")

	// MutationMacroSeqNo can be used to tell the server to use the seqno macro.
	MutationMacroSeqNo = MutationMacro("${Mutation.seqno}")

	// MutationMacroValueCRC32c can be used to tell the server to use the value_crc32c macro.
	MutationMacroValueCRC32c = MutationMacro("${Mutation.value_crc32c}")
)
