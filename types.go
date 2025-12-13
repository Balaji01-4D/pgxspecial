// this package contains special command types
package pgxspecial

import "github.com/jackc/pgx/v5"

type SpecialResultKind int

const (
	ResultKindRows SpecialResultKind = iota
	ResultKindDescribeTable
	ResultKindExtensionVerbose
)

// SpecialCommand represents a parsed and executable special command.
//
// It contains the normalized command name, descriptive metadata, and the handler
// function invoked during execution. SpecialCommand values are stored internally
type SpecialCommand struct {
	Cmd           string
	Syntax        string
	Description   string
	Handler       SpecialHandler
	CaseSensitive bool
}

// SpecialCommandRegistry describes a special command registration.
//
// It defines the command name, optional aliases, documentation metadata, and
// execution handler used when registering commands via RegisterCommand.
type SpecialCommandRegistry struct {
	Cmd           string
	Alias         []string
	Syntax        string
	Description   string
	Handler       SpecialHandler
	CaseSensitive bool
}

type SpecialCommandResult interface {
	ResultKind() SpecialResultKind
}

type RowResult struct {
	Rows pgx.Rows
}

func (r RowResult) ResultKind() SpecialResultKind {
	return ResultKindRows
}

type TableFooterMeta struct {
	Indexes          []string // lines under "Indexes:"
	CheckConstraints []string // "Check constraints:"
	ForeignKeys      []string // "Foreign-key constraints:"
	ReferencedBy     []string // "Referenced by:"
	ViewDefinition   *string  // "View definition:"

	RulesEnabled  []string // under "Rules:"
	RulesDisabled []string // "Disabled rules:"
	RulesAlways   []string // "Rules firing always:"
	RulesReplica  []string // "Rules firing on replica only:"

	TriggersEnabled  []string // "Triggers:"
	TriggersDisabled []string // "Disabled triggers:"
	TriggersAlways   []string // "Triggers firing always:"
	TriggersReplica  []string // "Triggers firing on replica only:"

	PartitionOf          []string // "Partition of:"
	PartitionConstraints []string // "Partition constraint:"
	PartitionKey         *string  // "Partition key:"
	Partitions           []string // "Partitions:" (or leave empty)
	PartitionsSummary    *string  // "Number of partitions ..." (non-verbose form)

	Inherits           []string // "Inherits"
	ChildTables        []string // "Child tables" (verbose)
	ChildTablesSummary *string  // "Number of child tables..."
	TypedTableOf       *string  // "Typed table of type:"
	HasOIDs            *bool    // "Has OIDs: yes|no"
	Options            *string  // "Options: ..."
	Server             *string  // "Server: ..."  (foreign tables)
	FDWOptions         *string  // "FDW Options: (...)" (foreign tables)
	OwnedBy            *string  // "Owned by:" (sequences)
}

// DescribeTableResult holds the result of a describe table command.
type DescribeTableResult struct {
	Columns       []string
	Data          [][]string
	TableMetaData TableFooterMeta
}

func (DescribeTableResult) ResultKind() SpecialResultKind {
	return ResultKindDescribeTable
}

type DescribeTableListResult struct {
	Results []DescribeTableResult
}

func (DescribeTableListResult) ResultKind() SpecialResultKind {
	return ResultKindDescribeTable
}

type ExtensionVerboseResult struct {
	Name        string
	Description []string
}

type ExtensionVerboseListResult struct {
	Results []ExtensionVerboseResult
}

func (ExtensionVerboseListResult) ResultKind() SpecialResultKind {
	return ResultKindExtensionVerbose
}
