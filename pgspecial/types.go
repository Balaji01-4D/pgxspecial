package pgspecial

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Result struct {
	Title   string
	Rows    pgx.Rows
	Columns []pgconn.FieldDescription
	Status  string
}

type SpecialCommand struct {
	Cmd           string
	Syntax        string
	Description   string
	Handler       SpecialHandler
	CaseSensitive bool
}

type SpecialCommandRegistry struct {
	Cmd          string
	Alias         []string
	Syntax        string
	Description   string
	Handler       SpecialHandler
	CaseSensitive bool
}
