package pgspecial

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type PGXConn struct {
	Conn *pgx.Conn
}

type Pool struct {
	Pool *pgxpool.Pool
}

type Result struct {
	Title   string
	Rows    pgx.Rows
	Columns []pgconn.FieldDescription
	Status  string
}

type SpecialHandler func(ctx context.Context, db DB, args string) (*Result, error)

type SpecialCommand struct {
	Name          string
	Description   string
	Handler       SpecialHandler
	CaseSensitive bool
}

var registry = map[string]SpecialCommand{}

func Register(cmd SpecialCommand) {
	registry[cmd.Name] = cmd
}

// func main() {
// 	p, _ := pgxpool.New(context.Background(), "post")
// 	r, _ := p.Query(context.Background(), "s")
// 	_ = r
// }
