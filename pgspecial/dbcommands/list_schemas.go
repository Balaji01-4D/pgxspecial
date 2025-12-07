package dbcommands

import (
	"context"
	"strconv"
	"strings"

	"github.com/balaji01-4d/pgspecial/pgspecial"
	"github.com/balaji01-4d/pgspecial/pgspecial/database"
	"github.com/jackc/pgx/v5"
)

func init() {
	pgspecial.RegisterCommand(pgspecial.SpecialCommandRegistry{
		Cmd:           "\\dn",
		Description:   "List schemas.",
		Syntax:        "\\dn[+] [pattern]",
		Handler:       ListSchemas,
		CaseSensitive: true,
	})
}

func ListSchemas(ctx context.Context, db database.DB, pattern string, verbose bool) (pgx.Rows, error) {
	var sb strings.Builder
	args := []any{}
	argIndex := 1

	sb.WriteString(`
	SELECT n.nspname AS name, pg_catalog.pg_get_userbyid(n.nspowner) AS owner
	`)

	if verbose {
		sb.WriteString(`
		, pg_catalog.array_to_string(n.nspacl, E'\n') AS access_privileges, pg_catalog.obj_description(n.oid, 'pg_namespace') AS description
		`)
	}
	sb.WriteString(`FROM pg_catalog.pg_namespace n WHERE n.nspname`)

	if pattern != "" {
		_, tablePattern := sqlNamePattern(pattern)

		if tablePattern != "" {
			sb.WriteString("~ $" + strconv.Itoa(argIndex) + " ")
			args = append(args, tablePattern)
		}
	} else {
		sb.WriteString(`
		!~ '^pg_' AND n.nspname <> 'information_schema'
		`)
	}

	sb.WriteString("ORDER BY 1")
	rows, err := db.Query(ctx, sb.String(), args...)
	return rows, err
}
