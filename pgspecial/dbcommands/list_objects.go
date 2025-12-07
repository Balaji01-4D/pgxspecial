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
	// \dt
	pgspecial.RegisterCommand(pgspecial.SpecialCommandRegistry{
		Cmd:         "\\dt",
		Description: "List tables.",
		Syntax:      "\\dt[+] [pattern]",
		Handler: func(ctx context.Context, db database.DB, pattern string, verbose bool) (pgx.Rows, error) {
			return ListObjects(ctx, db, pattern, verbose, []string{"r", "p", ""})
		},
		CaseSensitive: true,
	})

	// \dv
	pgspecial.RegisterCommand(pgspecial.SpecialCommandRegistry{
		Cmd:         "\\dv",
		Description: "List views.",
		Syntax:      "\\dv[+] [pattern]",
		Handler: func(ctx context.Context, db database.DB, pattern string, verbose bool) (pgx.Rows, error) {
			return ListObjects(ctx, db, pattern, verbose, []string{"v", "s", ""})
		},
		CaseSensitive: true,
	})

	// \dm
	pgspecial.RegisterCommand(pgspecial.SpecialCommandRegistry{
		Cmd:         "\\dm",
		Description: "List materialized views.",
		Syntax:      "\\dm[+] [pattern]",
		Handler: func(ctx context.Context, db database.DB, pattern string, verbose bool) (pgx.Rows, error) {
			return ListObjects(ctx, db, pattern, verbose, []string{"m", "s", ""})
		},
		CaseSensitive: true,
	})

	// \ds
	pgspecial.RegisterCommand(pgspecial.SpecialCommandRegistry{
		Cmd:         "\\ds",
		Description: "List sequences.",
		Syntax:      "\\ds[+] [pattern]",
		Handler: func(ctx context.Context, db database.DB, pattern string, verbose bool) (pgx.Rows, error) {
			return ListObjects(ctx, db, pattern, verbose, []string{"S", "s", ""})
		},
		CaseSensitive: true,
	})

	// \di
	pgspecial.RegisterCommand(pgspecial.SpecialCommandRegistry{
		Cmd:         "\\di",
		Description: "List indexes.",
		Syntax:      "\\di[+] [pattern]",
		Handler: func(ctx context.Context, db database.DB, pattern string, verbose bool) (pgx.Rows, error) {
			return ListObjects(ctx, db, pattern, verbose, []string{"i", "s", ""})
		},
		CaseSensitive: true,
	})
}

func ListObjects(ctx context.Context, db database.DB, pattern string, verbose bool, relkinds []string) (pgx.Rows, error) {
	var sb strings.Builder
	args := []any{}
	argIndex := 1

	schemaRe, tableRe := sqlNamePattern(pattern)

	sb.WriteString(
		`SELECT n.nspname as schema,
                    c.relname as name,
                    CASE c.relkind
                      WHEN 'r' THEN 'table' WHEN 'v' THEN 'view'
                      WHEN 'p' THEN 'partitioned table'
                      WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index'
                      WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special'
                      WHEN 'f' THEN 'foreign table' END
                    as type,
                    pg_catalog.pg_get_userbyid(c.relowner) as owner
	`)

	if verbose {
		sb.WriteString(`
		 ,pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as size,
            pg_catalog.obj_description(c.oid, 'pg_class') as description 
	`)
	}

	sb.WriteString(`
	FROM pg_catalog.pg_class c
	LEFT JOIN pg_catalog.pg_namespace n
	ON n.oid = c.relnamespace
	WHERE c.relkind = ANY($` + strconv.Itoa(argIndex) + `)
	`)
	args = append(args, relkinds)
	argIndex++

	if schemaRe != "" {
		sb.WriteString("  AND n.nspname ~ $" + strconv.Itoa(argIndex) + "\n")
		args = append(args, schemaRe)
		argIndex++
	} else {
		sb.WriteString(`
		AND n.nspname <> 'pg_catalog'
		AND n.nspname <> 'information_schema'
		AND n.nspname !~ '^pg_toast'
		AND pg_catalog.pg_table_is_visible(c.oid)
		`)
	}

	if tableRe != "" {
		sb.WriteString("  AND c.relname ~ $" + strconv.Itoa(argIndex) + "\n")
		args = append(args, tableRe)
	}

	sb.WriteString("ORDER BY 1, 2;")

	rows, err := db.Query(ctx, sb.String(), args...)
	return rows, err
}
