package pgspecial

import (
	"context"
	"strconv"
	"strings"
)

func sqlNamePattern(pattern string) (schema, table string) {
    inQuotes := false
    var buf strings.Builder
    var schemaBuf *string

    for i := 0; i < len(pattern); i++ {
        c := pattern[i]

        switch {
        case c == '"':
            if inQuotes && i+1 < len(pattern) && pattern[i+1] == '"' {
                buf.WriteByte('"')
                i++
            } else {
                inQuotes = !inQuotes
            }

        case !inQuotes && c >= 'A' && c <= 'Z':
            buf.WriteByte(byte(c + 32))

        case !inQuotes && c == '*':
            buf.WriteString(".*")

        case !inQuotes && c == '?':
            buf.WriteByte('.')

        case !inQuotes && c == '.':
            s := buf.String()
            schemaBuf = &s
            buf.Reset()

        default:
            if c == '$' || (inQuotes && strings.ContainsRune("|*+?()[]{}.^\\", rune(c))) {
                buf.WriteByte('\\')
            }
            buf.WriteByte(c)
        }
    }

    if buf.Len() > 0 {
        table = "^(" + buf.String() + ")$"
    }
    if schemaBuf != nil {
        schema = "^(" + *schemaBuf + ")$"
    }

    return schema, table
}


func ListDatabases(ctx context.Context, db DB, pattern string, verbose bool) (*Result, error) {
	var sb strings.Builder
	args := []any{}
	argIndex := 1


	sb.WriteString(
		`SELECT d.datname as name,
        pg_catalog.pg_get_userbyid(d.datdba) as owner,
        pg_catalog.pg_encoding_to_char(d.encoding) as encoding,
        d.datcollate as collate,
        d.datctype as ctype,
        pg_catalog.array_to_string(d.datacl, E'\n') AS access_privileges
		`)

	if verbose {
		sb.WriteString(
			`, 
			CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT')
				THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname))
				ELSE 'No Access'
            END as size,
            t.spcname as "Tablespace",
            pg_catalog.shobj_description(d.oid, 'pg_database') as description
	`)
	}

	sb.WriteString(`
	FROM pg_catalog.pg_database d
	`)

	if verbose {
		sb.WriteString(`JOIN pg_catalog.pg_tablespace t on d.dattablespace = t.oid`)
	}

	if pattern != "" {
		_, tablePattern := sqlNamePattern(pattern)

		if tablePattern != "" {
			sb.WriteString("\nWHERE d.datname ~ $" + strconv.Itoa(argIndex) + " ")
			args = append(args, tablePattern)
			argIndex++
		}
	}

	sb.WriteString("\nORDER BY 1;")
	rows, err := db.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}

	res := &Result{
		Title: "DATABASES",
		Rows: rows,
		Columns: rows.FieldDescriptions(),
		Status: "OKAY",
	}

	return res, nil
}
