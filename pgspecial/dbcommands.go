package pgspecial

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	RegisterCommand(SpecialCommandRegistry{
		Cmd:           "\\l",
		Alias:         []string{"\\list"},
		Description:   "List Databases",
		Syntax:        "\\l[+] [pattern]",
		Handler:       ListDatabases,
		CaseSensitive: true,
	})

	RegisterCommand(SpecialCommandRegistry{
		Cmd:    "\\dt",
		Syntax: "\\dt[+] [pattern]",
		Handler: func(ctx context.Context, db DB, pattern string, verbose bool) (pgx.Rows, error) {
			return ListObjects(ctx, db, pattern, verbose, []string{"r", "p"})
		},
		Description:   "List Tables",
		CaseSensitive: true,
	})

	RegisterCommand(SpecialCommandRegistry{
		Cmd:    "\\d",
		Syntax: "\\d[+] [pattern]",
		Handler: func(ctx context.Context, db DB, pattern string, verbose bool) (pgx.Rows, error) {
			rowsList, err := DescribeTableDetails(ctx, db, pattern, verbose)
			if err != nil {
				return nil, err
			}
			if len(rowsList) == 0 {
				return nil, nil
			}
			return rowsList[0], nil
		},
		Description:   "List or describe tables",
		CaseSensitive: true,
	})
}

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

func ListDatabases(ctx context.Context, db DB, pattern string, verbose bool) (pgx.Rows, error) {
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
			args = append(args, tablePattern)
		}
		sb.WriteString("\nWHERE d.datname ~ $" + strconv.Itoa(argIndex) + " ")
	}

	sb.WriteString("\nORDER BY 1;")
	rows, err := db.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func ListSchemas(ctx context.Context, db DB, pattern string, verbose bool) (pgx.Rows, error) {
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

func ListPrivileges(ctx context.Context, db DB, pattern string, verbose bool) (pgx.Rows, error) {
	var sb strings.Builder
	args := []any{}
	argIndex := 1

	sb.WriteString(`
	        SELECT n.nspname as schema,
          c.relname as name,
          CASE c.relkind WHEN 'r' THEN 'table'
                         WHEN 'v' THEN 'view'
                         WHEN 'm' THEN 'materialized view'
                         WHEN 'S' THEN 'sequence'
                         WHEN 'f' THEN 'foreign table'
                         WHEN 'p' THEN 'partitioned table' END as type,
          pg_catalog.array_to_string(c.relacl, E'\n') AS access_privileges,

          pg_catalog.array_to_string(ARRAY(
            SELECT attname || E':\n  ' || pg_catalog.array_to_string(attacl, E'\n  ')
            FROM pg_catalog.pg_attribute a
            WHERE attrelid = c.oid AND NOT attisdropped AND attacl IS NOT NULL
          ), E'\n') AS column_privileges,
          pg_catalog.array_to_string(ARRAY(
            SELECT polname
            || CASE WHEN NOT polpermissive THEN
               E' (RESTRICTIVE)'
               ELSE '' END
            || CASE WHEN polcmd != '*' THEN
                   E' (' || polcmd::pg_catalog.text || E'):'
               ELSE E':'
               END
            || CASE WHEN polqual IS NOT NULL THEN
                   E'\n  (u): ' || pg_catalog.pg_get_expr(polqual, polrelid)
               ELSE E''
               END
            || CASE WHEN polwithcheck IS NOT NULL THEN
                   E'\n  (c): ' || pg_catalog.pg_get_expr(polwithcheck, polrelid)
               ELSE E''
               END    || CASE WHEN polroles <> '{0}' THEN
                   E'\n  to: ' || pg_catalog.array_to_string(
                       ARRAY(
                           SELECT rolname
                           FROM pg_catalog.pg_roles
                           WHERE oid = ANY (polroles)
                           ORDER BY 1
                       ), E', ')
               ELSE E''
               END
            FROM pg_catalog.pg_policy pol
            WHERE polrelid = c.oid), E'\n')
            AS policies
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		  WHERE c.relkind IN ('r','v','m','S','f','p')
	`)

	if pattern != "" {
		schema, table := sqlNamePattern(pattern)
		if table != "" {
			sb.WriteString(" AND c.relname OPERATOR(pg_catalog.~) $" + strconv.Itoa(argIndex) + " COLLATE pg_catalog.default ")
			args = append(args, table)
			argIndex++
		}
		if schema != "" {
			sb.WriteString(" AND n.nspname OPERATOR(pg_catalog.~) $" + strconv.Itoa(argIndex) + " COLLATE pg_catalog.default ")
			args = append(args, schema)
		}
	} else {
		sb.WriteString(" AND pg_catalog.pg_table_is_visible(c.oid) ")
	}

	sb.WriteString("  AND n.nspname !~ '^pg_'")
	sb.WriteString(" ORDER BY 1, 2")
	rows, err := db.Query(ctx, sb.String(), args...)
	return rows, err
}

func ListRoles(ctx context.Context, db DB, pattern string, verbose bool) (pgx.Rows, error) {
	var sb strings.Builder
	args := []any{}
	argIndex := 1

	sb.WriteString(`
	   SELECT r.rolname,
                r.rolsuper,
                r.rolinherit,
                r.rolcreaterole,
                r.rolcreatedb,
                r.rolcanlogin,
                r.rolconnlimit,
                r.rolvaliduntil,
                ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid) as memberof,
	`)

	if verbose {
		sb.WriteString("pg_catalog.shobj_description(r.oid, 'pg_authid') AS description, ")
	}
	sb.WriteString(`
	 	r.rolreplication
			FROM pg_catalog.pg_roles r
	`)

	if pattern != "" {
		_, tablePattern := sqlNamePattern(pattern)
		if tablePattern != "" {
			sb.WriteString(" WHERE r.rolname ~ $" + strconv.Itoa(argIndex) + " ")
			args = append(args, tablePattern)
		}
	}

	sb.WriteString(" ORDER BY 1;")
	rows, err := db.Query(ctx, sb.String(), args...)
	return rows, err
}

func ListDefaultPrivileges(ctx context.Context, db DB, pattern string, verbose bool) (pgx.Rows, error) {
	var sb strings.Builder
	args := []any{}

	sb.WriteString(`
	 SELECT pg_catalog.pg_get_userbyid(d.defaclrole) AS owner,
    n.nspname AS schema,
    CASE d.defaclobjtype WHEN 'r' THEN 'table'
                         WHEN 'S' THEN 'sequence'
                         WHEN 'f' THEN 'function'
                         WHEN 'T' THEN 'type'
                         WHEN 'n' THEN 'schema' END as type,
    pg_catalog.array_to_string(d.defaclacl, E'\n') AS access_privileges
    FROM pg_catalog.pg_default_acl d
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = d.defaclnamespace
	`)
	if pattern != "" {
		sb.WriteString(`
		 WHERE (n.nspname OPERATOR(pg_catalog.~) $1 COLLATE pg_catalog.default
            OR pg_catalog.pg_get_userbyid(d.defaclrole) OPERATOR(pg_catalog.~) $1 COLLATE pg_catalog.default)
		`)
		args = append(args, fmt.Sprintf("^(%s)$", pattern))
	}
	sb.WriteString("ORDER BY 1, 2, 3;")
	rows, err := db.Query(ctx, sb.String(), args...)
	return rows, err
}

func ListTablespaces(ctx context.Context, db DB, pattern string, verbose bool) (pgx.Rows, error) {
	var sb strings.Builder
	args := []any{}
	argIndex := 1

	var isLocationSupported bool
	rows := db.QueryRow(ctx, `
	  SELECT EXISTS (
            SELECT * FROM pg_proc
            WHERE proname = 'pg_tablespace_location'
        )
	`)
	if err := rows.Scan(&isLocationSupported); err != nil {
		return nil, err
	}

	sb.WriteString(`
	SELECT
		n.spcname AS name,
		pg_catalog.pg_get_userbyid(n.spcowner) AS owner,
	`)
	if isLocationSupported {
		sb.WriteString("    pg_catalog.pg_tablespace_location(n.oid) AS location\n")
	} else {
		sb.WriteString("    'Not supported' AS location\n")
	}

	sb.WriteString(`
	FROM pg_catalog.pg_tablespace n
	`)

	if pattern != "" {
		_, tablePattern := sqlNamePattern(pattern)
		if tablePattern != "" {
			sb.WriteString(" WHERE n.spcname ~ $" + strconv.Itoa(argIndex) + " COLLATE pg_catalog.default ")
			args = append(args, tablePattern)
		}
	}

	sb.WriteString(" ORDER BY 1;")
	rowsResult, err := db.Query(ctx, sb.String(), args...)
	return rowsResult, err

}

func ListObjects(ctx context.Context, db DB, pattern string, verbose bool, relkinds []string) (pgx.Rows, error) {
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

func ListFunctions(ctx context.Context, db DB, pattern string, verbose bool) (pgx.Rows, error) {
	var sb strings.Builder
	args := []any{}
	argIndex := 1

	sb.WriteString(`
	            SELECT  n.nspname as schema,
                    p.proname as name,
                    pg_catalog.pg_get_function_result(p.oid)
                      as "Result data type",
                    pg_catalog.pg_get_function_arguments(p.oid)
                      as "Argument data types",
                     CASE
                        WHEN p.prokind = 'a' THEN 'agg'
                        WHEN p.prokind = 'w' THEN 'window'
                        WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype
                            THEN 'trigger'
                        ELSE 'normal'
                    END as type 
	`)

	if verbose {
		sb.WriteString(`
		 ,CASE
                 WHEN p.provolatile = 'i' THEN 'immutable'
                 WHEN p.provolatile = 's' THEN 'stable'
                 WHEN p.provolatile = 'v' THEN 'volatile'
            END as "Volatility",
            pg_catalog.pg_get_userbyid(p.proowner) as owner,
          l.lanname as "Language",
          p.prosrc as "Source code",
          pg_catalog.obj_description(p.oid, 'pg_proc') as description 
		`)
	}

	sb.WriteString(`
	   FROM    pg_catalog.pg_proc p
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
	`)

	if verbose {
		sb.WriteString(`
		LEFT JOIN pg_catalog.pg_language l
			ON l.oid = p.prolang
		`)
	}

	sb.WriteString(`
	 WHERE  
	`)

	schemaPattern, funcPattern := sqlNamePattern(pattern)

	if schemaPattern != "" {
		sb.WriteString("  n.nspname ~ $" + strconv.Itoa(argIndex) + " ")
		args = append(args, schemaPattern)
		argIndex++
	} else {
		sb.WriteString(" pg_catalog.pg_function_is_visible(p.oid) ")
	}

	if funcPattern != "" {
		sb.WriteString(" AND p.proname ~ $" + strconv.Itoa(argIndex) + " ")
		args = append(args, funcPattern)
	}

	if !(schemaPattern != "" || funcPattern != "") {
		sb.WriteString(`
		AND n.nspname <> 'pg_catalog'
		AND n.nspname <> 'information_schema' 	
		`)
	}

	sb.WriteString(" ORDER BY 1, 2, 4;")

	rows, err := db.Query(ctx, sb.String(), args...)
	return rows, err
}

func ListDatatypes(ctx context.Context, db DB, pattern string, verbose bool) (pgx.Rows, error) {
	var sb strings.Builder
	args := []any{}
	argIndex := 1

	sb.WriteString(`
SELECT n.nspname AS schema,
       pg_catalog.format_type(t.oid, NULL) AS name,
`)

	if verbose {
		sb.WriteString(`
       t.typname AS internal_name,
       CASE
           WHEN t.typrelid != 0 THEN 'tuple'
           WHEN t.typlen < 0 THEN 'var'
           ELSE t.typlen::text
       END AS size,
       pg_catalog.array_to_string(
           ARRAY(
               SELECT e.enumlabel
               FROM pg_catalog.pg_enum e
               WHERE e.enumtypid = t.oid
               ORDER BY e.enumsortorder
           ), E'\n') AS elements,
       pg_catalog.array_to_string(t.typacl, E'\n') AS access_privileges,
       pg_catalog.obj_description(t.oid, 'pg_type') AS description
`)
	} else {
		sb.WriteString(`
       pg_catalog.obj_description(t.oid, 'pg_type') AS description
`)
	}

	sb.WriteString(`
FROM pg_catalog.pg_type t
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE (t.typrelid = 0 OR
      (SELECT c.relkind='c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
  AND NOT EXISTS(
      SELECT 1 FROM pg_catalog.pg_type el
      WHERE el.oid = t.typelem AND el.typarray = t.oid
  )
`)

	schemaPattern, typePattern := sqlNamePattern(pattern)

	if schemaPattern != "" {
		sb.WriteString("  AND n.nspname ~ $" + strconv.Itoa(argIndex) + "\n")
		args = append(args, schemaPattern)
		argIndex++
	} else {
		sb.WriteString("  AND pg_catalog.pg_type_is_visible(t.oid)\n")
	}

	if typePattern != "" {
		sb.WriteString("  AND (t.typname ~ $" + strconv.Itoa(argIndex) +
			" OR pg_catalog.format_type(t.oid, NULL) ~ $" + strconv.Itoa(argIndex) + ")\n")
		args = append(args, typePattern)
	}

	if schemaPattern == "" && typePattern == "" {
		sb.WriteString(`
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
`)
	}

	sb.WriteString("ORDER BY 1, 2;")

	rows, err := db.Query(ctx, sb.String(), args...)

	return rows, err
}

func ListForeignTables(ctx context.Context, db DB, pattern string, verbose bool) (pgx.Rows, error) {
	var sb strings.Builder
	args := []any{}
	argIndex := 1

	sb.WriteString(`
SELECT 
    n.nspname AS schema,
    c.relname AS name,
    CASE c.relkind 
        WHEN 'r' THEN 'table'
        WHEN 'v' THEN 'view'
        WHEN 'm' THEN 'materialized view'
        WHEN 'i' THEN 'index'
        WHEN 'S' THEN 'sequence'
        WHEN 's' THEN 'special'
        WHEN 'f' THEN 'foreign table'
        WHEN 'p' THEN 'table'
        WHEN 'I' THEN 'index'
    END AS type,
    pg_catalog.pg_get_userbyid(c.relowner) AS owner
`)

	if verbose {
		sb.WriteString(`
  , pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) AS size
  , pg_catalog.obj_description(c.oid, 'pg_class') AS description
`)
	}

	sb.WriteString(`
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('f','')
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
`)

	if pattern != "" {
		_, tblPattern := sqlNamePattern(pattern)
		sb.WriteString("  AND c.relname OPERATOR(pg_catalog.~) $" + strconv.Itoa(argIndex) + "\n")
		args = append(args, tblPattern)
	}

	sb.WriteString("ORDER BY 1,2;")

	rows, err := db.Query(ctx, sb.String(), args...)
	return rows, err
}

func DescribeTableDetails(ctx context.Context, db DB, pattern string, verbose bool) ([]pgx.Rows, error) {
	if pattern == "" {
		rows, err := ListObjects(ctx, db, pattern, verbose, []string{"r", "p", "v", "m", "S", "f", ""})
		if err != nil {
			return nil, err
		}
		return []pgx.Rows{rows}, nil
	}

	schema, relname := sqlNamePattern(pattern)
	var sb strings.Builder
	args := []any{}
	argIndex := 1

	sb.WriteString(`
		SELECT c.oid, n.nspname, c.relname
		FROM pg_catalog.pg_class c
		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE 1=1
	`)

	if schema != "" {
		sb.WriteString(" AND n.nspname ~ $" + strconv.Itoa(argIndex))
		args = append(args, schema)
		argIndex++
	} else {
		sb.WriteString(" AND pg_catalog.pg_table_is_visible(c.oid)")
	}

	if relname != "" {
		sb.WriteString(" AND c.relname OPERATOR(pg_catalog.~) $" + strconv.Itoa(argIndex))
		args = append(args, relname)
		argIndex++
	}

	sb.WriteString(" ORDER BY 2, 3")

	fmt.Println(sb.String(), args)

	rows, err := db.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []pgx.Rows
	found := false

	for rows.Next() {
		found = true
		var oid uint32
		var nspname, relnameStr string
		if err := rows.Scan(&oid, &nspname, &relnameStr); err != nil {
			return nil, err
		}

		// Assuming DescribeOneTableDetails is defined in the same package
		// and returns (pgx.Rows, error)
		res, err := DescribeOneTableDetails(ctx, db, nspname, relnameStr, oid, verbose)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if !found {
		return nil, fmt.Errorf("did not find any relation named %s", pattern)
	}

	return results, nil
}

func DescribeOneTableDetails(ctx context.Context, db DB, schemaName, relationName string, oid uint32, verbose bool) (pgx.Rows, error) {
	var relKind string
	err := db.QueryRow(ctx, "SELECT relkind::text FROM pg_catalog.pg_class WHERE oid = $1", oid).Scan(&relKind)
	if err != nil {
		return nil, err
	}

	var sb strings.Builder
	args := []any{oid}

	sb.WriteString("SELECT ")

	cols := []string{
		`a.attname AS "Column"`,
		`pg_catalog.format_type(a.atttypid, a.atttypmod) AS "Type"`,
	}

	if relKind == "r" || relKind == "p" || relKind == "v" || relKind == "m" || relKind == "f" || relKind == "c" {
		cols = append(cols, `
			TRIM(BOTH ' ' FROM CONCAT(
				CASE WHEN a.attcollation <> t.typcollation THEN 'collate ' || c.collname ELSE '' END,
				CASE WHEN a.attnotnull THEN ' not null' ELSE '' END,
				CASE WHEN a.atthasdef THEN ' default ' || pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) ELSE '' END,
				CASE WHEN a.attidentity = 'a' THEN ' generated always as identity'
					 WHEN a.attidentity = 'd' THEN ' generated by default as identity'
					 WHEN a.attgenerated = 's' THEN ' generated always as (' || pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) || ') stored'
					 ELSE '' END
			)) AS "Modifiers"`)
	}

	if relKind == "i" || relKind == "I" {
		cols = append(cols, `pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE) AS "Definition"`)
	}

	if relKind == "f" {
		cols = append(cols, `CASE WHEN attfdwoptions IS NULL THEN '' ELSE '(' || array_to_string(ARRAY(SELECT quote_ident(option_name) || ' ' || quote_literal(option_value) FROM pg_options_to_table(attfdwoptions)), ', ') || ')' END AS "FDW Options"`)
	}

	if verbose {
		cols = append(cols, `a.attstorage AS "Storage"`)
		if relKind == "r" || relKind == "i" || relKind == "I" || relKind == "m" || relKind == "f" || relKind == "p" {
			cols = append(cols, `CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS "Stats target"`)
		}
		if relKind == "r" || relKind == "v" || relKind == "m" || relKind == "f" || relKind == "p" || relKind == "c" {
			cols = append(cols, `pg_catalog.col_description(a.attrelid, a.attnum) AS "Description"`)
		}
	}

	sb.WriteString(strings.Join(cols, ",\n"))

	sb.WriteString(`
		FROM pg_catalog.pg_attribute a
		LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		LEFT JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
		LEFT JOIN pg_catalog.pg_collation c ON c.oid = a.attcollation
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum
	`)

	return db.Query(ctx, sb.String(), args...)
}
