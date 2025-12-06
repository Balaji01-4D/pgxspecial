package pgspecial_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/balaji01-4d/pgspecial/pgspecial"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func connectTestDB(t *testing.T) pgspecial.DB {
	t.Helper()
	ctx := context.Background()
	db_url := os.Getenv("PGSPECIAL_TEST_DSN")
	db, err := pgxpool.New(ctx, db_url)

	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	return db
}

func CreateForeignTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tableName string) {
	t.Helper()

	// Create extension
	_, err := pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS postgres_fdw;`)
	if err != nil {
		t.Fatalf("failed to create extension: %v", err)
	}

	// Create server
	_, err = pool.Exec(ctx, `
        CREATE SERVER IF NOT EXISTS test_remote_server
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host 'localhost', dbname 'remotedb', port '5432');
    `)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// Create user mapping
	_, err = pool.Exec(ctx, `
        CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
        SERVER test_remote_server
        OPTIONS (user 'remote_user', password 'remote_pass');
    `)
	if err != nil {
		t.Fatalf("failed to create user mapping: %v", err)
	}

	// Create FOREIGN TABLE
	query := fmt.Sprintf(`
        CREATE FOREIGN TABLE IF NOT EXISTS %s (
            id    integer,
            name  text,
            email text
        )
        SERVER test_remote_server
        OPTIONS (schema_name 'public', table_name 'users');
    `, tableName)

	_, err = pool.Exec(ctx, query)
	if err != nil {
		t.Fatalf("failed to create foreign table %s: %v", tableName, err)
	}
}

func DropForeignTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tableName string) {
	t.Helper()

	query := fmt.Sprintf(`
        DROP FOREIGN TABLE IF EXISTS %s CASCADE;
    `, tableName)

	if _, err := pool.Exec(ctx, query); err != nil {
		t.Fatalf("failed to drop foreign table %s: %v", tableName, err)
	}
}

func CreateDatatype(t *testing.T, ctx context.Context, pool *pgxpool.Pool, typeName string) {
	t.Helper()

	// create an ENUM datatype
	query := fmt.Sprintf(`
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_type WHERE typname = '%s'
            ) THEN
                CREATE TYPE %s AS ENUM ('a', 'b', 'c');
            END IF;
        END$$;
    `, typeName, typeName)

	if _, err := pool.Exec(ctx, query); err != nil {
		t.Fatalf("failed to create datatype %s: %v", typeName, err)
	}
}

func DropDatatype(t *testing.T, ctx context.Context, pool *pgxpool.Pool, typeName string) {
	t.Helper()

	query := fmt.Sprintf(`
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_type WHERE typname = '%s'
            ) THEN
                DROP TYPE %s;
            END IF;
        END$$;
    `, typeName, typeName)

	if _, err := pool.Exec(ctx, query); err != nil {
		t.Fatalf("failed to drop datatype %s: %v", typeName, err)
	}
}

func CreateFunction(t *testing.T, ctx context.Context, pool *pgxpool.Pool, funcName string) {
	t.Helper()

	// Simple example function: returns integer 42
	query := fmt.Sprintf(`
        CREATE OR REPLACE FUNCTION %s()
        RETURNS int
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN 42;
        END;
        $$;
    `, funcName)

	if _, err := pool.Exec(ctx, query); err != nil {
		t.Fatalf("failed to create function %s: %v", funcName, err)
	}
}

func DropFunction(t *testing.T, ctx context.Context, pool *pgxpool.Pool, funcName string) {
	t.Helper()

	query := fmt.Sprintf(`
        DROP FUNCTION IF EXISTS %s() CASCADE;
    `, funcName)

	if _, err := pool.Exec(ctx, query); err != nil {
		t.Fatalf("failed to drop function %s: %v", funcName, err)
	}
}

func RowsToMaps(rows pgx.Rows) ([]map[string]interface{}, error) {
	cols := rows.FieldDescriptions()
	colCount := len(cols)

	var result []map[string]interface{}

	for rows.Next() {
		values := make([]interface{}, colCount)
		scanArgs := make([]interface{}, colCount)
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		m := make(map[string]interface{})
		for i, fd := range cols {
			m[string(fd.Name)] = values[i]
		}

		result = append(result, m)
	}

	return result, rows.Err()
}

func getColumnNames(fds []pgconn.FieldDescription) []string {
	columns := make([]string, len(fds))
	for i, fd := range fds {
		columns[i] = string(fd.Name)
	}
	return columns
}

func containsDB(rows []map[string]interface{}, name string) bool {
	for _, r := range rows {
		if n, ok := r["name"].(string); ok && n == name {
			return true
		}
	}
	return false
}

func containsByField(rows []map[string]interface{}, field, expected string) bool {
	for _, row := range rows {
		v := row[field]
		switch x := v.(type) {
		case string:
			if x == expected {
				return true
			}
		case []byte:
			if string(x) == expected {
				return true
			}
		}
	}
	return false
}

func TestListDatabases(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := ""
	verbose := false

	result, err := pgspecial.ListDatabases(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"name",
		"owner",
		"encoding",
		"collate",
		"ctype",
		"access_privileges",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 6 columns: Name Owner Encoding Collate Ctype Access privileges
	assert.Len(t, fds, 6)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.True(t, containsDB(allRows, "template0"))
	assert.True(t, containsDB(allRows, "template1"))
	assert.True(t, containsDB(allRows, "postgres"))
}

func TestListDatabasesVerbose(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := ""
	verbose := true

	result, err := pgspecial.ListDatabases(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"name",
		"owner",
		"encoding",
		"collate",
		"ctype",
		"access_privileges",
		"size",
		"Tablespace",
		"description",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 9 columns
	assert.Len(t, fds, 9)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.True(t, containsDB(allRows, "template0"))
	assert.True(t, containsDB(allRows, "template1"))
	assert.True(t, containsDB(allRows, "postgres"))
}

func TestListDatabaseWithExactPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := "postgres"
	verbose := false

	result, err := pgspecial.ListDatabases(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"name",
		"owner",
		"encoding",
		"collate",
		"ctype",
		"access_privileges",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 6 columns: Name Owner Encoding Collate Ctype Access privileges
	assert.Len(t, fds, 6)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 1, "Expected only one database matching the pattern")
	assert.False(t, containsDB(allRows, "template0"))
	assert.False(t, containsDB(allRows, "template1"))
	assert.True(t, containsDB(allRows, "postgres"))
}

func TestListDatabaseWithPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := "templ*"
	verbose := false

	result, err := pgspecial.ListDatabases(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"name",
		"owner",
		"encoding",
		"collate",
		"ctype",
		"access_privileges",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 6 columns: Name Owner Encoding Collate Ctype Access privileges
	assert.Len(t, fds, 6)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 2, "Expected only one database matching the pattern")
	assert.True(t, containsDB(allRows, "template0"))
	assert.True(t, containsDB(allRows, "template1"))
	assert.False(t, containsDB(allRows, "postgres"))
}

func TestListDatabaseWithNoMatchingPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := "pastgres" // typo intentional
	verbose := false

	result, err := pgspecial.ListDatabases(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"name",
		"owner",
		"encoding",
		"collate",
		"ctype",
		"access_privileges",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 6 columns: Name Owner Encoding Collate Ctype Access privileges
	assert.Len(t, fds, 6)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 0, "Expected no database matching the pattern")
}

func TestListRoles(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := ""
	verbose := false

	result, err := pgspecial.ListRoles(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListRoles failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"rolname",
		"rolsuper",
		"rolinherit",
		"rolcreaterole",
		"rolcreatedb",
		"rolcanlogin",
		"rolconnlimit",
		"rolvaliduntil",
		"memberof",
		"rolreplication",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 10 columns
	assert.Len(t, fds, 10)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}

	var essentialDefaultRoles = []string{
		"postgres",
		"pg_monitor",
		"pg_read_all_data",
		"pg_write_all_data",
	}

	for _, role := range essentialDefaultRoles {
		assert.True(t, containsByField(allRows, "rolname", role), "Expected role %s not found", role)
	}
}

func TestListRolesWithPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := "pg_w*"
	verbose := false

	result, err := pgspecial.ListRoles(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListRoles failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"rolname",
		"rolsuper",
		"rolinherit",
		"rolcreaterole",
		"rolcreatedb",
		"rolcanlogin",
		"rolconnlimit",
		"rolvaliduntil",
		"memberof",
		"rolreplication",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 10 columns
	assert.Len(t, fds, 10)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 2)
	var expectedRoles = []string{
		"pg_write_all_data",
		"pg_write_server_files",
	}
	for _, role := range expectedRoles {
		assert.True(t, containsByField(allRows, "rolname", role), "Expected role %s not found", role)
	}
}

func TestListRolesWithNoMatchingPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := "pg_xwrite*" // intentional typo
	verbose := false

	result, err := pgspecial.ListRoles(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListRoles failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"rolname",
		"rolsuper",
		"rolinherit",
		"rolcreaterole",
		"rolcreatedb",
		"rolcanlogin",
		"rolconnlimit",
		"rolvaliduntil",
		"memberof",
		"rolreplication",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 10 columns
	assert.Len(t, fds, 10)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 0, "Expected no roles matching the pattern")
}

func TestListRolesWithPatternVerbose(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := "pg_w*"
	verbose := true

	result, err := pgspecial.ListRoles(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListRoles failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"rolname",
		"rolsuper",
		"rolinherit",
		"rolcreaterole",
		"rolcreatedb",
		"rolcanlogin",
		"rolconnlimit",
		"rolvaliduntil",
		"memberof",
		"description",
		"rolreplication",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 11 columns
	assert.Len(t, fds, 11)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 2)
	var expectedRoles = []string{
		"pg_write_all_data",
		"pg_write_server_files",
	}
	for _, role := range expectedRoles {
		assert.True(t, containsByField(allRows, "rolname", role), "Expected role %s not found", role)
	}
}

func TestListRolesWithNoMatchingPatternVerbose(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := "pg_xwrite*" // intentional typo
	verbose := true

	result, err := pgspecial.ListRoles(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListRoles failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"rolname",
		"rolsuper",
		"rolinherit",
		"rolcreaterole",
		"rolcreatedb",
		"rolcanlogin",
		"rolconnlimit",
		"rolvaliduntil",
		"memberof",
		"description",
		"rolreplication",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 11 columns
	assert.Len(t, fds, 11)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 0, "Expected no roles matching the pattern")
}

func TestListTablespaces(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := ""
	verbose := false

	result, err := pgspecial.ListTablespaces(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListTablespaces failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"name",
		"owner",
		"location",
	}

	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 3 columns
	assert.Len(t, fds, 3)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.True(t, containsByField(allRows, "name", "pg_default"))
	assert.True(t, containsByField(allRows, "name", "pg_global"))
}

func TestListTablespacesWithPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := "pg_d*"
	verbose := false

	result, err := pgspecial.ListTablespaces(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListTablespaces failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"name",
		"owner",
		"location",
	}

	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 3 columns
	assert.Len(t, fds, 3)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 1)
	assert.True(t, containsByField(allRows, "name", "pg_default"))
	assert.False(t, containsByField(allRows, "name", "pg_global"))
}

func TestListTablespacesWithInvalidPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := "pg_xd*"
	verbose := false

	result, err := pgspecial.ListTablespaces(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListTablespaces failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"name",
		"owner",
		"location",
	}

	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 3 columns
	assert.Len(t, fds, 3)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 0)
}

func TestListForeignTables(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	tableNames := []string{"foreign_users", "foreign_orders", "foreign_products"}

	for _, tableName := range tableNames {
		// Setup: Create foreign table
		CreateForeignTable(t, ctx, db.(*pgxpool.Pool), tableName)
		defer DropForeignTable(t, ctx, db.(*pgxpool.Pool), tableName)
	}

	pattern := ""
	verbose := false

	result, err := pgspecial.ListForeignTables(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListForeignTables failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"type",
		"owner",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 4 columns
	assert.Len(t, fds, 4)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 3, "expecting 3")
	assert.True(t, containsByField(allRows, "name", tableNames[0]))
	assert.True(t, containsByField(allRows, "name", tableNames[1]))
	assert.True(t, containsByField(allRows, "name", tableNames[2]))
}

func TestListForeignTablesWithPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	tableName := "foreign_users"

	// Setup: Create foreign table
	CreateForeignTable(t, ctx, db.(*pgxpool.Pool), tableName)
	defer DropForeignTable(t, ctx, db.(*pgxpool.Pool), tableName)

	pattern := "foreign_*"
	verbose := false

	result, err := pgspecial.ListForeignTables(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListForeignTables failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"type",
		"owner",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 4 columns
	assert.Len(t, fds, 4)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 1)
	assert.True(t, containsByField(allRows, "name", tableName))
}

func TestListForeignTablesWithNoMatchingPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	tableName := "foreign_users"

	CreateForeignTable(t, ctx, db.(*pgxpool.Pool), tableName)
	defer DropForeignTable(t, ctx, db.(*pgxpool.Pool), tableName)

	pattern := "foreign_x*"
	verbose := false

	result, err := pgspecial.ListForeignTables(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListForeignTables failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"type",
		"owner",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 4 columns
	assert.Len(t, fds, 4)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 0)
}

func TestListForeignTablesVerbose(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	tableName := "foreign_users"

	// Setup: Create foreign table
	CreateForeignTable(t, ctx, db.(*pgxpool.Pool), tableName)
	defer DropForeignTable(t, ctx, db.(*pgxpool.Pool), tableName)

	pattern := ""
	verbose := true

	result, err := pgspecial.ListForeignTables(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListForeignTables failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"type",
		"owner",
		"size",
		"description",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 6 columns
	assert.Len(t, fds, 6)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.True(t, containsByField(allRows, "name", tableName))
}

func TestListForeignTablesVerboseWithPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	tableName := "foreign_users"

	// Setup: Create foreign table
	CreateForeignTable(t, ctx, db.(*pgxpool.Pool), tableName)
	defer DropForeignTable(t, ctx, db.(*pgxpool.Pool), tableName)

	pattern := "foreign_*"
	verbose := true

	result, err := pgspecial.ListForeignTables(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListForeignTables failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"type",
		"owner",
		"size",
		"description",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 6 columns
	assert.Len(t, fds, 6)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 1)
	assert.True(t, containsByField(allRows, "name", tableName))
}

func TestListForeignTablesVerboseWithNoMatchingPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	tableName := "foreign_users"

	CreateForeignTable(t, ctx, db.(*pgxpool.Pool), tableName)
	defer DropForeignTable(t, ctx, db.(*pgxpool.Pool), tableName)

	pattern := "foreign_x*"
	verbose := true

	result, err := pgspecial.ListForeignTables(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListForeignTables failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"type",
		"owner",
		"size",
		"description",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 6 columns
	assert.Len(t, fds, 6)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 0)
}

func TestListDatatypes(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	typeNames := []string{"mood_enum", "status_enum", "priority_enum"}

	for _, typeName := range typeNames {
		CreateDatatype(t, ctx, db.(*pgxpool.Pool), typeName)
		defer DropDatatype(t, ctx, db.(*pgxpool.Pool), typeName)
	}

	pattern := ""
	verbose := false

	result, err := pgspecial.ListDatatypes(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListDatatypes failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"description",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 3 columns
	assert.Len(t, fds, 3)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	for _, typeName := range typeNames {
		assert.True(t, containsByField(allRows, "name", typeName))
	}
}

func TestListDatatypesWithPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	typeNames := []string{"mood_enum", "status_enum", "priority_enum"}

	for _, typeName := range typeNames {
		CreateDatatype(t, ctx, db.(*pgxpool.Pool), typeName)
		defer DropDatatype(t, ctx, db.(*pgxpool.Pool), typeName)
	}

	pattern := "*_enum"
	verbose := false

	result, err := pgspecial.ListDatatypes(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListDatatypes failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"description",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 3 columns
	assert.Len(t, fds, 3)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	for _, typeName := range typeNames {
		assert.True(t, containsByField(allRows, "name", typeName))
	}
}

func TestListDatatypesWithNoMatchingPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	typeNames := []string{"mood_enum", "status_enum", "priority_enum"}

	for _, typeName := range typeNames {
		CreateDatatype(t, ctx, db.(*pgxpool.Pool), typeName)
		defer DropDatatype(t, ctx, db.(*pgxpool.Pool), typeName)
	}

	pattern := "type_xenum"
	verbose := false

	result, err := pgspecial.ListDatatypes(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListDatatypes failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"description",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 3 columns
	assert.Len(t, fds, 3)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 0)
}

func TestListDatatypesVerbose(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	typeNames := []string{"mood_enum", "status_enum", "priority_enum"}

	for _, typeName := range typeNames {
		CreateDatatype(t, ctx, db.(*pgxpool.Pool), typeName)
		defer DropDatatype(t, ctx, db.(*pgxpool.Pool), typeName)
	}

	pattern := ""
	verbose := true

	result, err := pgspecial.ListDatatypes(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListDatatypes failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	expectedColumns := []string{
		"schema",
		"name",
		"internal_name",
		"size",
		"elements",
		"access_privileges",
		"description",
	}
	assert.Equal(t, expectedColumns, getColumnNames(fds), "Column names do not match expected")
	// expecting 7 columns
	assert.Len(t, fds, 7)
	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	for _, typeName := range typeNames {
		assert.True(t, containsByField(allRows, "name", typeName))
	}

}

func TestListDatatypesVerboseWithPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	typeNames := []string{"mood_enum", "status_enum", "priority_enum"}

	for _, typeName := range typeNames {
		CreateDatatype(t, ctx, db.(*pgxpool.Pool), typeName)
		defer DropDatatype(t, ctx, db.(*pgxpool.Pool), typeName)
	}

	pattern := "*_enum"
	verbose := true

	result, err := pgspecial.ListDatatypes(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListDatatypes failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	expectedColumns := []string{
		"schema",
		"name",
		"internal_name",
		"size",
		"elements",
		"access_privileges",
		"description",
	}
	assert.Equal(t, expectedColumns, getColumnNames(fds), "Column names do not match expected")
	// expecting 7 columns
	assert.Len(t, fds, 7)
	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	for _, typeName := range typeNames {
		assert.True(t, containsByField(allRows, "name", typeName))
	}
}

func TestListDatatypesVerboseWithNoMatchingPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()
	ctx := context.Background()
	typeNames := []string{"mood_enum", "status_enum", "priority_enum"}

	for _, typeName := range typeNames {
		CreateDatatype(t, ctx, db.(*pgxpool.Pool), typeName)
		defer DropDatatype(t, ctx, db.(*pgxpool.Pool), typeName)
	}

	pattern := "type_xenum"
	verbose := true
	result, err := pgspecial.ListDatatypes(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListDatatypes failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	expectedColumns := []string{
		"schema",
		"name",
		"internal_name",
		"size",
		"elements",
		"access_privileges",
		"description",
	}
	assert.Equal(t, expectedColumns, getColumnNames(fds), "Column names do not match expected")
	// expecting 7 columns
	assert.Len(t, fds, 7)
	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 0)
}

func TestListFunctions(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	funcNames := []string{"calculate_discount", "get_user_status", "compute_tax"}

	for _, funcName := range funcNames {
		CreateFunction(t, ctx, db.(*pgxpool.Pool), funcName)
		defer DropFunction(t, ctx, db.(*pgxpool.Pool), funcName)
	}

	pattern := ""
	verbose := false

	result, err := pgspecial.ListFunctions(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListFunctions failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"Result data type",
		"Argument data types",
		"type",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 5 columns
	assert.Len(t, fds, 5)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	for _, funcName := range funcNames {
		assert.True(t, containsByField(allRows, "name", funcName))
	}
}

func TestListFunctionsWithPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()
	
	ctx := context.Background()
	funcNames := []string{"calculate_discount", "get_user_status", "compute_tax"}
	for _, funcName := range funcNames {
		CreateFunction(t, ctx, db.(*pgxpool.Pool), funcName)
		defer DropFunction(t, ctx, db.(*pgxpool.Pool), funcName)
	}

	pattern := "get_*"
	verbose := false

	result, err := pgspecial.ListFunctions(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListFunctions failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"Result data type",
		"Argument data types",
		"type",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 5 columns
	assert.Len(t, fds, 5)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.True(t, containsByField(allRows, "name", "get_user_status"))
	assert.False(t, containsByField(allRows, "name", "calculate_discount"))
	assert.False(t, containsByField(allRows, "name", "compute_tax"))
}

func TestListFunctionsWithNoMatchingPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	funcNames := []string{"calculate_discount", "get_user_status", "compute_tax"}

	for _, funcName := range funcNames {
		CreateFunction(t, ctx, db.(*pgxpool.Pool), funcName)
		defer DropFunction(t, ctx, db.(*pgxpool.Pool), funcName)
	}

	pattern := "fetch_*"
	verbose := false

	result, err := pgspecial.ListFunctions(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListFunctions failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"Result data type",
		"Argument data types",
		"type",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 5 columns
	assert.Len(t, fds, 5)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 0)
}

func TestListFunctionsVerbose(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	funcNames := []string{"calculate_discount", "get_user_status", "compute_tax"}

	for _, funcName := range funcNames {
		CreateFunction(t, ctx, db.(*pgxpool.Pool), funcName)
		defer DropFunction(t, ctx, db.(*pgxpool.Pool), funcName)
	}

	pattern := ""
	verbose := true

	result, err := pgspecial.ListFunctions(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListFunctions failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"Result data type",
		"Argument data types",
		"type",
		"Volatility",
		"owner",
		"Language",
		"Source code",
		"description",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 10 columns
	assert.Len(t, fds, 10)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	for _, funcName := range funcNames {
		assert.True(t, containsByField(allRows, "name", funcName))
	}
}

func TestListFunctionsVerboseWithPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	funcNames := []string{"calculate_discount", "get_user_status", "compute_tax"}

	for _, funcName := range funcNames {
		CreateFunction(t, ctx, db.(*pgxpool.Pool), funcName)
		defer DropFunction(t, ctx, db.(*pgxpool.Pool), funcName)
	}

	pattern := "get_*"
	verbose := true

	result, err := pgspecial.ListFunctions(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListFunctions failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"Result data type",
		"Argument data types",
		"type",
		"Volatility",
		"owner",
		"Language",
		"Source code",
		"description",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 10 columns
	assert.Len(t, fds, 10)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.True(t, containsByField(allRows, "name", "get_user_status"))
	assert.False(t, containsByField(allRows, "name", "calculate_discount"))
	assert.False(t, containsByField(allRows, "name", "compute_tax"))
}

func TestListFunctionsVerboseWithNoMatchingPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	ctx := context.Background()
	funcNames := []string{"calculate_discount", "get_user_status", "compute_tax"}

	for _, funcName := range funcNames {
		CreateFunction(t, ctx, db.(*pgxpool.Pool), funcName)
		defer DropFunction(t, ctx, db.(*pgxpool.Pool), funcName)
	}

	pattern := "fetch_*"
	verbose := true

	result, err := pgspecial.ListFunctions(context.Background(), db, pattern, verbose)
	if err != nil {
		t.Fatalf("ListFunctions failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"schema",
		"name",
		"Result data type",
		"Argument data types",
		"type",
		"Volatility",
		"owner",
		"Language",
		"Source code",
		"description",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 10 columns
	assert.Len(t, fds, 10)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 0)
}
