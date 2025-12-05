package pgspecial_test

import (
	"context"
	"os"
	"testing"

	"github.com/balaji01-4d/pgspecial/pgspecial"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func connectTestDB(t *testing.T) pgspecial.DB {
	ctx := context.Background()
	db_url := os.Getenv("PGSPECIAL_TEST_DSN")
	db, err := pgxpool.New(ctx, db_url)

	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	return db
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
	defer result.Rows.Close()

	fds := result.Columns
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
	allRows, err = RowsToMaps(result.Rows)
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
	defer result.Rows.Close()

	fds := result.Columns
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
	allRows, err = RowsToMaps(result.Rows)
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
	defer result.Rows.Close()

	fds := result.Columns
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
	allRows, err = RowsToMaps(result.Rows)
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
	defer result.Rows.Close()

	fds := result.Columns
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
	allRows, err = RowsToMaps(result.Rows)
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
	defer result.Rows.Close()

	fds := result.Columns
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
	allRows, err = RowsToMaps(result.Rows)
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
	defer result.Rows.Close()

	fds := result.Columns
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
	allRows, err = RowsToMaps(result.Rows)
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
	defer result.Rows.Close()

	fds := result.Columns
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
	allRows, err = RowsToMaps(result.Rows)
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
	defer result.Rows.Close()

	fds := result.Columns
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
	allRows, err = RowsToMaps(result.Rows)
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
	defer result.Rows.Close()

	fds := result.Columns
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
	allRows, err = RowsToMaps(result.Rows)
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
	defer result.Rows.Close()

	fds := result.Columns
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
	allRows, err = RowsToMaps(result.Rows)
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
	defer result.Rows.Close()

	fds := result.Columns
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
	allRows, err = RowsToMaps(result.Rows)
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
	defer result.Rows.Close()

	fds := result.Columns
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
	allRows, err = RowsToMaps(result.Rows)
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
	defer result.Rows.Close()

	fds := result.Columns
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
	allRows, err = RowsToMaps(result.Rows)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Len(t, allRows, 0)
}
