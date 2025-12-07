package dbcommands_test

import (
	"context"
	"testing"

	"github.com/balaji01-4d/pgspecial/pgspecial/dbcommands"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestListObjects(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	// Ensure we have at least one table
	ctx := context.Background()
	_, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS test_list_objects (id int)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec(ctx, "DROP TABLE IF EXISTS test_list_objects")

	pattern := "test_list_*"
	verbose := false
	// "r" for ordinary table
	relkinds := []string{"r"}

	result, err := dbcommands.ListObjects(ctx, db, pattern, verbose, relkinds)
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	defer result.Close()

	allRows, err := RowsToMaps(result)
	if err != nil {
		t.Fatal(err)
	}

	assert.True(t, containsByField(allRows, "name", "test_list_objects"))
}

func TestListPrivilegesWithPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := "pg_catalog.pg_class" // A known system table
	result, err := dbcommands.ListPrivileges(context.Background(), db, pattern, false)
	if err != nil {
		t.Fatalf("ListPrivileges with pattern failed: %v", err)
	}
	defer result.Close()

	// Just ensure it runs without error and returns rows (or empty rows if no privs found, but logic is covered)
	_, err = RowsToMaps(result)
	assert.NoError(t, err)
}

func TestListDefaultPrivilegesWithPattern(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	// Setup a specific role/privilege to query against if needed,
	// or just test the query generation logic with a pattern.
	pattern := "public"
	result, err := dbcommands.ListDefaultPrivileges(context.Background(), db, pattern, false)
	if err != nil {
		t.Fatalf("ListDefaultPrivileges with pattern failed: %v", err)
	}
	defer result.Close()

	_, err = RowsToMaps(result)
	assert.NoError(t, err)
}
