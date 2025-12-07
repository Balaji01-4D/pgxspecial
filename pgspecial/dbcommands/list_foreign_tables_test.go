package dbcommands_test

import (
	"context"
	"testing"

	"github.com/balaji01-4d/pgspecial/pgspecial/dbcommands"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

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

	result, err := dbcommands.ListForeignTables(context.Background(), db, pattern, verbose)
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

	result, err := dbcommands.ListForeignTables(context.Background(), db, pattern, verbose)
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

	result, err := dbcommands.ListForeignTables(context.Background(), db, pattern, verbose)
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

	result, err := dbcommands.ListForeignTables(context.Background(), db, pattern, verbose)
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

	result, err := dbcommands.ListForeignTables(context.Background(), db, pattern, verbose)
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

	result, err := dbcommands.ListForeignTables(context.Background(), db, pattern, verbose)
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
