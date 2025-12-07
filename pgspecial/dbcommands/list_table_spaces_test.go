package dbcommands_test

import (
	"context"
	"testing"

	"github.com/balaji01-4d/pgspecial/pgspecial/dbcommands"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestListTablespaces(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := ""
	verbose := false

	result, err := dbcommands.ListTablespaces(context.Background(), db, pattern, verbose)
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

	result, err := dbcommands.ListTablespaces(context.Background(), db, pattern, verbose)
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

	result, err := dbcommands.ListTablespaces(context.Background(), db, pattern, verbose)
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
