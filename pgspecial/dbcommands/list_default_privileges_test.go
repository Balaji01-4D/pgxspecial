package dbcommands_test

import (
	"context"
	"testing"

	"github.com/balaji01-4d/pgspecial/pgspecial/dbcommands"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestListDefaultPrivileges(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	pattern := ""

	_, err := db.Exec(context.Background(), "CREATE ROLE app_user")
	if err != nil {
		t.Fatalf("Failed to create role: %v", err)
	}
	defer db.Exec(context.Background(), "DROP ROLE IF EXISTS app_user")

	CreateDefaultPrivileges(t, context.Background(), db.(*pgxpool.Pool), "app_user")
	defer DropDefaultPrivileges(t, context.Background(), db.(*pgxpool.Pool), "app_user")
	result, err := dbcommands.ListDefaultPrivileges(context.Background(), db, pattern, false)
	if err != nil {
		t.Fatalf("ListDefaultPrivileges failed: %v", err)
	}
	defer result.Close()

	fds := result.FieldDescriptions()
	assert.NotNil(t, fds)

	columnsExpected := []string{
		"owner",
		"schema",
		"type",
		"access_privileges",
	}
	assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
	// expecting 4 columns
	assert.Len(t, fds, 4)

	var allRows []map[string]interface{}
	allRows, err = RowsToMaps(result)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}
	assert.Greater(t, len(allRows), 0, "Expected at least one default privilege entry")
}
