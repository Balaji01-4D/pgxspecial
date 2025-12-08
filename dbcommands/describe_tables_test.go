package dbcommands_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/balaji01-4d/pgxspecial/dbcommands"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestDescribeOneTableDetails(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	tables := []struct {
		name    string
		columns map[string]string
	}{
		{
			name: "test_table_1",
			columns: map[string]string{
				"id":   "SERIAL PRIMARY KEY",
				"name": "VARCHAR(100)",
			},
		},
		{
			name: "test_table_2",
			columns: map[string]string{
				"id":      "SERIAL PRIMARY KEY",
				"age":     "INT",
				"address": "TEXT",
			},
		},
	}

	pattern := ""
	verbose := false

	for _, table := range tables {
		oid := CreateTable(t, context.Background(), db.(*pgxpool.Pool), table.name, table.columns)
		defer DropTable(t, context.Background(), db.(*pgxpool.Pool), table.name)
		result, err := dbcommands.DescribeOneTableDetails(context.Background(), db, "public", pattern, oid, verbose)
		if err != nil {
			t.Fatalf("DescribeTables failed: %v", err)
		}
		defer result.Close()

		fds := result.FieldDescriptions()
		assert.NotNil(t, fds)

		columnsExpected := []string{
			"Column",
			"Type",
			"Modifiers",
		}
		assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
		// expecting 3 columns
		assert.Len(t, fds, 3)

		var allRows []map[string]interface{}
		allRows, err = RowsToMaps(result)
		if err != nil {
			t.Fatalf("Failed to read rows: %v", err)
		}
		// Check for columns from both tables
		for col_name := range table.columns {
			assert.True(t, containsByField(allRows, "Column", col_name), fmt.Sprintf("Expected column %s not found", col_name))
		}
	}

}

func TestDescribeOneTableDetailsVerbose(t *testing.T) {
	db := connectTestDB(t)
	defer db.(*pgxpool.Pool).Close()

	tables := []struct {
		name    string
		columns map[string]string
	}{
		{
			name: "test_table_1",
			columns: map[string]string{
				"id":         "SERIAL PRIMARY KEY",
				"name":       "VARCHAR(100)",
				"age":        "INT",
				"email":      "VARCHAR(100)",
				"created_at": "TIMESTAMP DEFAULT NOW()",
			},
		},
		{
			name: "test_table_2",
			columns: map[string]string{
				"id":      "SERIAL PRIMARY KEY",
				"age":     "INT",
				"address": "TEXT",
			},
		},
	}

	pattern := ""
	verbose := true

	for _, table := range tables {
		oid := CreateTable(t, context.Background(), db.(*pgxpool.Pool), table.name, table.columns)
		defer DropTable(t, context.Background(), db.(*pgxpool.Pool), table.name)
		result, err := dbcommands.DescribeOneTableDetails(context.Background(), db, "public", pattern, oid, verbose)
		if err != nil {
			t.Fatalf("DescribeTables failed: %v", err)
		}
		defer result.Close()

		fds := result.FieldDescriptions()
		assert.NotNil(t, fds)

		columnsExpected := []string{
			"Column",
			"Type",
			"Modifiers",
			"Storage",
			"Stats target",
			"Description",
		}
		assert.Equal(t, columnsExpected, getColumnNames(fds), "Column names do not match expected")
		// expecting 6 columns
		assert.Len(t, fds, 6)

		var allRows []map[string]interface{}
		allRows, err = RowsToMaps(result)
		if err != nil {
			t.Fatalf("Failed to read rows: %v", err)
		}
		// Check for columns from both tables
		for col_name := range table.columns {
			assert.True(t, containsByField(allRows, "Column", col_name), fmt.Sprintf("Expected column %s not found", col_name))
		}
	}

}

func CreateTable(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	tableName string,
	columns map[string]string,
) uint32 {
	t.Helper()

	if len(columns) == 0 {
		t.Fatal("columns map cannot be empty")
	}

	defs := make([]string, 0, len(columns))
	for col, typ := range columns {
		defs = append(defs, fmt.Sprintf("%s %s", col, typ))
	}

	createSQL := fmt.Sprintf(
		"CREATE TABLE %s (%s)",
		pgx.Identifier{tableName}.Sanitize(),
		strings.Join(defs, ", "),
	)

	if _, err := pool.Exec(ctx, createSQL); err != nil {
		t.Fatalf("failed to create table %s: %v", tableName, err)
	}

	var oid uint32
	err := pool.QueryRow(
		ctx,
		`SELECT oid FROM pg_class WHERE relname = $1 AND relkind = 'r'`,
		tableName,
	).Scan(&oid)

	if err != nil {
		t.Fatalf("failed to fetch OID for table %s: %v", tableName, err)
	}

	return oid
}

func DropTable(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	tableName string,
) {
	t.Helper()

	dropSQL := fmt.Sprintf(
		"DROP TABLE IF EXISTS %s",
		pgx.Identifier{tableName}.Sanitize(),
	)

	if _, err := pool.Exec(ctx, dropSQL); err != nil {
		t.Fatalf("failed to drop table %s: %v", tableName, err)
	}
}
