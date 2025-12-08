# pgxspecial (NOT PUBLISHED YET)

`pgxspecial` is a Go package that provides an API to execute meta-commands (also known as "special" or "backslash commands") on PostgreSQL, similar to the functionality found in tools like `psql`. It is inspired by the Python library [pgspecial](https://github.com/dbcli/pgspecial).

This library allows you to programmatically access structured data for various database objects and server information.

## Features

*   Execute `psql`-like backslash commands from your Go application.
*   Get structured data for tables, databases, roles, and more.
*   Easy-to-use API that integrates with `pgx/v5`.
*   Provides detailed metadata, such as indexes, constraints, and triggers for tables.

## Installation

To use `pgxspecial` in your project, you can use `go get`:

```sh
go get github.com/balaji01-4d/pgxspecial
```

## Usage

Here is a minimal example of how to use `pgxspecial` to describe a table:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/balaji01-4d/pgxspecial"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()

	// Replace with your database connection string
	connStr := "postgres://user:password@localhost:5432/database?sslmode=disable"

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("Unable to connect: %v\n", err)
	}
	defer pool.Close()

	tableName := "public.my_table"

	// Get detailed information about the table
	results, err := pgxspecial.DescribeTableDetails(ctx, pool, tableName, true)
	if err != nil {
		log.Fatalf("DescribeTableDetails error: %v", err)
	}

    // The result contains detailed information about the table,
    // including columns, indexes, constraints, triggers, and more.
	// For simplicity, we'll just print the column headers.
	if len(results) > 0 {
		fmt.Println("Columns:")
		for _, col := range results[0].Columns {
			fmt.Printf("- %s\n", col)
		}
	}
}
```

The `DescribeTableDetails` function returns a slice of `pgxspecial.DescribeTableResult`, which contains not only the columns and their types but also detailed metadata about the table.

## Supported Commands

`pgxspecial` currently supports the following commands:

*   `\d`: Describe table, view, sequence, or index.
*   `\d+`: Describe table, view, sequence, or index (more details).
*   `\l`: List all databases.
*   `\dT`: List all data types.
*   `\ddp`: List default privileges.
*   `\dD`: List all domains.
*   `\dE`: List all foreign tables.
*   `\df`: List all functions.
*   `\do`: List all operators.
*   `\dp`: List table, view, and sequence access privileges.
*   `\du`: List all roles.
*   `\dn`: List all schemas.
*   `\db`: List all tablespaces.
*   `\sf`: Show a function's definition.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue for any bugs, feature requests, or suggestions.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
