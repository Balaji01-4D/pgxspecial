package dbcommands

import (
	"context"
	"os"
	"os/exec"

	"github.com/balaji01-4d/pgxspecial/pgxspecial/database"
	"github.com/google/shlex"
	"github.com/jackc/pgx/v5"
)

func ShellCommand(ctx context.Context, db database.DB, args string) (pgx.Rows, error) {
    parts, err := shlex.Split(args)
    if err != nil {
        return nil, err
    }

    cmd := exec.CommandContext(ctx, parts[0], parts[1:]...)
    cmd.Stdout = os.Stdout
    cmd.Stderr = os.Stderr

    err = cmd.Run()

    return nil, err
}
