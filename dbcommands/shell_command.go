package dbcommands

import (
	"context"
	"os"
	"os/exec"

	"github.com/balaji01-4d/pgxspecial"
	"github.com/balaji01-4d/pgxspecial/database"
	"github.com/google/shlex"
	"github.com/jackc/pgx/v5"
)

func init() {
	pgxspecial.RegisterCommand(pgxspecial.SpecialCommandRegistry{
		Cmd:         "\\!",
		Description: "Execute a shell command.",
		Syntax:      "\\! command",
		Handler:     ShellCommand,
		CaseSensitive: true,
	})
}

func ShellCommand(ctx context.Context, db database.DB, args string, verbose bool) (pgx.Rows, error) {
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
