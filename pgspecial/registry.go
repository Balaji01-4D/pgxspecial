package pgspecial

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

type SpecialHandler func(ctx context.Context, db DB, args string, verbose bool) (pgx.Rows, error)

var command_map = map[string]SpecialCommand{}

func RegisterCommand(cmdRegistry SpecialCommandRegistry) {

	normalize := func(s string) string {
		if cmdRegistry.CaseSensitive {
			return s
		}
		return strings.ToLower(s)
	}

	cmd := SpecialCommand{
		Cmd:           cmdRegistry.Cmd,
		Description:   cmdRegistry.Description,
		Syntax:        cmdRegistry.Syntax,
		CaseSensitive: cmdRegistry.CaseSensitive,
		Handler:       cmdRegistry.Handler,
	}

	command_map[normalize(cmdRegistry.Cmd)] = cmd

	for _, alias := range cmdRegistry.Alias {
		command_map[normalize(alias)] = cmd
	}

}


func ExecuteSpecialCommand(ctx context.Context, db DB, input string) (pgx.Rows, bool, error) {
	if !strings.HasPrefix(input, "\\") {
		return nil, false, nil
	}

	checkVerbose := func(cmd string) (string, bool) {
		suff := "+"
		return strings.TrimSuffix(cmd, suff), strings.HasSuffix(cmd, suff)
	}

	fields := strings.Fields(input)
	cmd := fields[0]
	args := strings.TrimSpace(strings.TrimPrefix(input, cmd))
	
	cmd, verbose := checkVerbose(cmd)

	command, ok := command_map[cmd]
	if !ok {
		return nil, true, fmt.Errorf("Unknown Command: %s", cmd)
	}
	fmt.Println(cmd, args)
	res, err := command.Handler(ctx, db, args, verbose)
	if err != nil {
		return nil, true, err
	}
	return res, true, nil	
}
