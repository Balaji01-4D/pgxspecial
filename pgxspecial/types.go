package pgspecial



type SpecialCommand struct {
	Cmd           string
	Syntax        string
	Description   string
	Handler       SpecialHandler
	CaseSensitive bool
}

type SpecialCommandRegistry struct {
	Cmd          string
	Alias         []string
	Syntax        string
	Description   string
	Handler       SpecialHandler
	CaseSensitive bool
}
