package compute

// Command types
const (
	CommandSet   = "SET"
	CommandGet   = "GET"
	CommandDel   = "DEL"
	CommandHelp  = "HELP"
	CommandClear = "CLEAR"
)

// Response messages
const (
	ResponseOK = "OK"
)

// Help messages
const (
	HelpMessage = "Available commands:\n" +
		"  SET <key> <value>  - Set the value of a key\n" +
		"  GET <key>         - Get the value of a key\n" +
		"  DEL <key>         - Delete a key\n" +
		"  CLEAR             - Remove all keys\n" +
		"  help, ?           - Show this help message\n" +
		"  exit              - Exit the client"
)
