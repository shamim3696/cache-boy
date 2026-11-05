package transaction

// Queue holds commands sent between MULTI and EXEC.
type Queue struct {
	Commands [][]string
	// Failed is set when a queued command had an error (unknown command,
	// wrong arity); EXEC then aborts with EXECABORT.
	Failed bool
}
