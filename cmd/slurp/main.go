package main

import (
	"fmt"
	"os"
)

// Exit codes
const (
	ExitSuccess           = 0
	ExitGeneralError      = 1
	ExitInvalidArgs       = 2
	ExitSourceNotAccess   = 3
	ExitRangeNotSupported = 4
	ExitStorageError      = 5
	ExitSourceChanged     = 6
	ExitValidationFailed  = 7
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		printUsage()
		return ExitInvalidArgs
	}

	command := args[0]
	cmdArgs := args[1:]

	switch command {
	case "upload":
		return runUpload(cmdArgs)
	case "download":
		return runDownload(cmdArgs)
	case "validate":
		return runValidate(cmdArgs)
	case "fix":
		return runFix(cmdArgs)
	case "delete":
		return runDelete(cmdArgs)
	case "help", "-h", "--help":
		printUsage()
		return ExitSuccess
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		return ExitInvalidArgs
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `Usage: slurp <command> [options]

Commands:
  upload    Fetch from HTTP URL and store as sharded file in object storage
  download  Read sharded file from object storage to local file or stdout
  validate  Verify all shards exist and sizes match manifest
  fix       Re-download missing or corrupted shards from source
  delete    Remove sharded file and all shards from storage

Run 'slurp <command> -h' for command-specific help.`)
}
