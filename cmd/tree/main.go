package main

import (
	"flag"
	"os"
)

func main() {
	if len(os.Args) > 2 && os.Args[1] == "prepare" {
		flag.CommandLine = flag.NewFlagSet(os.Args[0]+" "+os.Args[1], flag.ExitOnError)
		prepareLoadMocks(os.Args[2:])

		return
	}

	runTree()
}
