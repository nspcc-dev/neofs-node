package main

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/internal/cmderr"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules"
)

func main() {
	err := modules.Execute()
	cmderr.ExitOnErr(fmt.Errorf("Error: %w\n", err))
}
