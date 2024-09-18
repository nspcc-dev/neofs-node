package main

import (
	"github.com/nspcc-dev/neofs-node/cmd/internal/cmderr"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules"
)

func main() {
	err := modules.Execute()
	cmderr.ExitOnErr(err)
}
