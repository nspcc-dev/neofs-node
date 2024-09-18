package main

import (
	"github.com/nspcc-dev/neofs-node/cmd/internal/cmderr"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	cmd "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules"
)

func main() {
	err := cmd.Execute()
	cmderr.ExitOnErr(common.WrapError(err))
}
