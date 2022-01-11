package cmd

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/autocomplete"
)

func init() {
	rootCmd.AddCommand(autocomplete.Command("neofs-cli"))
}
