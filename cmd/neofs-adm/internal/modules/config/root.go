package config

import (
	"github.com/spf13/cobra"
)

const configPathFlag = "path"

var (
	// RootCmd is a root command of config section.
	RootCmd = &cobra.Command{
		Use:   "config",
		Short: "Section for neofs-adm config related commands.",
	}

	initCmd = &cobra.Command{
		Use:   "init",
		Short: "Initialize basic neofs-adm configuration file.",
		Example: `neofs-adm config init
neofs-adm config init --path .config/neofs-adm.yml`,
		RunE: initConfig,
	}
)

func init() {
	RootCmd.AddCommand(initCmd)

	initCmd.Flags().String(configPathFlag, "", "path to config (default ~/.neofs/adm/config.yml)")
}
