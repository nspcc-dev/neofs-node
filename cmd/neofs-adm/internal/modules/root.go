package modules

import (
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/config"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/morph"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/storagecfg"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/util/autocomplete"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	rootCmd = &cobra.Command{
		Use:   "neofs-adm",
		Short: "NeoFS Administrative Tool",
		Long: `NeoFS Administrative Tool provides functions to setup and
manage NeoFS network deployment.`,
		RunE:         entryPoint,
		SilenceUsage: true,
	}

	configFlag = "config"
)

func init() {
	cobra.OnInitialize(func() { initConfig(rootCmd) })
	// we need to init viper config to bind viper and cobra configurations for
	// rpc endpoint, alphabet wallet dir, key credentials, etc.

	// use stdout as default output for cmd.Print()
	rootCmd.SetOut(os.Stdout)

	rootCmd.PersistentFlags().StringP(configFlag, "c", "", "config file")
	rootCmd.Flags().Bool("version", false, "application version")

	rootCmd.AddCommand(config.RootCmd)
	rootCmd.AddCommand(morph.RootCmd)
	rootCmd.AddCommand(storagecfg.RootCmd)

	rootCmd.AddCommand(autocomplete.Command("neofs-adm"))
}

func Execute() error {
	return rootCmd.Execute()
}

func entryPoint(cmd *cobra.Command, args []string) error {
	printVersion, err := cmd.Flags().GetBool("version")
	if err == nil && printVersion {
		fmt.Printf("Version: %s \nBuild: %s \nDebug: %s\n",
			misc.Version,
			misc.Build,
			misc.Debug,
		)
		return nil
	}

	return cmd.Usage()
}

func initConfig(cmd *cobra.Command) {
	configFile, err := cmd.Flags().GetString(configFlag)
	if err != nil || configFile == "" {
		return
	}

	viper.SetConfigType("yml")
	viper.SetConfigFile(configFile)
	_ = viper.ReadInConfig() // if config file is set but unavailable, ignore it
}
