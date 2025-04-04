package cmd

import (
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	"github.com/nspcc-dev/neofs-node/cmd/internal/cmderr"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	accountingCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/accounting"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/acl"
	bearerCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/bearer"
	containerCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/container"
	controlCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/control"
	netmapCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/netmap"
	objectCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/object"
	sessionCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/session"
	sgCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/storagegroup"
	utilCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/util"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/util/gendoc"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	envPrefix = "NEOFS_CLI"
)

// Global scope flags.
var (
	cfgFile string
)

// rootCmd represents the base command when called without any subcommands.
var rootCmd = &cobra.Command{
	Use:   "neofs-cli",
	Short: "Command Line Tool to work with NeoFS",
	Long: `NeoFS CLI provides all basic interactions with NeoFS and it's services.

It contains commands for interaction with NeoFS nodes using different versions
of neofs-api and some useful utilities for compiling ACL rules from JSON
notation, managing container access through protocol gates, querying network map
and much more!`,
	Args:          cobra.NoArgs,
	RunE:          entryPoint,
	SilenceErrors: true,
	SilenceUsage:  true,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	// use stdout as default output for cmd.Print()
	rootCmd.SetOut(os.Stdout)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "Config file (default is $HOME/.config/neofs-cli/config.yaml)")
	rootCmd.PersistentFlags().BoolP(commonflags.Verbose, commonflags.VerboseShorthand,
		false, commonflags.VerboseUsage)

	_ = viper.BindPFlag(commonflags.Verbose, rootCmd.PersistentFlags().Lookup(commonflags.Verbose))

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().Bool("version", false, "Application version and NeoFS API compatibility")

	rootCmd.AddCommand(acl.Cmd)
	rootCmd.AddCommand(bearerCli.Cmd)
	rootCmd.AddCommand(sessionCli.Cmd)
	rootCmd.AddCommand(accountingCli.Cmd)
	rootCmd.AddCommand(controlCli.Cmd)
	rootCmd.AddCommand(utilCli.Cmd)
	rootCmd.AddCommand(netmapCli.Cmd)
	rootCmd.AddCommand(objectCli.Cmd)
	rootCmd.AddCommand(sgCli.Cmd)
	rootCmd.AddCommand(containerCli.Cmd)
	rootCmd.AddCommand(gendoc.Command(rootCmd))
}

func entryPoint(cmd *cobra.Command, _ []string) error {
	printVersion, _ := cmd.Flags().GetBool("version")
	if printVersion {
		cmd.Print(misc.BuildInfo("NeoFS CLI"))

		return nil
	}

	return cmd.Usage()
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		cmderr.ExitOnErr(err)

		// Search config in `$HOME/.config/neofs-cli/` with name "config.yaml"
		viper.AddConfigPath(filepath.Join(home, ".config", "neofs-cli"))
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
	}

	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		common.PrintVerbose(rootCmd, "Using config file: %s", viper.ConfigFileUsed())
	}
}
