package modules

import (
	"fmt"
	"os"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	envPrefix = "NEOFS_PAYMENTD"
	cfgFile   string
)

var rootCmd = &cobra.Command{
	Use:   "neofs-paymentd",
	Short: "NeoFS Payment Daemon",
	Long:  ``}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.SetOut(os.Stdout)
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		viper.AddConfigPath(home)
		viper.SetConfigName(".config/neofs-paymentd")
	}

	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv()
}
