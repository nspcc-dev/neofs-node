package cmd

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"os"

	"github.com/mitchellh/go-homedir"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Global scope flags.
var (
	cfgFile    string
	privateKey string
	endpoint   string

	verbose bool
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "neofs-cli",
	Short: "Command Line Tool to work with NeoFS",
	Long: `NeoFS CLI provides all basic interactions with NeoFS and it's services.

It contains commands for interaction with NeoFS nodes using different versions
of neofs-api and some useful utilities for compiling ACL rules from JSON
notation, managing container access through protocol gates, querying network map
and much more!`,
}

var (
	errInvalidKey      = errors.New("provided key is incorrect")
	errInvalidEndpoint = errors.New("provided RPC endpoint is incorrect")
)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.config/neofs-cli/config.yaml)")
	rootCmd.PersistentFlags().StringVarP(&privateKey, "key", "k", "", "private key in hex, WIF or filepath")
	rootCmd.PersistentFlags().StringVarP(&endpoint, "rpc-endpoint", "r", "", "remote node address (as 'multiaddr' or '<host>:<port>')")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".main" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".config/neofs-cli")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

// getKey returns private key that was provided in global arguments.
func getKey() (*ecdsa.PrivateKey, error) {
	key, err := crypto.LoadPrivateKey(privateKey)
	if err != nil {
		return nil, errInvalidKey
	}

	return key, nil
}

// getEndpointAddress returns network address structure that stores multiaddr
// inside, parsed from global arguments.
func getEndpointAddress() (*network.Address, error) {
	addr, err := network.AddressFromString(endpoint)
	if err != nil {
		return nil, errInvalidEndpoint
	}

	return addr, nil
}
