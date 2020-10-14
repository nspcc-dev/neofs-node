package cmd

import (
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	"github.com/mitchellh/go-homedir"
	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	envPrefix = "NEOFS_CLI"

	generateKeyConst = "new"
)

// Global scope flags.
var (
	cfgFile string
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
	errCantGenerateKey = errors.New("can't generate new private key")
)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.config/neofs-cli/config.yaml)")

	rootCmd.PersistentFlags().StringP("key", "k", "", "private key in hex, WIF or filepath (use `--key new` to generate key for request)")
	_ = viper.BindPFlag("key", rootCmd.PersistentFlags().Lookup("key"))

	rootCmd.PersistentFlags().StringP("rpc-endpoint", "r", "", "remote node address (as 'multiaddr' or '<host>:<port>')")
	_ = viper.BindPFlag("rpc", rootCmd.PersistentFlags().Lookup("rpc-endpoint"))

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

	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		printVerbose("Using config file: %s", viper.ConfigFileUsed())
	}
}

// getKey returns private key that was provided in global arguments.
func getKey() (*ecdsa.PrivateKey, error) {
	privateKey := viper.GetString("key")
	if privateKey == generateKeyConst {
		buf := make([]byte, crypto.PrivateKeyCompressedSize)

		_, err := rand.Read(buf)
		if err != nil {
			return nil, errCantGenerateKey
		}

		printVerbose("Generating private key:", hex.EncodeToString(buf))

		return crypto.UnmarshalPrivateKey(buf)
	}

	key, err := crypto.LoadPrivateKey(privateKey)
	if err != nil {
		return nil, errInvalidKey
	}

	return key, nil
}

// getEndpointAddress returns network address structure that stores multiaddr
// inside, parsed from global arguments.
func getEndpointAddress() (*network.Address, error) {
	endpoint := viper.GetString("rpc")

	addr, err := network.AddressFromString(endpoint)
	if err != nil {
		return nil, errInvalidEndpoint
	}

	return addr, nil
}

// getSDKClient returns default neofs-api-go sdk client. Consider using
// opts... to provide TTL or other global configuration flags.
func getSDKClient() (*client.Client, error) {
	key, err := getKey()
	if err != nil {
		return nil, err
	}

	netAddr, err := getEndpointAddress()
	if err != nil {
		return nil, err
	}

	ipAddr, err := netAddr.IPAddrString()
	if err != nil {
		return nil, errInvalidEndpoint
	}

	return client.New(key, client.WithAddress(ipAddr))
}

// ownerFromString converts string with NEO3 wallet address to neofs owner ID.
func ownerFromString(s string) (*owner.ID, error) {
	var w owner.NEO3Wallet

	// todo: move this into neofs-api-go `owner.NEO3WalletFromString` function
	binaryWallet, err := base58.Decode(s)
	if err != nil || len(binaryWallet) != len(w) {
		return nil, errors.New("can't decode owner ID wallet address")
	}

	copy(w[:], binaryWallet)

	id := owner.NewID()
	id.SetNeo3Wallet(&w)

	return id, nil
}

func printVerbose(format string, a ...interface{}) {
	if verbose {
		fmt.Printf(format+"\n", a...)
	}
}
