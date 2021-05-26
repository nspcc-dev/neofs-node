package cmd

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/mitchellh/go-homedir"
	"github.com/nspcc-dev/neo-go/cli/flags"
	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-api-go/pkg"
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

	ttlDefaultValue = 2
)

const xHeadersFlag = "xhdr"

var xHeaders []string

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
	errInvalidAddress  = errors.New("--address option must be specified and valid")
	errInvalidPassword = errors.New("invalid password for the encrypted key")
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

	// use stdout as default output for cmd.Print()
	rootCmd.SetOut(os.Stdout)

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.config/neofs-cli/config.yaml)")

	rootCmd.PersistentFlags().StringP("key", "k", "", "private key in hex, WIF, NEP-2 or filepath (use `--key new` to generate key for request)")
	_ = viper.BindPFlag("key", rootCmd.PersistentFlags().Lookup("key"))

	rootCmd.PersistentFlags().StringP("address", "", "", "address of wallet account")
	_ = viper.BindPFlag("address", rootCmd.PersistentFlags().Lookup("address"))

	rootCmd.PersistentFlags().StringP("rpc-endpoint", "r", "", "remote node address (as 'multiaddr' or '<host>:<port>')")
	_ = viper.BindPFlag("rpc", rootCmd.PersistentFlags().Lookup("rpc-endpoint"))

	rootCmd.PersistentFlags().Uint32("ttl", ttlDefaultValue, "TTL value in request meta header")
	_ = viper.BindPFlag("ttl", rootCmd.PersistentFlags().Lookup("ttl"))

	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")

	rootCmd.PersistentFlags().StringSliceVarP(&xHeaders, xHeadersFlag, "x", nil,
		"Request X-Headers in form of Key=Value")
	_ = viper.BindPFlag(xHeadersFlag, rootCmd.PersistentFlags().Lookup(xHeadersFlag))

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

const nep2Base58Length = 58

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
	if err == nil {
		return key, nil
	}

	w, err := wallet.NewWalletFromFile(privateKey)
	if err == nil {
		return getKeyFromWallet(w, viper.GetString("address"))
	}

	if len(privateKey) == nep2Base58Length {
		return getKeyFromNEP2(privateKey)
	}

	return nil, errInvalidKey
}

func getKeyFromNEP2(encryptedWif string) (*ecdsa.PrivateKey, error) {
	pass, err := input.ReadPassword("Enter password > ")
	if err != nil {
		printVerbose("Can't read password: %v", err)
		return nil, errInvalidPassword
	}

	k, err := keys.NEP2Decrypt(encryptedWif, pass)
	if err != nil {
		printVerbose("Invalid key or password: %v", err)
		return nil, errInvalidPassword
	}

	return &k.PrivateKey, nil
}

func getKeyFromWallet(w *wallet.Wallet, addrStr string) (*ecdsa.PrivateKey, error) {
	if addrStr == "" {
		printVerbose("Address is empty")
		return nil, errInvalidAddress
	}

	addr, err := flags.ParseAddress(addrStr)
	if err != nil {
		printVerbose("Can't parse address: %s", addrStr)
		return nil, errInvalidAddress
	}

	acc := w.GetAccount(addr)
	if acc == nil {
		printVerbose("Can't find wallet account for %s", addrStr)
		return nil, errInvalidAddress
	}

	pass, err := input.ReadPassword("Enter password > ")
	if err != nil {
		printVerbose("Can't read password: %v", err)
		return nil, errInvalidPassword
	}

	if err := acc.Decrypt(pass); err != nil {
		printVerbose("Can't decrypt account: %v", err)
		return nil, errInvalidPassword
	}

	return &acc.PrivateKey().PrivateKey, nil
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
func getSDKClient(key *ecdsa.PrivateKey) (client.Client, error) {
	netAddr, err := getEndpointAddress()
	if err != nil {
		return nil, err
	}

	hostAddr, err := netAddr.HostAddrString()
	if err != nil {
		return nil, errInvalidEndpoint
	}

	options := []client.Option{
		client.WithAddress(hostAddr),
		client.WithDefaultPrivateKey(key),
	}

	if netAddr.TLSEnabled() {
		options = append(options, client.WithTLSConfig(&tls.Config{}))
	}

	c, err := client.New(options...)

	return c, err
}

func getTTL() uint32 {
	ttl := viper.GetUint32("ttl")
	printVerbose("TTL: %d", ttl)

	return ttl
}

// ownerFromString converts string with NEO3 wallet address to neofs owner ID.
func ownerFromString(s string) (*owner.ID, error) {
	result := owner.NewID()

	err := result.Parse(s)
	if err != nil {
		return nil, errors.New("can't decode owner ID wallet address")
	}

	return result, nil
}

func printVerbose(format string, a ...interface{}) {
	if verbose {
		fmt.Printf(format+"\n", a...)
	}
}

func parseXHeaders() []*pkg.XHeader {
	xs := make([]*pkg.XHeader, 0, len(xHeaders))

	for i := range xHeaders {
		kv := strings.SplitN(xHeaders[i], "=", 2)
		if len(kv) != 2 {
			panic(fmt.Errorf("invalid X-Header format: %s", xHeaders[i]))
		}

		x := pkg.NewXHeader()
		x.SetKey(kv[0])
		x.SetValue(kv[1])

		xs = append(xs, x)
	}

	return xs
}

func globalCallOptions() []client.CallOption {
	xHdrs := parseXHeaders()

	opts := make([]client.CallOption, 0, len(xHdrs)+1) // + TTL
	opts = append(opts, client.WithTTL(getTTL()))

	for i := range xHdrs {
		opts = append(opts, client.WithXHeader(xHdrs[i]))
	}

	return opts
}
