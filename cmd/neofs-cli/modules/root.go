package cmd

import (
	"crypto/ecdsa"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mitchellh/go-homedir"
	"github.com/nspcc-dev/neo-go/cli/flags"
	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/token"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	envPrefix = "NEOFS_CLI"
)

var xHeaders []string

// Global scope flags.
var (
	cfgFile string
)

const (
	// Common CLI flag keys, shorthands, default
	// values and their usage descriptions.
	generateKey          = "generate-key"
	generateKeyShorthand = "g"
	generateKeyDefault   = false
	generateKeyUsage     = "generate new private key"

	binaryKey          = "binary-key"
	binaryKeyShorthand = ""
	binaryKeyDefault   = ""
	binaryKeyUsage     = "path to the raw private key file"

	walletPath          = "wallet"
	walletPathShorthand = "w"
	walletPathDefault   = ""
	walletPathUsage     = "path to the wallet"

	wif          = "wif"
	wifShorthand = ""
	wifDefault   = ""
	wifUsage     = "WIF or NEP-2"

	address          = "address"
	addressShorthand = ""
	addressDefault   = ""
	addressUsage     = "address of wallet account"

	rpc          = "rpc-endpoint"
	rpcShorthand = "r"
	rpcDefault   = ""
	rpcUsage     = "remote node address (as 'multiaddr' or '<host>:<port>')"

	verbose          = "verbose"
	verboseShorthand = "v"
	verboseDefault   = false
	verboseUsage     = "verbose output"

	ttl          = "ttl"
	ttlShorthand = ""
	ttlDefault   = 2
	ttlUsage     = "TTL value in request meta header"

	xHeadersKey       = "xhdr"
	xHeadersShorthand = "x"
	xHeadersUsage     = "Request X-Headers in form of Key=Value"
)

var xHeadersDefault []string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "neofs-cli",
	Short: "Command Line Tool to work with NeoFS",
	Long: `NeoFS CLI provides all basic interactions with NeoFS and it's services.

It contains commands for interaction with NeoFS nodes using different versions
of neofs-api and some useful utilities for compiling ACL rules from JSON
notation, managing container access through protocol gates, querying network map
and much more!`,
	Run: entryPoint,
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
	err := rootCmd.Execute()
	exitOnErr(rootCmd, err)
}

func init() {
	cobra.OnInitialize(initConfig)

	// use stdout as default output for cmd.Print()
	rootCmd.SetOut(os.Stdout)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.config/neofs-cli/config.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().Bool("version", false, "Application version and NeoFS API compatibility")
}

func entryPoint(cmd *cobra.Command, _ []string) {
	printVersion, _ := cmd.Flags().GetBool("version")
	if printVersion {
		cmd.Printf(
			"Version: %s \nBuild: %s \nDebug: %s\n",
			misc.Version,
			misc.Build,
			misc.Debug,
		)

		return
	}

	_ = cmd.Usage()
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		exitOnErr(rootCmd, err)

		// Search config in `$HOME/.config/neofs-cli/` with name "config.yaml"
		viper.AddConfigPath(filepath.Join(home, ".config", "neofs-cli"))
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
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
	if viper.GetBool(generateKey) {
		priv, err := keys.NewPrivateKey()
		if err != nil {
			return nil, errCantGenerateKey
		}
		return &priv.PrivateKey, nil
	}

	if keyPath := viper.GetString(binaryKey); keyPath != "" {
		return getKeyFromFile(keyPath)
	}

	if walletPath := viper.GetString(walletPath); walletPath != "" {
		w, err := wallet.NewWalletFromFile(walletPath)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errInvalidKey, err)
		}
		return getKeyFromWallet(w, viper.GetString(address))
	}

	wif := viper.GetString(wif)
	if len(wif) == nep2Base58Length {
		return getKeyFromNEP2(wif)
	}

	priv, err := keys.NewPrivateKeyFromWIF(wif)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidKey, err)
	}

	return &priv.PrivateKey, nil
}

func getKeyFromFile(keyPath string) (*ecdsa.PrivateKey, error) {
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidKey, err)
	}

	priv, err := keys.NewPrivateKeyFromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidKey, err)
	}

	return &priv.PrivateKey, nil
}

func getKeyFromNEP2(encryptedWif string) (*ecdsa.PrivateKey, error) {
	pass, err := input.ReadPassword("Enter password > ")
	if err != nil {
		printVerbose("Can't read password: %v", err)
		return nil, errInvalidPassword
	}

	k, err := keys.NEP2Decrypt(encryptedWif, pass, keys.NEP2ScryptParams())
	if err != nil {
		printVerbose("Invalid key or password: %v", err)
		return nil, errInvalidPassword
	}

	return &k.PrivateKey, nil
}

func getKeyFromWallet(w *wallet.Wallet, addrStr string) (*ecdsa.PrivateKey, error) {
	var (
		addr util.Uint160
		err  error
	)

	if addrStr == "" {
		printVerbose("Using default wallet address")
		addr = w.GetChangeAddress()
	} else {
		addr, err = flags.ParseAddress(addrStr)
		if err != nil {
			printVerbose("Can't parse address: %s", addrStr)
			return nil, errInvalidAddress
		}
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

	if err := acc.Decrypt(pass, keys.NEP2ScryptParams()); err != nil {
		printVerbose("Can't decrypt account: %v", err)
		return nil, errInvalidPassword
	}

	return &acc.PrivateKey().PrivateKey, nil
}

// getEndpointAddress returns network address structure that stores multiaddr
// inside, parsed from global arguments.
func getEndpointAddress(endpointFlag string) (addr network.Address, err error) {
	endpoint := viper.GetString(endpointFlag)

	err = addr.FromString(endpoint)
	if err != nil {
		err = errInvalidEndpoint
	}

	return
}

type clientWithKey interface {
	SetClient(client.Client)
	SetKey(*ecdsa.PrivateKey)
}

// reads private key from command args and call prepareAPIClientWithKey with it.
func prepareAPIClient(cmd *cobra.Command, dst ...clientWithKey) {
	key, err := getKey()
	exitOnErr(cmd, errf("get private key: %w", err))

	prepareAPIClientWithKey(cmd, key, dst...)
}

// creates NeoFS API client and writes it to target along with the private key.
func prepareAPIClientWithKey(cmd *cobra.Command, key *ecdsa.PrivateKey, dst ...clientWithKey) {
	cli, err := getSDKClient(key)
	exitOnErr(cmd, errf("create API client: %w", err))

	for _, d := range dst {
		d.SetClient(cli)
		d.SetKey(key)
	}
}

type bearerPrm interface {
	SetBearerToken(prm *token.BearerToken)
}

func prepareBearerPrm(cmd *cobra.Command, prm bearerPrm) {
	btok, err := getBearerToken(cmd, bearerTokenFlag)
	exitOnErr(cmd, errf("bearer token: %w", err))

	prm.SetBearerToken(btok)
}

// getSDKClient returns default neofs-api-go sdk client. Consider using
// opts... to provide TTL or other global configuration flags.
func getSDKClient(key *ecdsa.PrivateKey) (client.Client, error) {
	netAddr, err := getEndpointAddress(rpc)
	if err != nil {
		return nil, err
	}

	options := []client.Option{
		client.WithAddress(netAddr.HostAddr()),
		client.WithDefaultPrivateKey(key),
	}

	if netAddr.TLSEnabled() {
		options = append(options, client.WithTLSConfig(&tls.Config{}))
	}

	c, err := client.New(options...)
	if err != nil {
		return nil, fmt.Errorf("coult not init api client:%w", err)
	}

	return c, err
}

func getTTL() uint32 {
	ttl := viper.GetUint32(ttl)
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
	if viper.GetBool(verbose) {
		fmt.Printf(format+"\n", a...)
	}
}

func parseXHeaders() []*session.XHeader {
	xs := make([]*session.XHeader, 0, len(xHeaders))

	for i := range xHeaders {
		kv := strings.SplitN(xHeaders[i], "=", 2)
		if len(kv) != 2 {
			panic(fmt.Errorf("invalid X-Header format: %s", xHeaders[i]))
		}

		x := session.NewXHeader()
		x.SetKey(kv[0])
		x.SetValue(kv[1])

		xs = append(xs, x)
	}

	return xs
}

// add common flags to the command:
// - key;
// - wallet;
// - WIF;
// - address;
// - RPC;
// - verbose;
func initCommonFlags(cmd *cobra.Command) {
	ff := cmd.Flags()

	ff.BoolP(generateKey, generateKeyShorthand, generateKeyDefault, generateKeyUsage)
	ff.StringP(binaryKey, binaryKeyShorthand, binaryKeyDefault, binaryKeyUsage)
	ff.StringP(walletPath, walletPathShorthand, walletPathDefault, walletPathUsage)
	ff.StringP(wif, wifShorthand, wifDefault, wifUsage)
	ff.StringP(address, addressShorthand, addressDefault, addressUsage)
	ff.StringP(rpc, rpcShorthand, rpcDefault, rpcUsage)
	ff.BoolP(verbose, verboseShorthand, verboseDefault, verboseUsage)
}

// bind common command flags to the viper
func bindCommonFlags(cmd *cobra.Command) {
	ff := cmd.Flags()

	_ = viper.BindPFlag(generateKey, ff.Lookup(generateKey))
	_ = viper.BindPFlag(binaryKey, ff.Lookup(binaryKey))
	_ = viper.BindPFlag(walletPath, ff.Lookup(walletPath))
	_ = viper.BindPFlag(wif, ff.Lookup(wif))
	_ = viper.BindPFlag(address, ff.Lookup(address))
	_ = viper.BindPFlag(rpc, ff.Lookup(rpc))
	_ = viper.BindPFlag(verbose, ff.Lookup(verbose))
}

func bindAPIFlags(cmd *cobra.Command) {
	ff := cmd.Flags()

	_ = viper.BindPFlag(ttl, ff.Lookup(ttl))
	_ = viper.BindPFlag(xHeadersKey, ff.Lookup(xHeadersKey))
}
