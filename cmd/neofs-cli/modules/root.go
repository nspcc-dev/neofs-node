package cmd

import (
	"crypto/ecdsa"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mitchellh/go-homedir"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	accountingCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/accounting"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/acl"
	bearerCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/bearer"
	controlCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/control"
	sessionCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/session"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/util/gendoc"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
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

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	common.ExitOnErr(rootCmd, "", err)
}

func init() {
	cobra.OnInitialize(initConfig)

	// use stdout as default output for cmd.Print()
	rootCmd.SetOut(os.Stdout)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.config/neofs-cli/config.yaml)")
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
	rootCmd.AddCommand(gendoc.Command(rootCmd))
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
		common.ExitOnErr(rootCmd, "", err)

		// Search config in `$HOME/.config/neofs-cli/` with name "config.yaml"
		viper.AddConfigPath(filepath.Join(home, ".config", "neofs-cli"))
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
	}

	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		common.PrintVerbose("Using config file: %s", viper.ConfigFileUsed())
	}
}

type clientWithKey interface {
	SetClient(*client.Client)
}

// reads private key from command args and call prepareAPIClientWithKey with it.
func prepareAPIClient(cmd *cobra.Command, dst ...clientWithKey) {
	p := key.GetOrGenerate(cmd)

	prepareAPIClientWithKey(cmd, p, dst...)
}

// creates NeoFS API client and writes it to target along with the private key.
func prepareAPIClientWithKey(cmd *cobra.Command, key *ecdsa.PrivateKey, dst ...clientWithKey) {
	cli := internalclient.GetSDKClientByFlag(cmd, key, commonflags.RPC)

	for _, d := range dst {
		d.SetClient(cli)
	}
}

type bearerPrm interface {
	SetBearerToken(prm *bearer.Token)
}

func prepareBearerPrm(cmd *cobra.Command, prm bearerPrm) {
	btok, err := getBearerToken(cmd, bearerTokenFlag)
	common.ExitOnErr(cmd, "bearer token: %w", err)

	prm.SetBearerToken(btok)
}

func getTTL() uint32 {
	ttl := viper.GetUint32(ttl)
	common.PrintVerbose("TTL: %d", ttl)

	return ttl
}

// userFromString decodes user ID from string input.
func userFromString(id *user.ID, s string) error {
	err := id.DecodeString(s)
	if err != nil {
		return fmt.Errorf("invalid user ID: %w", err)
	}

	return nil
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

func bindAPIFlags(cmd *cobra.Command) {
	ff := cmd.Flags()

	_ = viper.BindPFlag(ttl, ff.Lookup(ttl))
	_ = viper.BindPFlag(xHeadersKey, ff.Lookup(xHeadersKey))
}
