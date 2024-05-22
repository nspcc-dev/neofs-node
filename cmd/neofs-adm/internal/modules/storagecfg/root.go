package storagecfg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/chzyer/readline"
	"github.com/nspcc-dev/neo-go/cli/flags"
	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/gas"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nep17"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	netutil "github.com/nspcc-dev/neofs-node/pkg/network"

	"github.com/spf13/cobra"
)

const (
	walletFlag  = "wallet"
	accountFlag = "account"
)

const (
	defaultControlEndpoint = "localhost:8090"
	defaultDataEndpoint    = "localhost"
)

// RootCmd is a root command of config section.
var RootCmd = &cobra.Command{
	Use:   "storage-config [-w wallet] [-a acccount] [<path-to-config>]",
	Short: "Section for storage node configuration commands",
	Run:   storageConfig,
}

func init() {
	fs := RootCmd.Flags()

	fs.StringP(walletFlag, "w", "", "Path to wallet")
	fs.StringP(accountFlag, "a", "", "Wallet account")
}

type config struct {
	AnnouncedAddress string
	AuthorizedKeys   []string
	ControlEndpoint  string
	Endpoint         string
	TLSCert          string
	TLSKey           string
	MorphRPC         []string
	Attribute        struct {
		Locode string
	}
	Wallet struct {
		Path     string
		Account  string
		Password string
	}
	Relay        bool
	BlobstorPath string
	MetabasePath string
}

func storageConfig(cmd *cobra.Command, args []string) {
	var outPath string
	if len(args) != 0 {
		outPath = args[0]
	} else {
		outPath = getPath("File to write config at [./config.yml]: ")
		if outPath == "" {
			outPath = "./config.yml"
		}
	}

	historyPath := filepath.Join(os.TempDir(), "neofs-adm.history")
	readline.SetHistoryPath(historyPath)

	var c config

	c.Wallet.Path, _ = cmd.Flags().GetString(walletFlag)
	if c.Wallet.Path == "" {
		c.Wallet.Path = getPath("Path to the storage node wallet: ")
	}

	w, err := wallet.NewWalletFromFile(c.Wallet.Path)
	fatalOnErr(err)

	c.Wallet.Account, _ = cmd.Flags().GetString(accountFlag)
	if c.Wallet.Account == "" {
		addr := address.Uint160ToString(w.GetChangeAddress())
		c.Wallet.Account = getWalletAccount(w, fmt.Sprintf("Wallet account [%s]: ", addr))
		if c.Wallet.Account == "" {
			c.Wallet.Account = addr
		}
	}

	accH, err := flags.ParseAddress(c.Wallet.Account)
	fatalOnErr(err)

	acc := w.GetAccount(accH)
	if acc == nil {
		fatalOnErr(errors.New("can't find account in wallet"))
	}

	c.Wallet.Password, err = input.ReadPassword(fmt.Sprintf("Account password for %s: ", c.Wallet.Account))
	fatalOnErr(err)

	err = acc.Decrypt(c.Wallet.Password, keys.NEP2ScryptParams())
	fatalOnErr(err)

	c.AuthorizedKeys = append(c.AuthorizedKeys, acc.PublicKey().StringCompressed())

	var network string
	for {
		network = getString("Choose network [mainnet]/testnet: ")
		switch network {
		case "":
			network = "mainnet"
		case "testnet", "mainnet":
		default:
			cmd.Println(`Network must be either "mainnet" or "testnet"`)
			continue
		}
		break
	}

	c.MorphRPC = n3config[network].MorphRPC

	depositGas(cmd, acc, network)

	c.Attribute.Locode = getString("UN-LOCODE attribute in [XX YYY] format: ")
	var addr, port string
	for {
		c.AnnouncedAddress = getString("Publicly announced address: ")
		validator := netutil.Address{}
		err := validator.FromString(c.AnnouncedAddress)
		if err != nil {
			cmd.Println("Incorrect address format. See https://github.com/nspcc-dev/neofs-node/blob/master/pkg/network/address.go for details.")
			continue
		}
		uriAddr, err := url.Parse(validator.URIAddr())
		if err != nil {
			panic(fmt.Errorf("unexpected error: %w", err))
		}
		addr = uriAddr.Hostname()
		port = uriAddr.Port()
		ip, err := net.ResolveIPAddr("ip", addr)
		if err != nil {
			cmd.Printf("Can't resolve IP address %s: %v\n", addr, err)
			continue
		}

		if !ip.IP.IsGlobalUnicast() {
			cmd.Println("IP must be global unicast.")
			continue
		}
		cmd.Printf("Resolved IP address: %s\n", ip.String())

		_, err = strconv.ParseUint(port, 10, 16)
		if err != nil {
			cmd.Println("Port must be an integer.")
			continue
		}

		break
	}

	defaultAddr := net.JoinHostPort(defaultDataEndpoint, port)
	c.Endpoint = getString(fmt.Sprintf("Listening address [%s]: ", defaultAddr))
	if c.Endpoint == "" {
		c.Endpoint = defaultAddr
	}

	c.ControlEndpoint = getString(fmt.Sprintf("Listening address (control endpoint) [%s]: ", defaultControlEndpoint))
	if c.ControlEndpoint == "" {
		c.ControlEndpoint = defaultControlEndpoint
	}

	c.TLSCert = getPath("TLS Certificate (optional): ")
	if c.TLSCert != "" {
		c.TLSKey = getPath("TLS Key: ")
	}

	c.Relay = getConfirmation(false, "Use node as a relay? yes/[no]: ")
	if !c.Relay {
		p := getPath("Path to the storage directory (all available storage will be used): ")
		c.BlobstorPath = filepath.Join(p, "blob")
		c.MetabasePath = filepath.Join(p, "meta")
	}

	out := applyTemplate(c)
	fatalOnErr(os.WriteFile(outPath, out, 0o644))

	cmd.Println("Node is ready for work! Run `neofs-node -config " + outPath + "`")
}

func getWalletAccount(w *wallet.Wallet, prompt string) string {
	addrs := make([]readline.PrefixCompleterInterface, len(w.Accounts))
	for i := range w.Accounts {
		addrs[i] = readline.PcItem(w.Accounts[i].Address)
	}

	readline.SetAutoComplete(readline.NewPrefixCompleter(addrs...))
	defer readline.SetAutoComplete(nil)

	s, err := readline.Line(prompt)
	fatalOnErr(err)
	return strings.TrimSpace(s) // autocompleter can return a string with a trailing space
}

func getString(prompt string) string {
	s, err := readline.Line(prompt)
	fatalOnErr(err)
	if s != "" {
		_ = readline.AddHistory(s)
	}
	return s
}

type filenameCompleter struct{}

func (filenameCompleter) Do(line []rune, pos int) (newLine [][]rune, length int) {
	prefix := string(line[:pos])
	dir := filepath.Dir(prefix)
	de, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0
	}

	for i := range de {
		name := filepath.Join(dir, de[i].Name())
		if strings.HasPrefix(name, prefix) {
			tail := []rune(strings.TrimPrefix(name, prefix))
			if de[i].IsDir() {
				tail = append(tail, filepath.Separator)
			}
			newLine = append(newLine, tail)
		}
	}
	if pos != 0 {
		return newLine, pos - len([]rune(dir))
	}
	return newLine, 0
}

func getPath(prompt string) string {
	readline.SetAutoComplete(filenameCompleter{})
	defer readline.SetAutoComplete(nil)

	p, err := readline.Line(prompt)
	fatalOnErr(err)

	if p == "" {
		return p
	}

	_ = readline.AddHistory(p)

	abs, err := filepath.Abs(p)
	if err != nil {
		fatalOnErr(fmt.Errorf("can't create an absolute path: %w", err))
	}

	return abs
}

func getConfirmation(def bool, prompt string) bool {
	for {
		s, err := readline.Line(prompt)
		fatalOnErr(err)

		switch strings.ToLower(s) {
		case "y", "yes":
			return true
		case "n", "no":
			return false
		default:
			if len(s) == 0 {
				return def
			}
		}
	}
}

func applyTemplate(c config) []byte {
	tmpl, err := template.New("config").Parse(configTemplate)
	fatalOnErr(err)

	b := bytes.NewBuffer(nil)
	fatalOnErr(tmpl.Execute(b, c))

	return b.Bytes()
}

func fatalOnErr(err error) {
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func depositGas(cmd *cobra.Command, acc *wallet.Account, network string) {
	sideClient := initClient(n3config[network].MorphRPC)
	balanceHash, _ := util.Uint160DecodeStringLE(n3config[network].BalanceContract)

	sideActor, err := actor.NewSimple(sideClient, acc)
	if err != nil {
		fatalOnErr(fmt.Errorf("creating actor over side chain client: %w", err))
	}

	sideGas := nep17.NewReader(sideActor, balanceHash)
	accSH := acc.Contract.ScriptHash()

	balance, err := sideGas.BalanceOf(accSH)
	if err != nil {
		fatalOnErr(fmt.Errorf("side chain balance: %w", err))
	}

	ok := getConfirmation(false, fmt.Sprintf("Current NeoFS balance is %s, make a deposit? y/[n]: ",
		fixedn.ToString(balance, 12)))
	if !ok {
		return
	}

	amountStr := getString("Enter amount in GAS: ")
	amount, err := fixedn.FromString(amountStr, 8)
	if err != nil {
		fatalOnErr(fmt.Errorf("invalid amount: %w", err))
	}

	mainClient := initClient(n3config[network].RPC)
	neofsHash, _ := util.Uint160DecodeStringLE(n3config[network].NeoFSContract)

	mainActor, err := actor.NewSimple(mainClient, acc)
	if err != nil {
		fatalOnErr(fmt.Errorf("creating actor over main chain client: %w", err))
	}

	mainGas := gas.New(mainActor)

	txHash, _, err := mainGas.Transfer(accSH, neofsHash, amount, nil)
	if err != nil {
		fatalOnErr(fmt.Errorf("sending TX to the NeoFS contract: %w", err))
	}

	cmd.Print("Waiting for transactions to persist.")
	tick := time.NewTicker(time.Second / 2)
	defer tick.Stop()

	timer := time.NewTimer(time.Second * 20)
	defer timer.Stop()

	at := trigger.Application

loop:
	for {
		select {
		case <-tick.C:
			_, err := mainClient.GetApplicationLog(txHash, &at)
			if err == nil {
				cmd.Print("\n")
				break loop
			}
			cmd.Print(".")
		case <-timer.C:
			cmd.Printf("\nTimeout while waiting for transaction to persist.\n")
			if getConfirmation(false, "Continue configuration? yes/[no]: ") {
				return
			}
			os.Exit(1)
		}
	}
}

func initClient(rpc []string) *rpcclient.Client {
	var c *rpcclient.Client
	var err error

	shuffled := make([]string, len(rpc))
	copy(shuffled, rpc)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	for _, endpoint := range shuffled {
		c, err = rpcclient.New(context.Background(), "https://"+endpoint, rpcclient.Options{
			DialTimeout:    time.Second * 2,
			RequestTimeout: time.Second * 5,
		})
		if err != nil {
			continue
		}
		if err = c.Init(); err != nil {
			continue
		}
		return c
	}

	fatalOnErr(fmt.Errorf("can't create N3 client: %w", err))
	panic("unreachable")
}
