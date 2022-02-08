package storagecfg

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/chzyer/readline"
	"github.com/nspcc-dev/neo-go/cli/flags"
	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/spf13/cobra"
)

const (
	walletFlag  = "wallet"
	accountFlag = "account"
)

const (
	defaultControlEndpoint = "127.0.0.1:8090"
	defaultDataEndpoint    = "127.0.0.1"
)

// RootCmd is a root command of config section.
var RootCmd = &cobra.Command{
	Use:   "storage-config [-w wallet] [-a acccount] [<path-to-config>]",
	Short: "Section for storage node configuration commands.",
	Run:   storageConfig,
}

func init() {
	fs := RootCmd.Flags()

	fs.StringP(walletFlag, "w", "", "path to wallet")
	fs.StringP(accountFlag, "a", "", "wallet account")
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

	c.AuthorizedKeys = append(c.AuthorizedKeys, hex.EncodeToString(acc.PrivateKey().PublicKey().Bytes()))

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
		addr, port, err = net.SplitHostPort(c.AnnouncedAddress)
		if err != nil {
			cmd.Println("Address must have form A.B.C.D:PORT")
			continue
		}

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
	fatalOnErr(ioutil.WriteFile(outPath, out, 0644))

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
	s, err := input.ReadLine(prompt)
	fatalOnErr(err)
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

	res, err := sideClient.InvokeFunction(balanceHash, "balanceOf", []smartcontract.Parameter{{
		Type:  smartcontract.Hash160Type,
		Value: acc.Contract.ScriptHash(),
	}}, nil)
	fatalOnErr(err)

	if res.State != vm.HaltState.String() {
		fatalOnErr(fmt.Errorf("invalid response from balance contract: %s", res.FaultException))
	}

	var balance *big.Int
	if len(res.Stack) != 0 {
		balance, _ = res.Stack[0].TryInteger()
	}

	if balance == nil {
		fatalOnErr(errors.New("invalid response from balance contract"))
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

	gasHash, err := mainClient.GetNativeContractHash(nativenames.Gas)
	fatalOnErr(err)

	tx, err := mainClient.CreateNEP17TransferTx(acc, neofsHash, gasHash, amount.Int64(), 0, nil, nil)
	fatalOnErr(err)

	txHash, err := mainClient.SignAndPushTx(tx, acc, nil)
	fatalOnErr(err)

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

func initClient(rpc []string) *client.Client {
	var c *client.Client
	var err error

	shuffled := make([]string, len(rpc))
	copy(shuffled, rpc)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	for _, endpoint := range shuffled {
		c, err = client.New(context.Background(), "https://"+endpoint, client.Options{
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
