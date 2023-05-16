package morph

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/cli/flags"
	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/morph/internal"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/subnet"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func viperBindFlags(cmd *cobra.Command, flags ...string) {
	for i := range flags {
		_ = viper.BindPFlag(flags[i], cmd.Flags().Lookup(flags[i]))
	}
}

// subnet command section.
var cmdSubnet = &cobra.Command{
	Use:   "subnet",
	Short: "NeoFS subnet management",
	PreRun: func(cmd *cobra.Command, _ []string) {
		viperBindFlags(cmd,
			endpointFlag,
		)
	},
}

// shared flags of cmdSubnet sub-commands.
const (
	flagSubnet        = "subnet"  // subnet identifier
	flagSubnetGroup   = "group"   // subnet client group ID
	flagSubnetWallet  = "wallet"  // filepath to wallet
	flagSubnetAddress = "address" // address in the wallet, optional
)

// reads wallet from the filepath configured in flagSubnetWallet flag,
// looks for address specified in flagSubnetAddress flag (uses default
// address if flag is empty) and decrypts private key.
func readSubnetKey(key *keys.PrivateKey) error {
	// read wallet from file

	walletPath := viper.GetString(flagSubnetWallet)
	if walletPath == "" {
		return errors.New("missing path to wallet")
	}

	w, err := wallet.NewWalletFromFile(walletPath)
	if err != nil {
		return fmt.Errorf("read wallet from file: %w", err)
	}

	// read account from the wallet

	var (
		addr    util.Uint160
		addrStr = viper.GetString(flagSubnetAddress)
	)

	if addrStr == "" {
		addr = w.GetChangeAddress()
	} else {
		addr, err = flags.ParseAddress(addrStr)
		if err != nil {
			return fmt.Errorf("read wallet address: %w", err)
		}
	}

	acc := w.GetAccount(addr)
	if acc == nil {
		return fmt.Errorf("address %s not found in %s", addrStr, walletPath)
	}

	// read password
	pass, err := input.ReadPassword("Enter password > ")
	if err != nil {
		return fmt.Errorf("read password: %w", err)
	}

	// decrypt with just read password
	err = acc.Decrypt(pass, keys.NEP2ScryptParams())
	if err != nil {
		return fmt.Errorf("decrypt wallet: %w", err)
	}

	*key = *acc.PrivateKey()

	return nil
}

// create subnet command.
var cmdSubnetCreate = &cobra.Command{
	Use:   "create",
	Short: "Create NeoFS subnet",
	PreRun: func(cmd *cobra.Command, _ []string) {
		viperBindFlags(cmd,
			flagSubnetWallet,
			flagSubnetAddress,
		)
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		// read private key
		var key keys.PrivateKey

		err := readSubnetKey(&key)
		if err != nil {
			return fmt.Errorf("read private key: %w", err)
		}

		// generate subnet ID and marshal it
		var (
			id  subnetid.ID
			num uint32
		)

		for {
			num = rand.Uint32()

			id.SetNumeric(num)

			if !subnetid.IsZero(id) {
				break
			}
		}

		// declare creator ID and encode it
		var creator user.ID
		err = user.IDFromSigner(&creator, neofsecdsa.SignerRFC6979(key.PrivateKey))
		if err != nil {
			return fmt.Errorf("decoding user from key: %w", err)
		}

		// fill subnet info and encode it
		var info subnet.Info

		info.SetID(id)
		info.SetOwner(creator)

		err = invokeMethod(key, true, "put", id.Marshal(), key.PublicKey().Bytes(), info.Marshal())
		if err != nil {
			return fmt.Errorf("morph invocation: %w", err)
		}

		cmd.Printf("Create subnet request sent successfully. ID: %s.\n", &id)

		return nil
	},
}

// cmdSubnetRemove flags.
const (
	// subnet ID to be removed.
	flagSubnetRemoveID = flagSubnet
)

// errZeroSubnet is returned on attempts to work with zero subnet which is virtual.
var errZeroSubnet = errors.New("zero subnet")

// remove subnet command.
var cmdSubnetRemove = &cobra.Command{
	Use:   "remove",
	Short: "Remove NeoFS subnet",
	PreRun: func(cmd *cobra.Command, _ []string) {
		viperBindFlags(cmd,
			flagSubnetWallet,
			flagSubnetAddress,
			flagSubnetRemoveID,
		)
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		// read private key
		var key keys.PrivateKey

		err := readSubnetKey(&key)
		if err != nil {
			return fmt.Errorf("read private key: %w", err)
		}

		// read ID and encode it
		var id subnetid.ID

		err = id.DecodeString(viper.GetString(flagSubnetRemoveID))
		if err != nil {
			return fmt.Errorf("decode ID text: %w", err)
		}

		if subnetid.IsZero(id) {
			return errZeroSubnet
		}

		err = invokeMethod(key, false, "delete", id.Marshal())
		if err != nil {
			return fmt.Errorf("morph invocation: %w", err)
		}

		cmd.Println("Remove subnet request sent successfully")

		return nil
	},
}

// cmdSubnetGet flags.
const (
	// subnet ID to be read.
	flagSubnetGetID = flagSubnet
)

// get subnet command.
var cmdSubnetGet = &cobra.Command{
	Use:   "get",
	Short: "Read information about the NeoFS subnet",
	PreRun: func(cmd *cobra.Command, _ []string) {
		viperBindFlags(cmd,
			flagSubnetGetID,
		)
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		// read ID and encode it
		var id subnetid.ID

		err := id.DecodeString(viper.GetString(flagSubnetGetID))
		if err != nil {
			return fmt.Errorf("decode ID text: %w", err)
		}

		if subnetid.IsZero(id) {
			return errZeroSubnet
		}

		// use random key to fetch the data
		// we could use raw neo-go client to perform testInvoke
		// without keys, as it is done in other commands
		key, err := keys.NewPrivateKey()
		if err != nil {
			return fmt.Errorf("init subnet client: %w", err)
		}

		res, err := testInvokeMethod(*key, "get", id.Marshal())
		if err != nil {
			return fmt.Errorf("morph invocation: %w", err)
		}

		if len(res) == 0 {
			return errors.New("subnet does not exist")
		}

		data, err := client.BytesFromStackItem(res[0])
		if err != nil {
			return fmt.Errorf("decoding contract response: %w", err)
		}

		// decode info
		var info subnet.Info
		if err = info.Unmarshal(data); err != nil {
			return fmt.Errorf("decode subnet info: %w", err)
		}

		// print information
		cmd.Printf("Owner: %s\n", info.Owner())

		return nil
	},
}

// cmdSubnetAdmin subnet flags.
const (
	flagSubnetAdminSubnet = flagSubnet // subnet ID to be managed
	flagSubnetAdminID     = "admin"    // admin public key
	flagSubnetAdminClient = "client"   // manage client admins instead of node ones
)

// command to manage subnet admins.
var cmdSubnetAdmin = &cobra.Command{
	Use:   "admin",
	Short: "Manage administrators of the NeoFS subnet",
	PreRun: func(cmd *cobra.Command, args []string) {
		viperBindFlags(cmd,
			flagSubnetWallet,
			flagSubnetAddress,
			flagSubnetAdminSubnet,
			flagSubnetAdminID,
		)
	},
}

// cmdSubnetAdminAdd flags.
const (
	flagSubnetAdminAddGroup = flagSubnetGroup // client group ID
)

// common executor cmdSubnetAdminAdd and cmdSubnetAdminRemove commands.
func manageSubnetAdmins(cmd *cobra.Command, rm bool) error {
	// read private key
	var key keys.PrivateKey

	err := readSubnetKey(&key)
	if err != nil {
		return fmt.Errorf("read private key: %w", err)
	}

	// read ID and encode it
	var id subnetid.ID

	err = id.DecodeString(viper.GetString(flagSubnetAdminSubnet))
	if err != nil {
		return fmt.Errorf("decode ID text: %w", err)
	}

	if subnetid.IsZero(id) {
		return errZeroSubnet
	}

	// read admin key and decode it
	binAdminKey, err := hex.DecodeString(viper.GetString(flagSubnetAdminID))
	if err != nil {
		return fmt.Errorf("decode admin key text: %w", err)
	}

	var pubkey keys.PublicKey
	if err = pubkey.DecodeBytes(binAdminKey); err != nil {
		return fmt.Errorf("admin key format: %w", err)
	}

	// prepare call parameters
	prm := make([]interface{}, 0, 3)
	prm = append(prm, id.Marshal())

	var method string

	if viper.GetBool(flagSubnetAdminClient) {
		// read group ID and encode it
		var groupID internal.SubnetClientGroupID

		err = groupID.UnmarshalText([]byte(viper.GetString(flagSubnetAdminAddGroup)))
		if err != nil {
			return fmt.Errorf("decode group ID text: %w", err)
		}

		binGroupID, err := groupID.Marshal()
		if err != nil {
			return fmt.Errorf("marshal group ID: %w", err)
		}

		if rm {
			method = "removeClientAdmin"
		} else {
			method = "addClientAdmin"
		}

		prm = append(prm, binGroupID)
	} else {
		if rm {
			method = "removeNodeAdmin"
		} else {
			method = "addNodeAdmin"
		}
	}

	prm = append(prm, binAdminKey)

	err = invokeMethod(key, false, method, prm...)
	if err != nil {
		return fmt.Errorf("morph invocation: %w", err)
	}

	var op string

	if rm {
		op = "Remove"
	} else {
		op = "Add"
	}

	cmd.Printf("%s admin request sent successfully.\n", op)

	return nil
}

// command to add subnet admin.
var cmdSubnetAdminAdd = &cobra.Command{
	Use:   "add",
	Short: "Add admin to the NeoFS subnet",
	PreRun: func(cmd *cobra.Command, _ []string) {
		viperBindFlags(cmd,
			flagSubnetAdminAddGroup,
			flagSubnetAdminClient,
		)
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		return manageSubnetAdmins(cmd, false)
	},
}

// command to remove subnet admin.
var cmdSubnetAdminRemove = &cobra.Command{
	Use:   "remove",
	Short: "Remove admin of the NeoFS subnet",
	PreRun: func(cmd *cobra.Command, _ []string) {
		viperBindFlags(cmd,
			flagSubnetAdminClient,
		)
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		return manageSubnetAdmins(cmd, true)
	},
}

// cmdSubnetClient flags.
const (
	flagSubnetClientSubnet = flagSubnet            // ID of the subnet to be managed
	flagSubnetClientID     = flagSubnetAdminClient // client's NeoFS ID
	flagSubnetClientGroup  = flagSubnetGroup       // ID of the subnet client group
)

// command to manage subnet clients.
var cmdSubnetClient = &cobra.Command{
	Use:   "client",
	Short: "Manage clients of the NeoFS subnet",
	PreRun: func(cmd *cobra.Command, _ []string) {
		viperBindFlags(cmd,
			flagSubnetWallet,
			flagSubnetAddress,
			flagSubnetClientSubnet,
			flagSubnetClientID,
			flagSubnetClientGroup,
		)
	},
}

// common executor cmdSubnetClientAdd and cmdSubnetClientRemove commands.
func manageSubnetClients(cmd *cobra.Command, rm bool) error {
	// read private key
	var key keys.PrivateKey

	err := readSubnetKey(&key)
	if err != nil {
		return fmt.Errorf("read private key: %w", err)
	}

	// read ID and encode it
	var id subnetid.ID

	err = id.DecodeString(viper.GetString(flagSubnetClientSubnet))
	if err != nil {
		return fmt.Errorf("decode ID text: %w", err)
	}

	if subnetid.IsZero(id) {
		return errZeroSubnet
	}

	// read client ID and encode it
	var clientID user.ID

	err = clientID.DecodeString(viper.GetString(flagSubnetClientID))
	if err != nil {
		return fmt.Errorf("decode client ID text: %w", err)
	}

	// read group ID and encode it
	var groupID internal.SubnetClientGroupID

	err = groupID.UnmarshalText([]byte(viper.GetString(flagSubnetAdminAddGroup)))
	if err != nil {
		return fmt.Errorf("decode group ID text: %w", err)
	}

	binGroupID, err := groupID.Marshal()
	if err != nil {
		return fmt.Errorf("marshal group ID: %w", err)
	}

	var method string
	if rm {
		method = "removeUser"
	} else {
		method = "addUser"
	}

	err = invokeMethod(key, false, method, id.Marshal(), binGroupID, clientID.WalletBytes())
	if err != nil {
		return fmt.Errorf("morph invocation: %w", err)
	}

	var op string

	if rm {
		op = "Remove"
	} else {
		op = "Add"
	}

	cmd.Printf("%s client request sent successfully.\n", op)

	return nil
}

// command to add subnet client.
var cmdSubnetClientAdd = &cobra.Command{
	Use:   "add",
	Short: "Add client to the NeoFS subnet",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return manageSubnetClients(cmd, false)
	},
}

// command to remove subnet client.
var cmdSubnetClientRemove = &cobra.Command{
	Use:   "remove",
	Short: "Remove client of the NeoFS subnet",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return manageSubnetClients(cmd, true)
	},
}

// cmdSubnetNode flags.
const (
	flagSubnetNode       = "node"     // node ID
	flagSubnetNodeSubnet = flagSubnet // ID of the subnet to be managed
)

// common executor cmdSubnetNodeAdd and cmdSubnetNodeRemove commands.
func manageSubnetNodes(cmd *cobra.Command, rm bool) error {
	// read private key
	var key keys.PrivateKey

	err := readSubnetKey(&key)
	if err != nil {
		return fmt.Errorf("read private key: %w", err)
	}

	// read ID and encode it
	var id subnetid.ID

	err = id.DecodeString(viper.GetString(flagSubnetNodeSubnet))
	if err != nil {
		return fmt.Errorf("decode ID text: %w", err)
	}

	if subnetid.IsZero(id) {
		return errZeroSubnet
	}

	// read node  ID and encode it
	binNodeID, err := hex.DecodeString(viper.GetString(flagSubnetNode))
	if err != nil {
		return fmt.Errorf("decode node ID text: %w", err)
	}

	var pubkey keys.PublicKey
	if err = pubkey.DecodeBytes(binNodeID); err != nil {
		return fmt.Errorf("node ID format: %w", err)
	}

	var method string
	if rm {
		method = "removeNode"
	} else {
		method = "addNode"
	}

	err = invokeMethod(key, false, method, id.Marshal(), binNodeID)
	if err != nil {
		return fmt.Errorf("morph invocation: %w", err)
	}

	var op string

	if rm {
		op = "Remove"
	} else {
		op = "Add"
	}

	cmd.Printf("%s node request sent successfully.\n", op)

	return nil
}

// command to manage subnet nodes.
var cmdSubnetNode = &cobra.Command{
	Use:   "node",
	Short: "Manage nodes of the NeoFS subnet",
	PreRun: func(cmd *cobra.Command, _ []string) {
		viperBindFlags(cmd,
			flagSubnetWallet,
			flagSubnetNode,
			flagSubnetNodeSubnet,
		)
	},
}

// command to add subnet node.
var cmdSubnetNodeAdd = &cobra.Command{
	Use:   "add",
	Short: "Add node to the NeoFS subnet",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return manageSubnetNodes(cmd, false)
	},
}

// command to remove subnet node.
var cmdSubnetNodeRemove = &cobra.Command{
	Use:   "remove",
	Short: "Remove node from the NeoFS subnet",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return manageSubnetNodes(cmd, true)
	},
}

// returns function which calls PreRun on parent if it exists.
func inheritPreRun(preRun func(*cobra.Command, []string)) func(*cobra.Command, []string) {
	return func(cmd *cobra.Command, args []string) {
		par := cmd.Parent()
		if par != nil && par.PreRun != nil {
			par.PreRun(par, args)
		}

		if preRun != nil {
			preRun(cmd, args)
		}
	}
}

// inherits PreRun function of parent command in all sub-commands and
// adds them to the parent.
func addCommandInheritPreRun(par *cobra.Command, subs ...*cobra.Command) {
	for _, sub := range subs {
		sub.PreRun = inheritPreRun(sub.PreRun)
	}

	par.AddCommand(subs...)
}

// registers flags and binds sub-commands for subnet commands.
func init() {
	cmdSubnetCreate.Flags().StringP(flagSubnetWallet, "w", "", "Path to file with wallet")
	_ = cmdSubnetCreate.MarkFlagRequired(flagSubnetWallet)
	cmdSubnetCreate.Flags().StringP(flagSubnetAddress, "a", "", "Address in the wallet, optional")

	// get subnet flags
	cmdSubnetGet.Flags().String(flagSubnetGetID, "", "ID of the subnet to read")
	_ = cmdSubnetAdminAdd.MarkFlagRequired(flagSubnetGetID)

	// remove subnet flags
	cmdSubnetRemove.Flags().String(flagSubnetRemoveID, "", "ID of the subnet to remove")
	_ = cmdSubnetRemove.MarkFlagRequired(flagSubnetRemoveID)
	cmdSubnetRemove.Flags().StringP(flagSubnetWallet, "w", "", "Path to file with wallet")
	_ = cmdSubnetRemove.MarkFlagRequired(flagSubnetWallet)
	cmdSubnetRemove.Flags().StringP(flagSubnetAddress, "a", "", "Address in the wallet, optional")

	// subnet administer flags
	adminFlags := cmdSubnetAdmin.PersistentFlags()
	adminFlags.String(flagSubnetAdminSubnet, "", "ID of the subnet to manage administrators")
	_ = cmdSubnetAdmin.MarkFlagRequired(flagSubnetAdminSubnet)
	adminFlags.String(flagSubnetAdminID, "", "Hex-encoded public key of the admin")
	_ = cmdSubnetAdmin.MarkFlagRequired(flagSubnetAdminID)
	adminFlags.StringP(flagSubnetWallet, "w", "", "Path to file with wallet")
	_ = cmdSubnetAdmin.MarkFlagRequired(flagSubnetWallet)
	adminFlags.StringP(flagSubnetAddress, "a", "", "Address in the wallet, optional")

	// add admin flags
	cmdSubnetAdminAddFlags := cmdSubnetAdminAdd.Flags()
	cmdSubnetAdminAddFlags.String(flagSubnetAdminAddGroup, "", fmt.Sprintf(
		"Client group ID in text format (needed with --%s only)", flagSubnetAdminClient))
	cmdSubnetAdminAddFlags.Bool(flagSubnetAdminClient, false, "Add client admin instead of node one")

	// remove admin flags
	cmdSubnetAdminRemoveFlags := cmdSubnetAdminRemove.Flags()
	cmdSubnetAdminRemoveFlags.Bool(flagSubnetAdminClient, false, "Remove client admin instead of node one")

	// client managements flags
	clientFlags := cmdSubnetClient.PersistentFlags()
	clientFlags.String(flagSubnetClientSubnet, "", "ID of the subnet to be managed")
	_ = cmdSubnetClient.MarkFlagRequired(flagSubnetClientSubnet)
	clientFlags.String(flagSubnetClientGroup, "", "ID of the client group to work with")
	_ = cmdSubnetClient.MarkFlagRequired(flagSubnetClientGroup)
	clientFlags.String(flagSubnetClientID, "", "Client's user ID in NeoFS system in text format")
	_ = cmdSubnetClient.MarkFlagRequired(flagSubnetClientID)
	clientFlags.StringP(flagSubnetWallet, "w", "", "Path to file with wallet")
	_ = cmdSubnetClient.MarkFlagRequired(flagSubnetWallet)
	clientFlags.StringP(flagSubnetAddress, "a", "", "Address in the wallet, optional")

	// add all admin managing commands to corresponding command section
	addCommandInheritPreRun(cmdSubnetAdmin,
		cmdSubnetAdminAdd,
		cmdSubnetAdminRemove,
	)

	// add all client managing commands to corresponding command section
	addCommandInheritPreRun(cmdSubnetClient,
		cmdSubnetClientAdd,
		cmdSubnetClientRemove,
	)

	// subnet node flags
	nodeFlags := cmdSubnetNode.PersistentFlags()
	nodeFlags.StringP(flagSubnetWallet, "w", "", "Path to file with wallet")
	_ = cmdSubnetNode.MarkFlagRequired(flagSubnetWallet)
	nodeFlags.String(flagSubnetNode, "", "Hex-encoded public key of the node")
	_ = cmdSubnetNode.MarkFlagRequired(flagSubnetNode)
	nodeFlags.String(flagSubnetNodeSubnet, "", "ID of the subnet to manage nodes")
	_ = cmdSubnetNode.MarkFlagRequired(flagSubnetNodeSubnet)

	// add all node managing commands to corresponding command section
	addCommandInheritPreRun(cmdSubnetNode,
		cmdSubnetNodeAdd,
		cmdSubnetNodeRemove,
	)

	// subnet global flags
	cmdSubnetFlags := cmdSubnet.PersistentFlags()
	cmdSubnetFlags.StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
	_ = cmdSubnet.MarkFlagRequired(endpointFlag)

	// add all subnet commands to corresponding command section
	addCommandInheritPreRun(cmdSubnet,
		cmdSubnetCreate,
		cmdSubnetRemove,
		cmdSubnetGet,
		cmdSubnetAdmin,
		cmdSubnetClient,
		cmdSubnetNode,
	)
}

func testInvokeMethod(key keys.PrivateKey, method string, args ...interface{}) ([]stackitem.Item, error) {
	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return nil, fmt.Errorf("morph client creation: %w", err)
	}

	nnsCs, err := c.GetContractStateByID(1)
	if err != nil {
		return nil, fmt.Errorf("NNS contract resolving: %w", err)
	}

	cosigner := []transaction.Signer{
		{
			Account: key.PublicKey().GetScriptHash(),
			Scopes:  transaction.Global,
		},
	}

	inv := invoker.New(c, cosigner)

	subnetHash, err := nnsResolveHash(inv, nnsCs.Hash, subnetContract+".neofs")
	if err != nil {
		return nil, fmt.Errorf("subnet hash resolving: %w", err)
	}

	res, err := inv.Call(subnetHash, method, args...)
	if err != nil {
		return nil, fmt.Errorf("invocation parameters prepararion: %w", err)
	}

	err = checkInvocationResults(res)
	if err != nil {
		return nil, err
	}

	return res.Stack, nil
}

func invokeMethod(key keys.PrivateKey, tryNotary bool, method string, args ...interface{}) error {
	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("morph client creation: %w", err)
	}

	if tryNotary {
		cc, err := c.GetNativeContracts()
		if err != nil {
			return fmt.Errorf("native hashes: %w", err)
		}

		var notary bool
		var notaryHash util.Uint160
		for _, c := range cc {
			if c.Manifest.Name == nativenames.Notary {
				notary = len(c.UpdateHistory) > 0
				notaryHash = c.Hash

				break
			}
		}

		if notary {
			err = invokeNotary(c, key, method, notaryHash, args...)
			if err != nil {
				return fmt.Errorf("notary invocation: %w", err)
			}

			return nil
		}
	}

	err = invokeNonNotary(c, key, method, args...)
	if err != nil {
		return fmt.Errorf("non-notary invocation: %w", err)
	}

	return nil
}

func invokeNonNotary(c Client, key keys.PrivateKey, method string, args ...interface{}) error {
	nnsCs, err := c.GetContractStateByID(1)
	if err != nil {
		return fmt.Errorf("NNS contract resolving: %w", err)
	}

	acc := wallet.NewAccountFromPrivateKey(&key)

	cosigner := []transaction.Signer{
		{
			Account: key.PublicKey().GetScriptHash(),
			Scopes:  transaction.Global,
		},
	}

	cosignerAcc := []rpcclient.SignerAccount{
		{
			Signer:  cosigner[0],
			Account: acc,
		},
	}

	inv := invoker.New(c, cosigner)

	subnetHash, err := nnsResolveHash(inv, nnsCs.Hash, subnetContract+".neofs")
	if err != nil {
		return fmt.Errorf("subnet hash resolving: %w", err)
	}

	test, err := inv.Call(subnetHash, method, args...)
	if err != nil {
		return fmt.Errorf("test invocation: %w", err)
	}

	err = checkInvocationResults(test)
	if err != nil {
		return err
	}

	_, err = c.SignAndPushInvocationTx(test.Script, acc, test.GasConsumed, 0, cosignerAcc)
	if err != nil {
		return fmt.Errorf("sending transaction: %w", err)
	}

	return nil
}

func invokeNotary(c Client, key keys.PrivateKey, method string, notaryHash util.Uint160, args ...interface{}) error {
	nnsCs, err := c.GetContractStateByID(1)
	if err != nil {
		return fmt.Errorf("NNS contract resolving: %w", err)
	}

	alphabet, err := c.GetCommittee()
	if err != nil {
		return fmt.Errorf("alphabet list: %w", err)
	}

	multisigScript, err := smartcontract.CreateDefaultMultiSigRedeemScript(alphabet)
	if err != nil {
		return fmt.Errorf("alphabet multi-signature script: %w", err)
	}

	cosigners, err := notaryCosigners(c, notaryHash, nnsCs, key, hash.Hash160(multisigScript))
	if err != nil {
		return fmt.Errorf("cosigners collecting: %w", err)
	}

	inv := invoker.New(c, cosigners)

	subnetHash, err := nnsResolveHash(inv, nnsCs.Hash, subnetContract+".neofs")
	if err != nil {
		return fmt.Errorf("subnet hash resolving: %w", err)
	}

	// make test invocation of the method
	test, err := inv.Call(subnetHash, method, args...)
	if err != nil {
		return fmt.Errorf("test invocation: %w", err)
	}

	err = checkInvocationResults(test)
	if err != nil {
		return err
	}

	multisigAccount := &wallet.Account{
		Contract: &wallet.Contract{
			Script: multisigScript,
		},
	}

	bc, err := c.GetBlockCount()
	if err != nil {
		return fmt.Errorf("blockchain height: %w", err)
	}

	signersNumber := uint8(smartcontract.GetDefaultHonestNodeCount(len(alphabet)) + 1) // alphabet multisig + key signature

	// notaryRequestValidity is number of blocks during
	// witch notary request is considered valid
	const notaryRequestValidity = 100

	mainTx := &transaction.Transaction{
		Nonce:           rand.Uint32(),
		SystemFee:       test.GasConsumed,
		ValidUntilBlock: bc + notaryRequestValidity,
		Script:          test.Script,
		Attributes: []transaction.Attribute{
			{
				Type:  transaction.NotaryAssistedT,
				Value: &transaction.NotaryAssisted{NKeys: signersNumber},
			},
		},
		Signers: cosigners,
	}

	notaryFee, err := c.CalculateNotaryFee(signersNumber)
	if err != nil {
		return err
	}

	acc := wallet.NewAccountFromPrivateKey(&key)
	aa := notaryAccounts(multisigAccount, acc)

	err = c.AddNetworkFee(mainTx, notaryFee, aa...)
	if err != nil {
		return fmt.Errorf("notary network fee adding: %w", err)
	}

	mainTx.Scripts = notaryWitnesses(c, multisigAccount, acc, mainTx)

	_, err = c.SignAndPushP2PNotaryRequest(mainTx,
		[]byte{byte(opcode.RET)},
		-1,
		0,
		40,
		acc)
	if err != nil {
		return fmt.Errorf("sending notary request: %w", err)
	}

	return nil
}

func notaryCosigners(c Client, notaryHash util.Uint160, nnsCs *state.Contract,
	key keys.PrivateKey, alphabetAccount util.Uint160) ([]transaction.Signer, error) {
	proxyHash, err := nnsResolveHash(invoker.New(c, nil), nnsCs.Hash, proxyContract+".neofs")
	if err != nil {
		return nil, fmt.Errorf("proxy hash resolving: %w", err)
	}

	return []transaction.Signer{
		{
			Account: proxyHash,
			Scopes:  transaction.None,
		},
		{
			Account: alphabetAccount,
			Scopes:  transaction.Global,
		},
		{
			Account: hash.Hash160(key.PublicKey().GetVerificationScript()),
			Scopes:  transaction.Global,
		},
		{
			Account: notaryHash,
			Scopes:  transaction.None,
		},
	}, nil
}

func notaryAccounts(alphabet, acc *wallet.Account) []*wallet.Account {
	return []*wallet.Account{
		// proxy
		{
			Contract: &wallet.Contract{
				Deployed: true,
			},
		},
		alphabet,
		// caller's account
		acc,
		// last one is a placeholder for notary contract account
		{
			Contract: &wallet.Contract{},
		},
	}
}

func notaryWitnesses(c Client, alphabet, acc *wallet.Account, tx *transaction.Transaction) []transaction.Witness {
	ww := make([]transaction.Witness, 0, 4)

	// empty proxy contract witness
	ww = append(ww, transaction.Witness{
		InvocationScript:   []byte{},
		VerificationScript: []byte{},
	})

	// alphabet multi-address witness
	ww = append(ww, transaction.Witness{
		InvocationScript: append(
			[]byte{byte(opcode.PUSHDATA1), 64},
			make([]byte, 64)...,
		),
		VerificationScript: alphabet.GetVerificationScript(),
	})

	magicNumber, _ := c.GetNetwork()

	// caller's witness
	ww = append(ww, transaction.Witness{
		InvocationScript: append(
			[]byte{byte(opcode.PUSHDATA1), 64},
			acc.SignHashable(magicNumber, tx)...),
		VerificationScript: acc.GetVerificationScript(),
	})

	// notary contract witness
	ww = append(ww, transaction.Witness{
		InvocationScript: append(
			[]byte{byte(opcode.PUSHDATA1), 64},
			make([]byte, 64)...,
		),
		VerificationScript: []byte{},
	})

	return ww
}

func checkInvocationResults(res *result.Invoke) error {
	if res.State != "HALT" {
		return fmt.Errorf("test invocation state: %s, exception %s: ", res.State, res.FaultException)
	}

	if len(res.Script) == 0 {
		return errors.New("empty invocation script")
	}

	return nil
}
