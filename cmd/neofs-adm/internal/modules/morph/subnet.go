package morph

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/morph/internal"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	morphsubnet "github.com/nspcc-dev/neofs-node/pkg/morph/client/subnet"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/subnet"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// cmdSubnet flags.
const (
	// Neo RPC endpoint
	flagSubnetEndpoint = endpointFlag
	// filepath to private key
	flagSubnetKey = "key"
	// contract address
	flagSubnetContract = "contract"
	// disable notary
	flagSubnetNonNotary = "non-notary"
)

func viperBindFlags(cmd *cobra.Command, flags ...string) {
	for i := range flags {
		_ = viper.BindPFlag(flags[i], cmd.Flags().Lookup(flags[i]))
	}
}

// subnet command section.
var cmdSubnet = &cobra.Command{
	Use:   "subnet",
	Short: "NeoFS subnet management.",
	PersistentPreRun: func(cmd *cobra.Command, _ []string) {
		viperBindFlags(cmd,
			flagSubnetEndpoint,
			flagSubnetKey,
			flagSubnetContract,
			flagSubnetNonNotary,
		)
	},
}

// shared flags of cmdSubnet sub-commands.
const (
	// subnet identifier
	flagSubnet = "subnet"
	// subnet client group ID
	flagSubnetGroup = "group"
)

// reads private key from the filepath configured in flagSubnetKey flag.
func readSubnetKey(key *keys.PrivateKey) error {
	// read key from file
	keyPath := viper.GetString(flagSubnetKey)
	if keyPath == "" {
		return errors.New("missing path to private key")
	}

	data, err := os.ReadFile(keyPath)
	if err != nil {
		return fmt.Errorf("read private key file: %w", err)
	}

	// decode key
	k, err := keys.NewPrivateKeyFromBytes(data)
	if err != nil {
		return fmt.Errorf("decode private key: %w", err)
	}

	*key = *k

	return nil
}

// initializes morph subnet client with the specified private key.
//
// Parameters are read from:
//  * contract address: flagSubnetContract;
//  * endpoint: flagSubnetEndpoint;
//  * non-notary mode: flagSubnetNonNotary.
func initSubnetClient(c *morphsubnet.Client, key *keys.PrivateKey) error {
	// read endpoint
	endpoint := viper.GetString(flagSubnetEndpoint)
	if endpoint == "" {
		return errors.New("missing endpoint")
	}

	// read contract address
	contractAddr, err := util.Uint160DecodeStringLE(viper.GetString(flagSubnetContract))
	if err != nil {
		return fmt.Errorf("subnet contract address: %w", err)
	}

	// create base morph client
	cMorph, err := client.New(key, endpoint)
	if err != nil {
		return err
	}

	nonNotary := viper.GetBool(flagSubnetNonNotary)

	if !nonNotary {
		err = cMorph.EnableNotarySupport()
		if err != nil {
			return fmt.Errorf("enable notary support: %w", err)
		}
	}

	// calc client mode
	cMode := morphsubnet.NotaryNonAlphabet

	if nonNotary {
		cMode = morphsubnet.NonNotary
	}

	// init subnet morph client
	var prmInit morphsubnet.InitPrm

	prmInit.SetBaseClient(cMorph)
	prmInit.SetContractAddress(contractAddr)
	prmInit.SetMode(cMode)

	if err := c.Init(prmInit); err != nil {
		return fmt.Errorf("init call: %w", err)
	}

	return nil
}

// create subnet command.
var cmdSubnetCreate = &cobra.Command{
	Use:   "create",
	Short: "Create NeoFS subnet.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		// read private key
		var key keys.PrivateKey

		err := readSubnetKey(&key)
		if err != nil {
			return fmt.Errorf("read private key: %w", err)
		}

		// calculate wallet address from key
		n3Wallet, err := owner.NEO3WalletFromPublicKey(&key.PrivateKey.PublicKey)
		if err != nil {
			return fmt.Errorf("wallet from key: %w", err)
		}

		// generate subnet ID and marshal it
		var (
			id  subnetid.ID
			num uint32
		)

		for {
			num = uint32(rand.Uint64(rand.New(), math.MaxUint32))

			id.SetNumber(num)

			if !subnetid.IsZero(id) {
				break
			}
		}

		binID, err := id.Marshal()
		if err != nil {
			return fmt.Errorf("marshal subnet ID: %w", err)
		}

		// declare creator ID and encode it
		creator := *owner.NewIDFromNeo3Wallet(n3Wallet)

		// fill subnet info and encode it
		var info subnet.Info

		info.SetID(id)
		info.SetOwner(creator)

		binInfo, err := info.Marshal()
		if err != nil {
			return fmt.Errorf("marshal subnet info: %w", err)
		}

		// initialize morph subnet client
		var cSubnet morphsubnet.Client

		err = initSubnetClient(&cSubnet, &key)
		if err != nil {
			return fmt.Errorf("init subnet client: %w", err)
		}

		// prepare call parameters and create subnet
		var prm morphsubnet.PutPrm

		prm.SetID(binID)
		prm.SetOwner(key.PublicKey().Bytes())
		prm.SetInfo(binInfo)

		_, err = cSubnet.Put(prm)
		if err != nil {
			return fmt.Errorf("morph call: %w", err)
		}

		cmd.Printf("Create subnet request sent successfully. ID: %s.\n", &id)

		return nil
	},
}

// cmdSubnetRemove flags.
const (
	// subnet ID to be removed
	flagSubnetRemoveID = flagSubnet
)

// errZeroSubnet is returned on attempts to work with zero subnet which is virtual.
var errZeroSubnet = errors.New("zero subnet")

// remove subnet command.
var cmdSubnetRemove = &cobra.Command{
	Use:   "remove",
	Short: "Remove NeoFS subnet.",
	PreRun: func(cmd *cobra.Command, _ []string) {
		viperBindFlags(cmd,
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

		err = id.UnmarshalText([]byte(viper.GetString(flagSubnetRemoveID)))
		if err != nil {
			return fmt.Errorf("decode ID text: %w", err)
		}

		if subnetid.IsZero(id) {
			return errZeroSubnet
		}

		binID, err := id.Marshal()
		if err != nil {
			return fmt.Errorf("marshal subnet ID: %w", err)
		}

		// initialize morph subnet client
		var cSubnet morphsubnet.Client

		err = initSubnetClient(&cSubnet, &key)
		if err != nil {
			return fmt.Errorf("init subnet client: %w", err)
		}

		// prepare call parameters and remove subnet
		var prm morphsubnet.DeletePrm

		prm.SetID(binID)

		_, err = cSubnet.Delete(prm)
		if err != nil {
			return fmt.Errorf("morph call: %w", err)
		}

		cmd.Println("Remove subnet request sent successfully.")

		return nil
	},
}

// cmdSubnetGet flags.
const (
	// subnet ID to be read
	flagSubnetGetID = flagSubnet
)

// get subnet command.
var cmdSubnetGet = &cobra.Command{
	Use:   "get",
	Short: "Read information about the NeoFS subnet.",
	PreRun: func(cmd *cobra.Command, _ []string) {
		viperBindFlags(cmd,
			flagSubnetGetID,
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

		err = id.UnmarshalText([]byte(viper.GetString(flagSubnetGetID)))
		if err != nil {
			return fmt.Errorf("decode ID text: %w", err)
		}

		if subnetid.IsZero(id) {
			return errZeroSubnet
		}

		binID, err := id.Marshal()
		if err != nil {
			return fmt.Errorf("marshal subnet ID: %w", err)
		}

		// initialize morph subnet client
		var cSubnet morphsubnet.Client

		err = initSubnetClient(&cSubnet, &key)
		if err != nil {
			return fmt.Errorf("init subnet client: %w", err)
		}

		// prepare call parameters and read subnet
		var prm morphsubnet.GetPrm

		prm.SetID(binID)

		res, err := cSubnet.Get(prm)
		if err != nil {
			return fmt.Errorf("morph call: %w", err)
		}

		// decode info
		var info subnet.Info

		if err = info.Unmarshal(res.Info()); err != nil {
			return fmt.Errorf("decode subnet info: %w", err)
		}

		// print information
		var ownerID owner.ID

		info.ReadOwner(&ownerID)

		cmd.Printf("Owner: %s\n", ownerID)

		return nil
	},
}

// cmdSubnetAdmin subnet flags.
const (
	// subnet ID to be managed
	flagSubnetAdminSubnet = flagSubnet
	// admin public key
	flagSubnetAdminID = "id"
	// manage client admins instead of node ones
	flagSubnetAdminClient = "client"
)

// command to manage subnet admins.
var cmdSubnetAdmin = &cobra.Command{
	Use:   "admin",
	Short: "Manage administrators of the NeoFS subnet.",
	PreRun: func(cmd *cobra.Command, _ []string) {
		viperBindFlags(cmd,
			flagSubnetAdminSubnet,
			flagSubnetAdminID,
		)
	},
}

// cmdSubnetAdminAdd flags.
const (
	// client group ID
	flagSubnetAdminAddGroup = flagSubnetGroup
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

	err = id.UnmarshalText([]byte(viper.GetString(flagSubnetAdminSubnet)))
	if err != nil {
		return fmt.Errorf("decode ID text: %w", err)
	}

	if subnetid.IsZero(id) {
		return errZeroSubnet
	}

	binID, err := id.Marshal()
	if err != nil {
		return fmt.Errorf("marshal subnet ID: %w", err)
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
	var prm morphsubnet.ManageAdminsPrm

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

		prm.SetClient()
		prm.SetGroup(binGroupID)
	}

	prm.SetSubnet(binID)
	prm.SetAdmin(binAdminKey)

	if rm {
		prm.SetRemove()
	}

	// initialize morph subnet client
	var cSubnet morphsubnet.Client

	err = initSubnetClient(&cSubnet, &key)
	if err != nil {
		return fmt.Errorf("init subnet client: %w", err)
	}

	_, err = cSubnet.ManageAdmins(prm)
	if err != nil {
		return fmt.Errorf("morph call: %w", err)
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
	Short: "Add admin to the NeoFS subnet.",
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
	Short: "Remove admin of the NeoFS subnet.",
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
	// ID of the subnet to be managed
	flagSubnetClientSubnet = flagSubnet
	// client's NeoFS ID
	flagSubnetClientID = flagSubnetAdminClient
	// ID of the subnet client group
	flagSubnetClientGroup = flagSubnetGroup
)

// command to manage subnet clients.
var cmdSubnetClient = &cobra.Command{
	Use:   "client",
	Short: "Manage clients of the NeoFS subnet.",
	PreRun: func(cmd *cobra.Command, _ []string) {
		viperBindFlags(cmd,
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

	err = id.UnmarshalText([]byte(viper.GetString(flagSubnetClientSubnet)))
	if err != nil {
		return fmt.Errorf("decode ID text: %w", err)
	}

	if subnetid.IsZero(id) {
		return errZeroSubnet
	}

	binID, err := id.Marshal()
	if err != nil {
		return fmt.Errorf("marshal subnet ID: %w", err)
	}

	// read client ID and encode it
	var clientID owner.ID

	err = clientID.Parse(viper.GetString(flagSubnetClientID))
	if err != nil {
		return fmt.Errorf("decode client ID text: %w", err)
	}

	binClientID, err := clientID.Marshal()
	if err != nil {
		return fmt.Errorf("marshal client ID: %w", err)
	}

	var prm morphsubnet.ManageClientsPrm

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

		prm.SetGroup(binGroupID)
	}

	prm.SetSubnet(binID)
	prm.SetClient(binClientID)

	if rm {
		prm.SetRemove()
	}

	// initialize morph subnet client
	var cSubnet morphsubnet.Client

	err = initSubnetClient(&cSubnet, &key)
	if err != nil {
		return fmt.Errorf("init subnet client: %w", err)
	}

	_, err = cSubnet.ManageClients(prm)
	if err != nil {
		return fmt.Errorf("morph call: %w", err)
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
	Short: "Add client to the NeoFS subnet.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return manageSubnetClients(cmd, false)
	},
}

// command to remove subnet client.
var cmdSubnetClientRemove = &cobra.Command{
	Use:   "remove",
	Short: "Remove client of the NeoFS subnet.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return manageSubnetClients(cmd, true)
	},
}

// registers flags and binds sub-commands for subnet commands.
func init() {
	// get subnet flags
	cmdSubnetGet.Flags().String(flagSubnetGetID, "", "ID of the subnet to read")
	_ = cmdSubnetAdminAdd.MarkFlagRequired(flagSubnetGetID)

	// remove subnet flags
	cmdSubnetRemove.Flags().String(flagSubnetRemoveID, "", "ID of the subnet to remove")
	_ = cmdSubnetRemove.MarkFlagRequired(flagSubnetRemoveID)

	// subnet administer flags
	adminFlags := cmdSubnetAdmin.PersistentFlags()
	adminFlags.String(flagSubnetAdminSubnet, "", "ID of the subnet to manage administrators")
	_ = cmdSubnetAdmin.MarkFlagRequired(flagSubnetAdminSubnet)
	adminFlags.String(flagSubnetAdminID, "", "Hex-encoded public key of the admin")
	_ = cmdSubnetAdmin.MarkFlagRequired(flagSubnetAdminID)

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

	// add all admin managing commands to corresponding command section
	cmdSubnetAdmin.AddCommand(
		cmdSubnetAdminAdd,
		cmdSubnetAdminRemove,
	)

	// add all client managing commands to corresponding command section
	cmdSubnetClient.AddCommand(
		cmdSubnetClientAdd,
		cmdSubnetClientRemove,
	)

	// subnet global flags
	cmdSubnetFlags := cmdSubnet.PersistentFlags()
	cmdSubnetFlags.StringP(flagSubnetEndpoint, "r", "", "N3 RPC node endpoint")
	_ = cmdSubnet.MarkFlagRequired(flagSubnetEndpoint)
	cmdSubnetFlags.StringP(flagSubnetKey, "k", "", "Path to file with private key")
	_ = cmdSubnet.MarkFlagRequired(flagSubnetKey)
	cmdSubnetFlags.StringP(flagSubnetContract, "a", "", "Subnet contract address (string LE)")
	_ = cmdSubnet.MarkFlagRequired(flagSubnetContract)
	cmdSubnetFlags.Bool(flagSubnetNonNotary, false, "Flag to work in non-notary environment")

	// add all subnet commands to corresponding command section
	cmdSubnet.AddCommand(
		cmdSubnetCreate,
		cmdSubnetRemove,
		cmdSubnetGet,
		cmdSubnetAdmin,
		cmdSubnetClient,
	)
}
