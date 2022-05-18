package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/acl"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"github.com/nspcc-dev/neofs-sdk-go/policy"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	versionSDK "github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/spf13/cobra"
)

const (
	attributeDelimiter = "="

	awaitTimeout = 120 // in seconds
)

// keywords of predefined basic ACL values
const (
	basicACLPrivate  = "private"
	basicACLReadOnly = "public-read"
	basicACLPublic   = "public-read-write"
	basicACLAppend   = "public-append"

	basicACLNoFinalPrivate  = "eacl-private"
	basicACLNoFinalReadOnly = "eacl-public-read"
	basicACLNoFinalPublic   = "eacl-public-read-write"
	basicACLNoFinalAppend   = "eacl-public-append"
)

var wellKnownBasicACL = map[string]acl.BasicACL{
	basicACLPublic:   acl.PublicBasicRule,
	basicACLPrivate:  acl.PrivateBasicRule,
	basicACLReadOnly: acl.ReadOnlyBasicRule,
	basicACLAppend:   acl.PublicAppendRule,

	basicACLNoFinalPublic:   acl.EACLPublicBasicRule,
	basicACLNoFinalPrivate:  acl.EACLPrivateBasicRule,
	basicACLNoFinalReadOnly: acl.EACLReadOnlyBasicRule,
	basicACLNoFinalAppend:   acl.EACLPublicAppendRule,
}

const sessionTokenFlag = "session"

// path to a file with an encoded session token
var sessionTokenPath string

var (
	containerOwner string

	containerACL         string
	containerNonce       string
	containerPolicy      string
	containerAttributes  []string
	containerAwait       bool
	containerName        string
	containerNoTimestamp bool
	containerSubnet      string

	containerID string

	containerPathFrom string
	containerPathTo   string

	containerJSON bool

	eaclPathFrom string
)

var (
	errDeleteTimeout  = errors.New("timeout: container has not been removed from sidechain")
	errCreateTimeout  = errors.New("timeout: container has not been persisted on sidechain")
	errSetEACLTimeout = errors.New("timeout: EACL has not been persisted on sidechain")
)

// containerCmd represents the container command
var containerCmd = &cobra.Command{
	Use:   "container",
	Short: "Operations with containers",
	Long:  "Operations with containers",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// bind exactly that cmd's flags to
		// the viper before execution
		commonflags.Bind(cmd)
		bindAPIFlags(cmd)
	},
}

var listContainersCmd = &cobra.Command{
	Use:   "list",
	Short: "List all created containers",
	Long:  "List all created containers",
	Run: func(cmd *cobra.Command, args []string) {
		var idUser user.ID

		key := key.GetOrGenerate(cmd)

		if containerOwner == "" {
			user.IDFromKey(&idUser, key.PublicKey)
		} else {
			common.ExitOnErr(cmd, "", userFromString(&idUser, containerOwner))
		}

		var prm internalclient.ListContainersPrm

		prepareAPIClientWithKey(cmd, key, &prm)
		prm.SetAccount(idUser)

		res, err := internalclient.ListContainers(prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		// print to stdout
		prettyPrintContainerList(cmd, res.IDList())
	},
}

var createContainerCmd = &cobra.Command{
	Use:   "create",
	Short: "Create new container",
	Long: `Create new container and register it in the NeoFS. 
It will be stored in sidechain when inner ring will accepts it.`,
	Run: func(cmd *cobra.Command, args []string) {
		placementPolicy, err := parseContainerPolicy(containerPolicy)
		common.ExitOnErr(cmd, "", err)

		subnetID, err := parseSubnetID(containerSubnet)
		common.ExitOnErr(cmd, "could not parse subnetID: %w", err)

		placementPolicy.SetSubnetID(subnetID)

		attributes, err := parseAttributes(containerAttributes)
		common.ExitOnErr(cmd, "", err)

		basicACL, err := parseBasicACL(containerACL)
		common.ExitOnErr(cmd, "", err)

		nonce, err := parseNonce(containerNonce)
		common.ExitOnErr(cmd, "", err)

		key := key.GetOrGenerate(cmd)

		cnr := container.New()
		var tok *session.Container

		if sessionTokenPath != "" {
			tok = new(session.Container)
			common.ReadSessionToken(cmd, tok, sessionTokenPath)

			issuer := tok.Issuer()
			cnr.SetOwnerID(&issuer)
			cnr.SetSessionToken(tok)
		} else {
			var idOwner user.ID
			user.IDFromKey(&idOwner, key.PublicKey)

			cnr.SetOwnerID(&idOwner)
		}

		ver := versionSDK.Current()

		cnr.SetVersion(&ver)
		cnr.SetPlacementPolicy(placementPolicy)
		cnr.SetBasicACL(basicACL)
		cnr.SetAttributes(attributes)
		cnr.SetNonceUUID(nonce)
		cnr.SetSessionToken(tok)

		var (
			putPrm internalclient.PutContainerPrm
			getPrm internalclient.GetContainerPrm
		)

		prepareAPIClientWithKey(cmd, key, &putPrm, &getPrm)
		putPrm.SetContainer(*cnr)

		res, err := internalclient.PutContainer(putPrm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		id := res.ID()

		cmd.Println("container ID:", id)

		if containerAwait {
			cmd.Println("awaiting...")

			getPrm.SetContainer(*id)

			for i := 0; i < awaitTimeout; i++ {
				time.Sleep(1 * time.Second)

				_, err := internalclient.GetContainer(getPrm)
				if err == nil {
					cmd.Println("container has been persisted on sidechain")
					return
				}
			}

			common.ExitOnErr(cmd, "", errCreateTimeout)
		}
	},
}

var deleteContainerCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete existing container",
	Long: `Delete existing container. 
Only owner of the container has a permission to remove container.`,
	Run: func(cmd *cobra.Command, args []string) {
		id, err := parseContainerID(containerID)
		common.ExitOnErr(cmd, "", err)

		var tok *session.Container

		if sessionTokenPath != "" {
			tok = new(session.Container)
			common.ReadSessionToken(cmd, tok, sessionTokenPath)
		}

		var (
			delPrm internalclient.DeleteContainerPrm
			getPrm internalclient.GetContainerPrm
		)

		prepareAPIClient(cmd, &delPrm, &getPrm)
		delPrm.SetContainer(*id)

		if tok != nil {
			delPrm.WithinSession(*tok)
		}

		_, err = internalclient.DeleteContainer(delPrm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		cmd.Println("container delete method invoked")

		if containerAwait {
			cmd.Println("awaiting...")

			getPrm.SetContainer(*id)

			for i := 0; i < awaitTimeout; i++ {
				time.Sleep(1 * time.Second)

				_, err := internalclient.GetContainer(getPrm)
				if err != nil {
					cmd.Println("container has been removed:", containerID)
					return
				}
			}

			common.ExitOnErr(cmd, "", errDeleteTimeout)
		}
	},
}

var listContainerObjectsCmd = &cobra.Command{
	Use:   "list-objects",
	Short: "List existing objects in container",
	Long:  `List existing objects in container`,
	Run: func(cmd *cobra.Command, args []string) {
		id, err := parseContainerID(containerID)
		common.ExitOnErr(cmd, "", err)

		filters := new(object.SearchFilters)
		filters.AddRootFilter() // search only user created objects

		var prm internalclient.SearchObjectsPrm

		sessionObjectCtxAddress := addressSDK.NewAddress()
		sessionObjectCtxAddress.SetContainerID(*id)
		prepareSessionPrm(cmd, sessionObjectCtxAddress, &prm)
		prepareObjectPrm(cmd, &prm)
		prm.SetContainerID(id)
		prm.SetFilters(*filters)

		res, err := internalclient.SearchObjects(prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		objectIDs := res.IDList()

		for i := range objectIDs {
			cmd.Println(objectIDs[i].String())
		}
	},
}

var getContainerInfoCmd = &cobra.Command{
	Use:   "get",
	Short: "Get container field info",
	Long:  `Get container field info`,
	Run: func(cmd *cobra.Command, args []string) {
		var cnr *container.Container

		if containerPathFrom != "" {
			data, err := os.ReadFile(containerPathFrom)
			common.ExitOnErr(cmd, "can't read file: %w", err)

			cnr = container.New()
			err = cnr.Unmarshal(data)
			common.ExitOnErr(cmd, "can't unmarshal container: %w", err)
		} else {
			id, err := parseContainerID(containerID)
			common.ExitOnErr(cmd, "", err)

			var prm internalclient.GetContainerPrm

			prepareAPIClient(cmd, &prm)
			prm.SetContainer(*id)

			res, err := internalclient.GetContainer(prm)
			common.ExitOnErr(cmd, "rpc error: %w", err)

			cnr = res.Container()
		}

		prettyPrintContainer(cmd, cnr, containerJSON)

		if containerPathTo != "" {
			var (
				data []byte
				err  error
			)

			if containerJSON {
				data, err = cnr.MarshalJSON()
				common.ExitOnErr(cmd, "can't JSON encode container: %w", err)
			} else {
				data, err = cnr.Marshal()
				common.ExitOnErr(cmd, "can't binary encode container: %w", err)
			}

			err = os.WriteFile(containerPathTo, data, 0644)
			common.ExitOnErr(cmd, "can't write container to file: %w", err)
		}
	},
}

var getExtendedACLCmd = &cobra.Command{
	Use:   "get-eacl",
	Short: "Get extended ACL table of container",
	Long:  `Get extended ACL talbe of container`,
	Run: func(cmd *cobra.Command, args []string) {
		id, err := parseContainerID(containerID)
		common.ExitOnErr(cmd, "", err)

		var eaclPrm internalclient.EACLPrm

		prepareAPIClient(cmd, &eaclPrm)
		eaclPrm.SetContainer(*id)

		res, err := internalclient.EACL(eaclPrm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		eaclTable := res.EACL()

		sig := eaclTable.Signature()

		// TODO(@cthulhu-rider): #1387 avoid type conversion
		var sigV2 refs.Signature
		sig.WriteToV2(&sigV2)

		if containerPathTo == "" {
			cmd.Println("eACL: ")
			prettyPrintEACL(cmd, eaclTable)

			var sigV2 refs.Signature
			sig.WriteToV2(&sigV2)

			cmd.Println("Signature:")
			common.PrettyPrintJSON(cmd, &sigV2, "signature")

			return
		}

		var data []byte

		if containerJSON {
			data, err = eaclTable.MarshalJSON()
			common.ExitOnErr(cmd, "can't encode to JSON: %w", err)
		} else {
			data, err = eaclTable.Marshal()
			common.ExitOnErr(cmd, "can't encode to binary: %w", err)
		}

		cmd.Println("dumping data to file:", containerPathTo)

		cmd.Println("Signature:")
		common.PrettyPrintJSON(cmd, &sigV2, "signature")

		err = os.WriteFile(containerPathTo, data, 0644)
		common.ExitOnErr(cmd, "could not write eACL to file: %w", err)
	},
}

var setExtendedACLCmd = &cobra.Command{
	Use:   "set-eacl",
	Short: "Set new extended ACL table for container",
	Long: `Set new extended ACL table for container.
Container ID in EACL table will be substituted with ID from the CLI.`,
	Run: func(cmd *cobra.Command, args []string) {
		id, err := parseContainerID(containerID)
		common.ExitOnErr(cmd, "", err)

		eaclTable := common.ReadEACL(cmd, eaclPathFrom)

		var tok *session.Container

		if sessionTokenPath != "" {
			tok = new(session.Container)
			common.ReadSessionToken(cmd, tok, sessionTokenPath)
		}

		eaclTable.SetCID(*id)
		eaclTable.SetSessionToken(tok)

		var (
			setEACLPrm internalclient.SetEACLPrm
			getEACLPrm internalclient.EACLPrm
		)

		prepareAPIClient(cmd, &setEACLPrm, &getEACLPrm)
		setEACLPrm.SetTable(*eaclTable)

		_, err = internalclient.SetEACL(setEACLPrm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		if containerAwait {
			exp, err := eaclTable.Marshal()
			common.ExitOnErr(cmd, "broken EACL table: %w", err)

			cmd.Println("awaiting...")

			getEACLPrm.SetContainer(*id)

			for i := 0; i < awaitTimeout; i++ {
				time.Sleep(1 * time.Second)

				res, err := internalclient.EACL(getEACLPrm)
				if err == nil {
					// compare binary values because EACL could have been set already
					got, err := res.EACL().Marshal()
					if err != nil {
						continue
					}

					if bytes.Equal(exp, got) {
						cmd.Println("EACL has been persisted on sidechain")
						return
					}
				}
			}

			common.ExitOnErr(cmd, "", errSetEACLTimeout)
		}
	},
}

func initContainerListContainersCmd() {
	commonflags.Init(listContainersCmd)

	flags := listContainersCmd.Flags()

	flags.StringVar(&containerOwner, "owner", "", "owner of containers (omit to use owner from private key)")
}

func initContainerCreateCmd() {
	commonflags.Init(createContainerCmd)

	flags := createContainerCmd.Flags()

	flags.StringVar(&containerACL, "basic-acl", basicACLPrivate, fmt.Sprintf("hex encoded basic ACL value or keywords like '%s', '%s', '%s'", basicACLPublic, basicACLPrivate, basicACLNoFinalReadOnly))
	flags.StringVarP(&containerPolicy, "policy", "p", "", "QL-encoded or JSON-encoded placement policy or path to file with it")
	flags.StringSliceVarP(&containerAttributes, "attributes", "a", nil, "comma separated pairs of container attributes in form of Key1=Value1,Key2=Value2")
	flags.StringVarP(&containerNonce, "nonce", "n", "", "UUIDv4 nonce value for container")
	flags.BoolVar(&containerAwait, "await", false, "block execution until container is persisted")
	flags.StringVar(&containerName, "name", "", "container name attribute")
	flags.BoolVar(&containerNoTimestamp, "disable-timestamp", false, "disable timestamp container attribute")
	flags.StringVar(&containerSubnet, "subnet", "", "string representation of container subnetwork")
}

func initContainerDeleteCmd() {
	commonflags.Init(deleteContainerCmd)

	flags := deleteContainerCmd.Flags()

	flags.StringVar(&containerID, "cid", "", "container ID")
	flags.BoolVar(&containerAwait, "await", false, "block execution until container is removed")
}

func initContainerListObjectsCmd() {
	commonflags.Init(listContainerObjectsCmd)

	flags := listContainerObjectsCmd.Flags()

	flags.StringVar(&containerID, "cid", "", "container ID")
}

func initContainerInfoCmd() {
	commonflags.Init(getContainerInfoCmd)

	flags := getContainerInfoCmd.Flags()

	flags.StringVar(&containerID, "cid", "", "container ID")
	flags.StringVar(&containerPathTo, "to", "", "path to dump encoded container")
	flags.StringVar(&containerPathFrom, "from", "", "path to file with encoded container")
	flags.BoolVar(&containerJSON, "json", false, "print or dump container in JSON format")
}

func initContainerGetEACLCmd() {
	commonflags.Init(getExtendedACLCmd)

	flags := getExtendedACLCmd.Flags()

	flags.StringVar(&containerID, "cid", "", "container ID")
	flags.StringVar(&containerPathTo, "to", "", "path to dump encoded container (default: binary encoded)")
	flags.BoolVar(&containerJSON, "json", false, "encode EACL table in json format")
}

func initContainerSetEACLCmd() {
	commonflags.Init(setExtendedACLCmd)

	flags := setExtendedACLCmd.Flags()
	flags.StringVar(&containerID, "cid", "", "container ID")
	flags.StringVar(&eaclPathFrom, "table", "", "path to file with JSON or binary encoded EACL table")
	flags.BoolVar(&containerAwait, "await", false, "block execution until EACL is persisted")
}

func init() {
	containerChildCommand := []*cobra.Command{
		listContainersCmd,
		createContainerCmd,
		deleteContainerCmd,
		listContainerObjectsCmd,
		getContainerInfoCmd,
		getExtendedACLCmd,
		setExtendedACLCmd,
	}

	rootCmd.AddCommand(containerCmd)

	containerCmd.AddCommand(containerChildCommand...)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// containerCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// containerCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	initContainerListContainersCmd()
	initContainerCreateCmd()
	initContainerDeleteCmd()
	initContainerListObjectsCmd()
	initContainerInfoCmd()
	initContainerGetEACLCmd()
	initContainerSetEACLCmd()

	for _, containerCommand := range containerChildCommand {
		flags := containerCommand.Flags()

		flags.StringSliceVarP(&xHeaders, xHeadersKey, xHeadersShorthand, xHeadersDefault, xHeadersUsage)
		flags.Uint32P(ttl, ttlShorthand, ttlDefault, ttlUsage)
	}

	for _, cmd := range []*cobra.Command{
		createContainerCmd,
		deleteContainerCmd,
		setExtendedACLCmd,
	} {
		cmd.Flags().StringVar(
			&sessionTokenPath,
			sessionTokenFlag,
			"",
			"path to a JSON-encoded container session token",
		)
	}
}

func prettyPrintContainerList(cmd *cobra.Command, list []cid.ID) {
	for i := range list {
		cmd.Println(list[i].String())
	}
}

func parseSubnetID(val string) (sub *subnetid.ID, err error) {
	sub = &subnetid.ID{}

	if val != "" {
		err = sub.UnmarshalText([]byte(val))
	}

	return
}

func parseContainerPolicy(policyString string) (*netmap.PlacementPolicy, error) {
	_, err := os.Stat(policyString) // check if `policyString` is a path to file with placement policy
	if err == nil {
		common.PrintVerbose("Reading placement policy from file: %s", policyString)

		data, err := os.ReadFile(policyString)
		if err != nil {
			return nil, fmt.Errorf("can't read file with placement policy: %w", err)
		}

		policyString = string(data)
	}

	result, err := policy.Parse(policyString)
	if err == nil {
		common.PrintVerbose("Parsed QL encoded policy")
		return result, nil
	}

	result = netmap.NewPlacementPolicy()
	if err = result.UnmarshalJSON([]byte(policyString)); err == nil {
		common.PrintVerbose("Parsed JSON encoded policy")
		return result, nil
	}

	return nil, errors.New("can't parse placement policy")
}

func parseAttributes(attributes []string) ([]container.Attribute, error) {
	result := make([]container.Attribute, len(attributes), len(attributes)+2) // name + timestamp attributes

	for i := range attributes {
		kvPair := strings.Split(attributes[i], attributeDelimiter)
		if len(kvPair) != 2 {
			return nil, errors.New("invalid container attribute")
		}

		result[i].SetKey(kvPair[0])
		result[i].SetValue(kvPair[1])
	}

	if !containerNoTimestamp {
		index := len(result)
		result = append(result, container.Attribute{})
		result[index].SetKey(container.AttributeTimestamp)
		result[index].SetValue(strconv.FormatInt(time.Now().Unix(), 10))
	}

	if containerName != "" {
		index := len(result)
		result = append(result, container.Attribute{})
		result[index].SetKey(container.AttributeName)
		result[index].SetValue(containerName)
	}

	return result, nil
}

func parseBasicACL(basicACL string) (acl.BasicACL, error) {
	if value, ok := wellKnownBasicACL[basicACL]; ok {
		return value, nil
	}

	basicACL = strings.Trim(strings.ToLower(basicACL), "0x")

	value, err := strconv.ParseUint(basicACL, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("can't parse basic ACL: %s", basicACL)
	}

	return acl.BasicACL(value), nil
}

func parseNonce(nonce string) (uuid.UUID, error) {
	if nonce == "" {
		result := uuid.New()
		common.PrintVerbose("Generating container nonce: %s", result)

		return result, nil
	}

	uid, err := uuid.Parse(nonce)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("could not parse nonce: %w", err)
	}

	return uid, nil
}

func parseContainerID(idStr string) (*cid.ID, error) {
	if idStr == "" {
		return nil, errors.New("container ID is not set")
	}

	var id cid.ID

	err := id.DecodeString(idStr)
	if err != nil {
		return nil, errors.New("can't decode container ID value")
	}

	return &id, nil
}

func prettyPrintContainer(cmd *cobra.Command, cnr *container.Container, jsonEncoding bool) {
	if cnr == nil {
		return
	}

	if jsonEncoding {
		data, err := cnr.MarshalJSON()
		if err != nil {
			common.PrintVerbose("Can't convert container to json: %w", err)
			return
		}
		buf := new(bytes.Buffer)
		if err := json.Indent(buf, data, "", "  "); err != nil {
			common.PrintVerbose("Can't pretty print json: %w", err)
		}

		cmd.Println(buf)

		return
	}

	id := container.CalculateID(cnr)
	cmd.Println("container ID:", id)

	v := cnr.Version()
	cmd.Printf("version: %d.%d\n", v.Major(), v.Minor())

	cmd.Println("owner ID:", cnr.OwnerID())

	basicACL := cnr.BasicACL()
	prettyPrintBasicACL(cmd, acl.BasicACL(basicACL))

	for _, attribute := range cnr.Attributes() {
		if attribute.Key() == container.AttributeTimestamp {
			cmd.Printf("attribute: %s=%s (%s)\n",
				attribute.Key(),
				attribute.Value(),
				prettyPrintUnixTime(attribute.Value()))

			continue
		}

		cmd.Printf("attribute: %s=%s\n", attribute.Key(), attribute.Value())
	}

	nonce, err := cnr.NonceUUID()
	if err == nil {
		cmd.Println("nonce:", nonce)
	} else {
		cmd.Println("invalid nonce:", err)
	}

	cmd.Println("placement policy:")
	cmd.Println(strings.Join(policy.Encode(cnr.PlacementPolicy()), "\n"))
}

func prettyPrintEACL(cmd *cobra.Command, table *eacl.Table) {
	common.PrettyPrintJSON(cmd, table, "eACL")
}

func prettyPrintBasicACL(cmd *cobra.Command, basicACL acl.BasicACL) {
	cmd.Printf("basic ACL: %s", basicACL)
	for k, v := range wellKnownBasicACL {
		if v == basicACL {
			cmd.Printf(" (%s)\n", k)
			return
		}
	}
	cmd.Println()
}

func prettyPrintUnixTime(s string) string {
	unixTime, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return "malformed"
	}

	timestamp := time.Unix(unixTime, 0)

	return timestamp.String()
}
