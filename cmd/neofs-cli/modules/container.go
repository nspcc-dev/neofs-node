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
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/version"
	"github.com/nspcc-dev/neofs-sdk-go/acl"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/policy"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
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
	errDeleteTimeout         = errors.New("timeout: container has not been removed from sidechain")
	errCreateTimeout         = errors.New("timeout: container has not been persisted on sidechain")
	errSetEACLTimeout        = errors.New("timeout: EACL has not been persisted on sidechain")
	errUnsupportedEACLFormat = errors.New("unsupported eACL format")
)

// containerCmd represents the container command
var containerCmd = &cobra.Command{
	Use:   "container",
	Short: "Operations with containers",
	Long:  "Operations with containers",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// bind exactly that cmd's flags to
		// the viper before execution
		bindCommonFlags(cmd)
		bindAPIFlags(cmd)
	},
}

var listContainersCmd = &cobra.Command{
	Use:   "list",
	Short: "List all created containers",
	Long:  "List all created containers",
	Run: func(cmd *cobra.Command, args []string) {
		var oid *owner.ID

		key, err := getKey()
		exitOnErr(cmd, err)

		if containerOwner == "" {
			oid = owner.NewIDFromPublicKey(&key.PublicKey)
		} else {
			oid, err = ownerFromString(containerOwner)
			exitOnErr(cmd, err)
		}

		var prm internalclient.ListContainersPrm

		prepareAPIClientWithKey(cmd, key, &prm)
		prm.SetAccount(*oid)

		res, err := internalclient.ListContainers(prm)
		exitOnErr(cmd, errf("rpc error: %w", err))

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
		exitOnErr(cmd, err)

		subnetID, err := parseSubnetID(containerSubnet)
		exitOnErr(cmd, errf("could not parse subnetID: %w", err))

		placementPolicy.SetSubnetID(subnetID)

		attributes, err := parseAttributes(containerAttributes)
		exitOnErr(cmd, err)

		basicACL, err := parseBasicACL(containerACL)
		exitOnErr(cmd, err)

		nonce, err := parseNonce(containerNonce)
		exitOnErr(cmd, err)

		tok, err := getSessionToken(sessionTokenPath)
		exitOnErr(cmd, err)

		key, err := getKey()
		exitOnErr(cmd, err)

		var idOwner *owner.ID

		if idOwner = tok.OwnerID(); idOwner == nil {
			idOwner = owner.NewIDFromPublicKey(&key.PublicKey)
		}

		cnr := container.New()
		cnr.SetVersion(versionSDK.Current())
		cnr.SetPlacementPolicy(placementPolicy)
		cnr.SetBasicACL(basicACL)
		cnr.SetAttributes(attributes)
		cnr.SetNonceUUID(nonce)
		cnr.SetSessionToken(tok)
		cnr.SetOwnerID(idOwner)

		var (
			putPrm internalclient.PutContainerPrm
			getPrm internalclient.GetContainerPrm
		)

		prepareAPIClientWithKey(cmd, key, &putPrm, &getPrm)
		putPrm.SetContainer(*cnr)

		res, err := internalclient.PutContainer(putPrm)
		exitOnErr(cmd, errf("rpc error: %w", err))

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

			exitOnErr(cmd, errCreateTimeout)
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
		exitOnErr(cmd, err)

		tok, err := getSessionToken(sessionTokenPath)
		exitOnErr(cmd, err)

		var (
			delPrm internalclient.DeleteContainerPrm
			getPrm internalclient.GetContainerPrm
		)

		prepareAPIClient(cmd, &delPrm, &getPrm)
		delPrm.SetContainer(*id)

		if tok != nil {
			delPrm.SetSessionToken(*tok)
		}

		_, err = internalclient.DeleteContainer(delPrm)
		exitOnErr(cmd, errf("rpc error: %w", err))

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

			exitOnErr(cmd, errDeleteTimeout)
		}
	},
}

var listContainerObjectsCmd = &cobra.Command{
	Use:   "list-objects",
	Short: "List existing objects in container",
	Long:  `List existing objects in container`,
	Run: func(cmd *cobra.Command, args []string) {
		id, err := parseContainerID(containerID)
		exitOnErr(cmd, err)

		filters := new(object.SearchFilters)
		filters.AddRootFilter() // search only user created objects

		var prm internalclient.SearchObjectsPrm

		sessionObjectCtxAddress := addressSDK.NewAddress()
		sessionObjectCtxAddress.SetContainerID(id)
		prepareSessionPrm(cmd, sessionObjectCtxAddress, &prm)
		prepareObjectPrm(cmd, &prm)
		prm.SetContainerID(id)
		prm.SetFilters(*filters)

		res, err := internalclient.SearchObjects(prm)
		exitOnErr(cmd, errf("rpc error: %w", err))

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
			exitOnErr(cmd, errf("can't read file: %w", err))

			cnr = container.New()
			err = cnr.Unmarshal(data)
			exitOnErr(cmd, errf("can't unmarshal container: %w", err))
		} else {
			id, err := parseContainerID(containerID)
			exitOnErr(cmd, err)

			var prm internalclient.GetContainerPrm

			prepareAPIClient(cmd, &prm)
			prm.SetContainer(*id)

			res, err := internalclient.GetContainer(prm)
			exitOnErr(cmd, errf("rpc error: %w", err))

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
				exitOnErr(cmd, errf("can't JSON encode container: %w", err))
			} else {
				data, err = cnr.Marshal()
				exitOnErr(cmd, errf("can't binary encode container: %w", err))
			}

			err = os.WriteFile(containerPathTo, data, 0644)
			exitOnErr(cmd, errf("can't write container to file: %w", err))
		}
	},
}

var getExtendedACLCmd = &cobra.Command{
	Use:   "get-eacl",
	Short: "Get extended ACL table of container",
	Long:  `Get extended ACL talbe of container`,
	Run: func(cmd *cobra.Command, args []string) {
		id, err := parseContainerID(containerID)
		exitOnErr(cmd, err)

		var eaclPrm internalclient.EACLPrm

		prepareAPIClient(cmd, &eaclPrm)
		eaclPrm.SetContainer(*id)

		res, err := internalclient.EACL(eaclPrm)
		exitOnErr(cmd, errf("rpc error: %w", err))

		eaclTable := res.EACL()

		sig := eaclTable.Signature()

		if containerPathTo == "" {
			cmd.Println("eACL: ")
			prettyPrintEACL(cmd, eaclTable)

			cmd.Println("Signature:")
			printJSONMarshaler(cmd, sig, "signature")

			return
		}

		var data []byte

		if containerJSON {
			data, err = eaclTable.MarshalJSON()
			exitOnErr(cmd, errf("can't encode to JSON: %w", err))
		} else {
			data, err = eaclTable.Marshal()
			exitOnErr(cmd, errf("can't encode to binary: %w", err))
		}

		cmd.Println("dumping data to file:", containerPathTo)

		cmd.Println("Signature:")
		printJSONMarshaler(cmd, sig, "signature")

		err = os.WriteFile(containerPathTo, data, 0644)
		exitOnErr(cmd, errf("could not write eACL to file: %w", err))
	},
}

var setExtendedACLCmd = &cobra.Command{
	Use:   "set-eacl",
	Short: "Set new extended ACL table for container",
	Long: `Set new extended ACL table for container.
Container ID in EACL table will be substituted with ID from the CLI.`,
	Run: func(cmd *cobra.Command, args []string) {
		id, err := parseContainerID(containerID)
		exitOnErr(cmd, err)

		eaclTable, err := parseEACL(eaclPathFrom)
		exitOnErr(cmd, err)

		tok, err := getSessionToken(sessionTokenPath)
		exitOnErr(cmd, err)

		eaclTable.SetCID(id)
		eaclTable.SetSessionToken(tok)

		var (
			setEACLPrm internalclient.SetEACLPrm
			getEACLPrm internalclient.EACLPrm
		)

		prepareAPIClient(cmd, &setEACLPrm, &getEACLPrm)
		setEACLPrm.SetTable(*eaclTable)

		_, err = internalclient.SetEACL(setEACLPrm)
		exitOnErr(cmd, errf("rpc error: %w", err))

		if containerAwait {
			exp, err := eaclTable.Marshal()
			exitOnErr(cmd, errf("broken EACL table: %w", err))

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

			exitOnErr(cmd, errSetEACLTimeout)
		}
	},
}

func initContainerListContainersCmd() {
	initCommonFlags(listContainersCmd)

	flags := listContainersCmd.Flags()

	flags.StringVar(&containerOwner, "owner", "", "owner of containers (omit to use owner from private key)")
}

func initContainerCreateCmd() {
	initCommonFlags(createContainerCmd)

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
	initCommonFlags(deleteContainerCmd)

	flags := deleteContainerCmd.Flags()

	flags.StringVar(&containerID, "cid", "", "container ID")
	flags.BoolVar(&containerAwait, "await", false, "block execution until container is removed")
}

func initContainerListObjectsCmd() {
	initCommonFlags(listContainerObjectsCmd)

	flags := listContainerObjectsCmd.Flags()

	flags.StringVar(&containerID, "cid", "", "container ID")
}

func initContainerInfoCmd() {
	initCommonFlags(getContainerInfoCmd)

	flags := getContainerInfoCmd.Flags()

	flags.StringVar(&containerID, "cid", "", "container ID")
	flags.StringVar(&containerPathTo, "to", "", "path to dump encoded container")
	flags.StringVar(&containerPathFrom, "from", "", "path to file with encoded container")
	flags.BoolVar(&containerJSON, "json", false, "print or dump container in JSON format")
}

func initContainerGetEACLCmd() {
	initCommonFlags(getExtendedACLCmd)

	flags := getExtendedACLCmd.Flags()

	flags.StringVar(&containerID, "cid", "", "container ID")
	flags.StringVar(&containerPathTo, "to", "", "path to dump encoded container (default: binary encoded)")
	flags.BoolVar(&containerJSON, "json", false, "encode EACL table in json format")
}

func initContainerSetEACLCmd() {
	initCommonFlags(setExtendedACLCmd)

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

// getSessionToken reads `<path>` as JSON file with session token and parses it.
func getSessionToken(path string) (*session.Token, error) {
	// try to read session token from file
	var tok *session.Token

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("could not open file with session token: %w", err)
		}

		tok = session.NewToken()
		if err = tok.UnmarshalJSON(data); err != nil {
			return nil, fmt.Errorf("could not ummarshal session token from file: %w", err)
		}
	}

	return tok, nil
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
		printVerbose("Reading placement policy from file: %s", policyString)

		data, err := os.ReadFile(policyString)
		if err != nil {
			return nil, fmt.Errorf("can't read file with placement policy: %w", err)
		}

		policyString = string(data)
	}

	result, err := policy.Parse(policyString)
	if err == nil {
		printVerbose("Parsed QL encoded policy")
		return result, nil
	}

	result = netmap.NewPlacementPolicy()
	if err = result.UnmarshalJSON([]byte(policyString)); err == nil {
		printVerbose("Parsed JSON encoded policy")
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
		printVerbose("Generating container nonce: %s", result)

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

	id := cid.New()

	err := id.Parse(idStr)
	if err != nil {
		return nil, errors.New("can't decode container ID value")
	}

	return id, nil
}

func prettyPrintContainer(cmd *cobra.Command, cnr *container.Container, jsonEncoding bool) {
	if cnr == nil {
		return
	}

	if jsonEncoding {
		data, err := cnr.MarshalJSON()
		if err != nil {
			printVerbose("Can't convert container to json: %w", err)
			return
		}
		buf := new(bytes.Buffer)
		if err := json.Indent(buf, data, "", "  "); err != nil {
			printVerbose("Can't pretty print json: %w", err)
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

func parseEACL(eaclPath string) (*eacl.Table, error) {
	_, err := os.Stat(eaclPath) // check if `eaclPath` is an existing file
	if err != nil {
		return nil, errors.New("incorrect path to file with EACL")
	}

	printVerbose("Reading EACL from file: %s", eaclPath)

	data, err := os.ReadFile(eaclPath)
	if err != nil {
		return nil, fmt.Errorf("can't read file with EACL: %w", err)
	}

	table := eacl.NewTable()

	if err = table.UnmarshalJSON(data); err == nil {
		validateAndFixEACLVersion(table)
		printVerbose("Parsed JSON encoded EACL table")
		return table, nil
	}

	if err = table.Unmarshal(data); err == nil {
		validateAndFixEACLVersion(table)
		printVerbose("Parsed binary encoded EACL table")
		return table, nil
	}

	return nil, errUnsupportedEACLFormat
}

func validateAndFixEACLVersion(table *eacl.Table) {
	v := table.Version()
	if !version.IsValid(v) {
		table.SetVersion(*versionSDK.Current())
	}
}

func prettyPrintEACL(cmd *cobra.Command, table *eacl.Table) {
	printJSONMarshaler(cmd, table, "eACL")
}

func printJSONMarshaler(cmd *cobra.Command, j json.Marshaler, entity string) {
	data, err := j.MarshalJSON()
	if err != nil {
		printVerbose("Can't convert %s to json: %w", entity, err)
		return
	}
	buf := new(bytes.Buffer)
	if err := json.Indent(buf, data, "", "  "); err != nil {
		printVerbose("Can't pretty print json: %w", err)
		return
	}
	cmd.Println(buf)
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
