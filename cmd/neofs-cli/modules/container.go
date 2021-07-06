package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/acl"
	"github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/pkg/session"
	"github.com/nspcc-dev/neofs-node/pkg/core/version"
	"github.com/nspcc-dev/neofs-sdk-go/pkg/policy"
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
)

const sessionTokenFlag = "session"

// path to a file with encoded session token
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
}

var listContainersCmd = &cobra.Command{
	Use:   "list",
	Short: "List all created containers",
	Long:  "List all created containers",
	Run: func(cmd *cobra.Command, args []string) {
		var (
			response []*cid.ID
			oid      *owner.ID

			ctx = context.Background()
		)

		key, err := getKey()
		exitOnErr(cmd, err)

		cli, err := getSDKClient(key)
		exitOnErr(cmd, err)

		if containerOwner == "" {
			wallet, err := owner.NEO3WalletFromPublicKey(&key.PublicKey)
			exitOnErr(cmd, err)

			oid = owner.NewIDFromNeo3Wallet(wallet)
		} else {
			oid, err = ownerFromString(containerOwner)
			exitOnErr(cmd, err)
		}

		response, err = cli.ListContainers(ctx, oid, globalCallOptions()...)
		exitOnErr(cmd, errf("rpc error: %w", err))

		// print to stdout
		prettyPrintContainerList(cmd, response)
	},
}

var createContainerCmd = &cobra.Command{
	Use:   "create",
	Short: "Create new container",
	Long: `Create new container and register it in the NeoFS. 
It will be stored in sidechain when inner ring will accepts it.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()

		key, err := getKey()
		exitOnErr(cmd, err)

		cli, err := getSDKClient(key)
		exitOnErr(cmd, err)

		placementPolicy, err := parseContainerPolicy(containerPolicy)
		exitOnErr(cmd, err)

		attributes, err := parseAttributes(containerAttributes)
		exitOnErr(cmd, err)

		basicACL, err := parseBasicACL(containerACL)
		exitOnErr(cmd, err)

		nonce, err := parseNonce(containerNonce)
		exitOnErr(cmd, err)

		tok, err := getSessionToken(sessionTokenPath)
		exitOnErr(cmd, err)

		cnr := container.New()
		cnr.SetPlacementPolicy(placementPolicy)
		cnr.SetBasicACL(basicACL)
		cnr.SetAttributes(attributes)
		cnr.SetNonceUUID(nonce)
		cnr.SetSessionToken(tok)
		cnr.SetOwnerID(tok.OwnerID())

		id, err := cli.PutContainer(ctx, cnr, globalCallOptions()...)
		exitOnErr(cmd, errf("rpc error: %w", err))

		cmd.Println("container ID:", id)

		if containerAwait {
			cmd.Println("awaiting...")

			for i := 0; i < awaitTimeout; i++ {
				time.Sleep(1 * time.Second)

				_, err := cli.GetContainer(ctx, id, globalCallOptions()...)
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
		ctx := context.Background()

		key, err := getKey()
		exitOnErr(cmd, err)

		cli, err := getSDKClient(key)
		exitOnErr(cmd, err)

		id, err := parseContainerID(containerID)
		exitOnErr(cmd, err)

		tok, err := getSessionToken(sessionTokenPath)
		exitOnErr(cmd, err)

		callOpts := globalCallOptions()

		if tok != nil {
			callOpts = append(callOpts, client.WithSession(tok))
		}

		err = cli.DeleteContainer(ctx, id, callOpts...)
		exitOnErr(cmd, errf("rpc error: %w", err))

		cmd.Println("container delete method invoked")

		if containerAwait {
			cmd.Println("awaiting...")

			for i := 0; i < awaitTimeout; i++ {
				time.Sleep(1 * time.Second)

				_, err := cli.GetContainer(ctx, id, globalCallOptions()...)
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
		ctx := context.Background()

		key, err := getKey()
		exitOnErr(cmd, err)

		cli, err := getSDKClient(key)
		exitOnErr(cmd, err)

		id, err := parseContainerID(containerID)
		exitOnErr(cmd, err)

		sessionToken, err := cli.CreateSession(ctx, math.MaxUint64)
		exitOnErr(cmd, errf("can't create session token: %w", err))

		filters := new(object.SearchFilters)
		filters.AddRootFilter() // search only user created objects

		searchQuery := new(client.SearchObjectParams)
		searchQuery.WithContainerID(id)
		searchQuery.WithSearchFilters(*filters)

		objectIDs, err := cli.SearchObject(ctx, searchQuery,
			append(globalCallOptions(),
				client.WithSession(sessionToken),
			)...,
		)
		exitOnErr(cmd, errf("rpc error: %w", err))

		for i := range objectIDs {
			cmd.Println(objectIDs[i])
		}
	},
}

var getContainerInfoCmd = &cobra.Command{
	Use:   "get",
	Short: "Get container field info",
	Long:  `Get container field info`,
	Run: func(cmd *cobra.Command, args []string) {
		var (
			cnr *container.Container

			ctx = context.Background()
		)

		if containerPathFrom != "" {
			data, err := os.ReadFile(containerPathFrom)
			exitOnErr(cmd, errf("can't read file: %w", err))

			cnr = container.New()
			err = cnr.Unmarshal(data)
			exitOnErr(cmd, errf("can't unmarshal container: %w", err))
		} else {
			key, err := getKey()
			exitOnErr(cmd, err)

			cli, err := getSDKClient(key)
			exitOnErr(cmd, err)

			id, err := parseContainerID(containerID)
			exitOnErr(cmd, err)

			cnr, err = cli.GetContainer(ctx, id, globalCallOptions()...)
			exitOnErr(cmd, errf("rpc error: %w", err))
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
		ctx := context.Background()

		key, err := getKey()
		exitOnErr(cmd, err)

		cli, err := getSDKClient(key)
		exitOnErr(cmd, err)

		id, err := parseContainerID(containerID)
		exitOnErr(cmd, err)

		res, err := cli.GetEACL(ctx, id, globalCallOptions()...)
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
			exitOnErr(cmd, errf("can't enode to JSON: %w", err))
		} else {
			data, err = eaclTable.Marshal()
			exitOnErr(cmd, errf("can't enode to binary: %w", err))
		}

		cmd.Println("dumping data to file:", containerPathTo)

		cmd.Println("Signature:")
		printJSONMarshaler(cmd, sig, "signature")

		err = os.WriteFile(containerPathTo, data, 0644)
		exitOnErr(cmd, err)
	},
}

var setExtendedACLCmd = &cobra.Command{
	Use:   "set-eacl",
	Short: "Set new extended ACL table for container",
	Long: `Set new extended ACL table for container.
Container ID in EACL table will be substituted with ID from the CLI.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()

		key, err := getKey()
		exitOnErr(cmd, err)

		cli, err := getSDKClient(key)
		exitOnErr(cmd, err)

		id, err := parseContainerID(containerID)
		exitOnErr(cmd, err)

		eaclTable, err := parseEACL(eaclPathFrom)
		exitOnErr(cmd, err)

		tok, err := getSessionToken(sessionTokenPath)
		exitOnErr(cmd, err)

		eaclTable.SetCID(id)
		eaclTable.SetSessionToken(tok)

		err = cli.SetEACL(ctx, eaclTable, globalCallOptions()...)
		exitOnErr(cmd, errf("rpc error: %w", err))

		if containerAwait {
			exp, err := eaclTable.Marshal()
			exitOnErr(cmd, errf("broken EACL table: %w", err))

			cmd.Println("awaiting...")

			for i := 0; i < awaitTimeout; i++ {
				time.Sleep(1 * time.Second)

				tableSig, err := cli.GetEACL(ctx, id, globalCallOptions()...)
				if err == nil {
					// compare binary values because EACL could have been set already
					got, err := tableSig.EACL().Marshal()
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

func init() {
	rootCmd.AddCommand(containerCmd)
	containerCmd.AddCommand(listContainersCmd)
	containerCmd.AddCommand(createContainerCmd)
	containerCmd.AddCommand(deleteContainerCmd)
	containerCmd.AddCommand(listContainerObjectsCmd)
	containerCmd.AddCommand(getContainerInfoCmd)
	containerCmd.AddCommand(getExtendedACLCmd)
	containerCmd.AddCommand(setExtendedACLCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// containerCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// containerCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	// container list
	listContainersCmd.Flags().StringVar(&containerOwner, "owner", "", "owner of containers (omit to use owner from private key)")

	// container create
	createContainerCmd.Flags().StringVar(&containerACL, "basic-acl", basicACLPrivate,
		fmt.Sprintf("hex encoded basic ACL value or keywords '%s', '%s', '%s'", basicACLPublic, basicACLPrivate, basicACLReadOnly))
	createContainerCmd.Flags().StringVarP(&containerPolicy, "policy", "p", "",
		"QL-encoded or JSON-encoded placement policy or path to file with it")
	createContainerCmd.Flags().StringSliceVarP(&containerAttributes, "attributes", "a", nil,
		"comma separated pairs of container attributes in form of Key1=Value1,Key2=Value2")
	createContainerCmd.Flags().StringVarP(&containerNonce, "nonce", "n", "", "UUIDv4 nonce value for container")
	createContainerCmd.Flags().BoolVar(&containerAwait, "await", false, "block execution until container is persisted")
	createContainerCmd.Flags().StringVar(&containerName, "name", "", "container name attribute")
	createContainerCmd.Flags().BoolVar(&containerNoTimestamp, "disable-timestamp", false, "disable timestamp container attribute")

	// container delete
	deleteContainerCmd.Flags().StringVar(&containerID, "cid", "", "container ID")
	deleteContainerCmd.Flags().BoolVar(&containerAwait, "await", false, "block execution until container is removed")

	// container list-object
	listContainerObjectsCmd.Flags().StringVar(&containerID, "cid", "", "container ID")

	// container get
	getContainerInfoCmd.Flags().StringVar(&containerID, "cid", "", "container ID")
	getContainerInfoCmd.Flags().StringVar(&containerPathTo, "to", "", "path to dump encoded container")
	getContainerInfoCmd.Flags().StringVar(&containerPathFrom, "from", "", "path to file with encoded container")
	getContainerInfoCmd.Flags().BoolVar(&containerJSON, "json", false, "print or dump container in JSON format")

	// container get-eacl
	getExtendedACLCmd.Flags().StringVar(&containerID, "cid", "", "container ID")
	getExtendedACLCmd.Flags().StringVar(&containerPathTo, "to", "", "path to dump encoded container (default: binary encoded)")
	getExtendedACLCmd.Flags().BoolVar(&containerJSON, "json", false, "encode EACL table in json format")

	// container set-eacl
	setExtendedACLCmd.Flags().StringVar(&containerID, "cid", "", "container ID")
	setExtendedACLCmd.Flags().StringVar(&eaclPathFrom, "table", "", "path to file with JSON or binary encoded EACL table")
	setExtendedACLCmd.Flags().BoolVar(&containerAwait, "await", false, "block execution until EACL is persisted")

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
			return nil, err
		}

		tok = session.NewToken()
		if err = tok.UnmarshalJSON(data); err != nil {
			return nil, err
		}
	}

	return tok, nil
}

func prettyPrintContainerList(cmd *cobra.Command, list []*cid.ID) {
	for i := range list {
		cmd.Println(list[i])
	}
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

func parseAttributes(attributes []string) ([]*container.Attribute, error) {
	result := make([]*container.Attribute, 0, len(attributes)+2) // name + timestamp attributes

	for i := range attributes {
		kvPair := strings.Split(attributes[i], attributeDelimiter)
		if len(kvPair) != 2 {
			return nil, errors.New("invalid container attribute")
		}

		parsedAttribute := container.NewAttribute()
		parsedAttribute.SetKey(kvPair[0])
		parsedAttribute.SetValue(kvPair[1])

		result = append(result, parsedAttribute)
	}

	if !containerNoTimestamp {
		timestamp := container.NewAttribute()
		timestamp.SetKey(container.AttributeTimestamp)
		timestamp.SetValue(strconv.FormatInt(time.Now().Unix(), 10))

		result = append(result, timestamp)
	}

	if containerName != "" {
		cnrName := container.NewAttribute()
		cnrName.SetKey(container.AttributeName)
		cnrName.SetValue(containerName)

		result = append(result, cnrName)
	}

	return result, nil
}

func parseBasicACL(basicACL string) (uint32, error) {
	switch basicACL {
	case basicACLPublic:
		return acl.PublicBasicRule, nil
	case basicACLPrivate:
		return acl.PrivateBasicRule, nil
	case basicACLReadOnly:
		return acl.ReadOnlyBasicRule, nil
	default:
		basicACL = strings.Trim(strings.ToLower(basicACL), "0x")

		value, err := strconv.ParseUint(basicACL, 16, 32)
		if err != nil {
			return 0, fmt.Errorf("can't parse basic ACL: %s", basicACL)
		}

		return uint32(value), nil
	}
}

func parseNonce(nonce string) (uuid.UUID, error) {
	if nonce == "" {
		result := uuid.New()
		printVerbose("Generating container nonce: %s", result)

		return result, nil
	}

	return uuid.Parse(nonce)
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

	version := cnr.Version()
	cmd.Printf("version: %d.%d\n", version.Major(), version.Minor())

	cmd.Println("owner ID:", cnr.OwnerID())

	basicACL := cnr.BasicACL()
	cmd.Printf("basic ACL: %s", strconv.FormatUint(uint64(basicACL), 16))
	switch basicACL {
	case acl.PublicBasicRule:
		cmd.Printf(" (%s)\n", basicACLPublic)
	case acl.PrivateBasicRule:
		cmd.Printf(" (%s)\n", basicACLPrivate)
	case acl.ReadOnlyBasicRule:
		cmd.Printf(" (%s)\n", basicACLReadOnly)
	default:
		cmd.Println()
	}

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
		v := table.Version()
		if !version.IsValid(v) {
			table.SetVersion(*pkg.SDKVersion())
		}

		printVerbose("Parsed JSON encoded EACL table")
		return table, nil
	}

	return nil, fmt.Errorf("can't parse EACL table: %w", err)
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
