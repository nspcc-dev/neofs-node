package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
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
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/policy"
	"github.com/spf13/cobra"
)

const (
	attributeDelimiter = "="

	awaitTimeout = 120 // in seconds
)

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
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			response []*container.ID
			oid      *owner.ID
			err      error

			ctx = context.Background()
		)

		key, err := getKey()
		if err != nil {
			return err
		}

		cli, err := getSDKClient(key)
		if err != nil {
			return err
		}

		if containerOwner == "" {
			wallet, err := owner.NEO3WalletFromPublicKey(&key.PublicKey)
			if err != nil {
				return err
			}

			oid = owner.NewIDFromNeo3Wallet(wallet)
		} else {
			oid, err = ownerFromString(containerOwner)
			if err != nil {
				return err
			}
		}

		response, err = cli.ListContainers(ctx, oid, globalCallOptions()...)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		// print to stdout
		prettyPrintContainerList(response)

		return nil
	},
}

var createContainerCmd = &cobra.Command{
	Use:   "create",
	Short: "Create new container",
	Long: `Create new container and register it in the NeoFS. 
It will be stored in sidechain when inner ring will accepts it.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		key, err := getKey()
		if err != nil {
			return err
		}

		cli, err := getSDKClient(key)
		if err != nil {
			return err
		}

		placementPolicy, err := parseContainerPolicy(containerPolicy)
		if err != nil {
			return err
		}

		attributes, err := parseAttributes(containerAttributes)
		if err != nil {
			return err
		}

		basicACL, err := parseBasicACL(containerACL)
		if err != nil {
			return err
		}

		nonce, err := parseNonce(containerNonce)
		if err != nil {
			return err
		}

		cnr := container.New()
		cnr.SetPlacementPolicy(placementPolicy)
		cnr.SetBasicACL(basicACL)
		cnr.SetAttributes(attributes)
		cnr.SetNonceUUID(nonce)

		id, err := cli.PutContainer(ctx, cnr, globalCallOptions()...)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		fmt.Println("container ID:", id)

		if containerAwait {
			fmt.Println("awaiting...")

			for i := 0; i < awaitTimeout; i++ {
				time.Sleep(1 * time.Second)

				_, err := cli.GetContainer(ctx, id, globalCallOptions()...)
				if err == nil {
					fmt.Println("container has been persisted on sidechain")
					return nil
				}
			}

			return errors.New("timeout: container has not been persisted on sidechain")
		}

		return nil
	},
}

var deleteContainerCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete existing container",
	Long: `Delete existing container. 
Only owner of the container has a permission to remove container.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		key, err := getKey()
		if err != nil {
			return err
		}

		cli, err := getSDKClient(key)
		if err != nil {
			return err
		}

		id, err := parseContainerID(containerID)
		if err != nil {
			return err
		}

		err = cli.DeleteContainer(ctx, id, globalCallOptions()...)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		fmt.Println("container delete method invoked")

		if containerAwait {
			fmt.Println("awaiting...")

			for i := 0; i < awaitTimeout; i++ {
				time.Sleep(1 * time.Second)

				_, err := cli.GetContainer(ctx, id, globalCallOptions()...)
				if err != nil {
					fmt.Println("container has been removed:", containerID)
					return nil
				}
			}

			return errors.New("timeout: container has not been removed from sidechain")
		}

		return nil
	},
}

var listContainerObjectsCmd = &cobra.Command{
	Use:   "list-objects",
	Short: "List existing objects in container",
	Long:  `List existing objects in container`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		key, err := getKey()
		if err != nil {
			return err
		}

		cli, err := getSDKClient(key)
		if err != nil {
			return err
		}

		id, err := parseContainerID(containerID)
		if err != nil {
			return err
		}

		sessionToken, err := cli.CreateSession(ctx, math.MaxUint64)
		if err != nil {
			return fmt.Errorf("can't create session token: %w", err)
		}

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
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		for i := range objectIDs {
			fmt.Println(objectIDs[i])
		}

		return nil
	},
}

var getContainerInfoCmd = &cobra.Command{
	Use:   "get",
	Short: "Get container field info",
	Long:  `Get container field info`,
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			cnr *container.Container

			ctx = context.Background()
		)

		if containerPathFrom != "" {
			data, err := ioutil.ReadFile(containerPathFrom)
			if err != nil {
				return fmt.Errorf("can't read file: %w", err)
			}

			cnr = container.New()
			if err := cnr.Unmarshal(data); err != nil {
				return fmt.Errorf("can't unmarshal container: %w", err)
			}
		} else {
			key, err := getKey()
			if err != nil {
				return err
			}

			cli, err := getSDKClient(key)
			if err != nil {
				return err
			}

			id, err := parseContainerID(containerID)
			if err != nil {
				return err
			}

			cnr, err = cli.GetContainer(ctx, id, globalCallOptions()...)
			if err != nil {
				return fmt.Errorf("rpc error: %w", err)
			}
		}

		prettyPrintContainer(cnr, containerJSON)

		if containerPathTo != "" {
			var (
				data []byte
				err  error
			)

			if containerJSON {
				data, err = cnr.MarshalJSON()
				if err != nil {
					return fmt.Errorf("can't JSON encode container: %w", err)
				}
			} else {
				data, err = cnr.Marshal()
				if err != nil {
					return fmt.Errorf("can't binary encode container: %w", err)
				}
			}

			err = ioutil.WriteFile(containerPathTo, data, 0644)
			if err != nil {
				return fmt.Errorf("can't write container to file: %w", err)
			}
		}

		return nil
	},
}

var getExtendedACLCmd = &cobra.Command{
	Use:   "get-eacl",
	Short: "Get extended ACL table of container",
	Long:  `Get extended ACL talbe of container`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		key, err := getKey()
		if err != nil {
			return err
		}

		cli, err := getSDKClient(key)
		if err != nil {
			return err
		}

		id, err := parseContainerID(containerID)
		if err != nil {
			return err
		}

		res, err := cli.GetEACL(ctx, id, globalCallOptions()...)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		eaclTable := res.EACL()
		sig := res.Signature()

		if containerPathTo == "" {
			fmt.Println("eACL: ")
			prettyPrintEACL(eaclTable)

			fmt.Println("Signature:")
			printJSONMarshaler(sig, "signature")

			return nil
		}

		var data []byte

		if containerJSON {
			data, err = eaclTable.MarshalJSON()
			if err != nil {
				return fmt.Errorf("can't enode to JSON: %w", err)
			}
		} else {
			data, err = eaclTable.Marshal()
			if err != nil {
				return fmt.Errorf("can't enode to binary: %w", err)
			}
		}

		fmt.Println("dumping data to file:", containerPathTo)

		fmt.Println("Signature:")
		printJSONMarshaler(sig, "signature")

		return ioutil.WriteFile(containerPathTo, data, 0644)
	},
}

var setExtendedACLCmd = &cobra.Command{
	Use:   "set-eacl",
	Short: "Set new extended ACL table for container",
	Long: `Set new extended ACL table for container.
Container ID in EACL table will be substituted with ID from the CLI.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		key, err := getKey()
		if err != nil {
			return err
		}

		cli, err := getSDKClient(key)
		if err != nil {
			return err
		}

		id, err := parseContainerID(containerID)
		if err != nil {
			return err
		}

		eaclTable, err := parseEACL(eaclPathFrom)
		if err != nil {
			return err
		}

		eaclTable.SetCID(id)

		err = cli.SetEACL(ctx, eaclTable, globalCallOptions()...)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		if containerAwait {
			exp, err := eaclTable.Marshal()
			if err != nil {
				return errors.New("broken EACL table")
			}

			fmt.Println("awaiting...")

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
						fmt.Println("EACL has been persisted on sidechain")
						return nil
					}
				}
			}

			return errors.New("timeout: EACL has not been persisted on sidechain")
		}

		return nil
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
	createContainerCmd.Flags().StringVar(&containerACL, "basic-acl", "private",
		"hex encoded basic ACL value or keywords 'public', 'private', 'readonly'")
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
}

func prettyPrintContainerList(list []*container.ID) {
	for i := range list {
		fmt.Println(list[i])
	}
}

func parseContainerPolicy(policyString string) (*netmap.PlacementPolicy, error) {
	_, err := os.Stat(policyString) // check if `policyString` is a path to file with placement policy
	if err == nil {
		printVerbose("Reading placement policy from file: %s", policyString)

		data, err := ioutil.ReadFile(policyString)
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
	case "public":
		return acl.PublicBasicRule, nil
	case "private":
		return acl.PrivateBasicRule, nil
	case "readonly":
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

func parseContainerID(cid string) (*container.ID, error) {
	if cid == "" {
		return nil, errors.New("container ID is not set")
	}

	id := container.NewID()

	err := id.Parse(cid)
	if err != nil {
		return nil, errors.New("can't decode container ID value")
	}

	return id, nil
}

func prettyPrintContainer(cnr *container.Container, jsonEncoding bool) {
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

		fmt.Println(buf)

		return
	}

	id := container.CalculateID(cnr)
	fmt.Println("container ID:", id)

	version := cnr.Version()
	fmt.Printf("version: %d.%d\n", version.Major(), version.Minor())

	fmt.Println("owner ID:", cnr.OwnerID())

	basicACL := cnr.BasicACL()
	fmt.Printf("basic ACL: %s", strconv.FormatUint(uint64(basicACL), 16))
	switch basicACL {
	case acl.PublicBasicRule:
		fmt.Println(" (public)")
	case acl.PrivateBasicRule:
		fmt.Println(" (private)")
	case acl.ReadOnlyBasicRule:
		fmt.Println(" (readonly)")
	default:
		fmt.Println()
	}

	for _, attribute := range cnr.Attributes() {
		if attribute.Key() == container.AttributeTimestamp {
			fmt.Printf("attribute: %s=%s (%s)\n",
				attribute.Key(),
				attribute.Value(),
				prettyPrintUnixTime(attribute.Value()))

			continue
		}

		fmt.Printf("attribute: %s=%s\n", attribute.Key(), attribute.Value())
	}

	nonce, err := cnr.NonceUUID()
	if err == nil {
		fmt.Println("nonce:", nonce)
	} else {
		fmt.Println("invalid nonce:", err)
	}

	fmt.Println("placement policy:")
	fmt.Println(strings.Join(policy.Encode(cnr.PlacementPolicy()), "\n"))
}

func parseEACL(eaclPath string) (*eacl.Table, error) {
	_, err := os.Stat(eaclPath) // check if `eaclPath` is an existing file
	if err != nil {
		return nil, errors.New("incorrect path to file with EACL")
	}

	printVerbose("Reading EACL from file: %s", eaclPath)

	data, err := ioutil.ReadFile(eaclPath)
	if err != nil {
		return nil, fmt.Errorf("can't read file with EACL: %w", err)
	}

	table := eacl.NewTable()
	if err = table.UnmarshalJSON(data); err == nil {
		version := table.Version()
		if err := pkg.IsSupportedVersion(&version); err != nil {
			table.SetVersion(*pkg.SDKVersion())
		}

		printVerbose("Parsed JSON encoded EACL table")
		return table, nil
	}

	return nil, fmt.Errorf("can't parse EACL table: %w", err)
}

func prettyPrintEACL(table *eacl.Table) {
	printJSONMarshaler(table, "eACL")
}

func printJSONMarshaler(j json.Marshaler, entity string) {
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
	fmt.Println(buf)
}
