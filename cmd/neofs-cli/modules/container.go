package cmd

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/pkg/acl"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	v2container "github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-node/pkg/policy"
	"github.com/spf13/cobra"
)

const (
	attributeDelimiter = ":"

	awaitTimeout = 120 // in seconds
)

var (
	containerOwner string

	containerACL        string
	containerNonce      string
	containerPolicy     string
	containerAttributes []string
	containerAwait      bool

	containerID string
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

		cli, err := getSDKClient()
		if err != nil {
			return err
		}

		switch containerOwner {
		case "":
			response, err = cli.ListSelfContainers(ctx)
		default:
			oid, err = ownerFromString(containerOwner)
			if err != nil {
				return err
			}

			response, err = cli.ListContainers(ctx, oid)
		}

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

		cli, err := getSDKClient()
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
		cnr.SetNonce(nonce[:])

		id, err := cli.PutContainer(ctx, cnr)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		// todo: use stringers after neofs-api-go#147
		fmt.Println("container ID:", base58.Encode(id.ToV2().GetValue()))

		if containerAwait {
			fmt.Println("awaiting...")

			for i := 0; i < awaitTimeout; i++ {
				time.Sleep(1 * time.Second)

				_, err := cli.GetContainer(ctx, id)
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

		cli, err := getSDKClient()
		if err != nil {
			return err
		}

		id, err := parseContainerID(containerID)
		if err != nil {
			return err
		}

		err = cli.DeleteContainer(ctx, id)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		fmt.Println("container delete method invoked")

		if containerAwait {
			fmt.Println("awaiting...")

			for i := 0; i < awaitTimeout; i++ {
				time.Sleep(1 * time.Second)

				_, err := cli.GetContainer(ctx, id)
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

func init() {
	rootCmd.AddCommand(containerCmd)
	containerCmd.AddCommand(listContainersCmd)
	containerCmd.AddCommand(createContainerCmd)
	containerCmd.AddCommand(deleteContainerCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// containerCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// containerCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	listContainersCmd.Flags().StringVar(&containerOwner, "owner", "", "owner of containers (omit to use owner from private key)")

	createContainerCmd.Flags().StringVar(&containerACL, "basic-acl", "private",
		"hex encoded basic ACL value or keywords 'public', 'private', 'readonly'")
	createContainerCmd.Flags().StringVarP(&containerPolicy, "policy", "p", "",
		"QL-encoded or JSON-encoded placement policy or path to file with it")
	createContainerCmd.Flags().StringArrayVarP(&containerAttributes, "attribute", "a", nil,
		"colon separated pair of container attribute key and value, e.g. `target:cats`")
	createContainerCmd.Flags().StringVarP(&containerNonce, "nonce", "n", "", "UUIDv4 nonce value for container")
	createContainerCmd.Flags().BoolVar(&containerAwait, "await", false, "block execution until container is persisted")

	deleteContainerCmd.Flags().StringVar(&containerID, "cid", "", "container ID")
	deleteContainerCmd.Flags().BoolVar(&containerAwait, "await", false, "block execution until container is removed")
}

func prettyPrintContainerList(list []*container.ID) {
	for i := range list {
		// todo: use stringers after neofs-api-go#147
		fmt.Println(base58.Encode(list[i].ToV2().GetValue()))
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

	result, err = policy.FromJSON([]byte(policyString))
	if err == nil {
		printVerbose("Parsed JSON encoded policy")
		return result, nil
	}

	return nil, errors.New("can't parse placement policy")
}

func parseAttributes(attributes []string) ([]*v2container.Attribute, error) {
	result := make([]*v2container.Attribute, 0, len(attributes))

	for i := range attributes {
		kvPair := strings.Split(attributes[i], attributeDelimiter)
		if len(kvPair) != 2 {
			return nil, errors.New("invalid container attribute")
		}

		parsedAttribute := new(v2container.Attribute)
		parsedAttribute.SetKey(kvPair[0])
		parsedAttribute.SetValue(kvPair[1])

		result = append(result, parsedAttribute)
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
	// todo: use decoders after neofs-api-go#147
	data, err := base58.Decode(cid)
	if err != nil || len(data) != sha256.Size {
		return nil, errors.New("can't decode container ID value")
	}

	var buf [sha256.Size]byte
	copy(buf[:], data)

	id := container.NewID()
	id.SetSHA256(buf)

	return id, nil
}
