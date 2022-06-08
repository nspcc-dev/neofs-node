package container

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/acl"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	versionSDK "github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/spf13/cobra"
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

var (
	containerACL         string
	containerNonce       string
	containerPolicy      string
	containerAttributes  []string
	containerAwait       bool
	containerName        string
	containerNoTimestamp bool
	containerSubnet      string
)

var createContainerCmd = &cobra.Command{
	Use:   "create",
	Short: "Create new container",
	Long: `Create new container and register it in the NeoFS. 
It will be stored in sidechain when inner ring will accepts it.`,
	Run: func(cmd *cobra.Command, args []string) {
		placementPolicy, err := parseContainerPolicy(containerPolicy)
		common.ExitOnErr(cmd, "", err)

		if containerSubnet != "" {
			var subnetID subnetid.ID

			err = subnetID.DecodeString(containerSubnet)
			common.ExitOnErr(cmd, "could not parse subnetID: %w", err)

			placementPolicy.RestrictSubnet(subnetID)
		}

		attributes, err := parseAttributes(containerAttributes)
		common.ExitOnErr(cmd, "", err)

		basicACL, err := parseBasicACL(containerACL)
		common.ExitOnErr(cmd, "", err)

		nonce, err := parseNonce(containerNonce)
		common.ExitOnErr(cmd, "", err)

		key := key.GetOrGenerate(cmd)

		cnr := container.New()
		var tok *session.Container

		sessionTokenPath, _ := cmd.Flags().GetString(commonflags.SessionToken)
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

		cli := internalclient.GetSDKClientByFlag(cmd, key, commonflags.RPC)

		var putPrm internalclient.PutContainerPrm
		putPrm.SetClient(cli)
		putPrm.SetContainer(*cnr)

		res, err := internalclient.PutContainer(putPrm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		id := res.ID()

		cmd.Println("container ID:", id)

		if containerAwait {
			cmd.Println("awaiting...")

			var getPrm internalclient.GetContainerPrm
			getPrm.SetClient(cli)
			getPrm.SetContainer(id)

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

	var result netmap.PlacementPolicy

	err = result.DecodeString(policyString)
	if err == nil {
		common.PrintVerbose("Parsed QL encoded policy")
		return &result, nil
	}

	if err = result.UnmarshalJSON([]byte(policyString)); err == nil {
		common.PrintVerbose("Parsed JSON encoded policy")
		return &result, nil
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
