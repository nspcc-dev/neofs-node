package session

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	v2SubjectsFlag    = "subject"
	v2SubjectsNNSFlag = "subject-nns"
	v2ContainerFlag   = "container"
	v2ObjectsFlag     = "objects"
	v2VerbsFlag       = "verbs"
)

var createV2Cmd = &cobra.Command{
	Use:   "create-v2",
	Short: "Create V2 session token",
	Long: `Create V2 session token with subjects, contexts, and verbs.

V2 tokens support:
- Multiple subjects (accounts authorized to use the token)
- Multiple contexts (container + object operations)
- No server-side session key storage (no SessionCreate RPC needed)
- Delegation chains (future feature)

Example usage:
  neofs-cli session create-v2 \
    --wallet wallet.json \
    --r node.neofs.network:8080 \
    --lifetime 100 \
    --out token.json \
    --json \
    --subject NbUgTSFvPmsRxmGeWpuuGeJUoRoi6PErcM \
    --container 5HqniP5vq5xXr3FdijTSekrQJHu1WnADt2uLg7KSViZM \
    --verbs GET,HEAD,SEARCH

Default lifetime of session token is ` + strconv.Itoa(defaultLifetime) + ` epochs
if none of --` + commonflags.ExpireAt + ` or --` + commonflags.Lifetime + ` flags is specified.
`,
	Args: cobra.NoArgs,
	RunE: createSessionV2,
	PersistentPreRun: func(cmd *cobra.Command, _ []string) {
		_ = viper.BindPFlag(commonflags.WalletPath, cmd.Flags().Lookup(commonflags.WalletPath))
		_ = viper.BindPFlag(commonflags.Account, cmd.Flags().Lookup(commonflags.Account))
	},
}

func init() {
	createV2Cmd.Flags().Uint64P(commonflags.Lifetime, "l", defaultLifetime, "Number of epochs for token to stay valid")
	createV2Cmd.Flags().StringP(commonflags.WalletPath, commonflags.WalletPathShorthand, commonflags.WalletPathDefault, commonflags.WalletPathUsage)
	createV2Cmd.Flags().StringP(commonflags.Account, commonflags.AccountShorthand, commonflags.AccountDefault, commonflags.AccountUsage)
	createV2Cmd.Flags().String(outFlag, "", "File to write session token to")
	createV2Cmd.Flags().Bool(jsonFlag, false, "Output token in JSON")
	createV2Cmd.Flags().StringP(commonflags.RPC, commonflags.RPCShorthand, commonflags.RPCDefault, commonflags.RPCUsage)
	createV2Cmd.Flags().Uint64P(commonflags.ExpireAt, "e", 0, "The last active epoch for token to stay valid")

	// V2-specific flags
	createV2Cmd.Flags().StringSlice(v2SubjectsFlag, nil, "Subject user IDs (can be specified multiple times)")
	createV2Cmd.Flags().StringSlice(v2SubjectsNNSFlag, nil, "Subject NNS names (can be specified multiple times)")
	createV2Cmd.Flags().String(v2ContainerFlag, "", "Container ID for the context")
	createV2Cmd.Flags().StringSlice(v2ObjectsFlag, nil, "Object IDs for the context (empty = all objects in container)")
	createV2Cmd.Flags().String(v2VerbsFlag, "", "Comma-separated list of verbs (GET,PUT,HEAD,SEARCH,DELETE,RANGE,RANGEHASH,CONTAINERSET,CONTAINERPUT,CONTAINERDELETE)")

	_ = cobra.MarkFlagRequired(createV2Cmd.Flags(), commonflags.WalletPath)
	_ = cobra.MarkFlagRequired(createV2Cmd.Flags(), outFlag)
	_ = cobra.MarkFlagRequired(createV2Cmd.Flags(), commonflags.RPC)
	_ = cobra.MarkFlagRequired(createV2Cmd.Flags(), v2ContainerFlag)
	_ = cobra.MarkFlagRequired(createV2Cmd.Flags(), v2VerbsFlag)
	createV2Cmd.MarkFlagsOneRequired(commonflags.ExpireAt, commonflags.Lifetime)
	createV2Cmd.MarkFlagsOneRequired(v2SubjectsFlag, v2SubjectsNNSFlag)
}

func createSessionV2(cmd *cobra.Command, _ []string) error {
	privKey, err := key.Get(cmd)
	if err != nil {
		return err
	}

	var netAddr network.Address
	addrStr, _ := cmd.Flags().GetString(commonflags.RPC)
	if err := netAddr.FromString(addrStr); err != nil {
		return fmt.Errorf("can't parse endpoint: %w", err)
	}

	ctx := context.Background()
	endpoint, _ := cmd.Flags().GetString(commonflags.RPC)
	currEpoch, err := internalclient.GetCurrentEpoch(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("can't get current epoch: %w", err)
	}

	var exp uint64
	if exp, _ = cmd.Flags().GetUint64(commonflags.ExpireAt); exp == 0 {
		lifetime, _ := cmd.Flags().GetUint64(commonflags.Lifetime)
		exp = currEpoch + lifetime
	}
	if exp <= currEpoch {
		return errors.New("expiration epoch must be greater than current epoch")
	}

	var tokV2 session.TokenV2
	signer := user.NewAutoIDSigner(*privKey)

	tokV2.SetVersion(session.TokenV2CurrentVersion)
	tokV2.SetID(uuid.New())
	tokV2.SetNbf(currEpoch)
	tokV2.SetIat(currEpoch)
	tokV2.SetExp(exp)
	tokV2.SetIssuer(session.NewTarget(signer.UserID()))

	subjects, err := parseSubjects(cmd, netAddr)
	if err != nil {
		return err
	}
	tokV2.SetSubjects(subjects)

	common.PrintVerbose(cmd, "Token issuer: %s", signer.UserID().String())
	common.PrintVerbose(cmd, "Number of subjects: %d", len(subjects))
	for i, subj := range subjects {
		if subj.IsOwnerID() {
			common.PrintVerbose(cmd, "  Subject %d (UserID): %s", i+1, subj.OwnerID().String())
		} else if subj.IsNNS() {
			common.PrintVerbose(cmd, "  Subject %d (NNS): %s", i+1, subj.NNSName())
		}
	}

	contexts, err := parseContexts(cmd)
	if err != nil {
		return err
	}
	tokV2.SetContexts(contexts)

	common.PrintVerbose(cmd, "Number of contexts: %d", len(contexts))
	for i, ctx := range contexts {
		common.PrintVerbose(cmd, "  Context %d: container=%s, objects=%d, verbs=%d",
			i+1, ctx.Container().String(), len(ctx.Objects()), len(ctx.Verbs()))
	}

	if err := tokV2.Sign(signer); err != nil {
		return fmt.Errorf("failed to sign token: %w", err)
	}

	common.PrintVerbose(cmd, "Token signed successfully")

	var data []byte
	if toJSON, _ := cmd.Flags().GetBool(jsonFlag); toJSON {
		data, err = tokV2.MarshalJSON()
		if err != nil {
			return fmt.Errorf("can't marshal session token to JSON: %w", err)
		}
		common.PrintVerbose(cmd, "Token marshalled to JSON")
	} else {
		data = tokV2.Marshal()
		common.PrintVerbose(cmd, "Token marshalled to binary")
	}

	filename, _ := cmd.Flags().GetString(outFlag)
	err = os.WriteFile(filename, data, 0o644)
	if err != nil {
		return fmt.Errorf("can't write token to file: %w", err)
	}

	fmt.Printf("V2 session token successfully written to: %s\n", filename)
	return nil
}

func parseSubjects(cmd *cobra.Command, netAddr network.Address) ([]session.Target, error) {
	subjectIDs, _ := cmd.Flags().GetStringSlice(v2SubjectsFlag)
	subjectNNS, _ := cmd.Flags().GetStringSlice(v2SubjectsNNSFlag)

	if len(subjectIDs) == 0 && len(subjectNNS) == 0 {
		return nil, errors.New("at least one subject (--subject or --subject-nns) must be specified")
	}

	subjects := make([]session.Target, 0, len(subjectIDs)+len(subjectNNS))

	for _, idStr := range subjectIDs {
		var userID user.ID
		if err := userID.DecodeString(idStr); err != nil {
			return nil, fmt.Errorf("invalid subject user ID %q: %w", idStr, err)
		}
		subjects = append(subjects, session.NewTarget(userID))
	}

	for _, nnsName := range subjectNNS {
		if nnsName == "" {
			return nil, errors.New("NNS name cannot be empty")
		}
		subjects = append(subjects, session.NewTargetFromNNS(nnsName))
	}

	// Fetch node public keys from NetMap for the container
	cnrStr, _ := cmd.Flags().GetString(v2ContainerFlag)
	if cnrStr != "" {
		var cnrID cid.ID
		if err := cnrID.DecodeString(cnrStr); err != nil {
			common.PrintVerbose(cmd, "Warning: can't parse container ID for node fetching: %v", err)
		} else {
			nodeSubjects, err := fetchNodeSubjects(cmd, cnrID, netAddr)
			if err != nil {
				common.PrintVerbose(cmd, "Warning: can't fetch node subjects: %v", err)
			} else {
				subjects = append(subjects, nodeSubjects...)
				common.PrintVerbose(cmd, "Added %d node public keys as subjects from NetMap", len(nodeSubjects))
			}
		}
	}

	return subjects, nil
}

func fetchNodeSubjects(cmd *cobra.Command, cnrID cid.ID, netAddr network.Address) ([]session.Target, error) {
	ctx := context.Background()

	cli, err := internalclient.GetSDKClient(ctx, netAddr)
	if err != nil {
		return nil, fmt.Errorf("can't create SDK client: %w", err)
	}
	defer func() {
		_ = cli.Close()
	}()

	nm, err := cli.NetMapSnapshot(ctx, client.PrmNetMapSnapshot{})
	if err != nil {
		return nil, fmt.Errorf("can't get NetMap snapshot: %w", err)
	}

	cnrObj, err := cli.ContainerGet(ctx, cnrID, client.PrmContainerGet{})
	if err != nil {
		return nil, fmt.Errorf("can't get container: %w", err)
	}

	policy := cnrObj.PlacementPolicy()

	nodes, err := nm.ContainerNodes(policy, cnrID)
	if err != nil {
		return nil, fmt.Errorf("can't get container nodes: %w", err)
	}

	var subjects []session.Target
	seenKeys := make(map[string]bool)

	for _, replica := range nodes {
		for _, node := range replica {
			pubKeyBytes := node.PublicKey()
			keyStr := string(pubKeyBytes)
			if seenKeys[keyStr] {
				continue
			}
			seenKeys[keyStr] = true

			neoPubKey, err := keys.NewPublicKeyFromBytes(pubKeyBytes, elliptic.P256())
			if err != nil {
				common.PrintVerbose(cmd, "Warning: failed to parse node public key: %v", err)
				continue
			}

			userID := user.NewFromECDSAPublicKey(*(*ecdsa.PublicKey)(neoPubKey))

			subjects = append(subjects, session.NewTarget(userID))
		}
	}

	return subjects, nil
}

func parseContexts(cmd *cobra.Command) ([]session.ContextV2, error) {
	cnrStr, _ := cmd.Flags().GetString(v2ContainerFlag)
	var cnrID cid.ID
	if err := cnrID.DecodeString(cnrStr); err != nil {
		return nil, fmt.Errorf("invalid container ID: %w", err)
	}

	verbsStr, _ := cmd.Flags().GetString(v2VerbsFlag)
	verbs, err := parseVerbs(verbsStr)
	if err != nil {
		return nil, err
	}

	ctx := session.NewContextV2(cnrID, verbs)

	objStrs, _ := cmd.Flags().GetStringSlice(v2ObjectsFlag)
	if len(objStrs) > 0 {
		var objIDs []oid.ID
		for _, objStr := range objStrs {
			var objID oid.ID
			if err := objID.DecodeString(objStr); err != nil {
				return nil, fmt.Errorf("invalid object ID %q: %w", objStr, err)
			}
			objIDs = append(objIDs, objID)
		}
		ctx.SetObjects(objIDs)
	}

	return []session.ContextV2{ctx}, nil
}

func parseVerbs(verbsStr string) ([]session.VerbV2, error) {
	if verbsStr == "" {
		return nil, errors.New("verbs cannot be empty")
	}

	verbStrs := strings.Split(verbsStr, ",")
	verbs := make([]session.VerbV2, 0, len(verbStrs))

	for _, verbStr := range verbStrs {
		verbStr = strings.TrimSpace(strings.ToUpper(verbStr))

		var verb session.VerbV2
		switch verbStr {
		case "GET", "OBJECTGET":
			verb = session.VerbV2ObjectGet
		case "PUT", "OBJECTPUT":
			verb = session.VerbV2ObjectPut
		case "HEAD", "OBJECTHEAD":
			verb = session.VerbV2ObjectHead
		case "SEARCH", "OBJECTSEARCH":
			verb = session.VerbV2ObjectSearch
		case "DELETE", "OBJECTDELETE":
			verb = session.VerbV2ObjectDelete
		case "RANGE", "OBJECTRANGE":
			verb = session.VerbV2ObjectRange
		case "RANGEHASH", "OBJECTRANGEHASH", "RANGE_HASH", "OBJECT_RANGE_HASH":
			verb = session.VerbV2ObjectRangeHash
		case "CONTAINERSET", "CONTAINERSETACL", "CONTAINER_SET", "CONTAINER_SET_ACL":
			verb = session.VerbV2ContainerSetEACL
		case "CONTAINERPUT", "CONTAINER_PUT":
			verb = session.VerbV2ContainerPut
		case "CONTAINERDELETE", "CONTAINER_DELETE":
			verb = session.VerbV2ContainerDelete
		default:
			return nil, fmt.Errorf("unknown verb: %s (supported: GET,PUT,HEAD,SEARCH,DELETE,RANGE,RANGEHASH,CONTAINERSET,CONTAINERPUT,CONTAINERDELETE)", verbStr)
		}

		verbs = append(verbs, verb)
	}

	return verbs, nil
}
