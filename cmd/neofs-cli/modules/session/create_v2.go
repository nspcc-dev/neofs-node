package session

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	v2SubjectsFlag    = "subject"
	v2SubjectsNNSFlag = "subject-nns"
	v2FinalFlag       = "final"
	v2ContextFlag     = "context"
	v2OriginFlag      = "origin"

	defaultLifetimeV2 = 36000 // 10 hour
)

var createV2Cmd = &cobra.Command{
	Use:   "create-v2",
	Short: "Create V2 session token",
	Long: `Create V2 session token with subjects and multiple contexts.

V2 tokens support:
- Multiple subjects (accounts authorized to use the token)
- Multiple contexts (container + object operations)
- No server-side session key storage (no SessionCreate RPC needed)
- Token delegation chains via --origin flag

IMPORTANT: Contexts and verbs must be specified in sorted order for proper token validation.

Context format: containerID:verbs[:objectID1,objectID2,...]
- containerID: Container ID or "0" for wildcard (any container)
- verbs: Comma-separated list of operations (e.g., DELETE,GET,HEAD,PUT,SEARCH)
- objectIDs: Optional comma-separated list of specific object IDs

Example usage:
  neofs-cli session create-v2 \
    --wallet wallet.json \
    --lifetime 10000 \
    --out token.json \
    --json \
    --subject NbUgTSFvPmsRxmGeWpuuGeJUoRoi6PErcM \
	--context 0:CONTAINERPUT \
    --context 5HqniP5vq5xXr3FdijTSekrQJHu1WnADt2uLg7KSViZM:SEARCH:3JYV...obj1,3JYV...obj2
	--origin original-token.json

Default lifetime of session token is ` + strconv.Itoa(defaultLifetimeV2) + ` seconds
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
	createV2Cmd.Flags().Uint64P(commonflags.Lifetime, "l", defaultLifetimeV2, "Duration in seconds for token to stay valid")
	createV2Cmd.Flags().StringP(commonflags.WalletPath, commonflags.WalletPathShorthand, commonflags.WalletPathDefault, commonflags.WalletPathUsage)
	createV2Cmd.Flags().StringP(commonflags.Account, commonflags.AccountShorthand, commonflags.AccountDefault, commonflags.AccountUsage)
	createV2Cmd.Flags().String(outFlag, "", "File to write session token to")
	createV2Cmd.Flags().Bool(jsonFlag, false, "Output token in JSON")
	createV2Cmd.Flags().Uint64P(commonflags.ExpireAt, "e", 0, "Expiration time in seconds for token to stay valid")

	// V2-specific flags
	createV2Cmd.Flags().StringArray(v2SubjectsFlag, nil, "Subject user IDs (can be specified multiple times)")
	createV2Cmd.Flags().StringArray(v2SubjectsNNSFlag, nil, "Subject NNS names (can be specified multiple times)")
	createV2Cmd.Flags().Bool(v2FinalFlag, false, "Set the final flag in the token, disallowing further delegation")
	createV2Cmd.Flags().StringArray(v2ContextFlag, nil, "Context spec (repeatable): containerID:verbs[:objectID1,objectID2,...]. Use '0' for wildcard container. Contexts and verbs should be sorted.")
	createV2Cmd.Flags().String(v2OriginFlag, "", "Path to origin token file for token delegation chain")

	_ = cobra.MarkFlagRequired(createV2Cmd.Flags(), commonflags.WalletPath)
	_ = cobra.MarkFlagRequired(createV2Cmd.Flags(), outFlag)
	createV2Cmd.MarkFlagsOneRequired(commonflags.ExpireAt, commonflags.Lifetime)
	createV2Cmd.MarkFlagsOneRequired(v2SubjectsFlag, v2SubjectsNNSFlag)
	_ = cobra.MarkFlagRequired(createV2Cmd.Flags(), v2ContextFlag)
}

func createSessionV2(cmd *cobra.Command, _ []string) error {
	privKey, err := key.Get(cmd)
	if err != nil {
		return err
	}

	currentTime := time.Now()
	var expTime time.Time
	if exp, _ := cmd.Flags().GetUint64(commonflags.ExpireAt); exp == 0 {
		lifetime, _ := cmd.Flags().GetUint64(commonflags.Lifetime)
		lifetimeDuration := time.Duration(lifetime) * time.Second
		expTime = currentTime.Add(lifetimeDuration)
	} else {
		expTime = time.Unix(int64(exp), 0)
	}
	if expTime.Before(currentTime) {
		return errors.New("expiration time must be greater than current time")
	}

	var tokV2 session.Token
	signer := user.NewAutoIDSigner(*privKey)

	tokV2.SetVersion(session.TokenCurrentVersion)
	tokV2.SetNonce(session.RandomNonce())
	tokV2.SetNbf(currentTime)
	tokV2.SetIat(currentTime)
	tokV2.SetExp(expTime)

	final, _ := cmd.Flags().GetBool(v2FinalFlag)
	tokV2.SetFinal(final)

	originTok, err := loadOriginToken(cmd)
	if err != nil {
		return err
	}
	if originTok != nil {
		tokV2.SetOrigin(originTok)
		common.PrintVerbose(cmd, "Origin token set (delegation chain)")
	}

	subjects, err := parseSubjects(cmd)
	if err != nil {
		return err
	}
	err = tokV2.SetSubjects(subjects)
	if err != nil {
		return fmt.Errorf("can't set subjects: %w", err)
	}

	common.PrintVerbose(cmd, "Token issuer: %s", signer.UserID())
	common.PrintVerbose(cmd, "Number of subjects: %d", len(subjects))
	for i, subj := range subjects {
		if subj.IsUserID() {
			common.PrintVerbose(cmd, "  Subject %d (UserID): %s", i+1, subj.UserID())
		} else if subj.IsNNS() {
			common.PrintVerbose(cmd, "  Subject %d (NNS): %s", i+1, subj.NNSName())
		}
	}

	contexts, err := parseContexts(cmd)
	if err != nil {
		return err
	}
	err = tokV2.SetContexts(contexts)
	if err != nil {
		return fmt.Errorf("can't set contexts: %w", err)
	}

	common.PrintVerbose(cmd, "Number of contexts: %d", len(contexts))
	for i, ctx := range contexts {
		cnrStr := ctx.Container().String()
		if ctx.Container().IsZero() {
			cnrStr = "* (wildcard - any container)"
		}
		common.PrintVerbose(cmd, "  Context %d: container=%s, objects=%d, verbs=%d",
			i+1, cnrStr, len(ctx.Objects()), len(ctx.Verbs()))
	}

	if err := tokV2.Sign(signer); err != nil {
		return fmt.Errorf("failed to sign token: %w", err)
	}

	common.PrintVerbose(cmd, "Token signed successfully")

	if err := tokV2.Validate(); err != nil {
		return fmt.Errorf("created token validation failed: %w", err)
	}
	common.PrintVerbose(cmd, "Created token validated successfully")

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
	common.PrettyPrintJSON(cmd, tokV2, "Created V2 session token:")
	return nil
}

func parseSubjects(cmd *cobra.Command) ([]session.Target, error) {
	subjectIDs, _ := cmd.Flags().GetStringArray(v2SubjectsFlag)
	subjectNNS, _ := cmd.Flags().GetStringArray(v2SubjectsNNSFlag)

	if len(subjectIDs) == 0 && len(subjectNNS) == 0 {
		return nil, errors.New("at least one subject (--subject or --subject-nns) must be specified")
	}

	subjects := make([]session.Target, 0, len(subjectIDs)+len(subjectNNS))

	for _, idStr := range subjectIDs {
		var userID user.ID
		if err := userID.DecodeString(idStr); err != nil {
			return nil, fmt.Errorf("invalid subject user ID %q: %w", idStr, err)
		}
		subjects = append(subjects, session.NewTargetUser(userID))
	}

	for _, nnsName := range subjectNNS {
		if nnsName == "" {
			return nil, errors.New("NNS name cannot be empty")
		}
		subjects = append(subjects, session.NewTargetNamed(nnsName))
	}

	return subjects, nil
}

func parseContexts(cmd *cobra.Command) ([]session.Context, error) {
	ctxSpecs, _ := cmd.Flags().GetStringArray(v2ContextFlag)
	if len(ctxSpecs) == 0 {
		return nil, errors.New("--context must be specified at least once")
	}

	var contexts []session.Context
	for idx, spec := range ctxSpecs {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			return nil, fmt.Errorf("context #%d is empty", idx+1)
		}
		parts := strings.Split(spec, ":")
		if len(parts) < 2 {
			return nil, fmt.Errorf("context #%d must have at least 'containerID:verbs'", idx+1)
		}
		cnrStr := strings.TrimSpace(parts[0])
		verbsStr := strings.TrimSpace(parts[1])

		var cnrID cid.ID
		if cnrStr != "0" && cnrStr != "" {
			if err := cnrID.DecodeString(cnrStr); err != nil {
				return nil, fmt.Errorf("invalid container ID in context #%d: %w", idx+1, err)
			}
		}

		verbs, err := parseVerbs(verbsStr)
		if err != nil {
			return nil, fmt.Errorf("invalid verbs in context #%d: %w", idx+1, err)
		}

		ctx, err := session.NewContext(cnrID, verbs)
		if err != nil {
			return nil, fmt.Errorf("can't create context #%d: %w", idx+1, err)
		}

		// Optional objects list
		if len(parts) >= 3 {
			objSpec := strings.TrimSpace(parts[2])
			if objSpec != "" {
				objStrs := strings.Split(objSpec, ",")
				var objIDs []oid.ID
				for _, objStr := range objStrs {
					objStr = strings.TrimSpace(objStr)
					if objStr == "" {
						continue
					}
					var objID oid.ID
					if err := objID.DecodeString(objStr); err != nil {
						return nil, fmt.Errorf("invalid object ID %q in context #%d: %w", objStr, idx+1, err)
					}
					objIDs = append(objIDs, objID)
				}
				if len(objIDs) > 0 {
					if err := ctx.SetObjects(objIDs); err != nil {
						return nil, fmt.Errorf("can't set objects in context #%d: %w", idx+1, err)
					}
				}
			}
		}

		contexts = append(contexts, ctx)
	}
	return contexts, nil
}

func parseVerbs(verbsStr string) ([]session.Verb, error) {
	if verbsStr == "" {
		return nil, errors.New("verbs cannot be empty")
	}

	verbStrs := strings.Split(verbsStr, ",")
	verbs := make([]session.Verb, 0, len(verbStrs))

	for _, verbStr := range verbStrs {
		verbStr = strings.TrimSpace(strings.ToUpper(verbStr))

		var verb session.Verb
		switch verbStr {
		case "GET", "OBJECTGET":
			verb = session.VerbObjectGet
		case "PUT", "OBJECTPUT":
			verb = session.VerbObjectPut
		case "HEAD", "OBJECTHEAD":
			verb = session.VerbObjectHead
		case "SEARCH", "OBJECTSEARCH":
			verb = session.VerbObjectSearch
		case "DELETE", "OBJECTDELETE":
			verb = session.VerbObjectDelete
		case "RANGE", "OBJECTRANGE":
			verb = session.VerbObjectRange
		case "RANGEHASH", "OBJECTRANGEHASH", "RANGE_HASH", "OBJECT_RANGE_HASH":
			verb = session.VerbObjectRangeHash
		case "CONTAINERSET", "CONTAINERSETACL", "CONTAINER_SET", "CONTAINER_SET_ACL":
			verb = session.VerbContainerSetEACL
		case "CONTAINERPUT", "CONTAINER_PUT":
			verb = session.VerbContainerPut
		case "CONTAINERDELETE", "CONTAINER_DELETE":
			verb = session.VerbContainerDelete
		default:
			return nil, fmt.Errorf("unknown verb: %s (supported: GET,PUT,HEAD,SEARCH,DELETE,RANGE,RANGEHASH,CONTAINERSET,CONTAINERPUT,CONTAINERDELETE)", verbStr)
		}

		verbs = append(verbs, verb)
	}

	return verbs, nil
}

func loadOriginToken(cmd *cobra.Command) (*session.Token, error) {
	originPath, _ := cmd.Flags().GetString(v2OriginFlag)
	if originPath == "" {
		return nil, nil
	}

	data, err := os.ReadFile(originPath)
	if err != nil {
		return nil, fmt.Errorf("can't read origin token file: %w", err)
	}

	var originTok session.Token
	if err := originTok.UnmarshalJSON(data); err != nil {
		if err := originTok.Unmarshal(data); err != nil {
			return nil, fmt.Errorf("can't unmarshal origin token (tried JSON and binary): %w", err)
		}
	}

	if err := originTok.Validate(); err != nil {
		return nil, fmt.Errorf("origin token validation failed: %w", err)
	}

	return &originTok, nil
}
