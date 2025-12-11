package session

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/spf13/cobra"
)

var inspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "Inspect session token (V1 or V2)",
	Long: `Inspect and display information about a session token.

Supports both V1 (session.Object) and V2 (session.TokenV2) tokens.
Automatically detects the token version and displays relevant information.

Example usage:
  neofs-cli session inspect --token token.json
  neofs-cli session inspect --token token.bin
`,
	Args: cobra.NoArgs,
	RunE: inspectSession,
}

const (
	inspectTokenFlag = "token"
)

func init() {
	inspectCmd.Flags().String(inspectTokenFlag, "", "Path to session token file (JSON or binary)")
	_ = cobra.MarkFlagRequired(inspectCmd.Flags(), inspectTokenFlag)
}

func inspectSession(cmd *cobra.Command, _ []string) error {
	tokenPath, _ := cmd.Flags().GetString(inspectTokenFlag)

	data, err := os.ReadFile(tokenPath)
	if err != nil {
		return fmt.Errorf("failed to read token file: %w", err)
	}

	var tokV2 sessionv2.Token
	if err := tokV2.UnmarshalJSON(data); err == nil {
		return displayTokenV2(&tokV2)
	}

	if err := tokV2.Unmarshal(data); err == nil {
		return displayTokenV2(&tokV2)
	}

	var tokV1 session.Object
	if err := tokV1.UnmarshalJSON(data); err == nil {
		return displayTokenV1(cmd, &tokV1)
	}

	if err := tokV1.Unmarshal(data); err == nil {
		return displayTokenV1(cmd, &tokV1)
	}

	return fmt.Errorf("failed to parse token as V1 or V2 session token")
}

func displayTokenV2(tok *sessionv2.Token) error {
	fmt.Println("=== Session Token V2 ===")
	fmt.Printf("Version: %d\n", tok.Version())

	fmt.Printf("Issuer: %s\n", tok.Issuer().String())

	subjects := tok.Subjects()
	fmt.Printf("\nSubjects (%d):\n", len(subjects))
	for i, subj := range subjects {
		if subj.IsUserID() {
			fmt.Printf("\t%d. %s (UserID)\n", i+1, subj.UserID().String())
		} else if subj.IsNNS() {
			fmt.Printf("\t%d. %s (NNS)\n", i+1, subj.NNSName())
		}
	}

	fmt.Printf("\nLifetime:\n")
	fmt.Printf("\tIssued At (iat): %d\n", tok.Iat())
	fmt.Printf("\tNot Before (nbf): %d\n", tok.Nbf())
	fmt.Printf("\tExpires At (exp): %d\n", tok.Exp())

	contexts := tok.Contexts()
	fmt.Printf("\nContexts (%d):\n", len(contexts))
	for i, ctx := range contexts {
		fmt.Printf("\tContext %d:\n", i+1)
		fmt.Printf("\t\tContainer: %s\n", ctx.Container().String())

		objects := ctx.Objects()
		if len(objects) > 0 {
			fmt.Printf("\t\tObjects (%d):\n", len(objects))
			for j, obj := range objects {
				fmt.Printf("\t\t\t%d. %s\n", j+1, obj.String())
			}
		} else {
			fmt.Printf("\t\tObjects: All (unlimited)\n")
		}

		verbs := ctx.Verbs()
		fmt.Printf("\t\tVerbs (%d): ", len(verbs))
		for j, verb := range verbs {
			if j > 0 {
				fmt.Print(", ")
			}
			fmt.Print(verbV2ToString(verb))
		}
		fmt.Println()
	}

	fmt.Printf("\nValidation: ")
	if err := tok.Validate(); err != nil {
		fmt.Printf("INVALID (%v)\n", err)
	} else {
		fmt.Printf("VALID\n")
	}

	return nil
}

func displayTokenV1(cmd *cobra.Command, tok *session.Object) error {
	fmt.Println("=== Session Token V1 ===")
	fmt.Printf("ID: %s\n", tok.ID().String())

	issuer := tok.Issuer()
	fmt.Printf("Issuer: %s\n", issuer.String())

	fmt.Printf("\nLifetime:\n")
	fmt.Printf("\tIssued At (iat): %d\n", tok.Iat())
	fmt.Printf("\tNot Before (nbf): %d\n", tok.Nbf())
	fmt.Printf("\tExpires At (exp): %d\n", tok.Exp())

	common.PrettyPrintJSON(cmd, tok, "Token v1")
	jsonData, err := tok.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal token to JSON: %w", err)
	}

	var prettyJSON map[string]any
	if err := json.Unmarshal(jsonData, &prettyJSON); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	prettyData, err := json.MarshalIndent(prettyJSON, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to format JSON: %w", err)
	}

	fmt.Println(string(prettyData))
	return nil
}

func verbV2ToString(verb sessionv2.Verb) string {
	switch verb {
	case sessionv2.VerbObjectGet:
		return "GET"
	case sessionv2.VerbObjectPut:
		return "PUT"
	case sessionv2.VerbObjectHead:
		return "HEAD"
	case sessionv2.VerbObjectSearch:
		return "SEARCH"
	case sessionv2.VerbObjectDelete:
		return "DELETE"
	case sessionv2.VerbObjectRange:
		return "RANGE"
	case sessionv2.VerbObjectRangeHash:
		return "RANGEHASH"
	case sessionv2.VerbContainerPut:
		return "CONTAINER_PUT"
	case sessionv2.VerbContainerDelete:
		return "CONTAINER_DELETE"
	case sessionv2.VerbContainerSetEACL:
		return "CONTAINER_SET_EACL"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", verb)
	}
}
