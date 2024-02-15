package extended

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create extended ACL from the text representation",
	Long: `Create extended ACL from the text representation.

Rule consist of these blocks: <action> <operation> [<filter1> ...] [<target1> ...]

Action is 'allow' or 'deny'.

Operation is an object service verb: 'get', 'head', 'put', 'search', 'delete', 'getrange', or 'getrangehash'.

Filter consists of <typ>:<key><match><value>
  Typ is 'obj' for object applied filter or 'req' for request applied filter. 
  Key is a valid unicode string corresponding to object or request header key. 
    Well-known system object headers start with '$Object:' prefix.
    User defined headers start without prefix.
    Read more about filter keys at github.com/nspcc-dev/neofs-api/blob/master/proto-docs/acl.md#message-eaclrecordfilter
  Match is:
    '=' for string equality or, if no value, attribute absence;
    '!=' for string inequality;
    '>' | '>=' | '<' | '<=' for integer comparison.
  Value is a valid unicode string corresponding to object or request header value. Numeric filters must have base-10 integer values.

Target is 
  'user' for container owner, 
  'system' for Storage nodes in container and Inner Ring nodes,
  'others' for all other request senders, 
  'pubkey:<key1>,<key2>,...' for exact request sender, where <key> is a hex-encoded 33-byte public key.

When both '--rule' and '--file' arguments are used, '--rule' records will be placed higher in resulting extended ACL table.
`,
	Example: `neofs-cli acl extended create --cid EutHBsdT1YCzHxjCfQHnLPL1vFrkSyLSio4vkphfnEk -f rules.txt --out table.json
neofs-cli acl extended create --cid EutHBsdT1YCzHxjCfQHnLPL1vFrkSyLSio4vkphfnEk -r 'allow get obj:Key=Value others' -r 'deny put others' -r 'deny put obj:$Object:payloadLength<4096 others' -r 'deny get obj:Quality>=100 others'`,
	Args: cobra.NoArgs,
	Run:  createEACL,
}

func init() {
	createCmd.Flags().StringArrayP("rule", "r", nil, "Extended ACL table record to apply")
	createCmd.Flags().StringP("file", "f", "", "Read list of extended ACL table records from text file")
	createCmd.Flags().StringP("out", "o", "", "Save JSON formatted extended ACL table in file")
	createCmd.Flags().StringP(commonflags.CIDFlag, "", "", commonflags.CIDFlagUsage)

	_ = cobra.MarkFlagFilename(createCmd.Flags(), "file")
	_ = cobra.MarkFlagFilename(createCmd.Flags(), "out")
}

func createEACL(cmd *cobra.Command, _ []string) {
	rules, _ := cmd.Flags().GetStringArray("rule")
	fileArg, _ := cmd.Flags().GetString("file")
	outArg, _ := cmd.Flags().GetString("out")
	cidArg, _ := cmd.Flags().GetString(commonflags.CIDFlag)

	var containerID cid.ID
	if cidArg != "" {
		if err := containerID.DecodeString(cidArg); err != nil {
			cmd.PrintErrf("invalid container ID: %v\n", err)
			os.Exit(1)
		}
	}

	rulesFile, err := getRulesFromFile(fileArg)
	if err != nil {
		cmd.PrintErrf("can't read rules from file: %v\n", err)
		os.Exit(1)
	}

	rules = append(rules, rulesFile...)
	if len(rules) == 0 {
		cmd.PrintErrln("no extended ACL rules has been provided")
		os.Exit(1)
	}

	tb := eacl.NewTable()
	common.ExitOnErr(cmd, "unable to parse provided rules: %w", util.ParseEACLRules(tb, rules))

	err = util.ValidateEACLTable(tb)
	common.ExitOnErr(cmd, "table validation: %w", err)

	tb.SetCID(containerID)

	data, err := tb.MarshalJSON()
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}

	buf := new(bytes.Buffer)
	err = json.Indent(buf, data, "", "  ")
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}

	if len(outArg) == 0 {
		cmd.Println(buf)
		return
	}

	err = os.WriteFile(outArg, buf.Bytes(), 0o644)
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
}

func getRulesFromFile(filename string) ([]string, error) {
	if len(filename) == 0 {
		return nil, nil
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	return strings.Split(strings.TrimSpace(string(data)), "\n"), nil
}
