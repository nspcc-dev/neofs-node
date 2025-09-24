package extended

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

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
  'address:<adr1>,<adr2>,...' for exact request sender, where <adr> is a base58 25-byte address. Example: NSiVJYZej4XsxG5CUpdwn7VRQk8iiiDMPM.

When both '--rule' and '--file' arguments are used, '--rule' records will be placed higher in resulting extended ACL table.
`,
	Example: `neofs-cli acl extended create --cid EutHBsdT1YCzHxjCfQHnLPL1vFrkSyLSio4vkphfnEk -f rules.txt --out table.json
neofs-cli acl extended create --cid EutHBsdT1YCzHxjCfQHnLPL1vFrkSyLSio4vkphfnEk -r 'allow get obj:Key=Value others' -r 'deny put others' -r 'deny put obj:$Object:payloadLength<4096 others' -r 'deny get obj:Quality>=100 others'`,
	Args: cobra.NoArgs,
	RunE: createEACL,
}

func init() {
	createCmd.Flags().StringArrayP("rule", "r", nil, "Extended ACL table record to apply")
	createCmd.Flags().StringP("file", "f", "", "Read list of extended ACL table records from text file")
	createCmd.Flags().StringP("out", "o", "", "Save JSON formatted extended ACL table in file")
	createCmd.Flags().StringP(commonflags.CIDFlag, "", "", commonflags.CIDFlagUsage)

	_ = cobra.MarkFlagFilename(createCmd.Flags(), "file")
	_ = cobra.MarkFlagFilename(createCmd.Flags(), "out")
}

func createEACL(cmd *cobra.Command, _ []string) error {
	rules, _ := cmd.Flags().GetStringArray("rule")
	fileArg, _ := cmd.Flags().GetString("file")
	outArg, _ := cmd.Flags().GetString("out")
	cidArg, _ := cmd.Flags().GetString(commonflags.CIDFlag)

	var containerID cid.ID
	if cidArg != "" {
		if err := containerID.DecodeString(cidArg); err != nil {
			return fmt.Errorf("invalid container ID: %w", err)
		}
	}

	rulesFile, err := getRulesFromFile(fileArg)
	if err != nil {
		return fmt.Errorf("can't read rules from file: %w", err)
	}

	rules = append(rules, rulesFile...)
	if len(rules) == 0 {
		return errors.New("no extended ACL rules has been provided")
	}

	var tb eacl.Table
	if err := util.ParseEACLRules(&tb, rules); err != nil {
		return fmt.Errorf("unable to parse provided rules: %w", err)
	}

	err = util.ValidateEACLTable(tb)
	if err != nil {
		return fmt.Errorf("table validation: %w", err)
	}

	tb.SetCID(containerID)

	data, err := tb.MarshalJSON()
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	err = json.Indent(buf, data, "", "  ")
	if err != nil {
		return err
	}

	if len(outArg) == 0 {
		cmd.Println(buf)
		return nil
	}

	err = os.WriteFile(outArg, buf.Bytes(), 0o644)
	if err != nil {
		return err
	}

	return nil
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
