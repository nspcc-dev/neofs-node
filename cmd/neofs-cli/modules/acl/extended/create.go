package extended

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/flynn-archive/go-shlex"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
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
  Match is '=' for matching and '!=' for non-matching filter.
  Value is a valid unicode string corresponding to object or request header value.

Target is 
  'user' for container owner, 
  'system' for Storage nodes in container and Inner Ring nodes,
  'others' for all other request senders, 
  'pubkey:<key1>,<key2>,...' for exact request sender, where <key> is a hex-encoded 33-byte public key.

When both '--rule' and '--file' arguments are used, '--rule' records will be placed higher in resulting extended ACL table.
`,
	Example: `neofs-cli acl extended create --cid EutHBsdT1YCzHxjCfQHnLPL1vFrkSyLSio4vkphfnEk -f rules.txt --out table.json
neofs-cli acl extended create --cid EutHBsdT1YCzHxjCfQHnLPL1vFrkSyLSio4vkphfnEk -r 'allow get obj:Key=Value others' -r 'deny put others'`,
	Run: createEACL,
}

func init() {
	createCmd.Flags().StringArrayP("rule", "r", nil, "Extended ACL table record to apply")
	createCmd.Flags().StringP("file", "f", "", "Read list of extended ACL table records from from text file")
	createCmd.Flags().StringP("out", "o", "", "Save JSON formatted extended ACL table in file")
	createCmd.Flags().StringP("cid", "", "", "Container ID")

	_ = cobra.MarkFlagFilename(createCmd.Flags(), "file")
	_ = cobra.MarkFlagFilename(createCmd.Flags(), "out")
}

func createEACL(cmd *cobra.Command, _ []string) {
	rules, _ := cmd.Flags().GetStringArray("rule")
	fileArg, _ := cmd.Flags().GetString("file")
	outArg, _ := cmd.Flags().GetString("out")
	cidArg, _ := cmd.Flags().GetString("cid")

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

	for _, ruleStr := range rules {
		r, err := shlex.Split(ruleStr)
		if err != nil {
			cmd.PrintErrf("can't parse rule '%s': %v\n", ruleStr, err)
			os.Exit(1)
		}

		err = parseTable(tb, r)
		if err != nil {
			cmd.PrintErrf("can't create extended ACL record from rule '%s': %v\n", ruleStr, err)
			os.Exit(1)
		}
	}

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

	err = os.WriteFile(outArg, buf.Bytes(), 0644)
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

// parseTable parses eACL table from the following form:
// <action> <operation> [<filter1> ...] [<target1> ...]
//
// Examples:
// allow get req:X-Header=123 obj:Attr=value others:0xkey1,key2 system:key3 user:key4
//
//nolint:godot
func parseTable(tb *eacl.Table, args []string) error {
	if len(args) < 2 {
		return errors.New("at least 2 arguments must be provided")
	}

	var action eacl.Action
	if !action.FromString(strings.ToUpper(args[0])) {
		return errors.New("invalid action (expected 'allow' or 'deny')")
	}

	ops, err := parseOperations(args[1])
	if err != nil {
		return err
	}

	r, err := parseRecord(args[2:])
	if err != nil {
		return err
	}

	r.SetAction(action)

	for _, op := range ops {
		r := *r
		r.SetOperation(op)
		tb.AddRecord(&r)
	}

	return nil
}

func parseRecord(args []string) (*eacl.Record, error) {
	r := new(eacl.Record)
	for i := range args {
		ss := strings.SplitN(args[i], ":", 2)

		switch prefix := strings.ToLower(ss[0]); prefix {
		case "req", "obj": // filters
			if len(ss) != 2 {
				return nil, fmt.Errorf("invalid filter or target: %s", args[i])
			}

			i := strings.Index(ss[1], "=")
			if i < 0 {
				return nil, fmt.Errorf("invalid filter key-value pair: %s", ss[1])
			}

			var key, value string
			var op eacl.Match

			if 0 < i && ss[1][i-1] == '!' {
				key = ss[1][:i-1]
				op = eacl.MatchStringNotEqual
			} else {
				key = ss[1][:i]
				op = eacl.MatchStringEqual
			}

			value = ss[1][i+1:]

			typ := eacl.HeaderFromRequest
			if ss[0] == "obj" {
				typ = eacl.HeaderFromObject
			}

			r.AddFilter(typ, op, key, value)
		case "others", "system", "user", "pubkey": // targets
			var err error

			var pubs []ecdsa.PublicKey
			if len(ss) == 2 {
				pubs, err = parseKeyList(ss[1])
				if err != nil {
					return nil, err
				}
			}

			var role eacl.Role
			if prefix != "pubkey" {
				role, err = roleFromString(prefix)
				if err != nil {
					return nil, err
				}
			}

			eacl.AddFormedTarget(r, role, pubs...)

		default:
			return nil, fmt.Errorf("invalid prefix: %s", ss[0])
		}
	}

	return r, nil
}

func roleFromString(s string) (eacl.Role, error) {
	var r eacl.Role
	if !r.FromString(strings.ToUpper(s)) {
		return r, fmt.Errorf("unexpected role %s", s)
	}

	return r, nil
}

// parseKeyList parses list of hex-encoded public keys separated by comma.
func parseKeyList(s string) ([]ecdsa.PublicKey, error) {
	ss := strings.Split(s, ",")
	pubs := make([]ecdsa.PublicKey, len(ss))
	for i := range ss {
		st := strings.TrimPrefix(ss[i], "0x")
		pub, err := keys.NewPublicKeyFromString(st)
		if err != nil {
			return nil, fmt.Errorf("invalid public key '%s': %w", ss[i], err)
		}

		pubs[i] = ecdsa.PublicKey(*pub)
	}

	return pubs, nil
}

func parseOperations(s string) ([]eacl.Operation, error) {
	ss := strings.Split(s, ",")
	ops := make([]eacl.Operation, len(ss))

	for i := range ss {
		if !ops[i].FromString(strings.ToUpper(ss[i])) {
			return nil, fmt.Errorf("invalid operation: %s", ss[i])
		}
	}

	return ops, nil
}
