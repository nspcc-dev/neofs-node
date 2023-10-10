package util

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/flynn-archive/go-shlex"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

// PrettyPrintTableBACL print basic ACL in table format.
func PrettyPrintTableBACL(cmd *cobra.Command, bacl *acl.Basic) {
	// Header
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 1, 4, 4, ' ', 0)
	fmt.Fprintln(w, "\tRangeHASH\tRange\tSearch\tDelete\tPut\tHead\tGet")
	// Bits
	bits := []string{
		boolToString(bacl.Sticky()) + " " + boolToString(!bacl.Extendable()),
		getRoleBitsForOperation(bacl, acl.OpObjectHash), getRoleBitsForOperation(bacl, acl.OpObjectRange),
		getRoleBitsForOperation(bacl, acl.OpObjectSearch), getRoleBitsForOperation(bacl, acl.OpObjectDelete),
		getRoleBitsForOperation(bacl, acl.OpObjectPut), getRoleBitsForOperation(bacl, acl.OpObjectHead),
		getRoleBitsForOperation(bacl, acl.OpObjectGet),
	}
	fmt.Fprintln(w, strings.Join(bits, "\t"))
	// Footer
	footer := []string{"X F"}
	for i := 0; i < 7; i++ {
		footer = append(footer, "U S O B")
	}
	fmt.Fprintln(w, strings.Join(footer, "\t"))

	w.Flush()

	cmd.Println("  X-Sticky F-Final U-User S-System O-Others B-Bearer")
}

func getRoleBitsForOperation(bacl *acl.Basic, op acl.Op) string {
	return boolToString(bacl.IsOpAllowed(op, acl.RoleOwner)) + " " +
		boolToString(bacl.IsOpAllowed(op, acl.RoleContainer)) + " " +
		boolToString(bacl.IsOpAllowed(op, acl.RoleOthers)) + " " +
		boolToString(bacl.AllowedBearerRules(op))
}

func boolToString(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// PrettyPrintTableEACL print extended ACL in table format.
func PrettyPrintTableEACL(cmd *cobra.Command, table eacl.Table) {
	out := tablewriter.NewWriter(cmd.OutOrStdout())
	out.SetHeader([]string{"Operation", "Action", "Filters", "Targets"})
	out.SetAlignment(tablewriter.ALIGN_CENTER)
	out.SetRowLine(true)

	out.SetAutoWrapText(false)

	for _, r := range table.Records() {
		out.Append([]string{
			r.Op().String(),
			r.Action().String(),
			eaclFiltersToString(r.Filters()),
			eaclTargetToString(r),
		})
	}

	out.Render()
}

func eaclTargetToString(r eacl.Record) string {
	b := bytes.NewBuffer(nil)

	switch {
	case r.IsForRole(eacl.RoleContainerOwner):
		b.WriteString("User")
	case r.IsForRole(eacl.RoleSystem):
		b.WriteString("System")
	case r.IsForRole(eacl.RoleOthers):
		b.WriteString("Others")
	}

	for i, pub := range r.TargetBinaryKeys() {
		if i != 0 {
			b.WriteString("         ")
		}
		b.WriteString(hex.EncodeToString(pub))
		b.WriteString("\n")
	}

	return b.String()
}

func eaclFiltersToString(fs []eacl.Filter) string {
	b := bytes.NewBuffer(nil)
	tw := tabwriter.NewWriter(b, 0, 0, 1, ' ', 0)

	for _, f := range fs {
		switch f.HeaderType() {
		case eacl.HeaderFromObject:
			_, _ = tw.Write([]byte("O:\t"))
		case eacl.HeaderFromRequest:
			_, _ = tw.Write([]byte("R:\t"))
		case eacl.HeaderFromService:
			_, _ = tw.Write([]byte("S:\t"))
		default:
			_, _ = tw.Write([]byte("  \t"))
		}

		_, _ = tw.Write([]byte(f.HeaderKey()))

		switch f.Matcher() {
		case eacl.MatchStringEqual:
			_, _ = tw.Write([]byte("\t==\t"))
		case eacl.MatchStringNotEqual:
			_, _ = tw.Write([]byte("\t!=\t"))
		}

		_, _ = tw.Write([]byte(f.HeaderValue() + "\t"))
		_, _ = tw.Write([]byte("\n"))
	}

	_ = tw.Flush()

	// To have nice output with tabwriter, we must append newline
	// after the last line. Here we strip it to delete empty line
	// in the final output.
	s := b.String()
	if len(s) > 0 {
		s = s[:len(s)-1]
	}

	return s
}

// ParseEACLRules parses eACL table.
// Uses ParseEACLRule.
//
//nolint:godot
func ParseEACLRules(rules []string) (eacl.Table, error) {
	if len(rules) == 0 {
		return eacl.Table{}, errors.New("no extended ACL rules has been provided")
	}

	var records []eacl.Record

	for _, ruleStr := range rules {
		rs, err := ParseEACLRule(ruleStr)
		if err != nil {
			return eacl.Table{}, fmt.Errorf("can't create extended acl record from rule '%s': %v", ruleStr, err)
		}

		records = append(records, rs...)
	}

	return eacl.New(records), nil
}

// ParseEACLRule parses eACL table from the following form:
// <action> <operation> [<filter1> ...] [<target1> ...]
//
// Examples:
// allow get req:X-Header=123 obj:Attr=value user others pubkey:0xkey1,0xkey2
//
//nolint:godot
func ParseEACLRule(rule string) ([]eacl.Record, error) {
	r, err := shlex.Split(rule)
	if err != nil {
		return nil, fmt.Errorf("can't parse rule '%s': %v", rule, err)
	}
	return parseEACLRecords(r)
}

func parseEACLRecords(args []string) ([]eacl.Record, error) {
	if len(args) < 2 {
		return nil, errors.New("at least 2 arguments must be provided")
	}

	var action eacl.Action

	switch args[0] {
	default:
		return nil, errors.New("invalid action (expected 'allow' or 'deny')")
	case "allow":
		action = eacl.ActionAllow
	case "deny":
		action = eacl.ActionDeny
	}

	ss := strings.Split(args[1], ",")
	ops := make([]acl.Op, len(ss))

	for i := range ss {
		switch ss[i] {
		default:
			return nil, fmt.Errorf("unsupported operation: %s", ss[i])
		case "get":
			ops[i] = acl.OpObjectGet
		case "head":
			ops[i] = acl.OpObjectHead
		case "put":
			ops[i] = acl.OpObjectPut
		case "delete":
			ops[i] = acl.OpObjectDelete
		case "search":
			ops[i] = acl.OpObjectSearch
		case "getrange":
			ops[i] = acl.OpObjectRange
		case "getrangehash":
			ops[i] = acl.OpObjectHash
		}
	}

	var records []eacl.Record
	var roles []eacl.Role
	var pubKeys []neofscrypto.PublicKey
	var filters []eacl.Filter

	for _, argN := range args[2:] {
		ss := strings.SplitN(argN, ":", 2)
		switch prefix := strings.ToLower(ss[0]); prefix {
		case "req", "obj": // filters
			if len(ss) != 2 {
				return nil, fmt.Errorf("invalid filter or target: %s", argN)
			}

			i := strings.Index(ss[1], "=")
			if i < 0 {
				return nil, fmt.Errorf("invalid filter key-value pair: %s", ss[1])
			}

			var key, value string
			var matcher eacl.Matcher

			if 0 < i && ss[1][i-1] == '!' {
				key = ss[1][:i-1]
				matcher = eacl.MatchStringNotEqual
			} else {
				key = ss[1][:i]
				matcher = eacl.MatchStringEqual
			}

			value = ss[1][i+1:]

			typ := eacl.HeaderFromRequest
			if ss[0] == "obj" {
				typ = eacl.HeaderFromObject
			}

			filters = append(filters, eacl.NewFilter(typ, key, matcher, value))
		case "pubkey":
			if len(ss) < 2 {
				return nil, errors.New("missing public keys")
			}

			pubs, err := parseKeyList(ss[1])
			if err != nil {
				return nil, err
			}

			pubKeys = append(pubKeys, pubs...)
		case "user":
			roles = append(roles, eacl.RoleContainerOwner)
		case "others":
			roles = append(roles, eacl.RoleOthers)
		case "system":
			return nil, errors.New("system role access must not be modified")
		default:
			return nil, fmt.Errorf("invalid prefix: %s", ss[0])
		}
	}

	if len(roles)+len(pubKeys) == 0 {
		return nil, errors.New("neither roles nor public keys are specified")
	}

	for i := range ops {
		records = append(records, eacl.NewRecord(action, ops[i], eacl.NewTarget(roles, pubKeys), filters...))
	}

	return records, nil
}

// parseKeyList parses list of hex-encoded public keys separated by comma.
func parseKeyList(s string) ([]neofscrypto.PublicKey, error) {
	ss := strings.Split(s, ",")
	pubs := make([]neofscrypto.PublicKey, len(ss))
	for i := range ss {
		st := strings.TrimPrefix(ss[i], "0x")
		pub, err := keys.NewPublicKeyFromString(st)
		if err != nil {
			return nil, fmt.Errorf("invalid public key '%s': %w", ss[i], err)
		}

		pubs[i] = (*neofsecdsa.PublicKey)(pub)
	}

	return pubs, nil
}

// ValidateEACLTable validates eACL table:
//   - eACL table must not modify [eacl.RoleSystem] access.
func ValidateEACLTable(t eacl.Table) error {
	for _, record := range t.Records() {
		if record.IsForRole(eacl.RoleSystem) {
			return errors.New("it is prohibited to modify system access")
		}
	}

	return nil
}
