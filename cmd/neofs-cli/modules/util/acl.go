package util

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/flynn-archive/go-shlex"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
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
func PrettyPrintTableEACL(cmd *cobra.Command, table *eacl.Table) {
	out := tablewriter.NewWriter(cmd.OutOrStdout())
	out.SetHeader([]string{"Operation", "Action", "Filters", "Targets"})
	out.SetAlignment(tablewriter.ALIGN_CENTER)
	out.SetRowLine(true)

	out.SetAutoWrapText(false)

	for _, r := range table.Records() {
		out.Append([]string{
			r.Operation().String(),
			r.Action().String(),
			eaclFiltersToString(r.Filters()),
			eaclTargetsToString(r.Targets()),
		})
	}

	out.Render()
}

func eaclTargetsToString(ts []eacl.Target) string {
	b := bytes.NewBuffer(nil)
	for _, t := range ts {
		keysExists := len(t.BinaryKeys()) > 0
		switch t.Role() {
		case eacl.RoleUser:
			b.WriteString("User")
			if keysExists {
				b.WriteString(":    ")
			}
		case eacl.RoleSystem:
			b.WriteString("System")
			if keysExists {
				b.WriteString(":  ")
			}
		case eacl.RoleOthers:
			b.WriteString("Others")
			if keysExists {
				b.WriteString(":  ")
			}
		default:
			b.WriteString("Unknown")
			if keysExists {
				b.WriteString(": ")
			}
		}

		for i, pub := range t.BinaryKeys() {
			if i != 0 {
				b.WriteString("         ")
			}
			b.WriteString(hex.EncodeToString(pub))
			b.WriteString("\n")
		}
	}

	return b.String()
}

func eaclFiltersToString(fs []eacl.Filter) string {
	b := bytes.NewBuffer(nil)
	tw := tabwriter.NewWriter(b, 0, 0, 1, ' ', 0)

	for _, f := range fs {
		switch f.From() {
		case eacl.HeaderFromObject:
			_, _ = tw.Write([]byte("O:\t"))
		case eacl.HeaderFromRequest:
			_, _ = tw.Write([]byte("R:\t"))
		case eacl.HeaderFromService:
			_, _ = tw.Write([]byte("S:\t"))
		default:
			_, _ = tw.Write([]byte("  \t"))
		}

		_, _ = tw.Write([]byte(f.Key()))

		switch f.Matcher() {
		case eacl.MatchStringEqual:
			_, _ = tw.Write([]byte("\t==\t"))
		case eacl.MatchStringNotEqual:
			_, _ = tw.Write([]byte("\t!=\t"))
		case eacl.MatchUnknown:
		}

		_, _ = tw.Write([]byte(f.Value() + "\t"))
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
func ParseEACLRules(table *eacl.Table, rules []string) error {
	if len(rules) == 0 {
		return errors.New("no extended ACL rules has been provided")
	}

	for _, ruleStr := range rules {
		err := ParseEACLRule(table, ruleStr)
		if err != nil {
			return fmt.Errorf("can't create extended acl record from rule '%s': %v", ruleStr, err)
		}
	}
	return nil
}

// ParseEACLRule parses eACL table from the following form:
// <action> <operation> [<filter1> ...] [<target1> ...]
//
// Examples:
// allow get req:X-Header=123 obj:Attr=value others:0xkey1,key2 system:key3 user:key4
//
//nolint:godot
func ParseEACLRule(table *eacl.Table, rule string) error {
	r, err := shlex.Split(rule)
	if err != nil {
		return fmt.Errorf("can't parse rule '%s': %v", rule, err)
	}
	return parseEACLTable(table, r)
}

func parseEACLTable(tb *eacl.Table, args []string) error {
	if len(args) < 2 {
		return errors.New("at least 2 arguments must be provided")
	}

	var action eacl.Action
	if !action.FromString(strings.ToUpper(args[0])) {
		return errors.New("invalid action (expected 'allow' or 'deny')")
	}

	ops, err := eaclOperationsFromString(args[1])
	if err != nil {
		return err
	}

	r, err := parseEACLRecord(args[2:])
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

func parseEACLRecord(args []string) (*eacl.Record, error) {
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
				role, err = eaclRoleFromString(prefix)
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

// eaclRoleFromString parses eacl.Role from string.
func eaclRoleFromString(s string) (eacl.Role, error) {
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

// eaclOperationsFromString parses list of eacl.Operation separated by comma.
func eaclOperationsFromString(s string) ([]eacl.Operation, error) {
	ss := strings.Split(s, ",")
	ops := make([]eacl.Operation, len(ss))

	for i := range ss {
		if !ops[i].FromString(strings.ToUpper(ss[i])) {
			return nil, fmt.Errorf("invalid operation: %s", ss[i])
		}
	}

	return ops, nil
}
