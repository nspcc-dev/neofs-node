package util

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"text/tabwriter"

	"github.com/flynn-archive/go-shlex"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/user"
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
	for range 7 {
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
		rawSubjects := t.RawSubjects()
		keysExists := len(rawSubjects) > 0
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

		for i, rawSubject := range rawSubjects {
			if i != 0 {
				b.WriteString("         ")
			}
			b.WriteString(hex.EncodeToString(rawSubject))
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
		//nolint:exhaustive
		switch f.Matcher() {
		case eacl.MatchStringEqual:
			_, _ = tw.Write([]byte("\t==\t"))
		case eacl.MatchStringNotEqual:
			_, _ = tw.Write([]byte("\t!=\t"))
		case eacl.MatchNumGT:
			_, _ = tw.Write([]byte("\t>\t"))
		case eacl.MatchNumGE:
			_, _ = tw.Write([]byte("\t>=\t"))
		case eacl.MatchNumLT:
			_, _ = tw.Write([]byte("\t<\t"))
		case eacl.MatchNumLE:
			_, _ = tw.Write([]byte("\t<=\t"))
		case eacl.MatchNotPresent:
			_, _ = tw.Write([]byte("\tNULL\t"))
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
			return fmt.Errorf("can't create extended acl record from rule '%s': %w", ruleStr, err)
		}
	}
	return nil
}

// ParseEACLRule parses eACL table from the following form:
// <action> <operation> [<filter1> ...] [<target1> ...]
//
// Examples:
// allow get req:X-Header=123 obj:Attr=value user system address:addr1,addr2,addr3
//
//nolint:godot
func ParseEACLRule(table *eacl.Table, rule string) error {
	r, err := shlex.Split(rule)
	if err != nil {
		return fmt.Errorf("can't parse rule '%s': %w", rule, err)
	}
	return parseEACLTable(table, r)
}

func parseEACLTable(tb *eacl.Table, args []string) error {
	if len(args) < 2 {
		return errors.New("at least 2 arguments must be provided")
	}

	var action eacl.Action
	if !action.DecodeString(strings.ToUpper(args[0])) {
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

	records := make([]eacl.Record, 0, len(ops))

	for _, op := range ops {
		var record eacl.Record
		r.CopyTo(&record)

		record.SetOperation(op)
		records = append(records, record)
	}

	tb.SetRecords(append(tb.Records(), records...))

	return nil
}

func parseEACLRecord(args []string) (eacl.Record, error) {
	var filters []eacl.Filter
	var targets []eacl.Target

	for i := range args {
		ss := strings.SplitN(args[i], ":", 2)

		switch prefix := strings.ToLower(ss[0]); prefix {
		case "req", "obj": // filters
			if len(ss) != 2 {
				return eacl.Record{}, fmt.Errorf("invalid filter or target: %s", args[i])
			}

			key, value, op, err := parseKVWithOp(ss[1])
			if err != nil {
				return eacl.Record{}, fmt.Errorf("invalid filter key-value pair %s: %w", ss[1], err)
			}

			typ := eacl.HeaderFromRequest
			if ss[0] == "obj" {
				typ = eacl.HeaderFromObject
			}

			filters = append(filters, eacl.ConstructFilter(typ, key, op, value))
		case "others", "system", "user": // targets
			role, err := eaclRoleFromString(prefix)
			if err != nil {
				return eacl.Record{}, err
			}

			targets = append(targets, eacl.NewTargetByRole(role))
		case "address": // targets
			var (
				err      error
				accounts []user.ID
			)

			if len(ss) != 2 {
				return eacl.Record{}, fmt.Errorf("invalid address: %s", args[i])
			}

			accounts, err = parseAccountList(ss[1])
			if err != nil {
				return eacl.Record{}, err
			}

			targets = append(targets, eacl.NewTargetByAccounts(accounts))
		default:
			return eacl.Record{}, fmt.Errorf("invalid prefix: %s", ss[0])
		}
	}

	return eacl.ConstructRecord(eacl.ActionUnspecified, eacl.OperationUnspecified, targets, filters...), nil
}

func parseKVWithOp(s string) (string, string, eacl.Match, error) {
	i := strings.Index(s, "=")
	if i < 0 {
		if i = strings.Index(s, "<"); i >= 0 {
			if !validateDecimal(s[i+1:]) {
				return "", "", 0, fmt.Errorf("invalid base-10 integer value %q for attribute %q", s[i+1:], s[:i])
			}
			return s[:i], s[i+1:], eacl.MatchNumLT, nil
		} else if i = strings.Index(s, ">"); i >= 0 {
			if !validateDecimal(s[i+1:]) {
				return "", "", 0, fmt.Errorf("invalid base-10 integer value %q for attribute %q", s[i+1:], s[:i])
			}
			return s[:i], s[i+1:], eacl.MatchNumGT, nil
		}

		return "", "", 0, errors.New("missing op")
	}

	if len(s[i+1:]) == 0 {
		return s[:i], "", eacl.MatchNotPresent, nil
	}

	value := s[i+1:]

	if i == 0 {
		return "", value, eacl.MatchStringEqual, nil
	}

	switch s[i-1] {
	case '!':
		return s[:i-1], value, eacl.MatchStringNotEqual, nil
	case '<':
		if !validateDecimal(value) {
			return "", "", 0, fmt.Errorf("invalid base-10 integer value %q for attribute %q", value, s[:i-1])
		}
		return s[:i-1], value, eacl.MatchNumLE, nil
	case '>':
		if !validateDecimal(value) {
			return "", "", 0, fmt.Errorf("invalid base-10 integer value %q for attribute %q", value, s[:i-1])
		}
		return s[:i-1], value, eacl.MatchNumGE, nil
	default:
		return s[:i], value, eacl.MatchStringEqual, nil
	}
}

func validateDecimal(s string) bool {
	_, ok := new(big.Int).SetString(s, 10)
	return ok
}

// eaclRoleFromString parses eacl.Role from string.
func eaclRoleFromString(s string) (eacl.Role, error) {
	var r eacl.Role
	if !r.DecodeString(strings.ToUpper(s)) {
		return r, fmt.Errorf("unexpected role %s", s)
	}

	return r, nil
}

func parseAccountList(s string) ([]user.ID, error) {
	parts := strings.Split(s, ",")
	accounts := make([]user.ID, len(parts))

	for i := range parts {
		st := strings.TrimSpace(parts[i])
		acc, err := user.DecodeString(st)
		if err != nil {
			return nil, fmt.Errorf("invalid account %q: %w", parts[i], err)
		}

		accounts[i] = acc
	}

	return accounts, nil
}

// eaclOperationsFromString parses list of eacl.Operation separated by comma.
func eaclOperationsFromString(s string) ([]eacl.Operation, error) {
	ss := strings.Split(s, ",")
	ops := make([]eacl.Operation, len(ss))

	for i := range ss {
		if !ops[i].DecodeString(strings.ToUpper(ss[i])) {
			return nil, fmt.Errorf("invalid operation: %s", ss[i])
		}
	}

	return ops, nil
}

// ValidateEACLTable validates eACL table:
//   - eACL table must not modify [eacl.RoleSystem] access.
func ValidateEACLTable(t eacl.Table) error {
	var b big.Int
	for _, record := range t.Records() {
		for _, target := range record.Targets() {
			if target.Role() == eacl.RoleSystem {
				return errors.New("it is prohibited to modify system access")
			}
		}
		for _, f := range record.Filters() {
			//nolint:exhaustive
			switch f.Matcher() {
			case eacl.MatchNotPresent:
				if len(f.Value()) != 0 {
					return errors.New("non-empty value in absence filter")
				}
			case eacl.MatchNumGT, eacl.MatchNumGE, eacl.MatchNumLT, eacl.MatchNumLE:
				_, ok := b.SetString(f.Value(), 10)
				if !ok {
					return errors.New("numeric filter with non-decimal value")
				}
			}
		}
	}

	return nil
}
