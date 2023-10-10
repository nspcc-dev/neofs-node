package common

import (
	"errors"
	"os"

	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/spf13/cobra"
)

var errUnsupportedEACLFormat = errors.New("unsupported eACL format")

// ReadEACL reads extended ACL table from eaclPath.
func ReadEACL(cmd *cobra.Command, eaclPath string) eacl.Table {
	_, err := os.Stat(eaclPath) // check if `eaclPath` is an existing file
	if err != nil {
		ExitOnErr(cmd, "", errors.New("incorrect path to file with EACL"))
	}

	PrintVerbose(cmd, "Reading EACL from file: %s", eaclPath)

	data, err := os.ReadFile(eaclPath)
	ExitOnErr(cmd, "can't read file with EACL: %w", err)

	var table eacl.Table

	if err = table.UnmarshalJSON(data); err == nil {
		PrintVerbose(cmd, "Parsed JSON encoded EACL table")
		return table
	}

	if err = table.Unmarshal(data); err == nil {
		PrintVerbose(cmd, "Parsed binary encoded EACL table")
		return table
	}

	ExitOnErr(cmd, "", errUnsupportedEACLFormat)
	return table
}
