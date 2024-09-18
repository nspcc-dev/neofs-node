package common

import (
	"errors"
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-node/pkg/core/version"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	versionSDK "github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/spf13/cobra"
)

var errUnsupportedEACLFormat = fmt.Errorf("%w: unsupported eACL format", errors.ErrUnsupported)

// ReadEACL reads extended ACL table from eaclPath.
func ReadEACL(cmd *cobra.Command, eaclPath string) (eacl.Table, error) {
	_, err := os.Stat(eaclPath) // check if `eaclPath` is an existing file
	if err != nil {
		return eacl.Table{}, errors.New("incorrect path to file with EACL")
	}

	PrintVerbose(cmd, "Reading EACL from file: %s", eaclPath)

	data, err := os.ReadFile(eaclPath)
	if err != nil {
		return eacl.Table{}, fmt.Errorf("can't read file with EACL: %w", err)
	}

	table, err := eacl.UnmarshalJSON(data)
	if err == nil {
		validateAndFixEACLVersion(table)
		PrintVerbose(cmd, "Parsed JSON encoded EACL table")
		return table, nil
	}

	table, err = eacl.Unmarshal(data)
	if err == nil {
		validateAndFixEACLVersion(table)
		PrintVerbose(cmd, "Parsed binary encoded EACL table")
		return table, nil
	}

	return eacl.Table{}, errUnsupportedEACLFormat
}

func validateAndFixEACLVersion(table eacl.Table) {
	if !version.IsValid(table.Version()) {
		table.SetVersion(versionSDK.Current())
	}
}
