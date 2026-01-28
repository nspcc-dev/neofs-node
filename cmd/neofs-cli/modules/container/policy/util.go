package policy

import (
	"errors"
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/spf13/cobra"
)

// ParseContainerPolicy tries to parse the provided string as a path to file with placement policy,
// then as QL and JSON encoded policies. Returns an error if all attempts fail.
func ParseContainerPolicy(cmd *cobra.Command, policyString string) (*netmap.PlacementPolicy, error) {
	_, err := os.Stat(policyString) // check if `policyString` is a path to file with placement policy
	if err == nil {
		common.PrintVerbose(cmd, "Reading placement policy from file: %s", policyString)

		data, err := os.ReadFile(policyString)
		if err != nil {
			return nil, fmt.Errorf("can't read file with placement policy: %w", err)
		}

		policyString = string(data)
	}

	var result netmap.PlacementPolicy

	err = result.DecodeString(policyString)
	if err == nil {
		common.PrintVerbose(cmd, "Parsed QL encoded policy")
		return &result, nil
	}
	common.PrintVerbose(cmd, "Can't parse policy as QL: %v", err)

	if err = result.UnmarshalJSON([]byte(policyString)); err == nil {
		common.PrintVerbose(cmd, "Parsed JSON encoded policy")
		return &result, nil
	}
	common.PrintVerbose(cmd, "Can't parse policy as JSON: %v", err)

	return nil, errors.New("can't parse placement policy")
}
