package common

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
)

// ParseEpoch parses epoch argument. Second return value is true if
// the specified epoch is relative, and false otherwise.
func ParseEpoch(cmd *cobra.Command, flag string) (uint64, bool, error) {
	s, _ := cmd.Flags().GetString(flag)
	if len(s) == 0 {
		return 0, false, nil
	}

	relative := s[0] == '+'
	if relative {
		s = s[1:]
	}

	epoch, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, relative, fmt.Errorf("can't parse epoch for %s argument: %w", flag, err)
	}
	return epoch, relative, nil
}
