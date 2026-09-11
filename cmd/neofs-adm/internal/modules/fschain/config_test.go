package fschain

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestSetConfigCompletion(t *testing.T) {
	completions, directive := setConfig.ValidArgsFunction(setConfig, nil, "")

	require.Equal(t, []string{
		"BasicIncomeRate=",
		"ContainerFee=",
		"ContainerAliasFee=",
		"EigenTrustIterations=",
		"EpochDuration=",
		"MaxObjectSize=",
		"WithdrawFee=",
		"EigenTrustAlpha=",
	}, completions)
	require.Equal(t, cobra.ShellCompDirectiveNoSpace, directive)
}
