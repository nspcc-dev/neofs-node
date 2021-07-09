package morph

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func forceNewEpochCmd(cmd *cobra.Command, args []string) error {
	cmd.Println("endpoint:", viper.GetString(endpointFlag))
	cmd.Println("alphabet-wallets:", viper.GetString(alphabetWalletsFlag))

	return nil
}
