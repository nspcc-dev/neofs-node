package morph

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func initializeSideChainCmd(cmd *cobra.Command, args []string) error {
	// contract path is not part of the config
	contractsPath, err := cmd.Flags().GetString(contractsInitFlag)
	if err != nil {
		return err
	}

	cmd.Println("endpoint:", viper.GetString(endpointFlag))
	cmd.Println("alphabet-wallets:", viper.GetString(alphabetWalletsFlag))
	cmd.Println("contracts:", contractsPath)
	cmd.Println("epoch-duration:", viper.GetUint(epochDurationInitFlag))
	cmd.Println("max-object-size:", viper.GetUint(maxObjectSizeInitFlag))

	return nil
}
