package morph

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func generateAlphabetCreds(cmd *cobra.Command, args []string) error {
	// alphabet size is not part of the config
	size, err := cmd.Flags().GetUint(alphabetSizeFlag)
	if err != nil {
		return err
	}

	pwds := make([]string, size)
	for i := 0; i < int(size); i++ {
		pwds[i], err = config.AlphabetPassword(viper.GetViper(), i)
		if err != nil {
			return err
		}
	}

	cmd.Println("size:", size)
	cmd.Println("alphabet-wallets:", viper.GetString(alphabetWalletsFlag))
	for i := range pwds {
		cmd.Printf("wallet[%d]: %s\n", i, pwds[i])
	}

	return nil
}

func generateStorageCreds(cmd *cobra.Command, args []string) error {
	// storage wallet path is not part of the config
	storageWalletPath, err := cmd.Flags().GetString(storageWalletFlag)
	if err != nil {
		return err
	}

	cmd.Println("endpoint:", viper.GetString(endpointFlag))
	cmd.Println("alphabet-wallets:", viper.GetString(alphabetWalletsFlag))
	cmd.Println("storage-wallet:", storageWalletPath)

	return nil
}
