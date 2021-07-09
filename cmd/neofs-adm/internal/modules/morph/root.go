package morph

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	alphabetWalletsFlag   = "alphabet-wallets"
	alphabetSizeFlag      = "size"
	endpointFlag          = "rpc-endpoint"
	storageWalletFlag     = "storage-wallet"
	contractsInitFlag     = "contracts"
	maxObjectSizeInitFlag = "network.max_object_size"
	maxObjectSizeCLIFlag  = "max-object-size"
	epochDurationInitFlag = "network.epoch_duration"
	epochDurationCLIFlag  = "epoch-duration"
)

var (
	// RootCmd is a root command of config section.
	RootCmd = &cobra.Command{
		Use:   "morph",
		Short: "Section for morph network configuration commands.",
	}

	generateAlphabetCmd = &cobra.Command{
		Use:   "generate-alphabet",
		Short: "Generate alphabet wallets for consensus nodes of the morph network",
		PreRun: func(cmd *cobra.Command, _ []string) {
			// PreRun fixes https://github.com/spf13/viper/issues/233
			_ = viper.BindPFlag(alphabetWalletsFlag, cmd.Flags().Lookup(alphabetWalletsFlag))
		},
		RunE: generateAlphabetCreds,
	}

	initCmd = &cobra.Command{
		Use:   "init",
		Short: "Initialize side chain network with smart-contracts and network settings",
		PreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(alphabetWalletsFlag, cmd.Flags().Lookup(alphabetWalletsFlag))
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
			_ = viper.BindPFlag(epochDurationInitFlag, cmd.Flags().Lookup(epochDurationCLIFlag))
			_ = viper.BindPFlag(maxObjectSizeInitFlag, cmd.Flags().Lookup(maxObjectSizeCLIFlag))
		},
		RunE: initializeSideChainCmd,
	}

	generateStorageCmd = &cobra.Command{
		Use:   "generate-storage-wallet",
		Short: "Generate storage node wallet for the morph network",
		PreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(alphabetWalletsFlag, cmd.Flags().Lookup(alphabetWalletsFlag))
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
		},
		RunE: generateStorageCreds,
	}

	forceNewEpoch = &cobra.Command{
		Use:   "force-new-epoch",
		Short: "Create new NeoFS epoch event in the side chain",
		PreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(alphabetWalletsFlag, cmd.Flags().Lookup(alphabetWalletsFlag))
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
		},
		RunE: forceNewEpochCmd,
	}
)

func init() {
	RootCmd.AddCommand(generateAlphabetCmd)
	generateAlphabetCmd.Flags().String(alphabetWalletsFlag, "", "path to alphabet wallets dir")
	generateAlphabetCmd.Flags().Uint(alphabetSizeFlag, 7, "amount of alphabet wallets to generate")

	RootCmd.AddCommand(initCmd)
	initCmd.Flags().String(alphabetWalletsFlag, "", "path to alphabet wallets dir")
	initCmd.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
	initCmd.Flags().String(contractsInitFlag, "", "path to archive with compiled NeoFS contracts (default fetched from latest github release)")
	initCmd.Flags().Uint(epochDurationCLIFlag, 240, "amount of side chain blocks in one NeoFS epoch")
	initCmd.Flags().Uint(maxObjectSizeCLIFlag, 67108864, "max single object size in bytes")

	RootCmd.AddCommand(generateStorageCmd)
	generateStorageCmd.Flags().String(alphabetWalletsFlag, "", "path to alphabet wallets dir")
	generateStorageCmd.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
	generateStorageCmd.Flags().String(storageWalletFlag, "", "path to new storage node wallet")

	RootCmd.AddCommand(forceNewEpoch)
	forceNewEpoch.Flags().String(alphabetWalletsFlag, "", "path to alphabet wallets dir")
	forceNewEpoch.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
}
