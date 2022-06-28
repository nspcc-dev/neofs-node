package morph

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	alphabetWalletsFlag       = "alphabet-wallets"
	alphabetSizeFlag          = "size"
	endpointFlag              = "rpc-endpoint"
	storageWalletFlag         = "storage-wallet"
	storageWalletLabelFlag    = "label"
	storageGasCLIFlag         = "initial-gas"
	storageGasConfigFlag      = "storage.initial_gas"
	contractsInitFlag         = "contracts"
	maxObjectSizeInitFlag     = "network.max_object_size"
	maxObjectSizeCLIFlag      = "max-object-size"
	epochDurationInitFlag     = "network.epoch_duration"
	epochDurationCLIFlag      = "epoch-duration"
	incomeRateInitFlag        = "network.basic_income_rate"
	incomeRateCLIFlag         = "basic-income-rate"
	auditFeeInitFlag          = "network.fee.audit"
	auditFeeCLIFlag           = "audit-fee"
	containerFeeInitFlag      = "network.fee.container"
	containerAliasFeeInitFlag = "network.fee.container_alias"
	containerFeeCLIFlag       = "container-fee"
	containerAliasFeeCLIFlag  = "container-alias-fee"
	candidateFeeInitFlag      = "network.fee.candidate"
	candidateFeeCLIFlag       = "candidate-fee"
	withdrawFeeInitFlag       = "network.fee.withdraw"
	withdrawFeeCLIFlag        = "withdraw-fee"
	containerDumpFlag         = "dump"
	containerContractFlag     = "container-contract"
	containerIDsFlag          = "cid"
	refillGasAmountFlag       = "gas"
	walletAccountFlag         = "account"
	notaryDepositTillFlag     = "till"
	localDumpFlag             = "local-dump"
	protoConfigPath           = "protocol"
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
			_ = viper.BindPFlag(incomeRateInitFlag, cmd.Flags().Lookup(incomeRateCLIFlag))
			_ = viper.BindPFlag(auditFeeInitFlag, cmd.Flags().Lookup(auditFeeCLIFlag))
			_ = viper.BindPFlag(candidateFeeInitFlag, cmd.Flags().Lookup(candidateFeeCLIFlag))
			_ = viper.BindPFlag(containerFeeInitFlag, cmd.Flags().Lookup(containerFeeCLIFlag))
			_ = viper.BindPFlag(containerAliasFeeInitFlag, cmd.Flags().Lookup(containerAliasFeeCLIFlag))
			_ = viper.BindPFlag(withdrawFeeInitFlag, cmd.Flags().Lookup(withdrawFeeCLIFlag))
			_ = viper.BindPFlag(protoConfigPath, cmd.Flags().Lookup(protoConfigPath))
			_ = viper.BindPFlag(localDumpFlag, cmd.Flags().Lookup(localDumpFlag))
		},
		RunE: initializeSideChainCmd,
	}

	generateStorageCmd = &cobra.Command{
		Use:   "generate-storage-wallet",
		Short: "Generate storage node wallet for the morph network",
		PreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(alphabetWalletsFlag, cmd.Flags().Lookup(alphabetWalletsFlag))
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
			_ = viper.BindPFlag(storageGasConfigFlag, cmd.Flags().Lookup(storageGasCLIFlag))
		},
		RunE: generateStorageCreds,
	}

	refillGasCmd = &cobra.Command{
		Use:   "refill-gas",
		Short: "Refill GAS of storage node's wallet in the morph network",
		PreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(alphabetWalletsFlag, cmd.Flags().Lookup(alphabetWalletsFlag))
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
			_ = viper.BindPFlag(refillGasAmountFlag, cmd.Flags().Lookup(refillGasAmountFlag))
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return refillGas(cmd, refillGasAmountFlag, false)
		},
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

	removeNodes = &cobra.Command{
		Use:   "remove-nodes key1 [key2 [...]]",
		Short: "Remove storage nodes from the netmap",
		Long:  `Move nodes to the Offline state in the candidates list and tick an epoch to update the netmap`,
		PreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(alphabetWalletsFlag, cmd.Flags().Lookup(alphabetWalletsFlag))
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
		},
		RunE: removeNodesCmd,
	}

	setPolicy = &cobra.Command{
		Use:                   "set-policy [ExecFeeFactor=<n1>] [StoragePrice=<n2>] [FeePerByte=<n3>]",
		DisableFlagsInUseLine: true,
		Short:                 "Set global policy values",
		PreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(alphabetWalletsFlag, cmd.Flags().Lookup(alphabetWalletsFlag))
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
		},
		RunE: setPolicyCmd,
	}

	dumpContractHashesCmd = &cobra.Command{
		Use:   "dump-hashes",
		Short: "Dump deployed contract hashes.",
		PreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
		},
		RunE: dumpContractHashes,
	}

	dumpNetworkConfigCmd = &cobra.Command{
		Use:   "dump-config",
		Short: "Dump NeoFS network config.",
		PreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
		},
		RunE: dumpNetworkConfig,
	}

	dumpBalancesCmd = &cobra.Command{
		Use:   "dump-balances",
		Short: "Dump GAS balances",
		PreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
		},
		RunE: dumpBalances,
	}

	updateContractsCmd = &cobra.Command{
		Use:   "update-contracts",
		Short: "Update NeoFS contracts.",
		PreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(alphabetWalletsFlag, cmd.Flags().Lookup(alphabetWalletsFlag))
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
		},
		RunE: updateContracts,
	}

	dumpContainersCmd = &cobra.Command{
		Use:   "dump-containers",
		Short: "Dump NeoFS containers to file.",
		PreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
		},
		RunE: dumpContainers,
	}

	restoreContainersCmd = &cobra.Command{
		Use:   "restore-containers",
		Short: "Restore NeoFS containers from file.",
		PreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(alphabetWalletsFlag, cmd.Flags().Lookup(alphabetWalletsFlag))
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
		},
		RunE: restoreContainers,
	}

	depositNotaryCmd = &cobra.Command{
		Use:   "deposit-notary",
		Short: "Deposit GAS for notary service.",
		PreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
		},
		RunE: depositNotary,
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
	// Defaults are taken from neo-preodolenie.
	initCmd.Flags().Uint64(containerFeeCLIFlag, 1000, "container registration fee")
	initCmd.Flags().Uint64(containerAliasFeeCLIFlag, 500, "container alias fee")
	initCmd.Flags().String(protoConfigPath, "", "path to the consensus node configuration")
	initCmd.Flags().String(localDumpFlag, "", "path to the blocks dump file")

	RootCmd.AddCommand(generateStorageCmd)
	generateStorageCmd.Flags().String(alphabetWalletsFlag, "", "path to alphabet wallets dir")
	generateStorageCmd.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
	generateStorageCmd.Flags().String(storageWalletFlag, "", "path to new storage node wallet")
	generateStorageCmd.Flags().String(storageGasCLIFlag, "", "initial amount of GAS to transfer")
	generateStorageCmd.Flags().StringP(storageWalletLabelFlag, "l", "", "wallet label")

	RootCmd.AddCommand(forceNewEpoch)
	forceNewEpoch.Flags().String(alphabetWalletsFlag, "", "path to alphabet wallets dir")
	forceNewEpoch.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")

	RootCmd.AddCommand(removeNodes)
	removeNodes.Flags().String(alphabetWalletsFlag, "", "path to alphabet wallets dir")
	removeNodes.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")

	RootCmd.AddCommand(setPolicy)
	setPolicy.Flags().String(alphabetWalletsFlag, "", "path to alphabet wallets dir")
	setPolicy.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")

	RootCmd.AddCommand(dumpContractHashesCmd)
	dumpContractHashesCmd.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")

	RootCmd.AddCommand(dumpNetworkConfigCmd)
	dumpNetworkConfigCmd.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")

	RootCmd.AddCommand(dumpBalancesCmd)
	dumpBalancesCmd.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
	dumpBalancesCmd.Flags().BoolP(dumpBalancesStorageFlag, "s", false, "dump balances of storage nodes from the current netmap")
	dumpBalancesCmd.Flags().BoolP(dumpBalancesAlphabetFlag, "a", false, "dump balances of alphabet contracts")
	dumpBalancesCmd.Flags().BoolP(dumpBalancesProxyFlag, "p", false, "dump balances of the proxy contract")
	dumpBalancesCmd.Flags().Bool(dumpBalancesUseScriptHashFlag, false, "use script-hash format for addresses")

	RootCmd.AddCommand(updateContractsCmd)
	updateContractsCmd.Flags().String(alphabetWalletsFlag, "", "path to alphabet wallets dir")
	updateContractsCmd.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
	updateContractsCmd.Flags().String(contractsInitFlag, "", "path to archive with compiled NeoFS contracts (default fetched from latest github release)")

	RootCmd.AddCommand(dumpContainersCmd)
	dumpContainersCmd.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
	dumpContainersCmd.Flags().String(containerDumpFlag, "", "file where to save dumped containers")
	dumpContainersCmd.Flags().String(containerContractFlag, "", "container contract hash (for networks without NNS)")
	dumpContainersCmd.Flags().StringSlice(containerIDsFlag, nil, "containers to dump")

	RootCmd.AddCommand(restoreContainersCmd)
	restoreContainersCmd.Flags().String(alphabetWalletsFlag, "", "path to alphabet wallets dir")
	restoreContainersCmd.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
	restoreContainersCmd.Flags().String(containerDumpFlag, "", "file to restore containers from")
	restoreContainersCmd.Flags().StringSlice(containerIDsFlag, nil, "containers to restore")

	RootCmd.AddCommand(refillGasCmd)
	refillGasCmd.Flags().String(alphabetWalletsFlag, "", "path to alphabet wallets dir")
	refillGasCmd.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
	refillGasCmd.Flags().String(storageWalletFlag, "", "path to storage node wallet")
	refillGasCmd.Flags().String(refillGasAmountFlag, "", "additional amount of GAS to transfer")

	RootCmd.AddCommand(cmdSubnet)

	RootCmd.AddCommand(depositNotaryCmd)
	depositNotaryCmd.Flags().StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
	depositNotaryCmd.Flags().String(storageWalletFlag, "", "path to storage node wallet")
	depositNotaryCmd.Flags().String(walletAccountFlag, "", "wallet account address")
	depositNotaryCmd.Flags().String(refillGasAmountFlag, "", "amount of GAS to deposit")
	depositNotaryCmd.Flags().String(notaryDepositTillFlag, "", "notary deposit duration in blocks")
}
