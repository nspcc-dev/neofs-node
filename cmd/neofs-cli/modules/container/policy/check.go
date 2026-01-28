package policy

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/internal/cmdprinter"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/spf13/cobra"
)

var (
	policyFlag string
	short      bool
)

var checkCmd = &cobra.Command{
	Use:   "check",
	Short: "Check placement policy",
	Long: `Check placement policy parsing and validation.
Policy can be provided as QL-encoded string, JSON-encoded string or path to file with it.
Shows nodes that will be used for container placement based on current network map snapshot.
Note: this command uses a zero container ID for placement; when there are many nodes and
only a subset must be chosen, results may differ from a real container.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		placementPolicy, err := ParseContainerPolicy(cmd, policyFlag)
		if err != nil {
			return err
		}

		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}
		defer cli.Close()

		nm, err := cli.NetMapSnapshot(ctx, client.PrmNetMapSnapshot{})
		if err != nil {
			return fmt.Errorf("unable to get netmap snapshot to validate container placement: %w", err)
		}

		placementNodes, err := nm.ContainerNodes(*placementPolicy, cid.ID{})
		if err != nil {
			return fmt.Errorf("could not build container nodes based on given placement policy: %w", err)
		}

		repRuleNum := placementPolicy.NumberOfReplicas()
		for i := range repRuleNum {
			if placementPolicy.ReplicaNumberByIndex(i) > uint32(len(placementNodes[i])) {
				return fmt.Errorf(
					"the number of nodes '%d' in selector is not enough for the number of replicas '%d'",
					len(placementNodes[i]),
					placementPolicy.ReplicaNumberByIndex(i),
				)
			}
		}

		ecRules := placementPolicy.ECRules()
		for i := range ecRules {
			d := ecRules[i].DataPartNum()
			p := ecRules[i].ParityPartNum()
			n := uint32(len(placementNodes[repRuleNum+i]))
			if d > n || p > n || d+p > n {
				return fmt.Errorf(
					"the number of nodes '%d' in selector is not enough for EC rule '%d/%d'", n, d, p)
			}
		}

		cmdprinter.PrettyPrintPlacementPolicyNodes(cmd, placementNodes, *placementPolicy, short)
		return nil
	},
}

func initCheckCmd() {
	flags := checkCmd.Flags()

	flags.StringP(commonflags.RPC, commonflags.RPCShorthand, commonflags.RPCDefault, commonflags.RPCUsage)
	flags.DurationP(commonflags.Timeout, commonflags.TimeoutShorthand, commonflags.TimeoutDefault, commonflags.TimeoutUsage)
	flags.StringVarP(&policyFlag, "policy", "p", "", "QL-encoded or JSON-encoded placement policy or path to file with it")
	flags.BoolVar(&short, "short", false, "Shortens output of node info")

	_ = checkCmd.MarkFlagRequired(commonflags.RPC)
	_ = checkCmd.MarkFlagRequired("policy")
}
