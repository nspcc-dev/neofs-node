package invoke

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/audit"
	auditWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/audit/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	balanceWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/balance/wrapper"
	morphContainer "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	wrapContainer "github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	morphNetmap "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	wrapNetmap "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation"
	reputationWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation/wrapper"
	"github.com/pkg/errors"
)

// NewContainerClient creates wrapper to access data from container contract.
func NewContainerClient(cli *client.Client, contract util.Uint160, fee SideFeeProvider) (*wrapContainer.Wrapper, error) {
	staticClient, err := client.NewStatic(cli, contract, fee.SideChainFee())
	if err != nil {
		return nil, fmt.Errorf("can't create container static client: %w", err)
	}

	enhancedContainerClient, err := morphContainer.New(staticClient)
	if err != nil {
		return nil, fmt.Errorf("can't create container morph client: %w", err)
	}

	return wrapContainer.New(enhancedContainerClient)
}

// NewNetmapClient creates wrapper to access data from netmap contract.
func NewNetmapClient(cli *client.Client, contract util.Uint160, fee SideFeeProvider) (*wrapNetmap.Wrapper, error) {
	staticClient, err := client.NewStatic(cli, contract, fee.SideChainFee())
	if err != nil {
		return nil, fmt.Errorf("can't create netmap static client: %w", err)
	}

	enhancedNetmapClient, err := morphNetmap.New(staticClient)
	if err != nil {
		return nil, fmt.Errorf("can't create netmap morph client: %w", err)
	}

	return wrapNetmap.New(enhancedNetmapClient)
}

// NewAuditClient creates wrapper to work with Audit contract.
func NewAuditClient(cli *client.Client, contract util.Uint160, fee SideFeeProvider) (*auditWrapper.ClientWrapper, error) {
	staticClient, err := client.NewStatic(cli, contract, fee.SideChainFee())
	if err != nil {
		return nil, err
	}

	return auditWrapper.WrapClient(audit.New(staticClient)), nil
}

// NewBalanceClient creates wrapper to work with Balance contract.
func NewBalanceClient(cli *client.Client, contract util.Uint160, fee SideFeeProvider) (*balanceWrapper.Wrapper, error) {
	staticClient, err := client.NewStatic(cli, contract, fee.SideChainFee())
	if err != nil {
		return nil, errors.Wrap(err, "could not create static client of Balance contract")
	}

	enhancedBalanceClient, err := balance.New(staticClient)
	if err != nil {
		return nil, errors.Wrap(err, "could not create Balance contract client")
	}

	return balanceWrapper.New(enhancedBalanceClient)
}

// NewReputationClient creates wrapper to work with reputation contract.
func NewReputationClient(cli *client.Client, contract util.Uint160, fee SideFeeProvider) (*reputationWrapper.ClientWrapper, error) {
	staticClient, err := client.NewStatic(cli, contract, fee.SideChainFee())
	if err != nil {
		return nil, errors.Wrap(err, "could not create static client of reputation contract")
	}

	enhancedRepurationClient, err := reputation.New(staticClient)
	if err != nil {
		return nil, errors.Wrap(err, "could not create reputation contract client")
	}

	return reputationWrapper.WrapClient(enhancedRepurationClient), nil
}
