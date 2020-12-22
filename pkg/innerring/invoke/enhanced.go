package invoke

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/audit"
	auditWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/audit/wrapper"
	morphContainer "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	wrapContainer "github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	morphNetmap "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	wrapNetmap "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
)

const readOnlyFee = 0

// NewNoFeeContainerClient creates wrapper to access data from container contract.
func NewNoFeeContainerClient(cli *client.Client, contract util.Uint160) (*wrapContainer.Wrapper, error) {
	staticClient, err := client.NewStatic(cli, contract, readOnlyFee)
	if err != nil {
		return nil, fmt.Errorf("can't create container static client: %w", err)
	}

	enhancedContainerClient, err := morphContainer.New(staticClient)
	if err != nil {
		return nil, fmt.Errorf("can't create container morph client: %w", err)
	}

	return wrapContainer.New(enhancedContainerClient)
}

// NewNoFeeNetmapClient creates wrapper to access data from netmap contract.
func NewNoFeeNetmapClient(cli *client.Client, contract util.Uint160) (*wrapNetmap.Wrapper, error) {
	staticClient, err := client.NewStatic(cli, contract, readOnlyFee)
	if err != nil {
		return nil, fmt.Errorf("can't create netmap static client: %w", err)
	}

	enhancedNetmapClient, err := morphNetmap.New(staticClient)
	if err != nil {
		return nil, fmt.Errorf("can't create netmap morph client: %w", err)
	}

	return wrapNetmap.New(enhancedNetmapClient)
}

// NewNoFeeAuditClient creates wrapper to work with Audit contract.
func NewNoFeeAuditClient(cli *client.Client, contract util.Uint160) (*auditWrapper.ClientWrapper, error) {
	staticClient, err := client.NewStatic(cli, contract, readOnlyFee)
	if err != nil {
		return nil, err
	}

	return auditWrapper.WrapClient(audit.New(staticClient)), nil
}
