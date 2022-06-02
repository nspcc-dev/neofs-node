package container

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/spf13/cobra"
)

const (
	attributeDelimiter = "="

	awaitTimeout = 120 // in seconds
)

var (
	errCreateTimeout  = errors.New("timeout: container has not been persisted on sidechain")
	errDeleteTimeout  = errors.New("timeout: container has not been removed from sidechain")
	errSetEACLTimeout = errors.New("timeout: EACL has not been persisted on sidechain")
)

func parseContainerID(cmd *cobra.Command) cid.ID {
	if containerID == "" {
		common.ExitOnErr(cmd, "", errors.New("container ID is not set"))
	}

	var id cid.ID
	err := id.DecodeString(containerID)
	common.ExitOnErr(cmd, "can't decode container ID value: %w", err)
	return id
}
