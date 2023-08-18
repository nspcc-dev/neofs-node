package container

import (
	"context"
	"errors"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/spf13/cobra"
)

const (
	attributeDelimiter = "="

	awaitTimeout = time.Minute
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

// decodes session.Container from the file by path provided in
// commonflags.SessionToken flag. Returns nil if the path is not specified.
func getSession(cmd *cobra.Command) *session.Container {
	common.PrintVerbose(cmd, "Reading container session...")

	path, _ := cmd.Flags().GetString(commonflags.SessionToken)
	if path == "" {
		common.PrintVerbose(cmd, "Session not provided.")
		return nil
	}

	common.PrintVerbose(cmd, "Reading container session from the file [%s]...", path)

	var res session.Container

	err := common.ReadBinaryOrJSON(cmd, &res, path)
	common.ExitOnErr(cmd, "read container session: %v", err)

	common.PrintVerbose(cmd, "Session successfully read.")

	return &res
}

func getAwaitContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	return commonflags.GetCommandContextWithAwait(cmd, "await", awaitTimeout)
}
