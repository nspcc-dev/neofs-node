package container

import (
	"context"
	"errors"
	"fmt"
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

func parseContainerID() (cid.ID, error) {
	if containerID == "" {
		return cid.ID{}, errors.New("container ID is not set")
	}

	var id cid.ID
	err := id.DecodeString(containerID)
	if err != nil {
		return cid.ID{}, fmt.Errorf("can't decode container ID value: %w", err)
	}
	return id, nil
}

// decodes session.Container from the file by path provided in
// commonflags.SessionToken flag. Returns nil if the path is not specified.
func getSession(cmd *cobra.Command) (*session.Container, error) {
	common.PrintVerbose(cmd, "Reading container session...")

	path, _ := cmd.Flags().GetString(commonflags.SessionToken)
	if path == "" {
		common.PrintVerbose(cmd, "Session not provided.")
		return nil, nil
	}

	common.PrintVerbose(cmd, "Reading container session from the file [%s]...", path)

	var res session.Container

	err := common.ReadBinaryOrJSON(cmd, &res, path)
	if err != nil {
		return nil, fmt.Errorf("read container session: %w", err)
	}

	common.PrintVerbose(cmd, "Session successfully read.")

	return &res, err
}

func getAwaitContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	return commonflags.GetCommandContextWithAwait(cmd, "await", awaitTimeout)
}
