package control

import (
	"errors"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/spf13/cobra"
)

var listObjectsCmd = &cobra.Command{
	Use:   "list",
	Short: "Get list of all objects in the storage node",
	Long:  "Get list of all objects in the storage node",
	Args:  cobra.NoArgs,
	RunE:  listObjects,
}

func initControlObjectsListCmd() {
	initControlFlags(listObjectsCmd)
}

func listObjects(cmd *cobra.Command, _ []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk, err := key.Get(cmd)
	if err != nil {
		return err
	}

	req := &control.ListObjectsRequest{
		Body: &control.ListObjectsRequest_Body{},
	}
	err = signRequest(pk, req)
	if err != nil {
		return err
	}

	cli, err := getClient(ctx)
	if err != nil {
		return err
	}

	stream, err := cli.ListObjects(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("rpc error: %w", err)
		}

		body := resp.GetBody()
		if err := verifyResponse(resp.GetSignature(), body); err != nil {
			return err
		}

		for _, address := range body.GetObjectAddress() {
			cmd.Println(string(address))
		}
	}
}
