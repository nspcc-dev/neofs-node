package cmd

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/util/signature"
	"github.com/nspcc-dev/neofs-api-go/v2/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	"github.com/spf13/cobra"
)

var controlCmd = &cobra.Command{
	Use:   "control",
	Short: "Operations with storage node",
	Long:  `Operations with storage node`,
}

var healthCheckCmd = &cobra.Command{
	Use:   "healthcheck",
	Short: "Health check of the storage node",
	Long:  "Health check of the storage node",
	RunE:  healthCheck,
}

func init() {
	rootCmd.AddCommand(controlCmd)

	controlCmd.AddCommand(healthCheckCmd)

	controlCmd.AddCommand(snapshotCmd)
}

func getControlServiceClient() (control.ControlServiceClient, error) {
	netAddr, err := getEndpointAddress()
	if err != nil {
		return nil, err
	}

	ipAddr, err := netAddr.IPAddrString()
	if err != nil {
		return nil, errInvalidEndpoint
	}

	con, err := client.NewGRPCClientConn(
		client.WithNetworkAddress(ipAddr),
	)
	if err != nil {
		return nil, err
	}

	return control.NewControlServiceClient(con), nil
}

func healthCheck(cmd *cobra.Command, _ []string) error {
	key, err := getKey()
	if err != nil {
		return err
	}

	req := new(control.HealthCheckRequest)

	req.SetBody(new(control.HealthCheckRequest_Body))

	if err := controlSvc.SignMessage(key, req); err != nil {
		return err
	}

	cli, err := getControlServiceClient()
	if err != nil {
		return err
	}

	resp, err := cli.HealthCheck(context.Background(), req)
	if err != nil {
		return err
	}

	sign := resp.GetSignature()

	if err := signature.VerifyDataWithSource(resp, func() ([]byte, []byte) {
		return sign.GetKey(), sign.GetSign()
	}); err != nil {
		return err
	}

	cmd.Printf("Node status: %s\n", resp.GetBody().GetStatus())

	return nil
}
