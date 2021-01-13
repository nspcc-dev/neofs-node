package cmd

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/util/signature"
	"github.com/nspcc-dev/neofs-api-go/v2/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/private"
	privateSvc "github.com/nspcc-dev/neofs-node/pkg/services/private/server"
	"github.com/spf13/cobra"
)

var privateCmd = &cobra.Command{
	Use:   "private",
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
	rootCmd.AddCommand(privateCmd)

	privateCmd.AddCommand(healthCheckCmd)
}

func healthCheck(cmd *cobra.Command, _ []string) error {
	key, err := getKey()
	if err != nil {
		return err
	}

	req := new(private.HealthCheckRequest)

	req.SetBody(new(private.HealthCheckRequest_Body))

	if err := privateSvc.SignMessage(key, req); err != nil {
		return err
	}

	netAddr, err := getEndpointAddress()
	if err != nil {
		return err
	}

	ipAddr, err := netAddr.IPAddrString()
	if err != nil {
		return errInvalidEndpoint
	}

	con, err := client.NewGRPCClientConn(
		client.WithNetworkAddress(ipAddr),
	)
	if err != nil {
		return err
	}

	cli := private.NewPrivateServiceClient(con)

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
