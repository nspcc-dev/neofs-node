package control

import (
	"crypto/ecdsa"
	"fmt"
	"os"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	ircontrol "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	ircontrolsrv "github.com/nspcc-dev/neofs-node/pkg/services/control/ir/server"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/spf13/cobra"
)

const (
	healthcheckIRFlag = "ir"
)

var healthCheckCmd = &cobra.Command{
	Use:   "healthcheck",
	Short: "Health check of the NeoFS node",
	Long:  "Health check of the NeoFS node. Checks storage node by default, use --ir flag to work with Inner Ring.",
	Args:  cobra.NoArgs,
	RunE:  healthCheck,
}

func initControlHealthCheckCmd() {
	initControlFlags(healthCheckCmd)

	flags := healthCheckCmd.Flags()
	flags.Bool(healthcheckIRFlag, false, "Communicate with IR node")
}

func healthCheck(cmd *cobra.Command, _ []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk, err := key.Get(cmd)
	if err != nil {
		return err
	}

	cli, err := getClient(ctx)
	if err != nil {
		return err
	}

	if isIR, _ := cmd.Flags().GetBool(healthcheckIRFlag); isIR {
		return healthCheckIR(cmd, pk, cli)
	}

	req := new(control.HealthCheckRequest)
	req.SetBody(new(control.HealthCheckRequest_Body))

	err = signRequest(pk, req)
	if err != nil {
		return err
	}

	var resp *control.HealthCheckResponse
	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = control.HealthCheck(client, req)
		return err
	})
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	err = verifyResponse(resp.GetSignature(), resp.GetBody())
	if err != nil {
		return err
	}

	healthStatus := resp.GetBody().GetHealthStatus()

	cmd.Printf("Network status: %s\n", resp.GetBody().GetNetmapStatus())
	cmd.Printf("Health status: %s\n", healthStatus)

	if healthStatus != control.HealthStatus_READY {
		os.Exit(1)
	}
	return nil
}

func healthCheckIR(cmd *cobra.Command, key *ecdsa.PrivateKey, c *client.Client) error {
	req := new(ircontrol.HealthCheckRequest)

	req.SetBody(new(ircontrol.HealthCheckRequest_Body))

	err := ircontrolsrv.SignMessage(key, req)
	if err != nil {
		return fmt.Errorf("could not sign request: %w", err)
	}

	var resp *ircontrol.HealthCheckResponse
	err = c.ExecRaw(func(client *rawclient.Client) error {
		resp, err = ircontrol.HealthCheck(client, req)
		return err
	})
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	err = verifyResponse(resp.GetSignature(), resp.GetBody())
	if err != nil {
		return err
	}

	healthStatus := resp.GetBody().GetHealthStatus()

	cmd.Printf("Health status: %s\n", healthStatus)

	if healthStatus != ircontrol.HealthStatus_READY {
		os.Exit(1)
	}
	return nil
}
