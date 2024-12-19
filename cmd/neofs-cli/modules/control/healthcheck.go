package control

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	ircontrol "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	ircontrolsrv "github.com/nspcc-dev/neofs-node/pkg/services/control/ir/server"
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

	conn, err := connect(ctx)
	if err != nil {
		return err
	}

	if isIR, _ := cmd.Flags().GetBool(healthcheckIRFlag); isIR {
		return healthCheckIR(ctx, cmd, pk, ircontrol.NewControlServiceClient(conn))
	}

	req := new(control.HealthCheckRequest)
	req.SetBody(new(control.HealthCheckRequest_Body))

	err = signRequest(pk, req)
	if err != nil {
		return err
	}

	resp, err := control.NewControlServiceClient(conn).HealthCheck(ctx, req)
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

func healthCheckIR(ctx context.Context, cmd *cobra.Command, key *ecdsa.PrivateKey, c ircontrol.ControlServiceClient) error {
	req := new(ircontrol.HealthCheckRequest)

	req.SetBody(new(ircontrol.HealthCheckRequest_Body))

	err := ircontrolsrv.SignMessage(key, req)
	if err != nil {
		return fmt.Errorf("could not sign request: %w", err)
	}

	resp, err := c.HealthCheck(ctx, req)
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
