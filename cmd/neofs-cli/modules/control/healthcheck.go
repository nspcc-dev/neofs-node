package control

import (
	"crypto/ecdsa"
	"os"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
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
	Run:   healthCheck,
}

func initControlHealthCheckCmd() {
	initControlFlags(healthCheckCmd)

	flags := healthCheckCmd.Flags()
	flags.Bool(healthcheckIRFlag, false, "Communicate with IR node")
}

func healthCheck(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.Get(cmd)

	cli := getClient(ctx, cmd)

	if isIR, _ := cmd.Flags().GetBool(healthcheckIRFlag); isIR {
		healthCheckIR(cmd, pk, cli)
		return
	}

	req := new(control.HealthCheckRequest)
	req.SetBody(new(control.HealthCheckRequest_Body))

	signRequest(cmd, pk, req)

	var resp *control.HealthCheckResponse
	var err error
	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = control.HealthCheck(client, req)
		return err
	})
	common.ExitOnErr(cmd, "rpc error: %w", err)

	verifyResponse(cmd, resp.GetSignature(), resp.GetBody())

	healthStatus := resp.GetBody().GetHealthStatus()

	cmd.Printf("Network status: %s\n", resp.GetBody().GetNetmapStatus())
	cmd.Printf("Health status: %s\n", healthStatus)

	if healthStatus != control.HealthStatus_READY {
		os.Exit(1)
	}
}

func healthCheckIR(cmd *cobra.Command, key *ecdsa.PrivateKey, c *client.Client) {
	req := new(ircontrol.HealthCheckRequest)

	req.SetBody(new(ircontrol.HealthCheckRequest_Body))

	err := ircontrolsrv.SignMessage(key, req)
	common.ExitOnErr(cmd, "could not sign request: %w", err)

	var resp *ircontrol.HealthCheckResponse
	err = c.ExecRaw(func(client *rawclient.Client) error {
		resp, err = ircontrol.HealthCheck(client, req)
		return err
	})
	common.ExitOnErr(cmd, "rpc error: %w", err)

	verifyResponse(cmd, resp.GetSignature(), resp.GetBody())

	healthStatus := resp.GetBody().GetHealthStatus()

	cmd.Printf("Health status: %s\n", healthStatus)

	if healthStatus != ircontrol.HealthStatus_READY {
		os.Exit(1)
	}
}
