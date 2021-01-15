package cmd

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/util/signature"
	"github.com/nspcc-dev/neofs-api-go/v2/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	"github.com/pkg/errors"
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

var setNetmapStatusCmd = &cobra.Command{
	Use:   "set-status",
	Short: "Set status of the storage node in NeoFS network map",
	Long:  "Set status of the storage node in NeoFS network map",
	RunE:  setNetmapStatus,
}

const (
	netmapStatusFlag = "status"

	netmapStatusOnline  = "online"
	netmapStatusOffline = "offline"
)

var netmapStatus string

func init() {
	rootCmd.AddCommand(controlCmd)

	controlCmd.AddCommand(
		healthCheckCmd,
		setNetmapStatusCmd,
	)

	setNetmapStatusCmd.Flags().StringVarP(&netmapStatus, netmapStatusFlag, "", "",
		fmt.Sprintf("new netmap status keyword ('%s', '%s')",
			netmapStatusOnline,
			netmapStatusOffline,
		),
	)

	_ = setNetmapStatusCmd.MarkFlagRequired(netmapStatusFlag)
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

	cmd.Printf("Network status: %s\n", resp.GetBody().GetNetmapStatus())
	cmd.Printf("Health status: %s\n", resp.GetBody().GetHealthStatus())

	return nil
}

func setNetmapStatus(cmd *cobra.Command, _ []string) error {
	key, err := getKey()
	if err != nil {
		return err
	}

	var status control.NetmapStatus

	switch netmapStatus {
	default:
		return errors.Errorf("unsupported status %s", netmapStatus)
	case netmapStatusOnline:
		status = control.NetmapStatus_ONLINE
	case netmapStatusOffline:
		status = control.NetmapStatus_OFFLINE
	}

	req := new(control.SetNetmapStatusRequest)

	body := new(control.SetNetmapStatusRequest_Body)
	req.SetBody(body)

	body.SetStatus(status)

	if err := controlSvc.SignMessage(key, req); err != nil {
		return err
	}

	cli, err := getControlServiceClient()
	if err != nil {
		return err
	}

	resp, err := cli.SetNetmapStatus(context.Background(), req)
	if err != nil {
		return err
	}

	sign := resp.GetSignature()

	if err := signature.VerifyDataWithSource(resp, func() ([]byte, []byte) {
		return sign.GetKey(), sign.GetSign()
	}); err != nil {
		return err
	}

	cmd.Println("Network status update request successfully sent.")

	return nil
}
