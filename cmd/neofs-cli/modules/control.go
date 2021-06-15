package cmd

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/util/signature"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	ircontrol "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	ircontrolsrv "github.com/nspcc-dev/neofs-node/pkg/services/control/ir/server"
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

// control healthcheck flags
const (
	healthcheckIRFlag = "ir"
)

// control healthcheck vars
var (
	healthCheckIRVar bool
)

var netmapStatus string

func init() {
	rootCmd.AddCommand(controlCmd)

	controlCmd.AddCommand(
		healthCheckCmd,
		setNetmapStatusCmd,
		dropObjectsCmd,
	)

	setNetmapStatusCmd.Flags().StringVarP(&netmapStatus, netmapStatusFlag, "", "",
		fmt.Sprintf("new netmap status keyword ('%s', '%s')",
			netmapStatusOnline,
			netmapStatusOffline,
		),
	)

	_ = setNetmapStatusCmd.MarkFlagRequired(netmapStatusFlag)

	dropObjectsCmd.Flags().StringSliceVarP(&dropObjectsList, dropObjectsFlag, "o", nil,
		"List of object addresses to be removed in string format")

	_ = dropObjectsCmd.MarkFlagRequired(dropObjectsFlag)

	healthCheckCmd.Flags().BoolVar(&healthCheckIRVar, healthcheckIRFlag, false, "Communicate with IR node")
}

func healthCheck(cmd *cobra.Command, _ []string) error {
	key, err := getKey()
	if err != nil {
		return err
	}

	cli, err := getSDKClient(key)
	if err != nil {
		return err
	}

	if healthCheckIRVar {
		healthCheckIR(cmd, key, cli)
		return nil
	}

	req := new(control.HealthCheckRequest)

	req.SetBody(new(control.HealthCheckRequest_Body))

	if err := controlSvc.SignMessage(key, req); err != nil {
		return err
	}

	resp, err := control.HealthCheck(cli.Raw(), req)
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

func healthCheckIR(cmd *cobra.Command, key *ecdsa.PrivateKey, c client.Client) {
	req := new(ircontrol.HealthCheckRequest)

	req.SetBody(new(ircontrol.HealthCheckRequest_Body))

	if err := ircontrolsrv.SignMessage(key, req); err != nil {
		cmd.PrintErrln(fmt.Errorf("could not sign request: %w", err))
		return
	}

	resp, err := ircontrol.HealthCheck(c.Raw(), req)
	if err != nil {
		cmd.PrintErrln(fmt.Errorf("rpc failure: %w", err))
		return
	}

	sign := resp.GetSignature()

	if err := signature.VerifyDataWithSource(resp, func() ([]byte, []byte) {
		return sign.GetKey(), sign.GetSign()
	}); err != nil {
		cmd.PrintErrln(fmt.Errorf("invalid response signature: %w", err))
		return
	}

	cmd.Printf("Health status: %s\n", resp.GetBody().GetHealthStatus())
}

func setNetmapStatus(cmd *cobra.Command, _ []string) error {
	key, err := getKey()
	if err != nil {
		return err
	}

	var status control.NetmapStatus

	switch netmapStatus {
	default:
		return fmt.Errorf("unsupported status %s", netmapStatus)
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

	cli, err := getSDKClient(key)
	if err != nil {
		return err
	}

	resp, err := control.SetNetmapStatus(cli.Raw(), req)
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

const dropObjectsFlag = "objects"

var dropObjectsList []string

var dropObjectsCmd = &cobra.Command{
	Use:   "drop-objects",
	Short: "Drop objects from the node's local storage",
	Long:  "Drop objects from the node's local storage",
	RunE: func(cmd *cobra.Command, args []string) error {
		key, err := getKey()
		if err != nil {
			return err
		}

		binAddrList := make([][]byte, 0, len(dropObjectsList))

		for i := range dropObjectsList {
			a := object.NewAddress()

			err := a.Parse(dropObjectsList[i])
			if err != nil {
				return fmt.Errorf("could not parse address #%d: %w", i, err)
			}

			binAddr, err := a.Marshal()
			if err != nil {
				return fmt.Errorf("could not marshal the address: %w", err)
			}

			binAddrList = append(binAddrList, binAddr)
		}

		req := new(control.DropObjectsRequest)

		body := new(control.DropObjectsRequest_Body)
		req.SetBody(body)

		body.SetAddressList(binAddrList)

		if err := controlSvc.SignMessage(key, req); err != nil {
			return err
		}

		cli, err := getSDKClient(key)
		if err != nil {
			return err
		}

		resp, err := control.DropObjects(cli.Raw(), req)
		if err != nil {
			return err
		}

		sign := resp.GetSignature()

		if err := signature.VerifyDataWithSource(resp, func() ([]byte, []byte) {
			return sign.GetKey(), sign.GetSign()
		}); err != nil {
			return err
		}

		cmd.Println("Objects were successfully marked to be removed.")

		return nil
	},
}
