package cmd

import (
	"crypto/ecdsa"
	"crypto/tls"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	ircontrol "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	ircontrolsrv "github.com/nspcc-dev/neofs-node/pkg/services/control/ir/server"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/util/signature"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var controlCmd = &cobra.Command{
	Use:   "control",
	Short: "Operations with storage node",
	Long:  `Operations with storage node`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		ff := cmd.Flags()

		_ = viper.BindPFlag(generateKey, ff.Lookup(generateKey))
		_ = viper.BindPFlag(binaryKey, ff.Lookup(binaryKey))
		_ = viper.BindPFlag(walletPath, ff.Lookup(walletPath))
		_ = viper.BindPFlag(wif, ff.Lookup(wif))
		_ = viper.BindPFlag(address, ff.Lookup(address))
		_ = viper.BindPFlag(controlRPC, ff.Lookup(controlRPC))
		_ = viper.BindPFlag(verbose, ff.Lookup(verbose))
	},
}

var healthCheckCmd = &cobra.Command{
	Use:   "healthcheck",
	Short: "Health check of the storage node",
	Long:  "Health check of the storage node",
	Run:   healthCheck,
}

var setNetmapStatusCmd = &cobra.Command{
	Use:   "set-status",
	Short: "Set status of the storage node in NeoFS network map",
	Long:  "Set status of the storage node in NeoFS network map",
	Run:   setNetmapStatus,
}

const (
	netmapStatusFlag = "status"

	netmapStatusOnline  = "online"
	netmapStatusOffline = "offline"
)

const (
	controlRPC        = "endpoint"
	controlRPCDefault = ""
	controlRPCUsage   = "remote node control address (as 'multiaddr' or '<host>:<port>')"
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

func initControlHealthCheckCmd() {
	initCommonFlagsWithoutRPC(healthCheckCmd)

	flags := healthCheckCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.BoolVar(&healthCheckIRVar, healthcheckIRFlag, false, "Communicate with IR node")
}

func initControlSetNetmapStatusCmd() {
	initCommonFlagsWithoutRPC(setNetmapStatusCmd)

	flags := setNetmapStatusCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.StringVarP(&netmapStatus, netmapStatusFlag, "", "",
		fmt.Sprintf("new netmap status keyword ('%s', '%s')",
			netmapStatusOnline,
			netmapStatusOffline,
		),
	)

	_ = setNetmapStatusCmd.MarkFlagRequired(netmapStatusFlag)
}

func initControlDropObjectsCmd() {
	initCommonFlagsWithoutRPC(dropObjectsCmd)

	flags := dropObjectsCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.StringSliceVarP(&dropObjectsList, dropObjectsFlag, "o", nil,
		"List of object addresses to be removed in string format")

	_ = dropObjectsCmd.MarkFlagRequired(dropObjectsFlag)
}

func initControlSnapshotCmd() {
	initCommonFlagsWithoutRPC(snapshotCmd)

	flags := snapshotCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.BoolVar(&netmapSnapshotJSON, "json", false,
		"print netmap structure in JSON format")
}

func init() {
	rootCmd.AddCommand(controlCmd)

	controlCmd.AddCommand(
		healthCheckCmd,
		setNetmapStatusCmd,
		dropObjectsCmd,
		snapshotCmd,
	)

	initControlHealthCheckCmd()
	initControlSetNetmapStatusCmd()
	initControlDropObjectsCmd()
	initControlSnapshotCmd()
}

func healthCheck(cmd *cobra.Command, _ []string) {
	key, err := getKey()
	exitOnErr(cmd, err)

	cli, err := getControlSDKClient(key)
	exitOnErr(cmd, err)

	if healthCheckIRVar {
		healthCheckIR(cmd, key, cli)
		return
	}

	req := new(control.HealthCheckRequest)

	req.SetBody(new(control.HealthCheckRequest_Body))

	err = controlSvc.SignMessage(key, req)
	exitOnErr(cmd, errf("could not sign message: %w", err))

	resp, err := control.HealthCheck(cli.Raw(), req)
	exitOnErr(cmd, errf("rpc error: %w", err))

	sign := resp.GetSignature()

	err = signature.VerifyDataWithSource(
		resp,
		func() ([]byte, []byte) {
			return sign.GetKey(), sign.GetSign()
		},
	)
	exitOnErr(cmd, errf("invalid response signature: %w", err))

	cmd.Printf("Network status: %s\n", resp.GetBody().GetNetmapStatus())
	cmd.Printf("Health status: %s\n", resp.GetBody().GetHealthStatus())
}

func healthCheckIR(cmd *cobra.Command, key *ecdsa.PrivateKey, c client.Client) {
	req := new(ircontrol.HealthCheckRequest)

	req.SetBody(new(ircontrol.HealthCheckRequest_Body))

	err := ircontrolsrv.SignMessage(key, req)
	exitOnErr(cmd, errf("could not sign request: %w", err))

	resp, err := ircontrol.HealthCheck(c.Raw(), req)
	exitOnErr(cmd, errf("rpc error: %w", err))

	sign := resp.GetSignature()

	err = signature.VerifyDataWithSource(
		resp,
		func() ([]byte, []byte) {
			return sign.GetKey(), sign.GetSign()
		},
	)
	exitOnErr(cmd, errf("invalid response signature: %w", err))

	cmd.Printf("Health status: %s\n", resp.GetBody().GetHealthStatus())
}

func setNetmapStatus(cmd *cobra.Command, _ []string) {
	key, err := getKey()
	exitOnErr(cmd, err)

	var status control.NetmapStatus

	switch netmapStatus {
	default:
		exitOnErr(cmd, fmt.Errorf("unsupported status %s", netmapStatus))
	case netmapStatusOnline:
		status = control.NetmapStatus_ONLINE
	case netmapStatusOffline:
		status = control.NetmapStatus_OFFLINE
	}

	req := new(control.SetNetmapStatusRequest)

	body := new(control.SetNetmapStatusRequest_Body)
	req.SetBody(body)

	body.SetStatus(status)

	err = controlSvc.SignMessage(key, req)
	exitOnErr(cmd, errf("could not sign request: %w", err))

	cli, err := getControlSDKClient(key)
	exitOnErr(cmd, err)

	resp, err := control.SetNetmapStatus(cli.Raw(), req)
	exitOnErr(cmd, errf("rpc error: %w", err))

	sign := resp.GetSignature()

	err = signature.VerifyDataWithSource(
		resp,
		func() ([]byte, []byte) {
			return sign.GetKey(), sign.GetSign()
		},
	)
	exitOnErr(cmd, errf("invalid response signature: %w", err))

	cmd.Println("Network status update request successfully sent.")
}

const dropObjectsFlag = "objects"

var dropObjectsList []string

var dropObjectsCmd = &cobra.Command{
	Use:   "drop-objects",
	Short: "Drop objects from the node's local storage",
	Long:  "Drop objects from the node's local storage",
	Run: func(cmd *cobra.Command, args []string) {
		key, err := getKey()
		exitOnErr(cmd, err)

		binAddrList := make([][]byte, 0, len(dropObjectsList))

		for i := range dropObjectsList {
			a := object.NewAddress()

			err := a.Parse(dropObjectsList[i])
			if err != nil {
				exitOnErr(cmd, fmt.Errorf("could not parse address #%d: %w", i, err))
			}

			binAddr, err := a.Marshal()
			exitOnErr(cmd, errf("could not marshal the address: %w", err))

			binAddrList = append(binAddrList, binAddr)
		}

		req := new(control.DropObjectsRequest)

		body := new(control.DropObjectsRequest_Body)
		req.SetBody(body)

		body.SetAddressList(binAddrList)

		err = controlSvc.SignMessage(key, req)
		exitOnErr(cmd, errf("could not sign request: %w", err))

		cli, err := getControlSDKClient(key)
		exitOnErr(cmd, err)

		resp, err := control.DropObjects(cli.Raw(), req)
		exitOnErr(cmd, errf("rpc error: %w", err))

		sign := resp.GetSignature()

		err = signature.VerifyDataWithSource(
			resp,
			func() ([]byte, []byte) {
				return sign.GetKey(), sign.GetSign()
			},
		)
		exitOnErr(cmd, errf("invalid response signature: %w", err))

		cmd.Println("Objects were successfully marked to be removed.")
	},
}

var snapshotCmd = &cobra.Command{
	Use:   "netmap-snapshot",
	Short: "Get network map snapshot",
	Long:  "Get network map snapshot",
	Run: func(cmd *cobra.Command, args []string) {
		key, err := getKey()
		exitOnErr(cmd, err)

		req := new(control.NetmapSnapshotRequest)
		req.SetBody(new(control.NetmapSnapshotRequest_Body))

		err = controlSvc.SignMessage(key, req)
		exitOnErr(cmd, errf("could not sign request: %w", err))

		cli, err := getControlSDKClient(key)
		exitOnErr(cmd, err)

		resp, err := control.NetmapSnapshot(cli.Raw(), req)
		exitOnErr(cmd, errf("rpc error: %w", err))

		sign := resp.GetSignature()

		err = signature.VerifyDataWithSource(
			resp,
			func() ([]byte, []byte) {
				return sign.GetKey(), sign.GetSign()
			},
		)
		exitOnErr(cmd, errf("invalid response signature: %w", err))

		prettyPrintNetmap(cmd, resp.GetBody().GetNetmap(), netmapSnapshotJSON)
	},
}

// getControlSDKClient is the same getSDKClient but with
// another RPC endpoint flag.
func getControlSDKClient(key *ecdsa.PrivateKey) (client.Client, error) {
	netAddr, err := getEndpointAddress(controlRPC)
	if err != nil {
		return nil, err
	}

	options := []client.Option{
		client.WithAddress(netAddr.HostAddr()),
		client.WithDefaultPrivateKey(key),
	}

	if netAddr.TLSEnabled() {
		options = append(options, client.WithTLSConfig(&tls.Config{}))
	}

	c, err := client.New(options...)
	if err != nil {
		return nil, fmt.Errorf("coult not init api client:%w", err)
	}

	return c, err
}
