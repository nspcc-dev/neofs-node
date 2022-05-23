package control

import (
	"fmt"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/spf13/cobra"
)

const (
	netmapStatusFlag = "status"

	netmapStatusOnline      = "online"
	netmapStatusOffline     = "offline"
	netmapStatusMaintenance = "maintenance"
)

var setNetmapStatusCmd = &cobra.Command{
	Use:   "set-status",
	Short: "Set status of the storage node in NeoFS network map",
	Long:  "Set status of the storage node in NeoFS network map",
	Run:   setNetmapStatus,
}

func initControlSetNetmapStatusCmd() {
	commonflags.InitWithoutRPC(setNetmapStatusCmd)

	flags := setNetmapStatusCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.String(netmapStatusFlag, "",
		fmt.Sprintf("new netmap status keyword ('%s', '%s', '%s')",
			netmapStatusOnline,
			netmapStatusOffline,
			netmapStatusMaintenance,
		),
	)

	_ = setNetmapStatusCmd.MarkFlagRequired(netmapStatusFlag)
}

func setNetmapStatus(cmd *cobra.Command, _ []string) {
	pk := getKey(cmd)

	var status control.NetmapStatus

	switch st, _ := cmd.Flags().GetString(netmapStatusFlag); st {
	default:
		common.ExitOnErr(cmd, "", fmt.Errorf("unsupported status %s", st))
	case netmapStatusOnline:
		status = control.NetmapStatus_ONLINE
	case netmapStatusOffline:
		status = control.NetmapStatus_OFFLINE
	case netmapStatusMaintenance:
		status = control.NetmapStatus_MAINTENANCE
	}

	body := new(control.SetNetmapStatusRequest_Body)
	body.SetStatus(status)

	req := new(control.SetNetmapStatusRequest)
	req.SetBody(body)

	signRequest(cmd, pk, req)

	cli := getClient(cmd, pk)

	var resp *control.SetNetmapStatusResponse
	var err error
	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = control.SetNetmapStatus(client, req)
		return err
	})
	common.ExitOnErr(cmd, "rpc error: %w", err)

	verifyResponse(cmd, resp.GetSignature(), resp.GetBody())

	cmd.Println("Network status update request successfully sent.")
}
