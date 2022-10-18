package control

import (
	"fmt"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	initControlFlags(setNetmapStatusCmd)

	flags := setNetmapStatusCmd.Flags()
	flags.String(netmapStatusFlag, "",
		fmt.Sprintf("New netmap status keyword ('%s', '%s', '%s')",
			netmapStatusOnline,
			netmapStatusOffline,
			netmapStatusMaintenance,
		),
	)

	_ = setNetmapStatusCmd.MarkFlagRequired(netmapStatusFlag)
}

func setNetmapStatus(cmd *cobra.Command, _ []string) {
	pk := key.Get(cmd)

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

		common.PrintVerbose("Reading network settings to check allowance of \"%s\" mode...", st)

		if !viper.IsSet(commonflags.RPC) {
			common.ExitOnErr(cmd, "",
				fmt.Errorf("flag --%s (-%s) is not set, you must specify it for \"%s\" mode",
					commonflags.RPC,
					commonflags.RPCShorthand,
					st,
				),
			)
		}

		cli := internalclient.GetSDKClientByFlag(cmd, pk, commonflags.RPC)

		var prm internalclient.NetworkInfoPrm
		prm.SetClient(cli)

		res, err := internalclient.NetworkInfo(prm)
		common.ExitOnErr(cmd, "receive network info: %v", err)

		if !res.NetworkInfo().MaintenanceModeAllowed() {
			common.ExitOnErr(cmd, "", fmt.Errorf("\"%s\" mode is not allowed by the network", st))
		}

		common.PrintVerbose("\"%s\" mode is allowed, continue processing...", st)
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
