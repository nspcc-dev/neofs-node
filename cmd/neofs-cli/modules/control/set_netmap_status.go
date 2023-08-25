package control

import (
	"fmt"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
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
	Args:  cobra.NoArgs,
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

	flags.BoolP(commonflags.ForceFlag, commonflags.ForceFlagShorthand, false,
		"Force turning to local maintenance")
}

func setNetmapStatus(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.Get(cmd)
	body := new(control.SetNetmapStatusRequest_Body)
	force, _ := cmd.Flags().GetBool(commonflags.ForceFlag)

	printIgnoreForce := func(st control.NetmapStatus) {
		if force {
			common.PrintVerbose(cmd, "Ignore --%s flag for %s state.", commonflags.ForceFlag, st)
		}
	}

	switch st, _ := cmd.Flags().GetString(netmapStatusFlag); st {
	default:
		common.ExitOnErr(cmd, "", fmt.Errorf("unsupported status %s", st))
	case netmapStatusOnline:
		body.SetStatus(control.NetmapStatus_ONLINE)
		printIgnoreForce(control.NetmapStatus_ONLINE)
	case netmapStatusOffline:
		body.SetStatus(control.NetmapStatus_OFFLINE)
		printIgnoreForce(control.NetmapStatus_OFFLINE)
	case netmapStatusMaintenance:
		body.SetStatus(control.NetmapStatus_MAINTENANCE)

		if force {
			body.SetForceMaintenance()
			common.PrintVerbose(cmd, "Local maintenance will be forced.")
		}
	}

	req := new(control.SetNetmapStatusRequest)
	req.SetBody(body)

	signRequest(cmd, pk, req)

	cli := getClient(ctx, cmd)

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
