package control

import (
	"fmt"

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
	RunE:  setNetmapStatus,
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

func setNetmapStatus(cmd *cobra.Command, _ []string) error {
	pk, err := key.Get(cmd)
	if err != nil {
		return err
	}
	body := new(control.SetNetmapStatusRequest_Body)

	switch st, _ := cmd.Flags().GetString(netmapStatusFlag); st {
	default:
		return fmt.Errorf("unsupported status %s", st)
	case netmapStatusOnline:
		body.SetStatus(control.NetmapStatus_ONLINE)
	case netmapStatusOffline:
		body.SetStatus(control.NetmapStatus_OFFLINE)
	case netmapStatusMaintenance:
		body.SetStatus(control.NetmapStatus_MAINTENANCE)
	}

	req := new(control.SetNetmapStatusRequest)
	req.SetBody(body)

	err = signRequest(pk, req)
	if err != nil {
		return err
	}

	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	cli, err := getClient(ctx)
	if err != nil {
		return err
	}

	resp, err := cli.SetNetmapStatus(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	err = verifyResponse(resp.GetSignature(), resp.GetBody())
	if err != nil {
		return err
	}

	cmd.Println("Network status update request successfully sent.")
	return nil
}
