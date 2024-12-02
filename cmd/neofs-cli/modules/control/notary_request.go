package control

import (
	"errors"
	"fmt"
	"strings"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	ircontrol "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	ircontrolsrv "github.com/nspcc-dev/neofs-node/pkg/services/control/ir/server"
	"github.com/spf13/cobra"
)

const notaryMethodFlag = "method"

var notaryRequestCmd = &cobra.Command{
	Use:   "request",
	Short: "Create and send a notary request",
	Long: "Create and send a notary request with one of the following methods:\n" +
		"- newEpoch, transaction for creating of new NeoFS epoch event in FS chain, no args\n" +
		"- setConfig, transaction to add/update global config value in the NeoFS network, args in the form key1=val1\n" +
		"- removeNode, transaction to move nodes to the Offline state in the candidates list, args are the public keys of the nodes",
	RunE: notaryRequest,
}

func initControlNotaryRequestCmd() {
	initControlFlags(notaryRequestCmd)

	flags := notaryRequestCmd.Flags()
	flags.String(notaryMethodFlag, "", "Requested method")
}

func notaryRequest(cmd *cobra.Command, args []string) error {
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

	req := new(ircontrol.NotaryRequestRequest)
	body := new(ircontrol.NotaryRequestRequest_Body)
	req.SetBody(body)

	method, _ := cmd.Flags().GetString(notaryMethodFlag)
	body.SetMethod(method)

	switch method {
	case "newEpoch":
		if len(args) > 0 {
			cmd.Println("method 'newEpoch', but the args provided, they will be ignored")
		}
	case "setConfig":
		if len(args) == 0 {
			return fmt.Errorf("no arguments provided for 'setConfig'")
		}

		bodyArgs := make([]string, 0, len(args)*2)
		for _, arg := range args {
			k, v, err := parseConfigPair(arg)
			if err != nil {
				return err
			}

			bodyArgs = append(bodyArgs, k, fmt.Sprint(v))
		}
		body.SetArgs(bodyArgs)
	case "removeNode":
		if len(args) == 0 {
			return errors.New("method 'removeNode', at least 1 argument must be provided - the public key of the node")
		}
		body.SetArgs(args)
	}

	err = ircontrolsrv.SignMessage(pk, req)
	if err != nil {
		return fmt.Errorf("could not sign request: %w", err)
	}

	var resp *ircontrol.NotaryRequestResponse
	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = ircontrol.NotaryRequest(client, req)
		return err
	})
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	err = verifyResponse(resp.GetSignature(), resp.GetBody())
	if err != nil {
		return err
	}

	hashes := resp.GetBody().GetHash()

	cmd.Printf("Tx Hash: %s\n", hashes)
	return nil
}

func parseConfigPair(kvStr string) (key string, val string, err error) {
	kv := strings.SplitN(kvStr, "=", 2)
	if len(kv) != 2 {
		return "", "", fmt.Errorf("invalid parameter format: must be 'key=val', got: %s", kvStr)
	}

	key = kv[0]
	val = kv[1]

	return
}
