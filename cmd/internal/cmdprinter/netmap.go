package cmdprinter

import (
	"encoding/hex"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/spf13/cobra"
)

// PrettyPrintNodeInfo print information about network node with given indent and index.
// To avoid printing attribute list use short parameter.
func PrettyPrintNodeInfo(cmd *cobra.Command, node netmap.NodeInfo,
	index int, indent string, short bool) {
	var strState string

	switch {
	default:
		strState = "STATE_UNSUPPORTED"
	case node.IsOnline():
		strState = "ONLINE"
	case node.IsOffline():
		strState = "OFFLINE"
	case node.IsMaintenance():
		strState = "MAINTENANCE"
	}

	cmd.Printf("%sNode %d: %s %s ", indent, index+1, hex.EncodeToString(node.PublicKey()), strState)

	for endpoint := range node.NetworkEndpoints() {
		cmd.Printf("%s ", endpoint)
	}
	cmd.Println()

	if !short {
		for key, value := range node.Attributes() {
			cmd.Printf("%s\t%s: %s\n", indent, key, value)
		}
	}
}

// PrettyPrintNetMap print information about network map.
func PrettyPrintNetMap(cmd *cobra.Command, nm netmap.NetMap) {
	cmd.Println("Epoch:", nm.Epoch())

	nodes := nm.Nodes()
	for i := range nodes {
		PrettyPrintNodeInfo(cmd, nodes[i], i, "", false)
	}
}
