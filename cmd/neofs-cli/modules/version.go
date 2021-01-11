package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/nspcc-dev/neofs-node/misc"

	"github.com/spf13/cobra"
)

var (
	// versionCmd represents the version command
	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Print version and exit",
		Run:   versionRun,
	}
)

var flagJSON bool

type VersionInfo struct {
	Version string `json:"Version,omitempty"`
	Build   string `json:"Build,omitempty"`
	Debug   string `json:"Debug,omitempty"`
}

func init() {
	rootCmd.AddCommand(versionCmd)
	versionCmd.Flags().BoolVarP(&flagJSON, "json", "j", false, "Print version information in JSON")
}

func versionRun(cmd *cobra.Command, args []string) {
	versionInfo := VersionInfo{Version: misc.Version, Build: misc.Build, Debug: misc.Debug}

	if flagJSON {
		bytes, _ := json.Marshal(versionInfo)
		fmt.Printf("%s", string(bytes)+"\n")
		return
	}

	fmt.Printf("Version: %s \nBuild: %s \nDebug: %s\n",
		versionInfo.Version,
		versionInfo.Build,
		versionInfo.Debug)
}
