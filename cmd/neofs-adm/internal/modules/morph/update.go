package morph

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func updateContracts(cmd *cobra.Command, _ []string) error {
	wCtx, err := newInitializeContext(cmd, viper.GetViper())
	if err != nil {
		return fmt.Errorf("initialization error: %w", err)
	}

	if err := wCtx.deployContracts(updateMethodName); err != nil {
		return err
	}

	return wCtx.deployNNS("update")
}
