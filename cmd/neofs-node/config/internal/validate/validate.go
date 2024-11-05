package validate

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/internal/configvalidator"
	"github.com/spf13/viper"
)

// ValidateStruct validates the viper config structure.
func ValidateStruct(v *viper.Viper) error {
	var cfg valideConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return fmt.Errorf("unable to decode config: %w", err)
	}

	if err := configvalidator.CheckForUnknownFields(v.AllSettings(), cfg); err != nil {
		return err
	}

	return nil
}
