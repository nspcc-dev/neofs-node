package main

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-ir/internal/validate"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestValidateDefaultConfig(t *testing.T) {
	v := viper.New()

	defaultConfiguration(v)

	require.NoError(t, validate.ValidateStruct(v))
}
