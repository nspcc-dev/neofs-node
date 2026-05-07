package common

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
)

// EpochContext is a std context extended with epoch data.
type EpochContext struct {
	context.Context
	E uint64
}

func (ctx *EpochContext) Epoch() uint64 {
	return ctx.E
}

type NopReputationWriter struct{}

func (NopReputationWriter) Write(reputation.Trust) error {
	return nil
}

func (NopReputationWriter) Close() error {
	return nil
}

const invalidPrmValFmt = "invalid parameter %s (%T):%v"

func PanicOnPrmValue(n string, v any) {
	panic(fmt.Sprintf(invalidPrmValFmt, n, v, v))
}
