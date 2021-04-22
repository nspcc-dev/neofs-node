package common

import (
	"context"
	"fmt"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
)

type EpochContext struct {
	context.Context
	E uint64
}

func (ctx *EpochContext) Epoch() uint64 {
	return ctx.E
}

type NopReputationWriter struct{}

func (NopReputationWriter) Write(common.Context, reputation.Trust) error {
	return nil
}

func (NopReputationWriter) Close() error {
	return nil
}

type OnlyKeyRemoteServerInfo struct {
	Key []byte
}

func (i *OnlyKeyRemoteServerInfo) PublicKey() []byte {
	return i.Key
}

func (*OnlyKeyRemoteServerInfo) Address() string {
	return ""
}

const invalidPrmValFmt = "invalid parameter %s (%T):%v"

func PanicOnPrmValue(n string, v interface{}) {
	panic(fmt.Sprintf(invalidPrmValFmt, n, v, v))
}
