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

// OnlyKeyRemoteServerInfo if implementation of reputation.ServerInfo
// interface but with only public key data.
type OnlyKeyRemoteServerInfo struct {
	Key []byte
}

func (i *OnlyKeyRemoteServerInfo) PublicKey() []byte {
	return i.Key
}

func (*OnlyKeyRemoteServerInfo) IterateAddresses(func(string) bool) {
}

func (*OnlyKeyRemoteServerInfo) NumberOfAddresses() int {
	return 0
}

const invalidPrmValFmt = "invalid parameter %s (%T):%v"

func PanicOnPrmValue(n string, v interface{}) {
	panic(fmt.Sprintf(invalidPrmValFmt, n, v, v))
}
