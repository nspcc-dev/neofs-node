package main

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	sessionSvc "github.com/nspcc-dev/neofs-node/pkg/services/session"
	"github.com/nspcc-dev/neofs-node/pkg/util/state/session"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type sessionStorage interface {
	sessionSvc.KeyStorage
	GetToken(ownerID user.ID, tokenID []byte) *session.PrivateToken
	RemoveOldTokens(epoch uint64)

	Close() error
}

func initSessionService(c *cfg) {
	addNewEpochNotificationHandler(c, func(ev event.Event) {
		c.privateTokenStore.RemoveOldTokens(ev.(netmap.NewEpoch).EpochNumber())
	})

	server := sessionSvc.New(&c.key.PrivateKey, c, c.privateTokenStore)

	for _, srv := range c.cfgGRPC.servers {
		protosession.RegisterSessionServiceServer(srv, server)
	}
}
