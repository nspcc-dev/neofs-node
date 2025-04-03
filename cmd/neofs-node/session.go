package main

import (
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	sessionSvc "github.com/nspcc-dev/neofs-node/pkg/services/session"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage/persistent"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage/temporary"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type sessionStorage interface {
	sessionSvc.KeyStorage
	Get(ownerID user.ID, tokenID []byte) *storage.PrivateToken
	RemoveOld(epoch uint64)

	Close() error
}

func initSessionService(c *cfg) {
	if persistentSessionPath := c.appCfg.Node.PersistentSessions.Path; persistentSessionPath != "" {
		persisessions, err := persistent.NewTokenStore(persistentSessionPath,
			persistent.WithLogger(c.log),
			persistent.WithTimeout(time.Second),
			persistent.WithEncryptionKey(&c.key.PrivateKey),
		)
		if err != nil {
			panic(fmt.Errorf("could not create persistent session token storage: %w", err))
		}

		c.privateTokenStore = persisessions
	} else {
		c.privateTokenStore = temporary.NewTokenStore()
	}

	c.onShutdown(func() {
		_ = c.privateTokenStore.Close()
	})

	addNewEpochNotificationHandler(c, func(ev event.Event) {
		c.privateTokenStore.RemoveOld(ev.(netmap.NewEpoch).EpochNumber())
	})

	server := sessionSvc.New(&c.key.PrivateKey, c, c.privateTokenStore)

	for _, srv := range c.cfgGRPC.servers {
		protosession.RegisterSessionServiceServer(srv, server)
	}
}
