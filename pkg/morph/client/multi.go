package client

import (
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"go.uber.org/zap"
)

type multiClient struct {
	cfg cfg

	account *wallet.Account

	sharedNotary *notary // notary config needed for single client construction

	endpoints  []string
	clientsMtx sync.Mutex
	clients    map[string]*Client
}

// createForAddress creates single Client instance using provided endpoint.
//
// note: must be wrapped into mutex lock.
func (x *multiClient) createForAddress(addr string) (*Client, error) {
	cli, err := client.New(x.cfg.ctx, addr, client.Options{
		DialTimeout: x.cfg.dialTimeout,
	})
	if err != nil {
		return nil, err
	}

	err = cli.Init() // magic number is set there based on RPC node answer
	if err != nil {
		return nil, err
	}

	c := &Client{
		singleClient: &singleClient{
			logger:       x.cfg.logger,
			client:       cli,
			acc:          x.account,
			waitInterval: x.cfg.waitInterval,
			signer:       x.cfg.signer,
			notary:       x.sharedNotary,
		},
	}

	x.clients[addr] = c

	return c, nil
}

func (x *multiClient) iterateClients(f func(*Client) error) error {
	var (
		firstErr error
		err      error
	)

	for i := range x.endpoints {
		select {
		case <-x.cfg.ctx.Done():
			return x.cfg.ctx.Err()
		default:
		}

		x.clientsMtx.Lock()

		c, cached := x.clients[x.endpoints[i]]
		if !cached {
			c, err = x.createForAddress(x.endpoints[i])
		}

		x.clientsMtx.Unlock()

		if !cached && err != nil {
			x.cfg.logger.Error("could not open morph client connection",
				zap.String("endpoint", x.endpoints[i]),
				zap.String("err", err.Error()),
			)
		} else {
			err = f(c)
		}

		if err == nil {
			return nil
		}

		// we dont need to continue the process after the logical error was encountered
		if errNeoFS := unwrapNeoFSError(err); errNeoFS != nil {
			return errNeoFS
		}

		// set first error once
		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
