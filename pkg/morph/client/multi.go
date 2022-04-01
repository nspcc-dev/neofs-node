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
	clientsMtx sync.RWMutex
	// lastSuccess is an index in endpoints array relating to a last
	// used endpoint.
	lastSuccess int
	clients     map[string]*Client
}

// createForAddress creates single Client instance using provided endpoint.
func (x *multiClient) createForAddress(addr string) (*Client, error) {
	cli, err := client.New(x.cfg.ctx, addr, client.Options{
		DialTimeout:     x.cfg.dialTimeout,
		MaxConnsPerHost: x.cfg.maxConnPerHost,
	})
	if err != nil {
		return nil, err
	}

	err = cli.Init() // magic number is set there based on RPC node answer
	if err != nil {
		return nil, err
	}

	var c *Client

	x.clientsMtx.Lock()
	// While creating 2 clients in parallel is ok, we don't want to
	// use a client missing from `x.clients` map as it can lead
	// to unexpected bugs.
	if x.clients[addr] == nil {
		sCli := blankSingleClient(cli, x.account, &x.cfg)
		sCli.notary = x.sharedNotary

		c = &Client{
			cache:        newClientCache(),
			singleClient: sCli,
		}
		x.clients[addr] = c
	} else {
		c = x.clients[addr]
	}
	x.clientsMtx.Unlock()

	return c, nil
}

// iterateClients executes f on each client until nil error is returned.
// When nil error is returned, lastSuccess field is updated.
// The iteration order is non-deterministic and shouldn't be relied upon.
func (x *multiClient) iterateClients(f func(*Client) error) error {
	var (
		firstErr error
		err      error
	)

	x.clientsMtx.RLock()
	start := x.lastSuccess
	x.clientsMtx.RUnlock()

	for i := 0; i < len(x.endpoints); i++ {
		index := (start + i) % len(x.endpoints)

		x.clientsMtx.RLock()
		c, cached := x.clients[x.endpoints[index]]
		x.clientsMtx.RUnlock()
		if !cached {
			c, err = x.createForAddress(x.endpoints[index])
		}

		if !cached && err != nil {
			x.cfg.logger.Error("could not open morph client connection",
				zap.String("endpoint", x.endpoints[index]),
				zap.String("err", err.Error()),
			)
		} else {
			err = f(c)
		}

		if err == nil {
			if i != 0 {
				x.clientsMtx.Lock()
				x.lastSuccess = index
				x.clientsMtx.Unlock()
			}
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
