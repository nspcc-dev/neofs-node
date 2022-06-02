package morph

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/rpc/response/result"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Client represents N3 client interface capable of test-invoking scripts
// and sending signed transactions to chain.
type Client interface {
	GetBlockCount() (uint32, error)
	GetContractStateByID(int32) (*state.Contract, error)
	GetContractStateByHash(util.Uint160) (*state.Contract, error)
	GetNativeContracts() ([]state.NativeContract, error)
	GetNetwork() (netmode.Magic, error)
	GetApplicationLog(util.Uint256, *trigger.Type) (*result.ApplicationLog, error)
	CreateTxFromScript([]byte, *wallet.Account, int64, int64, []client.SignerAccount) (*transaction.Transaction, error)
	NEP17BalanceOf(util.Uint160, util.Uint160) (int64, error)
	InvokeScript([]byte, []transaction.Signer) (*result.Invoke, error)
	SendRawTransaction(*transaction.Transaction) (util.Uint256, error)
	GetCommittee() (keys.PublicKeys, error)
	CalculateNotaryFee(uint8) (int64, error)
	AddNetworkFee(*transaction.Transaction, int64, ...*wallet.Account) error
	SignAndPushInvocationTx([]byte, *wallet.Account, int64, fixedn.Fixed8, []client.SignerAccount) (util.Uint256, error)
	SignAndPushP2PNotaryRequest(*transaction.Transaction, []byte, int64, int64, uint32, *wallet.Account) (*payload.P2PNotaryRequest, error)
}

type clientContext struct {
	Client       Client
	Hashes       []util.Uint256
	WaitDuration time.Duration
	PollInterval time.Duration
}

func getN3Client(v *viper.Viper) (Client, error) {
	// number of opened connections
	// by neo-go client per one host
	const (
		maxConnsPerHost = 10
		requestTimeout  = time.Second * 10
	)

	ctx := context.Background()
	endpoint := v.GetString(endpointFlag)
	if endpoint == "" {
		return nil, errors.New("missing endpoint")
	}
	c, err := client.New(ctx, endpoint, client.Options{
		MaxConnsPerHost: maxConnsPerHost,
		RequestTimeout:  requestTimeout,
	})
	if err != nil {
		return nil, err
	}
	if err := c.Init(); err != nil {
		return nil, err
	}
	return c, nil
}

func defaultClientContext(c Client) *clientContext {
	return &clientContext{
		Client:       c,
		WaitDuration: time.Second * 30,
		PollInterval: time.Second,
	}
}

func (c *clientContext) sendTx(tx *transaction.Transaction, cmd *cobra.Command, await bool) error {
	h, err := c.Client.SendRawTransaction(tx)
	if err != nil {
		return err
	}

	if h != tx.Hash() {
		return fmt.Errorf("sent and actual tx hashes mismatch:\n\tsent: %v\n\tactual: %v", tx.Hash().StringLE(), h.StringLE())
	}

	c.Hashes = append(c.Hashes, h)

	if await {
		return c.awaitTx(cmd)
	}
	return nil
}
