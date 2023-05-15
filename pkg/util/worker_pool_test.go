package util

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/management"
	"github.com/nspcc-dev/neofs-node/pkg/util/config"
	"github.com/stretchr/testify/require"
)

func TestSyncWorkerPool(t *testing.T) {
	t.Run("submit to released pool", func(t *testing.T) {
		p := NewPseudoWorkerPool()
		p.Release()
		require.Equal(t, ErrPoolClosed, p.Submit(func() {}))
	})
	t.Run("create and wait", func(t *testing.T) {
		p := NewPseudoWorkerPool()
		ch1, ch2 := make(chan struct{}), make(chan struct{})
		wg := new(sync.WaitGroup)
		wg.Add(2)
		go func(t *testing.T) {
			defer wg.Done()
			err := p.Submit(newControlledReturnFunc(ch1))
			require.NoError(t, err)
		}(t)
		go func(t *testing.T) {
			defer wg.Done()
			err := p.Submit(newControlledReturnFunc(ch2))
			require.NoError(t, err)
		}(t)

		// Make sure functions were submitted.
		<-ch1
		<-ch2
		p.Release()
		require.Equal(t, ErrPoolClosed, p.Submit(func() {}))

		close(ch1)
		close(ch2)
		wg.Wait()
	})
}

// newControlledReturnFunc returns function which signals in ch after
// it has started and waits for some value in channel to return.
// ch must be unbuffered.
func newControlledReturnFunc(ch chan struct{}) func() {
	return func() {
		ch <- struct{}{}
		<-ch
	}
}

func TestIk(t *testing.T) {
	const endpoint = "https://rpc1.morph.t5.fs.neo.org:51331"

	acc, err := config.LoadAccount("/home/ll/projects/neo/go/.docker/wallets/wallet1.json", "", "one")
	if err != nil {
		log.Fatal("load account ", err)
	}

	c, err := rpcclient.New(context.Background(), endpoint, rpcclient.Options{})
	require.NoError(t, err)
	require.NoError(t, c.Init())
	defer c.Close()

	var auditState *state.Contract

	for id := int32(1); ; id++ {
		st, err := c.GetContractStateByID(id)
		if err != nil {
			if strings.Contains(err.Error(), "Unknown contract") {
				break
			}
			log.Fatal(err)
		}

		if st.Manifest.Name == "NeoFS Audit" {
			j, err := json.Marshal(st.Manifest)
			if err == nil {
				fmt.Println(string(j))
			}
			auditState = st
			break
		}
	}

	require.NotNil(t, auditState)

	j, err := json.Marshal(auditState.Manifest)
	require.NoError(t, err)

	fmt.Println(string(j))

	a, err := actor.NewSimple(c, acc)
	require.NoError(t, err)

	auditState.Manifest.Groups = nil

	ctr := management.New(a)
	_, err = ctr.DeployUnsigned(&auditState.NEF, &auditState.Manifest, []interface{}{})
	require.NoError(t, err)
}
