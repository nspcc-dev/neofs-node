package morph

import (
	"errors"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nep11"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	recursiveFlag = "recursive"
)

func renewDomain(cmd *cobra.Command, _ []string) error {
	dom, err := cmd.Flags().GetString(nameDomainFlag)
	if err != nil {
		return err
	}
	recursive, _ := cmd.Flags().GetBool(recursiveFlag)
	wCtx, err := newInitializeContext(cmd, viper.GetViper())
	if err != nil {
		return err
	}
	defer wCtx.close()
	nns, err := wCtx.Client.GetContractStateByID(1)
	if err != nil {
		return err
	}
	var domains = make([]string, 0, 1)
	if recursive {
		var n11r = nep11.NewNonDivisibleReader(wCtx.ReadOnlyInvoker, nns.Hash)
		tokIter, err := n11r.Tokens()
		if err != nil {
			return err
		}
		for toks, err := tokIter.Next(10); len(toks) != 0 && err == nil; toks, err = tokIter.Next(10) {
			for i := range toks {
				var name = string(toks[i])
				if name != dom && !strings.HasSuffix(name, "."+dom) {
					continue
				}
				domains = append(domains, name)
			}
		}
	} else {
		avail, err := unwrap.Bool(wCtx.ReadOnlyInvoker.Call(nns.Hash, "isAvailable"))
		if err == nil && avail {
			return errors.New("domain is not registered or expired")
		}
		domains = append(domains, dom)
	}

	bw := io.NewBufBinWriter()
	for i := range domains {
		emit.AppCall(bw.BinWriter, nns.Hash, "renew", callflag.All, domains[i])
		if bw.Err != nil {
			return bw.Err
		}
		// Default registration price is 10 GAS, adding more domains
		// into the script makes test execution to fail.
		if err := wCtx.sendConsensusTx(bw.Bytes()); err != nil {
			return err
		}
		bw.Reset()
	}
	return wCtx.awaitTx()
}
