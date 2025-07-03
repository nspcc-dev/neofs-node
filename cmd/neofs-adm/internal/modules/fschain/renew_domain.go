package fschain

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
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
	nnsHash, err := nns.InferHash(wCtx.Client)
	if err != nil {
		return err
	}
	nnsReader := nns.NewReader(wCtx.ReadOnlyInvoker, nnsHash)
	var domains = make([]string, 0, 1)
	if recursive {
		tokIter, err := nnsReader.Tokens()
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
		avail, err := nnsReader.IsAvailable(dom)
		if err == nil && avail {
			return errors.New("domain is not registered or expired")
		}
		domains = append(domains, dom)
	}

	b := smartcontract.NewBuilder()
	for i := range domains {
		b.InvokeMethod(nnsHash, "renew", domains[i])

		script, err := b.Script()
		if err != nil {
			return fmt.Errorf("renew script: %w", err)
		}

		// Default registration price is 10 GAS, adding more domains
		// into the script makes test execution to fail.
		if err := wCtx.sendConsensusTx(script); err != nil {
			return err
		}
		b.Reset()
	}
	return wCtx.awaitTx()
}
