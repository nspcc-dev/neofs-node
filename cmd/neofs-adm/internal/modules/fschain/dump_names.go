package fschain

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	nameDomainFlag = "domain"
)

type nameExp struct {
	name string
	exp  int64
}

func dumpNames(cmd *cobra.Command, _ []string) error {
	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}
	nnsReader, err := nns.NewInferredReader(c, invoker.New(c, nil))
	if err != nil {
		return fmt.Errorf("can't find NNS contract: %w", err)
	}
	tokIter, err := nnsReader.Tokens()
	if err != nil {
		return err
	}
	zone, _ := cmd.Flags().GetString(nameDomainFlag)
	var res = make([]nameExp, 0)
	for toks, err := tokIter.Next(10); len(toks) != 0 && err == nil; toks, err = tokIter.Next(10) {
		for i := range toks {
			var name = string(toks[i])
			if !strings.ContainsRune(name, '.') {
				continue // TLD, can't get properties.
			}
			if zone != "" && !strings.HasSuffix(name, "."+zone) {
				continue
			}
			props, err := nnsReader.Properties(toks[i])
			if err != nil {
				cmd.PrintErrf("Error getting properties for %s: %v\n", name, err)
				continue
			}
			exp, err := expirationFromProperties(props)
			if err != nil {
				cmd.PrintErrf("Error getting expiration from properties for %s: %v\n", name, err)
				continue
			}
			res = append(res, nameExp{name: name, exp: exp})
		}
	}

	sort.Slice(res, func(i, j int) bool {
		var (
			iParts = strings.Split(res[i].name, ".")
			jParts = strings.Split(res[j].name, ".")
		)
		if len(iParts) != len(jParts) {
			return len(iParts) < len(jParts)
		}
		for k := len(iParts) - 1; k >= 0; k-- {
			var c = strings.Compare(iParts[k], jParts[k])
			if c != 0 {
				return c == -1
			}
		}
		return false
	})

	buf := bytes.NewBuffer(nil)
	tw := tabwriter.NewWriter(buf, 0, 2, 2, ' ', 0)
	for i := range res {
		_, _ = tw.Write([]byte(fmt.Sprintf("%s\t%s\n",
			res[i].name, time.UnixMilli(res[i].exp).String())))
	}
	_ = tw.Flush()

	cmd.Print(buf.String())

	return nil
}
