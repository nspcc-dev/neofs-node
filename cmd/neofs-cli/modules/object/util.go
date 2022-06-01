package object

import (
	"fmt"
	"strings"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	bearerTokenFlag = "bearer"

	rawFlag     = "raw"
	rawFlagDesc = "Set raw request option"
)

type RPCParameters interface {
	SetBearerToken(prm *bearer.Token)
	SetTTL(uint32)
	SetXHeaders([]string)
}

// InitBearer adds bearer token flag to a command.
func InitBearer(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.String(bearerTokenFlag, "", "File with signed JSON or binary encoded bearer token")
}

// Prepare prepares object-related parameters for a command.
func Prepare(cmd *cobra.Command, prms ...RPCParameters) {
	ttl := viper.GetUint32(commonflags.TTL)
	common.PrintVerbose("TTL: %d", ttl)

	for i := range prms {
		btok := common.ReadBearerToken(cmd, bearerTokenFlag)

		prms[i].SetBearerToken(btok)
		prms[i].SetTTL(ttl)
		prms[i].SetXHeaders(parseXHeaders(cmd))
	}
}

func parseXHeaders(cmd *cobra.Command) []string {
	xHeaders, _ := cmd.Flags().GetStringSlice(commonflags.XHeadersKey)
	xs := make([]string, 0, 2*len(xHeaders))

	for i := range xHeaders {
		kv := strings.SplitN(xHeaders[i], "=", 2)
		if len(kv) != 2 {
			panic(fmt.Errorf("invalid X-Header format: %s", xHeaders[i]))
		}

		xs = append(xs, kv[0], kv[1])
	}

	return xs
}

func readObjectAddress(cmd *cobra.Command, cnr *cid.ID, obj *oid.ID) oid.Address {
	readCID(cmd, cnr)
	readOID(cmd, obj)

	var addr oid.Address
	addr.SetContainer(*cnr)
	addr.SetObject(*obj)
	return addr
}

func readCID(cmd *cobra.Command, id *cid.ID) {
	err := id.DecodeString(cmd.Flag("cid").Value.String())
	common.ExitOnErr(cmd, "decode container ID string: %w", err)
}

func readOID(cmd *cobra.Command, id *oid.ID) {
	err := id.DecodeString(cmd.Flag("oid").Value.String())
	common.ExitOnErr(cmd, "decode object ID string: %w", err)
}
