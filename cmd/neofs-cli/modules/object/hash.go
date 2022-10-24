package object

import (
	"encoding/hex"
	"fmt"
	"strings"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

const getRangeHashSaltFlag = "salt"

const (
	hashSha256 = "sha256"
	hashTz     = "tz"
	rangeSep   = ":"
)

var objectHashCmd = &cobra.Command{
	Use:   "hash",
	Short: "Get object hash",
	Long:  "Get object hash",
	Run:   getObjectHash,
}

func initObjectHashCmd() {
	commonflags.Init(objectHashCmd)

	flags := objectHashCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = objectHashCmd.MarkFlagRequired("cid")

	flags.String("oid", "", "Object ID")
	_ = objectHashCmd.MarkFlagRequired("oid")

	flags.String("range", "", "Range to take hash from in the form offset1:length1,...")
	flags.String("type", hashSha256, "Hash type. Either 'sha256' or 'tz'")
	flags.String(getRangeHashSaltFlag, "", "Salt in hex format")
}

func getObjectHash(cmd *cobra.Command, _ []string) {
	var cnr cid.ID
	var obj oid.ID

	objAddr := readObjectAddress(cmd, &cnr, &obj)

	ranges, err := getRangeList(cmd)
	common.ExitOnErr(cmd, "", err)
	typ, err := getHashType(cmd)
	common.ExitOnErr(cmd, "", err)

	strSalt := strings.TrimPrefix(cmd.Flag(getRangeHashSaltFlag).Value.String(), "0x")

	salt, err := hex.DecodeString(strSalt)
	common.ExitOnErr(cmd, "could not decode salt: %w", err)

	pk := key.GetOrGenerate(cmd)

	tz := typ == hashTz
	fullHash := len(ranges) == 0
	if fullHash {
		var headPrm internalclient.HeadObjectPrm
		Prepare(cmd, &headPrm)
		headPrm.SetAddress(objAddr)

		// get hash of full payload through HEAD (may be user can do it through dedicated command?)
		res, err := internalclient.HeadObject(headPrm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		var cs checksum.Checksum
		var csSet bool

		if tz {
			cs, csSet = res.Header().PayloadHomomorphicHash()
		} else {
			cs, csSet = res.Header().PayloadChecksum()
		}

		if csSet {
			cmd.Println(hex.EncodeToString(cs.Value()))
		} else {
			cmd.Println("Missing checksum in object header.")
		}

		return
	}

	cli := internalclient.GetSDKClientByFlag(cmd, pk, commonflags.RPC)

	var hashPrm internalclient.HashPayloadRangesPrm
	hashPrm.SetClient(cli)
	Prepare(cmd, &hashPrm)
	readSession(cmd, &hashPrm, pk, cnr, obj)
	hashPrm.SetAddress(objAddr)
	hashPrm.SetSalt(salt)
	hashPrm.SetRanges(ranges)

	if tz {
		hashPrm.TZ()
	}

	res, err := internalclient.HashPayloadRanges(hashPrm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	hs := res.HashList()

	for i := range hs {
		cmd.Printf("Offset=%d (Length=%d)\t: %s\n", ranges[i].GetOffset(), ranges[i].GetLength(),
			hex.EncodeToString(hs[i]))
	}
}

func getHashType(cmd *cobra.Command) (string, error) {
	rawType := cmd.Flag("type").Value.String()
	switch typ := strings.ToLower(rawType); typ {
	case hashSha256, hashTz:
		return typ, nil
	default:
		return "", fmt.Errorf("invalid hash type: %s", typ)
	}
}
