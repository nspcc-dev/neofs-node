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
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
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
	Args:  cobra.NoArgs,
	RunE:  getObjectHash,
}

func initObjectHashCmd() {
	commonflags.Init(objectHashCmd)
	initFlagSession(objectHashCmd, "RANGEHASH")

	flags := objectHashCmd.Flags()

	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = objectHashCmd.MarkFlagRequired(commonflags.CIDFlag)

	flags.String(commonflags.OIDFlag, "", commonflags.OIDFlagUsage)
	_ = objectHashCmd.MarkFlagRequired(commonflags.OIDFlag)

	flags.String("range", "", "Range to take hash from in the form offset1:length1,... Full object payload length if not specified")
	flags.String("type", hashSha256, "Hash type. Either 'sha256' or 'tz'")
	flags.String(getRangeHashSaltFlag, "", "Salt in hex format")
}

func getObjectHash(cmd *cobra.Command, _ []string) error {
	var cnr cid.ID
	var obj oid.ID

	_, err := readObjectAddress(cmd, &cnr, &obj)
	if err != nil {
		return err
	}

	ranges, err := getRangeList(cmd)
	if err != nil {
		return err
	}
	typ, err := getHashType(cmd)
	if err != nil {
		return err
	}

	strSalt := strings.TrimPrefix(cmd.Flag(getRangeHashSaltFlag).Value.String(), "0x")

	salt, err := hex.DecodeString(strSalt)
	if err != nil {
		return fmt.Errorf("could not decode salt: %w", err)
	}

	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}

	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
	if err != nil {
		return err
	}
	defer cli.Close()

	tz := typ == hashTz
	fullHash := len(ranges) == 0
	if fullHash {
		common.PrintVerbose(cmd, "Get the hash of the full object payload.")
		var headPrm client.PrmObjectHead
		err = Prepare(cmd, &headPrm)
		if err != nil {
			return err
		}

		// get hash of full payload through HEAD (may be user can do it through dedicated command?)
		hdr, err := cli.ObjectHead(ctx, cnr, obj, user.NewAutoIDSigner(*pk), headPrm)
		if err != nil {
			return fmt.Errorf("rpc error: read object header via client: %w", err)
		}

		var cs checksum.Checksum
		var csSet bool

		if tz {
			cs, csSet = hdr.PayloadHomomorphicHash()
		} else {
			cs, csSet = hdr.PayloadChecksum()
		}

		if csSet {
			cmd.Println(hex.EncodeToString(cs.Value()))
		} else {
			cmd.Println("Missing checksum in object header.")
		}

		return nil
	}

	var hashPrm client.PrmObjectHash
	err = Prepare(cmd, &hashPrm)
	if err != nil {
		return err
	}
	err = readSession(cmd, &hashPrm, pk, cnr, obj)
	if err != nil {
		return err
	}
	hashPrm.UseSalt(salt)

	rngs := make([]uint64, 2*len(ranges))
	for i := range ranges {
		rngs[2*i] = ranges[i].GetOffset()
		rngs[2*i+1] = ranges[i].GetLength()
	}
	hashPrm.SetRangeList(rngs...)

	if tz {
		hashPrm.TillichZemorAlgo()
	}

	hs, err := cli.ObjectHash(ctx, cnr, obj, user.NewAutoIDSigner(*pk), hashPrm)
	if err != nil {
		return fmt.Errorf("rpc error: read payload hashes via client: %w", err)
	}

	for i := range hs {
		cmd.Printf("Offset=%d (Length=%d)\t: %s\n", ranges[i].GetOffset(), ranges[i].GetLength(),
			hex.EncodeToString(hs[i]))
	}
	return nil
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
