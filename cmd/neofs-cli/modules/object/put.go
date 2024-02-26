package object

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cheggaaa/pb"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

const (
	noProgressFlag = "no-progress"
)

var putExpiredOn uint64

var objectPutCmd = &cobra.Command{
	Use:   "put",
	Short: "Put object to NeoFS",
	Long:  "Put object to NeoFS",
	Args:  cobra.NoArgs,
	Run:   putObject,
}

func initObjectPutCmd() {
	commonflags.Init(objectPutCmd)
	initFlagSession(objectPutCmd, "PUT")

	flags := objectPutCmd.Flags()

	flags.String(fileFlag, "", "File with object payload")
	_ = objectPutCmd.MarkFlagFilename(fileFlag)
	_ = objectPutCmd.MarkFlagRequired(fileFlag)

	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)

	flags.StringSlice("attributes", []string{}, "User attributes in form of Key1=Value1,Key2=Value2")
	flags.Bool("disable-filename", false, "Do not set well-known filename attribute")
	flags.Bool("disable-timestamp", false, "Do not set well-known timestamp attribute")
	flags.Uint64VarP(&putExpiredOn, commonflags.ExpireAt, "e", 0, "The last active epoch in the life of the object")
	flags.Uint64P(commonflags.Lifetime, "l", 0, "Number of epochs for object to stay valid")
	flags.Bool(noProgressFlag, false, "Do not show progress bar")

	flags.Bool(binaryFlag, false, "Deserialize object structure from given file.")
	objectPutCmd.MarkFlagsMutuallyExclusive(commonflags.ExpireAt, commonflags.Lifetime)
}

func putObject(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	binary, _ := cmd.Flags().GetBool(binaryFlag)
	cidVal, _ := cmd.Flags().GetString(commonflags.CIDFlag)

	if !binary && cidVal == "" {
		common.ExitOnErr(cmd, "", fmt.Errorf("required flag \"%s\" not set", commonflags.CIDFlag))
	}
	pk := key.GetOrGenerate(cmd)

	var ownerID user.ID
	var cnr cid.ID

	filename, _ := cmd.Flags().GetString(fileFlag)
	f, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		common.ExitOnErr(cmd, "", fmt.Errorf("can't open file '%s': %w", filename, err))
	}
	var payloadReader io.Reader = f
	obj := object.New()

	if binary {
		buf, err := os.ReadFile(filename)
		common.ExitOnErr(cmd, "unable to read given file: %w", err)
		objTemp := object.New()
		//TODO(@acid-ant): #1932 Use streams to marshal/unmarshal payload
		common.ExitOnErr(cmd, "can't unmarshal object from given file: %w", objTemp.Unmarshal(buf))
		payloadReader = bytes.NewReader(objTemp.Payload())
		cnr, _ = objTemp.ContainerID()
		ownerID = *objTemp.OwnerID()
	} else {
		fi, err := f.Stat()
		common.ExitOnErr(cmd, "read file stat: %w", err)

		obj.SetPayloadSize(uint64(fi.Size()))

		readCID(cmd, &cnr)
		ownerID = user.ResolveFromECDSAPublicKey(pk.PublicKey)
	}

	attrs, err := parseObjectAttrs(cmd)
	common.ExitOnErr(cmd, "can't parse object attributes: %w", err)

	expiresOn, _ := cmd.Flags().GetUint64(commonflags.ExpireAt)
	lifetime, _ := cmd.Flags().GetUint64(commonflags.Lifetime)
	if lifetime > 0 {
		endpoint, _ := cmd.Flags().GetString(commonflags.RPC)
		currEpoch, err := internalclient.GetCurrentEpoch(ctx, endpoint)
		common.ExitOnErr(cmd, "Request current epoch: %w", err)

		expiresOn = currEpoch + lifetime
	}

	if expiresOn > 0 {
		var expAttrFound bool
		expAttrValue := strconv.FormatUint(expiresOn, 10)

		for i := range attrs {
			if attrs[i].Key() == object.AttributeExpirationEpoch {
				attrs[i].SetValue(expAttrValue)
				expAttrFound = true
				break
			}
		}

		if !expAttrFound {
			index := len(attrs)
			attrs = append(attrs, object.Attribute{})
			attrs[index].SetKey(object.AttributeExpirationEpoch)
			attrs[index].SetValue(expAttrValue)
		}
	}

	obj.SetContainerID(cnr)
	obj.SetOwnerID(&ownerID)
	obj.SetAttributes(attrs...)

	var prm internalclient.PutObjectPrm
	prm.SetPrivateKey(*pk)
	ReadOrOpenSession(ctx, cmd, &prm, pk, cnr, nil)
	Prepare(cmd, &prm)
	prm.SetHeader(obj)

	var p *pb.ProgressBar

	noProgress, _ := cmd.Flags().GetBool(noProgressFlag)
	if noProgress {
		prm.SetPayloadReader(payloadReader)
	} else {
		if binary {
			p = pb.New(len(obj.Payload()))
			p.Output = cmd.OutOrStdout()
			prm.SetPayloadReader(p.NewProxyReader(payloadReader))
			prm.SetHeaderCallback(func(o *object.Object) { p.Start() })
		} else {
			fi, err := f.Stat()
			if err != nil {
				cmd.PrintErrf("Failed to get file size, progress bar is disabled: %v\n", err)
				prm.SetPayloadReader(f)
			} else {
				p = pb.New64(fi.Size())
				p.Output = cmd.OutOrStdout()
				prm.SetPayloadReader(p.NewProxyReader(f))
				prm.SetHeaderCallback(func(o *object.Object) {
					p.Start()
				})
			}
		}
	}

	res, err := internalclient.PutObject(ctx, prm)
	if p != nil {
		p.Finish()
	}
	common.ExitOnErr(cmd, "rpc error: %w", err)

	cmd.Printf("[%s] Object successfully stored\n", filename)
	cmd.Printf("  OID: %s\n  CID: %s\n", res.ID(), cnr)
}

func parseObjectAttrs(cmd *cobra.Command) ([]object.Attribute, error) {
	var rawAttrs []string

	rawAttrs, err := cmd.Flags().GetStringSlice("attributes")
	common.ExitOnErr(cmd, "can't get attributes: %w", err)

	attrs := make([]object.Attribute, len(rawAttrs), len(rawAttrs)+2) // name + timestamp attributes
	for i := range rawAttrs {
		kv := strings.SplitN(rawAttrs[i], "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid attribute format: %s", rawAttrs[i])
		}
		attrs[i].SetKey(kv[0])
		attrs[i].SetValue(kv[1])
	}

	disableFilename, _ := cmd.Flags().GetBool("disable-filename")
	if !disableFilename {
		filename := filepath.Base(cmd.Flag(fileFlag).Value.String())
		index := len(attrs)
		attrs = append(attrs, object.Attribute{})
		attrs[index].SetKey(object.AttributeFileName)
		attrs[index].SetValue(filename)
	}

	disableTime, _ := cmd.Flags().GetBool("disable-timestamp")
	if !disableTime {
		index := len(attrs)
		attrs = append(attrs, object.Attribute{})
		attrs[index].SetKey(object.AttributeTimestamp)
		attrs[index].SetValue(strconv.FormatInt(time.Now().Unix(), 10))
	}

	return attrs, nil
}
