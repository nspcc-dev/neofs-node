package object

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cheggaaa/pb"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectwire "github.com/nspcc-dev/neofs-node/internal/object"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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
	RunE:  putObject,
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

func putObject(cmd *cobra.Command, _ []string) error {
	binary, _ := cmd.Flags().GetBool(binaryFlag)
	cidVal, _ := cmd.Flags().GetString(commonflags.CIDFlag)

	if !binary && cidVal == "" {
		return fmt.Errorf("required flag \"%s\" not set", commonflags.CIDFlag)
	}
	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}

	var ownerID user.ID
	var cnr cid.ID

	filename, _ := cmd.Flags().GetString(fileFlag)
	f, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("can't open file '%s': %w", filename, err)
	}
	var payloadReader io.Reader = f
	obj := new(object.Object)

	if binary {
		hdrObj, payloadPrefix, err := objectwire.ReadHeaderPrefix(f)
		if err != nil {
			return fmt.Errorf("read binary object header: %w", err)
		}
		cnr = hdrObj.GetContainerID()
		ownerID = hdrObj.Owner()
		obj.SetPayloadSize(hdrObj.PayloadSize())
		payloadReader = io.MultiReader(bytes.NewReader(payloadPrefix), f)
	} else {
		fi, err := f.Stat()
		if err != nil {
			return fmt.Errorf("read file stat: %w", err)
		}

		obj.SetPayloadSize(uint64(fi.Size()))

		err = readCID(cmd, &cnr)
		if err != nil {
			return err
		}
		ownerID = user.NewFromECDSAPublicKey(pk.PublicKey)
	}

	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	attrs, err := parseObjectAttrs(cmd, ctx)
	if err != nil {
		return fmt.Errorf("can't parse object attributes: %w", err)
	}

	obj.SetContainerID(cnr)
	obj.SetOwner(ownerID)
	obj.SetAttributes(attrs...)

	var prm client.PrmObjectPutInit

	cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
	if err != nil {
		return err
	}
	defer func() { _ = cli.Close() }()

	err = ReadOrOpenSessionViaClient(ctx, cmd, &prm, cli, pk, cnr)
	if err != nil {
		return err
	}
	err = Prepare(cmd, &prm)
	if err != nil {
		return err
	}

	var p *pb.ProgressBar

	wrt, err := cli.ObjectPutInit(ctx, *obj, user.NewAutoIDSigner(*pk), prm)
	if err == nil {
		if noProgress, _ := cmd.Flags().GetBool(noProgressFlag); !noProgress {
			if binary {
				p = pb.New64(int64(obj.PayloadSize()))
				p.Output = cmd.OutOrStdout()
				p.Start()

				payloadReader = p.NewProxyReader(payloadReader)
			} else {
				fi, err := f.Stat()
				if err != nil {
					cmd.PrintErrf("Failed to get file size, progress bar is disabled: %v\n", err)
				} else {
					p = pb.New64(fi.Size())
					p.Output = cmd.OutOrStdout()
					p.Start()

					payloadReader = p.NewProxyReader(payloadReader)
				}
			}
		}

		const defaultBufferSizePut = 3 << 20 // Maximum chunk size is 3 MiB in the SDK.
		sz := obj.PayloadSize()
		if sz == 0 || sz > defaultBufferSizePut {
			sz = defaultBufferSizePut
		}
		buf := make([]byte, sz)

		if _, err = io.CopyBuffer(wrt, payloadReader, buf); err != nil {
			err = fmt.Errorf("copy data into object stream: %w", err)
		} else if err = wrt.Close(); err != nil {
			err = fmt.Errorf("finish object stream: %w", err)
		}
	} else {
		err = fmt.Errorf("init object writing: %w", err)
	}
	if p != nil {
		p.Finish()
	}
	if err != nil && !errors.Is(err, apistatus.ErrIncomplete) {
		return fmt.Errorf("rpc error: %w", err)
	}

	var statusString = "successfully"
	if errors.Is(err, apistatus.ErrIncomplete) {
		statusString = "partially (incomplete status)"
	}

	cmd.Printf("[%s] Object %s stored\n", filename, statusString)
	cmd.Printf("  OID: %s\n  CID: %s\n", wrt.GetResult().StoredObjectID(), cnr)

	return err
}

func parseObjectAttrs(cmd *cobra.Command, ctx context.Context) ([]object.Attribute, error) {
	var rawAttrs []string

	disableTime, _ := cmd.Flags().GetBool("disable-timestamp")
	disableFilename, _ := cmd.Flags().GetBool("disable-filename")

	expiresOn, _ := cmd.Flags().GetUint64(commonflags.ExpireAt)
	lifetime, _ := cmd.Flags().GetUint64(commonflags.Lifetime)

	if lifetime > 0 {
		endpoint, _ := cmd.Flags().GetString(commonflags.RPC)
		currEpoch, err := internalclient.GetCurrentEpoch(ctx, endpoint)
		if err != nil {
			return nil, fmt.Errorf("request current epoch: %w", err)
		}

		expiresOn = currEpoch + lifetime
	}
	expAttrValue := strconv.FormatUint(expiresOn, 10)
	var expAttrFound bool

	rawAttrs, err := cmd.Flags().GetStringSlice("attributes")
	if err != nil {
		return nil, fmt.Errorf("can't get attributes: %w", err)
	}

	attrs := make([]object.Attribute, len(rawAttrs), len(rawAttrs)+3) // name + timestamp + expiration epoch attributes
	for i := range rawAttrs {
		k, v, found := strings.Cut(rawAttrs[i], "=")
		if !found {
			return nil, fmt.Errorf("invalid attribute format: %s", rawAttrs[i])
		}

		if k == object.AttributeTimestamp && !disableTime {
			return nil, errors.New("can't override default timestamp attribute, use '--disable-timestamp' flag")
		}

		if k == object.AttributeFileName && !disableFilename {
			return nil, errors.New("can't override default filename attribute, use '--disable-filename' flag")
		}

		if k == object.AttributeExpirationEpoch {
			expAttrFound = true

			if expiresOn > 0 && v != expAttrValue {
				return nil, errors.New("the value of the expiration attribute and the value from '--expire-at' or '--lifetime' flags are not equal, " +
					"you need to use one of them or make them equal")
			}
		}

		if k == "" {
			return nil, errors.New("empty attribute key")
		} else if v == "" {
			return nil, fmt.Errorf("empty attribute value for key %s", k)
		}

		attrs[i].SetKey(k)
		attrs[i].SetValue(v)
	}

	if !disableFilename {
		filename := filepath.Base(cmd.Flag(fileFlag).Value.String())
		index := len(attrs)
		attrs = append(attrs, object.Attribute{})
		attrs[index].SetKey(object.AttributeFileName)
		attrs[index].SetValue(filename)
	}

	if !disableTime {
		index := len(attrs)
		attrs = append(attrs, object.Attribute{})
		attrs[index].SetKey(object.AttributeTimestamp)
		attrs[index].SetValue(strconv.FormatInt(time.Now().Unix(), 10))
	}

	if expiresOn > 0 && !expAttrFound {
		index := len(attrs)
		attrs = append(attrs, object.Attribute{})
		attrs[index].SetKey(object.AttributeExpirationEpoch)
		attrs[index].SetValue(expAttrValue)
	}

	return attrs, nil
}
