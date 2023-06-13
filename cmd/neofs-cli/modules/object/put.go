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
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

const (
	noProgressFlag   = "no-progress"
	notificationFlag = "notify"
)

var putExpiredOn uint64

var objectPutCmd = &cobra.Command{
	Use:   "put",
	Short: "Put object to NeoFS",
	Long:  "Put object to NeoFS",
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

	flags.String("attributes", "", "User attributes in form of Key1=Value1,Key2=Value2")
	flags.Bool("disable-filename", false, "Do not set well-known filename attribute")
	flags.Bool("disable-timestamp", false, "Do not set well-known timestamp attribute")
	flags.Uint64VarP(&putExpiredOn, commonflags.ExpireAt, "e", 0, "The last active epoch in the life of the object")
	flags.Bool(noProgressFlag, false, "Do not show progress bar")

	flags.String(notificationFlag, "", "Object notification in the form of *epoch*:*topic*; '-' topic means using default")
	flags.Bool(binaryFlag, false, "Deserialize object structure from given file.")
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
		readCID(cmd, &cnr)
		err = user.IDFromSigner(&ownerID, neofsecdsa.SignerRFC6979(*pk))
		common.ExitOnErr(cmd, "decoding user from key", err)
	}

	attrs, err := parseObjectAttrs(cmd)
	common.ExitOnErr(cmd, "can't parse object attributes: %w", err)

	expiresOn, _ := cmd.Flags().GetUint64(commonflags.ExpireAt)
	if expiresOn > 0 {
		var expAttrFound bool
		expAttrValue := strconv.FormatUint(expiresOn, 10)

		for i := range attrs {
			if attrs[i].Key() == objectV2.SysAttributeExpEpoch {
				attrs[i].SetValue(expAttrValue)
				expAttrFound = true
				break
			}
		}

		if !expAttrFound {
			index := len(attrs)
			attrs = append(attrs, object.Attribute{})
			attrs[index].SetKey(objectV2.SysAttributeExpEpoch)
			attrs[index].SetValue(expAttrValue)
		}
	}

	obj.SetContainerID(cnr)
	obj.SetOwnerID(&ownerID)
	obj.SetAttributes(attrs...)

	notificationInfo, err := parseObjectNotifications(cmd)
	common.ExitOnErr(cmd, "can't parse object notification information: %w", err)

	if notificationInfo != nil {
		obj.SetNotification(*notificationInfo)
	}

	var prm internalclient.PutObjectPrm
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

	raw := cmd.Flag("attributes").Value.String()
	if len(raw) != 0 {
		rawAttrs = strings.Split(raw, ",")
	}

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

func parseObjectNotifications(cmd *cobra.Command) (*object.NotificationInfo, error) {
	const (
		separator       = ":"
		useDefaultTopic = "-"
	)

	raw := cmd.Flag(notificationFlag).Value.String()
	if raw == "" {
		return nil, nil
	}

	rawSlice := strings.SplitN(raw, separator, 2)
	if len(rawSlice) != 2 {
		return nil, fmt.Errorf("notification must be in the form of: *epoch*%s*topic*, got %s", separator, raw)
	}

	ni := new(object.NotificationInfo)

	epoch, err := strconv.ParseUint(rawSlice[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("could not parse notification epoch %s: %w", rawSlice[0], err)
	}

	ni.SetEpoch(epoch)

	if rawSlice[1] == "" {
		return nil, fmt.Errorf("incorrect empty topic: use %s to force using default topic", useDefaultTopic)
	}

	if rawSlice[1] != useDefaultTopic {
		ni.SetTopic(rawSlice[1])
	}

	return ni, nil
}
