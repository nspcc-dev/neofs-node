package storagegroup

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/storagegroup"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	storagegroupSDK "github.com/nspcc-dev/neofs-sdk-go/storagegroup"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

const sgMembersFlag = "members"

var sgMembers []string

var sgPutCmd = &cobra.Command{
	Use:   "put",
	Short: "Put storage group to NeoFS",
	Long:  "Put storage group to NeoFS",
	Args:  cobra.NoArgs,
	Run:   putSG,
}

func initSGPutCmd() {
	commonflags.Init(sgPutCmd)

	flags := sgPutCmd.Flags()

	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = sgPutCmd.MarkFlagRequired(commonflags.CIDFlag)

	flags.StringSliceVarP(&sgMembers, sgMembersFlag, "m", nil, "ID list of storage group members")
	_ = sgPutCmd.MarkFlagRequired(sgMembersFlag)

	flags.Uint64(commonflags.Lifetime, 0, "Storage group lifetime in epochs")
	flags.Uint64P(commonflags.ExpireAt, "e", 0, "The last active epoch of the storage group")
	sgPutCmd.MarkFlagsOneRequired(commonflags.ExpireAt, commonflags.Lifetime)
}

func putSG(cmd *cobra.Command, _ []string) {
	// Track https://github.com/nspcc-dev/neofs-node/issues/2595.
	exp, _ := cmd.Flags().GetUint64(commonflags.ExpireAt)
	lifetime, _ := cmd.Flags().GetUint64(commonflags.Lifetime)
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.GetOrGenerate(cmd)

	ownerID := user.ResolveFromECDSAPublicKey(pk.PublicKey)

	var cnr cid.ID
	readCID(cmd, &cnr)

	members := make([]oid.ID, len(sgMembers))
	uniqueFilter := make(map[oid.ID]struct{}, len(sgMembers))

	for i := range sgMembers {
		err := members[i].DecodeString(sgMembers[i])
		common.ExitOnErr(cmd, "could not parse object ID: %w", err)

		if _, alreadyExists := uniqueFilter[members[i]]; alreadyExists {
			common.ExitOnErr(cmd, "", fmt.Errorf("%s member in not unique", members[i]))
		}

		uniqueFilter[members[i]] = struct{}{}
	}

	var (
		headPrm   internalclient.HeadObjectPrm
		putPrm    internalclient.PutObjectPrm
		getCnrPrm internalclient.GetContainerPrm
		getPrm    internalclient.GetObjectPrm
	)

	cli := internalclient.GetSDKClientByFlag(ctx, cmd, commonflags.RPC)
	getCnrPrm.SetClient(cli)
	getCnrPrm.SetContainer(cnr)

	resGetCnr, err := internalclient.GetContainer(ctx, getCnrPrm)
	common.ExitOnErr(cmd, "get container RPC call: %w", err)

	objectCli.OpenSessionViaClient(ctx, cmd, &putPrm, cli, pk, cnr, nil)
	objectCli.Prepare(cmd, &headPrm, &putPrm)

	headPrm.SetRawFlag(true)
	headPrm.SetClient(cli)
	headPrm.SetPrivateKey(*pk)

	headPrm.SetRawFlag(true)
	getPrm.SetClient(cli)
	getPrm.SetPrivateKey(*pk)
	objectCli.Prepare(cmd, &getPrm)

	sg, err := storagegroup.CollectMembers(sgHeadReceiver{
		ctx:     ctx,
		cmd:     cmd,
		key:     pk,
		ownerID: &ownerID,
		prmHead: headPrm,
		cli:     cli,
		getPrm:  getPrm,
	}, cnr, members, !resGetCnr.Container().IsHomomorphicHashingDisabled())
	common.ExitOnErr(cmd, "could not collect storage group members: %w", err)

	if lifetime != 0 {
		var netInfoPrm internalclient.NetworkInfoPrm
		netInfoPrm.SetClient(cli)

		ni, err := internalclient.NetworkInfo(ctx, netInfoPrm)
		common.ExitOnErr(cmd, "can't fetch network info: %w", err)
		currEpoch := ni.NetworkInfo().CurrentEpoch()
		exp = currEpoch + lifetime
	}

	sg.SetExpirationEpoch(exp)

	obj := object.New()
	obj.SetContainerID(cnr)
	obj.SetOwnerID(&ownerID)

	storagegroupSDK.WriteToObject(*sg, obj)

	putPrm.SetPrivateKey(*pk)
	putPrm.SetHeader(obj)

	res, err := internalclient.PutObject(ctx, putPrm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	cmd.Println("Storage group successfully stored")
	cmd.Printf("  ID: %s\n  CID: %s\n", res.ID(), cnr)
}

type sgHeadReceiver struct {
	ctx     context.Context
	cmd     *cobra.Command
	key     *ecdsa.PrivateKey
	ownerID *user.ID
	prmHead internalclient.HeadObjectPrm
	cli     *client.Client
	getPrm  internalclient.GetObjectPrm
}

type payloadWriter struct {
	payload []byte
}

func (pw *payloadWriter) Write(p []byte) (n int, err error) {
	pw.payload = append(pw.payload, p...)
	return len(p), nil
}

func (c sgHeadReceiver) Get(addr oid.Address) (object.Object, error) {
	pw := &payloadWriter{}
	c.getPrm.SetPayloadWriter(pw)
	c.getPrm.SetAddress(addr)

	res, err := internalclient.GetObject(c.ctx, c.getPrm)
	if err != nil {
		return object.Object{}, fmt.Errorf("rpc error: %w", err)
	}

	obj := res.Header()
	obj.SetPayload(pw.payload)

	return *obj, nil
}

func (c sgHeadReceiver) Head(addr oid.Address) (any, error) {
	c.prmHead.SetAddress(addr)

	res, err := internalclient.HeadObject(c.ctx, c.prmHead)

	var errSplitInfo *object.SplitInfoError

	switch {
	default:
		return nil, err
	case err == nil:
		return res.Header(), nil
	case errors.As(err, &errSplitInfo):
		return errSplitInfo.SplitInfo(), nil
	}
}
