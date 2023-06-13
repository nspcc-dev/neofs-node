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
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
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
	_ = sgPutCmd.MarkFlagRequired(commonflags.Lifetime)
}

func putSG(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.GetOrGenerate(cmd)

	var ownerID user.ID
	err := user.IDFromSigner(&ownerID, neofsecdsa.SignerRFC6979(*pk))
	common.ExitOnErr(cmd, "decoding user from key", err)

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
	)

	cli := internalclient.GetSDKClientByFlag(ctx, cmd, pk, commonflags.RPC)
	getCnrPrm.SetClient(cli)
	getCnrPrm.SetContainer(cnr)

	resGetCnr, err := internalclient.GetContainer(ctx, getCnrPrm)
	common.ExitOnErr(cmd, "get container RPC call: %w", err)

	objectCli.OpenSessionViaClient(ctx, cmd, &putPrm, cli, pk, cnr, nil)
	objectCli.Prepare(cmd, &headPrm, &putPrm)

	headPrm.SetRawFlag(true)
	headPrm.SetClient(cli)

	sg, err := storagegroup.CollectMembers(sgHeadReceiver{
		cmd:     cmd,
		key:     pk,
		ownerID: &ownerID,
		prm:     headPrm,
	}, cnr, members, !container.IsHomomorphicHashingDisabled(resGetCnr.Container()))
	common.ExitOnErr(cmd, "could not collect storage group members: %w", err)

	var netInfoPrm internalclient.NetworkInfoPrm
	netInfoPrm.SetClient(cli)

	ni, err := internalclient.NetworkInfo(ctx, netInfoPrm)
	common.ExitOnErr(cmd, "can't fetch network info: %w", err)

	lifetime, _ := cmd.Flags().GetUint64(commonflags.Lifetime)
	sg.SetExpirationEpoch(ni.NetworkInfo().CurrentEpoch() + lifetime)

	obj := object.New()
	obj.SetContainerID(cnr)
	obj.SetOwnerID(&ownerID)

	storagegroupSDK.WriteToObject(*sg, obj)

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
	prm     internalclient.HeadObjectPrm
}

func (c sgHeadReceiver) Head(addr oid.Address) (interface{}, error) {
	c.prm.SetAddress(addr)

	res, err := internalclient.HeadObject(c.ctx, c.prm)

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
