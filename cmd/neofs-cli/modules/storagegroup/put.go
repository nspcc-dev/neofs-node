package storagegroup

import (
	"bytes"
	"crypto/ecdsa"
	"errors"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/object"
	sessionCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/session"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/storagegroup"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
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

	flags.String(cidFlag, "", "Container ID")
	_ = sgPutCmd.MarkFlagRequired(cidFlag)

	flags.StringSliceVarP(&sgMembers, sgMembersFlag, "m", nil, "ID list of storage group members")
	_ = sgPutCmd.MarkFlagRequired(sgMembersFlag)
}

func putSG(cmd *cobra.Command, _ []string) {
	pk := key.GetOrGenerate(cmd)

	var ownerID user.ID
	user.IDFromKey(&ownerID, pk.PublicKey)

	var cnr cid.ID
	readCID(cmd, &cnr)

	members := make([]oidSDK.ID, len(sgMembers))

	for i := range sgMembers {
		err := members[i].DecodeString(sgMembers[i])
		common.ExitOnErr(cmd, "could not parse object ID: %w", err)
	}

	var (
		headPrm internalclient.HeadObjectPrm
		putPrm  internalclient.PutObjectPrm
	)

	sessionCli.Prepare(cmd, cnr, nil, pk, &putPrm)
	objectCli.Prepare(cmd, &headPrm, &putPrm)

	headPrm.SetRawFlag(true)

	sg, err := storagegroup.CollectMembers(sgHeadReceiver{
		cmd:     cmd,
		key:     pk,
		ownerID: &ownerID,
		prm:     headPrm,
	}, cnr, members)
	common.ExitOnErr(cmd, "could not collect storage group members: %w", err)

	sgContent, err := sg.Marshal()
	common.ExitOnErr(cmd, "could not marshal storage group: %w", err)

	obj := object.New()
	obj.SetContainerID(cnr)
	obj.SetOwnerID(&ownerID)
	obj.SetType(object.TypeStorageGroup)

	putPrm.SetHeader(obj)
	putPrm.SetPayloadReader(bytes.NewReader(sgContent))

	res, err := internalclient.PutObject(putPrm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	cmd.Println("Storage group successfully stored")
	cmd.Printf("  ID: %s\n  CID: %s\n", res.ID(), cnr)
}

type sgHeadReceiver struct {
	cmd     *cobra.Command
	key     *ecdsa.PrivateKey
	ownerID *user.ID
	prm     internalclient.HeadObjectPrm
}

func (c sgHeadReceiver) Head(addr oid.Address) (interface{}, error) {
	obj := addr.Object()

	sessionCli.Prepare(c.cmd, addr.Container(), &obj, c.key, &c.prm)
	c.prm.SetAddress(addr)

	res, err := internalclient.HeadObject(c.prm)

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
