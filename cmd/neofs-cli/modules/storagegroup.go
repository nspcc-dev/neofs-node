package cmd

import (
	"bytes"
	"crypto/ecdsa"
	"errors"
	"fmt"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/storagegroup"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	storagegroupAPI "github.com/nspcc-dev/neofs-sdk-go/storagegroup"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

// storagegroupCmd represents the storagegroup command
var storagegroupCmd = &cobra.Command{
	Use:   "storagegroup",
	Short: "Operations with Storage Groups",
	Long:  `Operations with Storage Groups`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// bind exactly that cmd's flags to
		// the viper before execution
		commonflags.Bind(cmd)
		bindAPIFlags(cmd)
	},
}

var sgPutCmd = &cobra.Command{
	Use:   "put",
	Short: "Put storage group to NeoFS",
	Long:  "Put storage group to NeoFS",
	Run:   putSG,
}

var sgGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Get storage group from NeoFS",
	Long:  "Get storage group from NeoFS",
	Run:   getSG,
}

var sgListCmd = &cobra.Command{
	Use:   "list",
	Short: "List storage groups in NeoFS container",
	Long:  "List storage groups in NeoFS container",
	Run:   listSG,
}

var sgDelCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete storage group from NeoFS",
	Long:  "Delete storage group from NeoFS",
	Run:   delSG,
}

const (
	sgMembersFlag = "members"
	sgIDFlag      = "id"
)

var (
	sgMembers []string
	sgID      string
)

func initSGPutCmd() {
	commonflags.Init(sgPutCmd)

	flags := sgPutCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = sgPutCmd.MarkFlagRequired("cid")

	flags.StringSliceVarP(&sgMembers, sgMembersFlag, "m", nil, "ID list of storage group members")
	_ = sgPutCmd.MarkFlagRequired(sgMembersFlag)
}

func initSGGetCmd() {
	commonflags.Init(sgGetCmd)

	flags := sgGetCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = sgGetCmd.MarkFlagRequired("cid")

	flags.StringVarP(&sgID, sgIDFlag, "", "", "storage group identifier")
	_ = sgGetCmd.MarkFlagRequired(sgIDFlag)
}

func initSGListCmd() {
	commonflags.Init(sgListCmd)

	sgListCmd.Flags().String("cid", "", "Container ID")
	_ = sgListCmd.MarkFlagRequired("cid")
}

func initSGDeleteCmd() {
	commonflags.Init(sgDelCmd)

	flags := sgDelCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = sgDelCmd.MarkFlagRequired("cid")

	flags.StringVarP(&sgID, sgIDFlag, "", "", "storage group identifier")
	_ = sgDelCmd.MarkFlagRequired(sgIDFlag)
}

func init() {
	storageGroupChildCommands := []*cobra.Command{
		sgPutCmd,
		sgGetCmd,
		sgListCmd,
		sgDelCmd,
	}

	rootCmd.AddCommand(storagegroupCmd)
	storagegroupCmd.AddCommand(storageGroupChildCommands...)

	for _, sgCommand := range storageGroupChildCommands {
		flags := sgCommand.Flags()

		flags.String(bearerTokenFlag, "", "File with signed JSON or binary encoded bearer token")
		flags.StringSliceVarP(&xHeaders, xHeadersKey, xHeadersShorthand, xHeadersDefault, xHeadersUsage)
		flags.Uint32P(ttl, ttlShorthand, ttlDefault, ttlUsage)
	}

	initSGPutCmd()
	initSGGetCmd()
	initSGListCmd()
	initSGDeleteCmd()
}

type sgHeadReceiver struct {
	cmd     *cobra.Command
	key     *ecdsa.PrivateKey
	ownerID *user.ID
	prm     internalclient.HeadObjectPrm
}

func (c sgHeadReceiver) Head(addr *addressSDK.Address) (interface{}, error) {
	prepareSessionPrmWithOwner(c.cmd, addr, c.key, c.ownerID, &c.prm)
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

func putSG(cmd *cobra.Command, _ []string) {
	pk := key.GetOrGenerate(cmd)

	ownerID, err := getOwnerID(pk)
	common.ExitOnErr(cmd, "", err)

	cnr, err := getCID(cmd)
	common.ExitOnErr(cmd, "", err)

	members := make([]oidSDK.ID, len(sgMembers))

	for i := range sgMembers {
		err = members[i].DecodeString(sgMembers[i])
		common.ExitOnErr(cmd, "could not parse object ID: %w", err)
	}

	var (
		headPrm internalclient.HeadObjectPrm
		putPrm  internalclient.PutObjectPrm
	)

	sessionObjectCtxAddress := addressSDK.NewAddress()
	sessionObjectCtxAddress.SetContainerID(*cnr)
	prepareSessionPrmWithOwner(cmd, sessionObjectCtxAddress, pk, ownerID, &putPrm)
	prepareObjectPrm(cmd, &headPrm, &putPrm)

	headPrm.SetRawFlag(true)

	sg, err := storagegroup.CollectMembers(sgHeadReceiver{
		cmd:     cmd,
		key:     pk,
		ownerID: ownerID,
		prm:     headPrm,
	}, cnr, members)
	common.ExitOnErr(cmd, "could not collect storage group members: %w", err)

	sgContent, err := sg.Marshal()
	common.ExitOnErr(cmd, "could not marshal storage group: %w", err)

	obj := object.New()
	obj.SetContainerID(*cnr)
	obj.SetOwnerID(ownerID)
	obj.SetType(object.TypeStorageGroup)

	putPrm.SetHeader(obj)
	putPrm.SetPayloadReader(bytes.NewReader(sgContent))

	res, err := internalclient.PutObject(putPrm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	cmd.Println("Storage group successfully stored")
	cmd.Printf("  ID: %s\n  CID: %s\n", res.ID(), cnr)
}

func getSGID() (*oidSDK.ID, error) {
	var oid oidSDK.ID
	err := oid.DecodeString(sgID)
	if err != nil {
		return nil, fmt.Errorf("could not parse storage group ID: %w", err)
	}

	return &oid, nil
}

func getSG(cmd *cobra.Command, _ []string) {
	cnr, err := getCID(cmd)
	common.ExitOnErr(cmd, "", err)

	id, err := getSGID()
	common.ExitOnErr(cmd, "", err)

	addr := addressSDK.NewAddress()
	addr.SetContainerID(*cnr)
	addr.SetObjectID(*id)

	buf := bytes.NewBuffer(nil)

	var prm internalclient.GetObjectPrm

	prepareSessionPrm(cmd, addr, &prm)
	prepareObjectPrmRaw(cmd, &prm)
	prm.SetAddress(addr)
	prm.SetPayloadWriter(buf)

	_, err = internalclient.GetObject(prm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	var sg storagegroupAPI.StorageGroup

	err = sg.Unmarshal(buf.Bytes())
	common.ExitOnErr(cmd, "could not unmarshal storage group: %w", err)

	cmd.Printf("Expiration epoch: %d\n", sg.ExpirationEpoch())
	cmd.Printf("Group size: %d\n", sg.ValidationDataSize())
	printChecksum(cmd, "Group hash", sg.ValidationDataHash)

	if members := sg.Members(); len(members) > 0 {
		cmd.Println("Members:")

		for i := range members {
			cmd.Printf("\t%s\n", members[i].String())
		}
	}
}

func listSG(cmd *cobra.Command, _ []string) {
	cnr, err := getCID(cmd)
	common.ExitOnErr(cmd, "", err)

	var prm internalclient.SearchObjectsPrm

	sessionObjectCtxAddress := addressSDK.NewAddress()
	sessionObjectCtxAddress.SetContainerID(*cnr)
	prepareSessionPrm(cmd, sessionObjectCtxAddress, &prm)
	prepareObjectPrm(cmd, &prm)
	prm.SetContainerID(cnr)
	prm.SetFilters(storagegroup.SearchQuery())

	res, err := internalclient.SearchObjects(prm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	ids := res.IDList()

	cmd.Printf("Found %d storage groups.\n", len(ids))

	for i := range ids {
		cmd.Println(ids[i].String())
	}
}

func delSG(cmd *cobra.Command, _ []string) {
	cnr, err := getCID(cmd)
	common.ExitOnErr(cmd, "", err)

	id, err := getSGID()
	common.ExitOnErr(cmd, "", err)

	addr := addressSDK.NewAddress()
	addr.SetContainerID(*cnr)
	addr.SetObjectID(*id)

	var prm internalclient.DeleteObjectPrm

	prepareSessionPrm(cmd, addr, &prm)
	prepareObjectPrm(cmd, &prm)
	prm.SetAddress(addr)

	res, err := internalclient.DeleteObject(prm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	tombstone := res.TombstoneAddress()

	var strID string

	idTomb, ok := tombstone.ObjectID()
	if ok {
		strID = idTomb.String()
	} else {
		strID = "<empty>"
	}

	cmd.Println("Storage group removed successfully.")
	cmd.Printf("  Tombstone: %s\n", strID)
}
