package cmd

import (
	"bytes"
	"errors"
	"fmt"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	storagegroupAPI "github.com/nspcc-dev/neofs-api-go/pkg/storagegroup"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/storagegroup"
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
		bindCommonFlags(cmd)
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
	initCommonFlags(sgPutCmd)

	flags := sgPutCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = sgPutCmd.MarkFlagRequired("cid")

	flags.StringSliceVarP(&sgMembers, sgMembersFlag, "m", nil, "ID list of storage group members")
	_ = sgPutCmd.MarkFlagRequired(sgMembersFlag)
}

func initSGGetCmd() {
	initCommonFlags(sgGetCmd)

	flags := sgGetCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = sgGetCmd.MarkFlagRequired("cid")

	flags.StringVarP(&sgID, sgIDFlag, "", "", "storage group identifier")
	_ = sgGetCmd.MarkFlagRequired(sgIDFlag)
}

func initSGListCmd() {
	initCommonFlags(sgListCmd)

	sgListCmd.Flags().String("cid", "", "Container ID")
	_ = sgListCmd.MarkFlagRequired("cid")
}

func initSGDeleteCmd() {
	initCommonFlags(sgDelCmd)

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
	prm internalclient.HeadObjectPrm
}

func (c sgHeadReceiver) Head(addr *objectSDK.Address) (interface{}, error) {
	c.prm.SetAddress(addr)

	res, err := internalclient.HeadObject(c.prm)

	var errSplitInfo *objectSDK.SplitInfoError

	switch {
	default:
		return nil, err
	case err == nil:
		return object.NewFromSDK(res.Header()), nil
	case errors.As(err, &errSplitInfo):
		return errSplitInfo.SplitInfo(), nil
	}
}

func putSG(cmd *cobra.Command, _ []string) {
	key, err := getKey()
	exitOnErr(cmd, err)

	ownerID, err := getOwnerID(key)
	exitOnErr(cmd, err)

	cid, err := getCID(cmd)
	exitOnErr(cmd, err)

	members := make([]*objectSDK.ID, 0, len(sgMembers))

	for i := range sgMembers {
		id := objectSDK.NewID()

		err = id.Parse(sgMembers[i])
		exitOnErr(cmd, errf("could not parse object ID: %w", err))

		members = append(members, id)
	}

	var (
		headPrm internalclient.HeadObjectPrm
		putPrm  internalclient.PutObjectPrm
	)

	prepareSessionPrmWithOwner(cmd, key, ownerID, &headPrm, &putPrm)
	prepareObjectPrm(cmd, &headPrm, &putPrm)

	headPrm.SetRawFlag(true)

	sg, err := storagegroup.CollectMembers(sgHeadReceiver{
		prm: headPrm,
	}, cid, members)
	exitOnErr(cmd, errf("could not collect storage group members: %w", err))

	sgContent, err := sg.Marshal()
	exitOnErr(cmd, errf("could not marshal storage group: %w", err))

	obj := objectSDK.NewRaw()
	obj.SetContainerID(cid)
	obj.SetOwnerID(ownerID)
	obj.SetType(objectSDK.TypeStorageGroup)

	putPrm.SetHeader(obj.Object())
	putPrm.SetPayloadReader(bytes.NewReader(sgContent))

	res, err := internalclient.PutObject(putPrm)
	exitOnErr(cmd, errf("rpc error: %w", err))

	cmd.Println("Storage group successfully stored")
	cmd.Printf("  ID: %s\n  CID: %s\n", res.ID(), cid)
}

func getSGID() (*objectSDK.ID, error) {
	oid := objectSDK.NewID()
	err := oid.Parse(sgID)
	if err != nil {
		return nil, fmt.Errorf("could not parse storage group ID: %w", err)
	}

	return oid, nil
}

func getSG(cmd *cobra.Command, _ []string) {
	cid, err := getCID(cmd)
	exitOnErr(cmd, err)

	id, err := getSGID()
	exitOnErr(cmd, err)

	addr := objectSDK.NewAddress()
	addr.SetContainerID(cid)
	addr.SetObjectID(id)

	buf := bytes.NewBuffer(nil)

	var prm internalclient.GetObjectPrm

	prepareSessionPrm(cmd, &prm)
	prepareObjectPrmRaw(cmd, &prm)
	prm.SetAddress(addr)
	prm.SetPayloadWriter(buf)

	_, err = internalclient.GetObject(prm)
	exitOnErr(cmd, errf("rpc error: %w", err))

	sg := storagegroupAPI.New()

	err = sg.Unmarshal(buf.Bytes())
	exitOnErr(cmd, errf("could not unmarshal storage group: %w", err))

	cmd.Printf("Expiration epoch: %d\n", sg.ExpirationEpoch())
	cmd.Printf("Group size: %d\n", sg.ValidationDataSize())
	cmd.Printf("Group hash: %s\n", sg.ValidationDataHash())

	if members := sg.Members(); len(members) > 0 {
		cmd.Println("Members:")

		for i := range members {
			cmd.Printf("\t%s\n", members[i])
		}
	}
}

func listSG(cmd *cobra.Command, _ []string) {
	cid, err := getCID(cmd)
	exitOnErr(cmd, err)

	var prm internalclient.SearchObjectsPrm

	prepareSessionPrm(cmd, &prm)
	prepareObjectPrm(cmd, &prm)
	prm.SetContainerID(cid)
	prm.SetFilters(storagegroup.SearchQuery())

	res, err := internalclient.SearchObjects(prm)
	exitOnErr(cmd, errf("rpc error: %w", err))

	ids := res.IDList()

	cmd.Printf("Found %d storage groups.\n", len(ids))

	for _, id := range ids {
		cmd.Println(id)
	}
}

func delSG(cmd *cobra.Command, _ []string) {
	cid, err := getCID(cmd)
	exitOnErr(cmd, err)

	id, err := getSGID()
	exitOnErr(cmd, err)

	addr := objectSDK.NewAddress()
	addr.SetContainerID(cid)
	addr.SetObjectID(id)

	var prm internalclient.DeleteObjectPrm

	prepareSessionPrm(cmd, &prm)
	prepareObjectPrm(cmd, &prm)
	prm.SetAddress(addr)

	res, err := internalclient.DeleteObject(prm)
	exitOnErr(cmd, errf("rpc error: %w", err))

	tombstone := res.TombstoneAddress()

	cmd.Println("Storage group removed successfully.")
	cmd.Printf("  Tombstone: %s\n", tombstone.ObjectID())
}
