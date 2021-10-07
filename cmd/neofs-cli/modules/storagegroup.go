package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/session"
	storagegroupAPI "github.com/nspcc-dev/neofs-api-go/pkg/storagegroup"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
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
	sgBearerFlag  = "bearer"
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

		flags.String(sgBearerFlag, "", "File with signed JSON or binary encoded bearer token")
		flags.StringSliceVarP(&xHeaders, xHeadersKey, xHeadersShorthand, xHeadersDefault, xHeadersUsage)
		flags.Uint32P(ttl, ttlShorthand, ttlDefault, ttlUsage)
	}

	initSGPutCmd()
	initSGGetCmd()
	initSGListCmd()
	initSGDeleteCmd()
}

type sgHeadReceiver struct {
	ctx context.Context

	tok *session.Token

	c client.Client

	bearerToken *token.BearerToken
}

func (c *sgHeadReceiver) Head(addr *objectSDK.Address) (interface{}, error) {
	obj, err := c.c.GetObjectHeader(c.ctx,
		new(client.ObjectHeaderParams).
			WithAddress(addr).
			WithRawFlag(true),
		client.WithTTL(2),
		client.WithSession(c.tok),
		client.WithBearer(c.bearerToken),
	)

	var errSplitInfo *objectSDK.SplitInfoError

	switch {
	default:
		return nil, err
	case err == nil:
		return object.NewFromSDK(obj), nil
	case errors.As(err, &errSplitInfo):
		return errSplitInfo.SplitInfo(), nil
	}
}

func sgBearerToken(cmd *cobra.Command) (*token.BearerToken, error) {
	return getBearerToken(cmd, sgBearerFlag)
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

	bearerToken, err := sgBearerToken(cmd)
	exitOnErr(cmd, err)

	ctx := context.Background()

	cli, tok, err := initSession(ctx, key)
	exitOnErr(cmd, err)

	sg, err := storagegroup.CollectMembers(&sgHeadReceiver{
		ctx:         ctx,
		tok:         tok,
		c:           cli,
		bearerToken: bearerToken,
	}, cid, members)
	exitOnErr(cmd, errf("could not collect storage group members: %w", err))

	sgContent, err := sg.Marshal()
	exitOnErr(cmd, errf("could not marshal storage group: %w", err))

	obj := objectSDK.NewRaw()
	obj.SetContainerID(cid)
	obj.SetOwnerID(ownerID)
	obj.SetType(objectSDK.TypeStorageGroup)
	obj.SetPayload(sgContent)

	oid, err := cli.PutObject(ctx,
		new(client.PutObjectParams).
			WithObject(obj.Object()),
		append(globalCallOptions(),
			client.WithSession(tok),
			client.WithBearer(bearerToken),
		)...,
	)
	exitOnErr(cmd, errf("rpc error: %w", err))

	cmd.Println("Storage group successfully stored")
	cmd.Printf("  ID: %s\n  CID: %s\n", oid, cid)
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
	key, err := getKey()
	exitOnErr(cmd, err)

	cid, err := getCID(cmd)
	exitOnErr(cmd, err)

	id, err := getSGID()
	exitOnErr(cmd, err)

	bearerToken, err := sgBearerToken(cmd)
	exitOnErr(cmd, err)

	addr := objectSDK.NewAddress()
	addr.SetContainerID(cid)
	addr.SetObjectID(id)

	ctx := context.Background()

	cli, tok, err := initSession(ctx, key)
	exitOnErr(cmd, err)

	obj, err := cli.GetObject(ctx,
		new(client.GetObjectParams).
			WithAddress(addr),
		append(globalCallOptions(),
			client.WithSession(tok),
			client.WithBearer(bearerToken),
		)...,
	)
	exitOnErr(cmd, errf("rpc error: %w", err))

	sg := storagegroupAPI.New()

	err = sg.Unmarshal(obj.Payload())
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
	key, err := getKey()
	exitOnErr(cmd, err)

	cid, err := getCID(cmd)
	exitOnErr(cmd, err)

	bearerToken, err := sgBearerToken(cmd)
	exitOnErr(cmd, err)

	ctx := context.Background()

	cli, tok, err := initSession(ctx, key)
	exitOnErr(cmd, err)

	ids, err := cli.SearchObject(ctx,
		new(client.SearchObjectParams).
			WithContainerID(cid).
			WithSearchFilters(storagegroup.SearchQuery()),
		append(globalCallOptions(),
			client.WithSession(tok),
			client.WithBearer(bearerToken),
		)...,
	)
	exitOnErr(cmd, errf("rpc error: %w", err))

	cmd.Printf("Found %d storage groups.\n", len(ids))

	for _, id := range ids {
		cmd.Println(id)
	}
}

func delSG(cmd *cobra.Command, _ []string) {
	key, err := getKey()
	exitOnErr(cmd, err)

	cid, err := getCID(cmd)
	exitOnErr(cmd, err)

	id, err := getSGID()
	exitOnErr(cmd, err)

	bearerToken, err := sgBearerToken(cmd)
	exitOnErr(cmd, err)

	ctx := context.Background()

	cli, tok, err := initSession(ctx, key)
	exitOnErr(cmd, err)

	addr := objectSDK.NewAddress()
	addr.SetContainerID(cid)
	addr.SetObjectID(id)

	tombstone, err := client.DeleteObject(ctx, cli,
		new(client.DeleteObjectParams).
			WithAddress(addr),
		append(globalCallOptions(),
			client.WithSession(tok),
			client.WithBearer(bearerToken),
		)...,
	)
	exitOnErr(cmd, errf("rpc error: %w", err))

	cmd.Println("Storage group removed successfully.")
	cmd.Printf("  Tombstone: %s\n", tombstone.ObjectID())
}
