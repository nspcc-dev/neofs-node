package cmd

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	storagegroupAPI "github.com/nspcc-dev/neofs-api-go/pkg/storagegroup"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/storagegroup"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// storagegroupCmd represents the storagegroup command
var storagegroupCmd = &cobra.Command{
	Use:   "storagegroup",
	Short: "Operations with Storage Groups",
	Long:  `Operations with Storage Groups`,
}

var sgPutCmd = &cobra.Command{
	Use:   "put",
	Short: "Put storage group to NeoFS",
	Long:  "Put storage group to NeoFS",
	RunE:  putSG,
}

var sgGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Get storage group from NeoFS",
	Long:  "Get storage group from NeoFS",
	RunE:  getSG,
}

var sgListCmd = &cobra.Command{
	Use:   "list",
	Short: "List storage groups in NeoFS container",
	Long:  "List storage groups in NeoFS container",
	RunE:  listSG,
}

var sgDelCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete storage group from NeoFS",
	Long:  "Delete storage group from NeoFS",
	RunE:  delSG,
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

func init() {
	rootCmd.AddCommand(storagegroupCmd)

	storagegroupCmd.PersistentFlags().String(sgBearerFlag, "",
		"File with signed JSON or binary encoded bearer token")

	storagegroupCmd.AddCommand(sgPutCmd)
	sgPutCmd.Flags().String("cid", "", "Container ID")
	_ = sgPutCmd.MarkFlagRequired("cid")
	sgPutCmd.Flags().StringSliceVarP(&sgMembers, sgMembersFlag, "m", nil,
		"ID list of storage group members")
	_ = sgPutCmd.MarkFlagRequired(sgMembersFlag)

	storagegroupCmd.AddCommand(sgGetCmd)
	sgGetCmd.Flags().String("cid", "", "Container ID")
	_ = sgGetCmd.MarkFlagRequired("cid")
	sgGetCmd.Flags().StringVarP(&sgID, sgIDFlag, "", "", "storage group identifier")
	_ = sgGetCmd.MarkFlagRequired(sgIDFlag)

	storagegroupCmd.AddCommand(sgListCmd)
	sgListCmd.Flags().String("cid", "", "Container ID")
	_ = sgListCmd.MarkFlagRequired("cid")

	storagegroupCmd.AddCommand(sgDelCmd)
	sgDelCmd.Flags().String("cid", "", "Container ID")
	_ = sgDelCmd.MarkFlagRequired("cid")
	sgDelCmd.Flags().StringVarP(&sgID, sgIDFlag, "", "", "storage group identifier")
	_ = sgDelCmd.MarkFlagRequired(sgIDFlag)
}

type sgHeadReceiver struct {
	ctx context.Context

	tok *token.SessionToken

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

func putSG(cmd *cobra.Command, _ []string) error {
	ownerID, err := getOwnerID()
	if err != nil {
		return err
	}

	cid, err := getCID(cmd)
	if err != nil {
		return err
	}

	members := make([]*objectSDK.ID, 0, len(sgMembers))

	for i := range sgMembers {
		id := objectSDK.NewID()
		if err := id.Parse(sgMembers[i]); err != nil {
			return err
		}

		members = append(members, id)
	}

	bearerToken, err := sgBearerToken(cmd)
	if err != nil {
		return err
	}

	ctx := context.Background()

	cli, tok, err := initSession(ctx)
	if err != nil {
		return err
	}

	sg, err := storagegroup.CollectMembers(&sgHeadReceiver{
		ctx:         ctx,
		tok:         tok,
		c:           cli,
		bearerToken: bearerToken,
	}, cid, members)
	if err != nil {
		return err
	}

	sgContent, err := sg.Marshal()
	if err != nil {
		return err
	}

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
	if err != nil {
		return fmt.Errorf("can't put storage group: %w", err)
	}

	cmd.Println("Storage group successfully stored")
	cmd.Printf("  ID: %s\n  CID: %s\n", oid, cid)

	return nil
}

func getSGID() (*objectSDK.ID, error) {
	oid := objectSDK.NewID()
	err := oid.Parse(sgID)

	return oid, err
}

func getSG(cmd *cobra.Command, _ []string) error {
	cid, err := getCID(cmd)
	if err != nil {
		return err
	}

	id, err := getSGID()
	if err != nil {
		return err
	}

	bearerToken, err := sgBearerToken(cmd)
	if err != nil {
		return err
	}

	addr := objectSDK.NewAddress()
	addr.SetContainerID(cid)
	addr.SetObjectID(id)

	ctx := context.Background()

	cli, tok, err := initSession(ctx)
	if err != nil {
		return err
	}

	obj, err := cli.GetObject(ctx,
		new(client.GetObjectParams).
			WithAddress(addr),
		append(globalCallOptions(),
			client.WithSession(tok),
			client.WithBearer(bearerToken),
		)...,
	)
	if err != nil {
		return fmt.Errorf("can't get storage group: %w", err)
	}

	sg := storagegroupAPI.New()
	if err := sg.Unmarshal(obj.Payload()); err != nil {
		return err
	}

	cmd.Printf("Expiration epoch: %d\n", sg.ExpirationEpoch())
	cmd.Printf("Group size: %d\n", sg.ValidationDataSize())
	cmd.Printf("Group hash: %s\n", sg.ValidationDataHash())

	if members := sg.Members(); len(members) > 0 {
		cmd.Println("Members:")

		for i := range members {
			cmd.Printf("\t%s\n", members[i])
		}
	}

	return nil
}

func listSG(cmd *cobra.Command, _ []string) error {
	cid, err := getCID(cmd)
	if err != nil {
		return err
	}

	bearerToken, err := sgBearerToken(cmd)
	if err != nil {
		return err
	}

	ctx := context.Background()

	cli, tok, err := initSession(ctx)
	if err != nil {
		return err
	}

	ids, err := cli.SearchObject(ctx,
		new(client.SearchObjectParams).
			WithContainerID(cid).
			WithSearchFilters(storagegroup.SearchQuery()),
		append(globalCallOptions(),
			client.WithSession(tok),
			client.WithBearer(bearerToken),
		)...,
	)
	if err != nil {
		return fmt.Errorf("can't search storage groups: %w", err)
	}

	cmd.Printf("Found %d storage groups.\n", len(ids))

	for _, id := range ids {
		cmd.Println(id)
	}

	return nil
}

func delSG(cmd *cobra.Command, _ []string) error {
	cid, err := getCID(cmd)
	if err != nil {
		return err
	}

	id, err := getSGID()
	if err != nil {
		return err
	}

	bearerToken, err := sgBearerToken(cmd)
	if err != nil {
		return err
	}

	ctx := context.Background()

	cli, tok, err := initSession(ctx)
	if err != nil {
		return err
	}

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
	if err != nil {
		return fmt.Errorf("can't get storage group: %w", err)
	}

	cmd.Println("Storage group removed successfully.")
	cmd.Printf("  Tombstone: %s\n", tombstone.ObjectID())

	return nil
}
