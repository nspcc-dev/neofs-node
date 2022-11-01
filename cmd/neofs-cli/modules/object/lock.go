package object

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

// object lock command.
var objectLockCmd = &cobra.Command{
	Use:   "lock",
	Short: "Lock object in container",
	Long:  "Lock object in container",
	Run: func(cmd *cobra.Command, _ []string) {
		cidRaw, _ := cmd.Flags().GetString("cid")

		var cnr cid.ID
		err := cnr.DecodeString(cidRaw)
		common.ExitOnErr(cmd, "Incorrect container arg: %v", err)

		oidsRaw, _ := cmd.Flags().GetStringSlice("oid")

		lockList := make([]oid.ID, len(oidsRaw))

		for i := range oidsRaw {
			err = lockList[i].DecodeString(oidsRaw[i])
			common.ExitOnErr(cmd, fmt.Sprintf("Incorrect object arg #%d: %%v", i+1), err)
		}

		key := key.GetOrGenerate(cmd)

		var idOwner user.ID
		user.IDFromKey(&idOwner, key.PublicKey)

		var lock objectSDK.Lock
		lock.WriteMembers(lockList)

		exp, _ := cmd.Flags().GetUint64(commonflags.ExpireAt)
		lifetime, _ := cmd.Flags().GetUint64(commonflags.Lifetime)
		if exp == 0 && lifetime == 0 { // mutual exclusion is ensured by cobra
			common.ExitOnErr(cmd, "", errors.New("either expiration epoch of a lifetime is required"))
		}

		if lifetime != 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()

			endpoint, _ := cmd.Flags().GetString(commonflags.RPC)

			currEpoch, err := internalclient.GetCurrentEpoch(ctx, endpoint)
			common.ExitOnErr(cmd, "Request current epoch: %w", err)

			exp += currEpoch
		}

		var expirationAttr objectSDK.Attribute
		expirationAttr.SetKey(objectV2.SysAttributeExpEpoch)
		expirationAttr.SetValue(strconv.FormatUint(exp, 10))

		obj := objectSDK.New()
		obj.SetContainerID(cnr)
		obj.SetOwnerID(&idOwner)
		obj.SetType(objectSDK.TypeLock)
		obj.SetAttributes(expirationAttr)
		obj.SetPayload(lock.Marshal())

		var prm internalclient.PutObjectPrm
		ReadOrOpenSession(cmd, &prm, key, cnr, nil)
		Prepare(cmd, &prm)
		prm.SetHeader(obj)

		res, err := internalclient.PutObject(prm)
		common.ExitOnErr(cmd, "Store lock object in NeoFS: %w", err)

		cmd.Printf("Lock object ID: %s\n", res.ID())
		cmd.Println("Objects successfully locked.")
	},
}

func initCommandObjectLock() {
	commonflags.Init(objectLockCmd)
	initFlagSession(objectLockCmd, "PUT")

	ff := objectLockCmd.Flags()

	ff.String("cid", "", "Container ID")
	_ = objectLockCmd.MarkFlagRequired("cid")

	ff.StringSlice("oid", nil, "Object ID")
	_ = objectLockCmd.MarkFlagRequired("oid")

	ff.Uint64P(commonflags.ExpireAt, "e", 0, "Lock expiration epoch")

	ff.Uint64(commonflags.Lifetime, 0, "Lock lifetime")
	objectLockCmd.MarkFlagsMutuallyExclusive(commonflags.ExpireAt, commonflags.Lifetime)
}
