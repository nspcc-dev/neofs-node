package cmd

import (
	"fmt"
	"strconv"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

const lockExpiresOnFlag = "expires-on"

// object lock command.
var cmdObjectLock = &cobra.Command{
	Use:   "lock CONTAINER OBJECT...",
	Short: "Lock object in container",
	Long:  "Lock object in container",
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		var cnr cid.ID

		err := cnr.DecodeString(args[0])
		common.ExitOnErr(cmd, "Incorrect container arg: %v", err)

		argsList := args[1:]

		lockList := make([]oid.ID, len(argsList))

		for i := range argsList {
			err = lockList[i].DecodeString(argsList[i])
			common.ExitOnErr(cmd, fmt.Sprintf("Incorrect object arg #%d: %%v", i+1), err)
		}

		key := key.GetOrGenerate(cmd)

		idOwner, err := getOwnerID(key)
		common.ExitOnErr(cmd, "", err)

		var lock objectSDK.Lock
		lock.WriteMembers(lockList)

		expEpoch, err := cmd.Flags().GetUint64(lockExpiresOnFlag)
		common.ExitOnErr(cmd, "Incorrect expiration epoch: %w", err)

		var expirationAttr objectSDK.Attribute
		expirationAttr.SetKey(objectV2.SysAttributeExpEpoch)
		expirationAttr.SetValue(strconv.FormatUint(expEpoch, 10))

		obj := objectSDK.New()
		obj.SetContainerID(cnr)
		obj.SetOwnerID(idOwner)
		obj.SetType(objectSDK.TypeLock)
		obj.SetAttributes(expirationAttr)
		obj.SetPayload(lock.Marshal())

		var prm internalclient.PutObjectPrm

		prepareSessionPrmWithOwner(cmd, objectcore.AddressOf(obj), key, idOwner, &prm)
		prepareObjectPrm(cmd, &prm)
		prm.SetHeader(obj)

		_, err = internalclient.PutObject(prm)
		common.ExitOnErr(cmd, "Store lock object in NeoFS: %w", err)

		cmd.Println("Objects successfully locked.")
	},
}

func initCommandObjectLock() {
	commonflags.Init(cmdObjectLock)

	cmdObjectLock.Flags().Uint64P(lockExpiresOnFlag, "e", 0, "Lock expiration epoch")
	_ = cmdObjectLock.MarkFlagRequired(lockExpiresOnFlag)
}
