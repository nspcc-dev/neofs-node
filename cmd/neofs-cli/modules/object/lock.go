package object

import (
	"context"
	"fmt"
	"strconv"
	"time"

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

var objectLockCmd = &cobra.Command{
	Use:   "lock",
	Short: "Lock object in container",
	Long:  "Lock object in container",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		cidRaw, _ := cmd.Flags().GetString(commonflags.CIDFlag)

		var cnr cid.ID
		err := cnr.DecodeString(cidRaw)
		if err != nil {
			return fmt.Errorf("Incorrect container arg: %w", err)
		}

		oidsRaw, _ := cmd.Flags().GetStringSlice(commonflags.OIDFlag)

		lockList := make([]oid.ID, len(oidsRaw))

		for i := range oidsRaw {
			err = lockList[i].DecodeString(oidsRaw[i])
			if err != nil {
				return fmt.Errorf(fmt.Sprintf("Incorrect object arg #%d: %%v", i+1), err)
			}
		}

		key, err := key.GetOrGenerate(cmd)
		if err != nil {
			return err
		}

		var lock objectSDK.Lock
		lock.WriteMembers(lockList)

		exp, _ := cmd.Flags().GetUint64(commonflags.ExpireAt)
		lifetime, _ := cmd.Flags().GetUint64(commonflags.Lifetime)

		if lifetime != 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()

			endpoint, _ := cmd.Flags().GetString(commonflags.RPC)

			currEpoch, err := internalclient.GetCurrentEpoch(ctx, endpoint)
			if err != nil {
				return fmt.Errorf("Request current epoch: %w", err)
			}

			exp = currEpoch + lifetime
		}

		common.PrintVerbose(cmd, "Lock object will expire after %d epoch", exp)

		var expirationAttr objectSDK.Attribute
		expirationAttr.SetKey(objectSDK.AttributeExpirationEpoch)
		expirationAttr.SetValue(strconv.FormatUint(exp, 10))

		obj := objectSDK.New()
		obj.SetContainerID(cnr)
		obj.SetOwner(user.NewFromECDSAPublicKey(key.PublicKey))
		obj.SetType(objectSDK.TypeLock)
		obj.SetAttributes(expirationAttr)
		obj.SetPayload(lock.Marshal())

		var prm internalclient.PutObjectPrm
		prm.SetPrivateKey(*key)

		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}
		defer cli.Close()

		err = ReadOrOpenSessionViaClient(ctx, cmd, &prm, cli, key, cnr)
		if err != nil {
			return err
		}

		err = Prepare(cmd, &prm)
		if err != nil {
			return err
		}
		prm.SetHeader(obj)

		res, err := internalclient.PutObject(ctx, prm)
		if err != nil {
			return fmt.Errorf("Store lock object in NeoFS: %w", err)
		}

		cmd.Printf("Lock object ID: %s\n", res.ID())
		cmd.Println("Objects successfully locked.")

		return nil
	},
}

func initCommandObjectLock() {
	commonflags.Init(objectLockCmd)
	initFlagSession(objectLockCmd, "PUT")

	ff := objectLockCmd.Flags()

	ff.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = objectLockCmd.MarkFlagRequired(commonflags.CIDFlag)

	ff.StringSlice(commonflags.OIDFlag, nil, commonflags.OIDFlagUsage)
	_ = objectLockCmd.MarkFlagRequired(commonflags.OIDFlag)

	ff.Uint64P(commonflags.ExpireAt, "e", 0, "The last active epoch for the lock")

	ff.Uint64(commonflags.Lifetime, 0, "Lock lifetime")
	objectLockCmd.MarkFlagsOneRequired(commonflags.ExpireAt, commonflags.Lifetime)
}
