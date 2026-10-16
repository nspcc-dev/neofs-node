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
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
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
			return fmt.Errorf("incorrect container arg: %w", err)
		}

		oidsRaw, _ := cmd.Flags().GetStringSlice(commonflags.OIDFlag)

		lockList := make([]oid.ID, len(oidsRaw))

		for i := range oidsRaw {
			err = lockList[i].DecodeString(oidsRaw[i])
			if err != nil {
				return fmt.Errorf(fmt.Sprintf("Incorrect object arg #%d: %%v", i+1), err)
			}
		}

		expTime, err := parseExpirationTime(cmd)
		if err != nil {
			return err
		}
		key, err := key.GetOrGenerate(cmd)
		if err != nil {
			return err
		}

		exp, _ := cmd.Flags().GetUint64(commonflags.ExpireAt)
		lifetime, _ := cmd.Flags().GetUint64(commonflags.Lifetime)

		if lifetime != 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()

			endpoint, _ := cmd.Flags().GetString(commonflags.RPC)

			currEpoch, err := internalclient.GetCurrentEpoch(ctx, endpoint)
			if err != nil {
				return fmt.Errorf("request current epoch: %w", err)
			}

			exp = currEpoch + lifetime
		}

		if expTime.IsZero() {
			common.PrintVerbose(cmd, "Lock object will expire after %d epoch", exp)
		} else {
			common.PrintVerbose(cmd, "Lock object will expire after %s", expTime.UTC().Format(time.RFC3339))
		}

		var prm client.PrmObjectPutInit

		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}
		defer cli.Close()

		subjects, err := parseSessionSubjects(cmd, ctx, cli)
		if err != nil {
			return err
		}
		err = ReadOrOpenSessionViaClient(ctx, cmd, &prm, cli, key, subjects, cnr)
		if err != nil {
			return err
		}

		err = Prepare(cmd, &prm)
		if err != nil {
			return err
		}

		for _, locked := range lockList {
			obj := object.New(cnr, user.NewFromECDSAPublicKey(key.PublicKey))
			if expTime.IsZero() {
				obj.SetAttributes(object.NewAttribute(object.AttributeExpirationEpoch, strconv.FormatUint(exp, 10))) //nolint:staticcheck // Support legacy epoch expiration.
			} else if err := obj.SetExpirationTime(expTime); err != nil {
				return err
			}
			obj.AssociateLocked(locked)

			wrt, err := cli.ObjectPutInit(ctx, *obj, user.NewAutoIDSigner(*key), prm)
			if err != nil {
				err = fmt.Errorf("init object writing: %w", err)
			} else if err = wrt.Close(); err != nil {
				err = fmt.Errorf("finish object stream: %w", err)
			}
			if err != nil {
				return fmt.Errorf("store lock object for %s in NeoFS: %w", locked, err)
			}

			cmd.Printf("Lock object ID for %s locked object: %s\n", locked, wrt.GetResult().StoredObjectID())
		}
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
	ff.String(commonflags.ExpireAtTime, "", "Expiration time in RFC3339 format")
	objectLockCmd.MarkFlagsOneRequired(commonflags.ExpireAt, commonflags.Lifetime, commonflags.ExpireAtTime)
	objectLockCmd.MarkFlagsMutuallyExclusive(commonflags.ExpireAtTime, commonflags.ExpireAt)
	objectLockCmd.MarkFlagsMutuallyExclusive(commonflags.ExpireAtTime, commonflags.Lifetime)

	ff.StringSlice(commonflags.SessionSubjectFlag, nil, commonflags.SessionSubjectFlagUsage)
	ff.StringSlice(commonflags.SessionSubjectNNSFlag, nil, commonflags.SessionSubjectNNSFlagUsage)
}
