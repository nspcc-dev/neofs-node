package fschain

import (
	"fmt"
	"math/big"
	"strconv"

	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	cnrrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	quotaCmd = &cobra.Command{
		Use:   "quota",
		Short: "Manage used space quotas",
		PersistentPreRun: func(cmd *cobra.Command, _ []string) {
			_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
			_ = viper.BindPFlag(walletFlag, cmd.Flags().Lookup(walletFlag))
			_ = viper.BindPFlag(walletAccountFlag, cmd.Flags().Lookup(walletAccountFlag))
		},
		Args: cobra.NoArgs,
	}

	quotaContainerCmd = &cobra.Command{
		Use:     "container",
		Example: "container --cid <cID> -r <endpoint> -w <wallet> [--soft] -- [value]",
		Short:   "Manage container space quota values, if <value> is missing, prints already set values",
		RunE:    quotaContainerFunc,
		Args:    quotaArgCheckFunc,
	}

	quotaUserCmd = &cobra.Command{
		Use:     "user",
		Example: "user --account <account> -r <endpoint> -w <wallet> [--soft] -- [value]",
		Short:   "Manage user space quota values, if <value> is missing, prints already set values",
		RunE:    quotaUserFunc,
		Args:    quotaArgCheckFunc,
	}
)

func quotaArgCheckFunc(_ *cobra.Command, args []string) error {
	switch len(args) {
	case 0:
		return nil
	case 1:
		_, err := strconv.Atoi(args[0])
		if err != nil {
			return fmt.Errorf("invalid (non-numeric) '%s' quota value: %w", args[0], err)
		}

		return nil
	default:
		return fmt.Errorf("accepts at most 1 arg, received %d", len(args))
	}
}

func quotaContainerFunc(cmd *cobra.Command, args []string) error {
	var cID cid.ID
	cIDString, err := cmd.Flags().GetString(containerIDFlag)
	if err != nil {
		panic(fmt.Errorf("reading %s flag: %w", containerIDFlag, err))
	}
	if err := cID.DecodeString(cIDString); err != nil {
		return fmt.Errorf("invalid container ID: %w", err)
	}

	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	if len(args) == 0 {
		inv := invoker.New(c, nil)
		nnsReader, err := nns.NewInferredReader(c, inv)
		if err != nil {
			return fmt.Errorf("can't find NNS contract: %w", err)
		}
		cnrHash, err := nnsReader.ResolveFSContract(nns.NameContainer)
		if err != nil {
			return fmt.Errorf("container contract hash resolution: %w", err)
		}
		cnrReader := cnrrpc.NewReader(inv, cnrHash)
		q, err := cnrReader.ContainerQuota(util.Uint256(cID[:]))
		if err != nil {
			return fmt.Errorf("container contract rpc call: %w", err)
		}

		cmd.Printf("%s container quotas:\nSoft limit: %d\nHard limit: %d\n", cID.EncodeToString(), q.SoftLimit, q.HardLimit)
	} else {
		v, _ := strconv.Atoi(args[0])
		vBig := big.NewInt(int64(v))

		w, err := wallet.NewWalletFromFile(viper.GetString(walletFlag))
		if err != nil {
			return fmt.Errorf("decode Neo wallet from file: %w", err)
		}
		var accAddr util.Uint160
		if strAccAddr := viper.GetString(walletAccountFlag); strAccAddr != "" {
			accAddr, err = address.StringToUint160(strAccAddr)
			if err != nil {
				return fmt.Errorf("invalid Neo account address in flag --%s: %q", walletAccountFlag, strAccAddr)
			}
		} else {
			accAddr = w.GetChangeAddress()
		}
		acc, err := unlockWallet(w, accAddr)
		if err != nil {
			return err
		}

		act, err := actor.NewSimple(c, acc)
		if err != nil {
			return fmt.Errorf("can't create actor: %w", err)
		}
		nnsReader, err := nns.NewInferredReader(c, act)
		if err != nil {
			return fmt.Errorf("can't find NNS contract: %w", err)
		}
		cnrHash, err := nnsReader.ResolveFSContract(nns.NameContainer)
		if err != nil {
			return fmt.Errorf("container contract hash resolution: %w", err)
		}

		var (
			cnrActor     = cnrrpc.New(act, cnrHash)
			softLimit, _ = cmd.Flags().GetBool(quotasSoftLimitFlag)
			txH          util.Uint256
			vub          uint32
		)
		if softLimit {
			txH, vub, err = cnrActor.SetSoftContainerQuota(util.Uint256(cID[:]), vBig)
		} else {
			txH, vub, err = cnrActor.SetHardContainerQuota(util.Uint256(cID[:]), vBig)
		}
		if err != nil {
			return fmt.Errorf("sending transaction error: %w", err)
		}
		cmd.Printf("Sent %s transaction, awaiting for results...\n", txH)

		_, err = act.WaitSuccess(cmd.Context(), txH, vub, err)
		if err != nil {
			return fmt.Errorf("transaction has not been accepted: %w", err)
		}

		cmd.Printf("New limit (%d) has been accepted, transaction: %s\n", v, txH)
	}

	return nil
}

func quotaUserFunc(cmd *cobra.Command, args []string) error {
	uString, err := cmd.Flags().GetString(walletAccountFlag)
	if err != nil {
		panic(fmt.Errorf("reading %s flag: %w", walletAccountFlag, err))
	}
	if uString == "" {
		return fmt.Errorf("user account is required")
	}
	u, err := user.DecodeString(uString)
	if err != nil {
		return fmt.Errorf("invalid user account: %w", err)
	}

	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	if len(args) == 0 {
		inv := invoker.New(c, nil)
		nnsReader, err := nns.NewInferredReader(c, inv)
		if err != nil {
			return fmt.Errorf("can't find NNS contract: %w", err)
		}
		cnrHash, err := nnsReader.ResolveFSContract(nns.NameContainer)
		if err != nil {
			return fmt.Errorf("container contract hash resolution: %w", err)
		}
		cnrReader := cnrrpc.NewReader(inv, cnrHash)
		q, err := cnrReader.UserQuota(u[:])
		if err != nil {
			return fmt.Errorf("container contract rpc call: %w", err)
		}

		cmd.Printf("%s user quotas:\nSoft limit: %d\nHard limit: %d\n", u.EncodeToString(), q.SoftLimit, q.HardLimit)
	} else {
		v, _ := strconv.Atoi(args[0])
		vBig := big.NewInt(int64(v))

		w, err := wallet.NewWalletFromFile(viper.GetString(walletFlag))
		if err != nil {
			return fmt.Errorf("decode Neo wallet from file: %w", err)
		}
		accAddr, err := address.StringToUint160(uString)
		if err != nil {
			return fmt.Errorf("invalid Neo account address in flag --%s: %q", walletAccountFlag, uString)
		}
		acc, err := unlockWallet(w, accAddr)
		if err != nil {
			return err
		}

		act, err := actor.NewSimple(c, acc)
		if err != nil {
			return fmt.Errorf("can't create actor: %w", err)
		}
		nnsReader, err := nns.NewInferredReader(c, act)
		if err != nil {
			return fmt.Errorf("can't find NNS contract: %w", err)
		}
		cnrHash, err := nnsReader.ResolveFSContract(nns.NameContainer)
		if err != nil {
			return fmt.Errorf("container contract hash resolution: %w", err)
		}

		var (
			cnrActor     = cnrrpc.New(act, cnrHash)
			softLimit, _ = cmd.Flags().GetBool(quotasSoftLimitFlag)
			txH          util.Uint256
			vub          uint32
		)
		if softLimit {
			txH, vub, err = cnrActor.SetSoftUserQuota(u[:], vBig)
		} else {
			txH, vub, err = cnrActor.SetHardUserQuota(u[:], vBig)
		}
		if err != nil {
			return fmt.Errorf("sending transaction error: %w", err)
		}
		cmd.Printf("Sent %s transaction, awaiting for results...\n", txH)

		_, err = act.WaitSuccess(cmd.Context(), txH, vub, err)
		if err != nil {
			return fmt.Errorf("transaction has not been accepted: %w", err)
		}

		cmd.Printf("New limit (%d) has been accepted, transaction: %s\n", v, txH)
	}

	return nil
}

func unlockWallet(w *wallet.Wallet, accAddr util.Uint160) (*wallet.Account, error) {
	acc := w.GetAccount(accAddr)
	if acc == nil {
		return nil, fmt.Errorf("account %s not found in the wallet", address.Uint160ToString(accAddr))
	}

	prompt := fmt.Sprintf("Enter password for %s >", address.Uint160ToString(accAddr))
	pass, err := input.ReadPassword(prompt)
	if err != nil {
		return nil, fmt.Errorf("failed to read account password: %w", err)
	}

	err = acc.Decrypt(pass, w.Scrypt)
	if err != nil {
		return nil, fmt.Errorf("failed to unlock the account with password: %w", err)
	}

	return acc, nil
}
