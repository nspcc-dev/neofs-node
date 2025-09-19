package fschain

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	nnsrpc "github.com/nspcc-dev/neofs-contract/rpc/nns"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// as described in NEP-18 Specification https://github.com/neo-project/proposals/pull/133
const nnsNeoAddressTextRecordPrefix = "address="

// tokenNotFound is the exception returned from NNS contract for unregistered domains.
const tokenNotFound = "token not found"

func verifiedNodesDomainAccessList(cmd *cobra.Command, _ []string) error {
	vpr := viper.GetViper()

	n3Client, err := getN3Client(vpr)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}

	nnsContractAddr, err := nnsrpc.InferHash(n3Client)
	if err != nil {
		return fmt.Errorf("get NeoFS NNS contract address: %w", err)
	}

	domain := vpr.GetString(domainFlag)
	nnsContract := nnsrpc.NewReader(invoker.New(n3Client, nil), nnsContractAddr)

	records, err := nnsContract.Resolve(domain, nnsrpc.TXT)
	if err != nil {
		var ex unwrap.Exception
		if errors.As(err, &ex) && strings.Contains(string(ex), tokenNotFound) {
			cmd.Println("Domain not found.")
			return nil
		}

		return fmt.Errorf("get all text records of the NNS domain %q: %w", domain, err)
	}

	if len(records) == 0 {
		cmd.Println("List is empty.")
		return nil
	}

	for i := range records {
		neoAddr := strings.TrimPrefix(records[i], nnsNeoAddressTextRecordPrefix)
		if len(neoAddr) == len(records[i]) {
			cmd.Printf("%s (not a Neo address)\n", records[i])
			continue
		}

		cmd.Println(neoAddr)
	}

	return nil
}

func verifiedNodesDomainSetAccessList(cmd *cobra.Command, _ []string) error {
	vpr := viper.GetViper()

	strNeoAddresses := vpr.GetStringSlice(neoAddressesFlag)
	strPublicKeys := vpr.GetStringSlice(publicKeysFlag)
	if len(strNeoAddresses)*len(strPublicKeys) != 0 {
		// just to make sure
		panic("mutually exclusive flags bypassed Cobra")
	}

	var err error
	var additionalRecords []string

	if len(strNeoAddresses) > 0 {
		for i := range strNeoAddresses {
			for j := i + 1; j < len(strNeoAddresses); j++ {
				if strNeoAddresses[i] == strNeoAddresses[j] {
					return fmt.Errorf("duplicated Neo address %s", strNeoAddresses[i])
				}
			}

			_, err = address.StringToUint160(strNeoAddresses[i])
			if err != nil {
				return fmt.Errorf("address #%d is invalid: %w", i, err)
			}

			additionalRecords = append(additionalRecords, nnsNeoAddressTextRecordPrefix+strNeoAddresses[i])
		}
	} else {
		additionalRecords = make([]string, len(strPublicKeys))

		for i := range strPublicKeys {
			for j := i + 1; j < len(strPublicKeys); j++ {
				if strPublicKeys[i] == strPublicKeys[j] {
					return fmt.Errorf("duplicated public key %s", strPublicKeys[i])
				}
			}

			pubKey, err := keys.NewPublicKeyFromString(strPublicKeys[i])
			if err != nil {
				return fmt.Errorf("public key #%d is not a HEX-encoded public key: %w", i, err)
			}

			additionalRecords[i] = nnsNeoAddressTextRecordPrefix + address.Uint160ToString(pubKey.GetScriptHash())
		}
	}

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

	acc := w.GetAccount(accAddr)
	if acc == nil {
		return fmt.Errorf("account %s not found in the wallet", address.Uint160ToString(accAddr))
	}

	prompt := fmt.Sprintf("Enter password for %s >", address.Uint160ToString(accAddr))
	pass, err := input.ReadPassword(prompt)
	if err != nil {
		return fmt.Errorf("failed to read account password: %w", err)
	}

	err = acc.Decrypt(pass, keys.NEP2ScryptParams())
	if err != nil {
		return fmt.Errorf("failed to unlock the account with password: %w", err)
	}

	n3Client, err := getN3Client(vpr)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}

	nnsContractAddr, err := nnsrpc.InferHash(n3Client)
	if err != nil {
		return fmt.Errorf("get NeoFS NNS contract address: %w", err)
	}

	actr, err := actor.NewSimple(n3Client, acc)
	if err != nil {
		return fmt.Errorf("init committee actor: %w", err)
	}

	nnsContract := nnsrpc.New(actr, nnsContractAddr)
	domain := vpr.GetString(domainFlag)
	scriptBuilder := smartcontract.NewBuilder()

	records, err := nnsContract.Resolve(domain, nnsrpc.TXT)
	if err != nil {
		var ex unwrap.Exception
		if !errors.As(err, &ex) || !strings.Contains(string(ex), tokenNotFound) {
			return fmt.Errorf("get all text records of the NNS domain %q: %w", domain, err)
		}

		domainToRegister := domain
		if labels := strings.Split(domainToRegister, "."); len(labels) > 2 {
			// we need explicitly register L2 domain like 'some-org.neofs'
			// and then just add records to inferior domains
			domainToRegister = labels[len(labels)-2] + "." + labels[len(labels)-1]
		}

		scriptBuilder.InvokeMethod(nnsContractAddr, "register",
			domainToRegister, acc.ScriptHash(), "ops@nspcc.ru", int64(3600), int64(600), int64(defaultExpirationTime), int64(3600))
	}

	hasOtherRecord := false
	mAlreadySetIndices := make(map[int]struct{}, len(additionalRecords))

	for i := range records {
		if slices.Contains(additionalRecords, records[i]) {
			mAlreadySetIndices[i] = struct{}{}
			continue
		}

		hasOtherRecord = true

		break
	}

	if !hasOtherRecord && len(mAlreadySetIndices) == len(additionalRecords) {
		cmd.Println("Current list is already the same, skip.")
		return nil
	}

	if hasOtherRecord {
		// there is no way to delete particular record, so clean all first
		scriptBuilder.InvokeMethod(nnsContractAddr, "deleteRecords",
			domain, nnsrpc.TXT.Int64())
	}

	for i := range additionalRecords {
		if !hasOtherRecord {
			if _, ok := mAlreadySetIndices[i]; ok {
				continue
			}
		}

		scriptBuilder.InvokeMethod(nnsContractAddr, "addRecord",
			domain, nnsrpc.TXT.Int64(), additionalRecords[i])
	}

	txScript, err := scriptBuilder.Script()
	if err != nil {
		return fmt.Errorf("build transaction script: %w", err)
	}

	txh, vub, err := actr.SendRun(txScript)
	_, err = actr.Wait(context.TODO(), txh, vub, err)
	if err != nil {
		return fmt.Errorf("send transction with built script and wait for it to be accepted: %w", err)
	}

	cmd.Println("Access list has been successfully updated.")

	return nil
}
