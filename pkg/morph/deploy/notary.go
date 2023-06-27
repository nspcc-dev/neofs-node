package deploy

import (
	"bytes"
	"context"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/core/mempoolevent"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/gas"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nns"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/rolemgmt"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	randutil "github.com/nspcc-dev/neofs-node/pkg/util/rand"
	"go.uber.org/zap"
)

// enableNotaryPrm groups parameters of Notary service initialization parameters
// for the committee.
type enableNotaryPrm struct {
	logger *zap.Logger

	blockchain Blockchain

	// based on blockchain
	monitor *blockchainMonitor

	nnsOnChainAddress util.Uint160
	systemEmail       string

	committee              keys.PublicKeys
	localAcc               *wallet.Account
	localAccCommitteeIndex int
}

// enableNotary makes Notary service ready-to-go for the committee members.
func enableNotary(ctx context.Context, prm enableNotaryPrm) error {
	var tick func()
	var err error

	if len(prm.committee) == 1 {
		prm.logger.Info("committee is single-acc, no multi-signature needed for Notary role designation")

		tick, err = initDesignateNotaryRoleToLocalAccountTick(prm)
		if err != nil {
			return fmt.Errorf("construct action designating Notary role to the local account: %w", err)
		}
	} else {
		prm.logger.Info("committee is multi-acc, multi-signature is needed for Notary role designation")

		if prm.localAccCommitteeIndex == 0 {
			tick, err = initDesignateNotaryRoleAsLeaderTick(prm)
			if err != nil {
				return fmt.Errorf("construct action designating Notary role to the multi-acc committee as leader: %w", err)
			}
		} else {
			tick, err = initDesignateNotaryRoleAsSignerTick(prm)
			if err != nil {
				return fmt.Errorf("construct action designating Notary role to the multi-acc committee as signer: %w", err)
			}
		}
	}

	roleContract := rolemgmt.NewReader(invoker.New(prm.blockchain, nil))

	for ; ; prm.monitor.waitForNextBlock(ctx) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Notary service to be enabled for the committee: %w", ctx.Err())
		default:
		}

		prm.logger.Info("checking Notary role of the committee members...")

		accsWithNotaryRole, err := roleContract.GetDesignatedByRole(noderoles.P2PNotary, prm.monitor.currentHeight())
		if err != nil {
			prm.logger.Error("failed to check role of the committee, will try again later", zap.Error(err))
			continue
		}

		someoneWithoutNotaryRole := len(accsWithNotaryRole) < len(prm.committee)
		if !someoneWithoutNotaryRole {
			for i := range prm.committee {
				if !accsWithNotaryRole.Contains(prm.committee[i]) {
					someoneWithoutNotaryRole = true
					break
				}
			}
		}
		if !someoneWithoutNotaryRole {
			prm.logger.Info("all committee members have a Notary role")
			return nil
		}

		prm.logger.Info("not all members of the committee have a Notary role, designation is needed")

		tick()
	}
}

// initDesignateNotaryRoleToLocalAccountTick returns a function that preserves
// context of the Notary role designation to the local account between calls.
func initDesignateNotaryRoleToLocalAccountTick(prm enableNotaryPrm) (func(), error) {
	localActor, err := actor.NewSimple(prm.blockchain, prm.localAcc)
	if err != nil {
		return nil, fmt.Errorf("init transaction sender from local account: %w", err)
	}

	roleContract := rolemgmt.New(localActor)

	// multi-tick context
	var sentTxValidUntilBlock uint32

	return func() {
		if sentTxValidUntilBlock > 0 && sentTxValidUntilBlock <= prm.monitor.currentHeight() {
			prm.logger.Info("previously sent transaction designating Notary role to the local account may still be relevant, will wait for the outcome")
			return
		}

		if sentTxValidUntilBlock > 0 {
			prm.logger.Info("transaction designating Notary role to the local account was sent earlier, checking relevance...")

			if cur := prm.monitor.currentHeight(); cur <= sentTxValidUntilBlock {
				prm.logger.Info("previously sent transaction designating Notary role to the local account may still be relevant, will wait for the outcome",
					zap.Uint32("current height", cur), zap.Uint32("retry after height", sentTxValidUntilBlock))
				return
			}

			prm.logger.Info("previously sent transaction designating Notary role to the local account expired without side-effect")
		}

		prm.logger.Info("sending new transaction designating Notary role to the local account...")

		var err error

		_, vub, err := roleContract.DesignateAsRole(noderoles.P2PNotary, keys.PublicKeys{prm.localAcc.PublicKey()})
		if err != nil {
			sentTxValidUntilBlock = 0
			if isErrNotEnoughGAS(err) {
				prm.logger.Info("not enough GAS to designate Notary role to the local account, will try again later")
			} else {
				prm.logger.Error("failed to send transaction designating Notary role to the local account, will try again later", zap.Error(err))
			}
			return
		}

		sentTxValidUntilBlock = vub

		prm.logger.Info("transaction designating Notary role to the local account has been successfully sent, will wait for the outcome")
	}, nil
}

// initDesignateNotaryRoleAsLeaderTick returns a function that preserves context
// of the Notary role designation to the multi-acc committee between calls. The
// operation is performed by the leading committee member which is assigned to
// collect signatures for the corresponding transaction.
func initDesignateNotaryRoleAsLeaderTick(prm enableNotaryPrm) (func(), error) {
	committeeMultiSigM := smartcontract.GetMajorityHonestNodeCount(len(prm.committee))
	committeeMultiSigAcc := wallet.NewAccountFromPrivateKey(prm.localAcc.PrivateKey())

	err := committeeMultiSigAcc.ConvertMultisig(committeeMultiSigM, prm.committee)
	if err != nil {
		return nil, fmt.Errorf("compose committee multi-signature account: %w", err)
	}

	localActor, err := actor.NewSimple(prm.blockchain, prm.localAcc)
	if err != nil {
		return nil, fmt.Errorf("init transaction sender from local account: %w", err)
	}

	committeeSigners := []actor.SignerAccount{
		{
			Signer: transaction.Signer{
				Account: prm.localAcc.ScriptHash(),
				Scopes:  transaction.None,
			},
			Account: prm.localAcc,
		},
		{
			Signer: transaction.Signer{
				Account: committeeMultiSigAcc.ScriptHash(),
				Scopes:  transaction.CalledByEntry,
			},
			Account: committeeMultiSigAcc,
		},
	}

	committeeActor, err := actor.New(prm.blockchain, committeeSigners)
	if err != nil {
		return nil, fmt.Errorf("init transaction sender with committee signers: %w", err)
	}

	invkr := invoker.New(prm.blockchain, nil)
	roleContract := rolemgmt.New(committeeActor)

	// multi-tick context
	var registerDomainTxValidUntilBlock uint32
	var setDomainRecordTxValidUntilBlock uint32
	var tx *transaction.Transaction
	var mCommitteeIndexToSignature map[int][]byte
	var designateRoleTxValidUntilBlock uint32
	var txFullySigned bool

	resetTx := func() {
		tx = nil
		setDomainRecordTxValidUntilBlock = 0
		for k := range mCommitteeIndexToSignature {
			delete(mCommitteeIndexToSignature, k)
		}
		designateRoleTxValidUntilBlock = 0
		txFullySigned = false
	}

	return func() {
		l := prm.logger.With(zap.String("domain", domainDesignateNotaryTx))

		l.Info("synchronizing shared data of the transaction designating Notary role to the committee with NNS domain record...")

		var sharedTxData sharedTransactionData

		generateAndShareTxData := func(recordExists bool) {
			resetTx()

			prm.logger.Info("generating shared data for the transaction designating Notary role to the committee...")

			ver, err := prm.blockchain.GetVersion()
			if err != nil {
				prm.logger.Error("failed request Neo protocol configuration, will try again later", zap.Error(err))
				return
			}

			// localActor.CalculateValidUntilBlock is not used because it is rather "idealized"
			// in terms of the accessibility of committee member nodes. So, we need a more
			// practically viable timeout to reduce the chance of transaction re-creation.
			const defaultValidUntilBlockIncrement = 120 // ~30m for 15s block interval
			var txValidUntilBlock uint32

			if defaultValidUntilBlockIncrement <= ver.Protocol.MaxValidUntilBlockIncrement {
				txValidUntilBlock = prm.monitor.currentHeight() + defaultValidUntilBlockIncrement
			} else {
				txValidUntilBlock = prm.monitor.currentHeight() + ver.Protocol.MaxValidUntilBlockIncrement
			}

			strSharedTxData := sharedTransactionData{
				sender:          localActor.Sender(),
				validUntilBlock: txValidUntilBlock,
				nonce:           randutil.Uint32(),
			}.encodeToString()

			l.Info("sending new transaction setting domain record in the NNS...")

			var vub uint32
			if recordExists {
				_, vub, err = localActor.SendCall(prm.nnsOnChainAddress, methodNNSSetRecord,
					domainDesignateNotaryTx, int64(nns.TXT), 0, strSharedTxData)
			} else {
				_, vub, err = localActor.SendCall(prm.nnsOnChainAddress, methodNNSAddRecord,
					domainDesignateNotaryTx, int64(nns.TXT), strSharedTxData)
			}
			if err != nil {
				setDomainRecordTxValidUntilBlock = 0
				if isErrNotEnoughGAS(err) {
					prm.logger.Info("not enough GAS to set NNS domain record, will try again later")
				} else {
					prm.logger.Error("failed to send transaction setting NNS domain record, will try again later", zap.Error(err))
				}
				return
			}

			setDomainRecordTxValidUntilBlock = vub

			l.Info("transaction setting NNS domain record has been successfully sent, will wait for the outcome")
		}

		strSharedTxData, err := lookupNNSDomainRecord(invkr, prm.nnsOnChainAddress, domainDesignateNotaryTx)
		if err != nil {
			if errors.Is(err, errMissingDomain) {
				l.Info("NNS domain is missing, registration is needed")

				if registerDomainTxValidUntilBlock > 0 {
					l.Info("transaction registering NNS domain was sent earlier, checking relevance...")

					if cur := prm.monitor.currentHeight(); cur <= registerDomainTxValidUntilBlock {
						l.Info("previously sent transaction registering NNS domain may still be relevant, will wait for the outcome",
							zap.Uint32("current height", cur), zap.Uint32("retry after height", registerDomainTxValidUntilBlock))
						return
					}

					l.Info("previously sent transaction registering NNS domain expired without side-effect")
				}

				l.Info("sending new transaction registering domain in the NNS...")

				_, vub, err := localActor.SendCall(prm.nnsOnChainAddress, methodNNSRegister,
					domainDesignateNotaryTx, localActor.Sender(), prm.systemEmail, nnsRefresh, nnsRetry, nnsExpire, nnsMinimum)
				if err != nil {
					registerDomainTxValidUntilBlock = 0
					if isErrNotEnoughGAS(err) {
						prm.logger.Info("not enough GAS to register domain in the NNS, will try again later")
					} else {
						prm.logger.Error("failed to send transaction registering domain in the NNS, will try again later", zap.Error(err))
					}
					return
				}

				registerDomainTxValidUntilBlock = vub

				l.Info("transaction registering domain in the NNS has been successfully sent, will wait for the outcome")

				return
			} else if !errors.Is(err, errMissingDomainRecord) {
				l.Error("failed to lookup NNS domain record, will try again later", zap.Error(err))
				return
			}

			l.Info("missing record of the NNS domain, needed to be set")

			if setDomainRecordTxValidUntilBlock > 0 {
				l.Info("transaction setting NNS domain record was sent earlier, checking relevance...")

				if cur := prm.monitor.currentHeight(); cur <= setDomainRecordTxValidUntilBlock {
					l.Info("previously sent transaction setting NNS domain record may still be relevant, will wait for the outcome",
						zap.Uint32("current height", cur), zap.Uint32("retry after height", setDomainRecordTxValidUntilBlock))
					return
				}

				l.Info("previously sent transaction setting NNS domain record expired without side-effect")
			}

			generateAndShareTxData(false)
			return
		}

		err = sharedTxData.decodeString(strSharedTxData)
		if err != nil {
			l.Error("failed to decode shared data of the transaction got from the NNS domain record, will wait for a background fix",
				zap.Error(err))
			return
		}

		if cur := prm.monitor.currentHeight(); cur > sharedTxData.validUntilBlock {
			l.Error("previously used shared data of the transaction expired, need a reset",
				zap.Uint32("expires after height", sharedTxData.validUntilBlock), zap.Uint32("current height", cur))
			generateAndShareTxData(true)
			return
		}

		l.Info("shared data of the transaction designating Notary role to the committee synchronized successfully",
			zap.Uint32("nonce", sharedTxData.nonce), zap.Uint32("expires after height", sharedTxData.validUntilBlock),
			zap.Stringer("sender", sharedTxData.sender),
		)

		if tx == nil || !sharedTxDataMatches(tx, sharedTxData) {
			prm.logger.Info("making new transaction designating Notary role to the committee...")

			tx, err = makeUnsignedDesignateCommitteeNotaryTx(roleContract, prm.committee, sharedTxData)
			if err != nil {
				prm.logger.Error("failed to make unsigned transaction designating Notary role to the committee, will try again later",
					zap.Error(err))
				return
			}

			prm.logger.Info("transaction designating Notary role to the committee initialized, signing...")

			netMagic := localActor.GetNetwork()

			err = prm.localAcc.SignTx(netMagic, tx)
			if err != nil {
				prm.logger.Error("failed to sign transaction designating Notary role to the committee by local node's account, will try again later",
					zap.Error(err))
				return
			}

			err = committeeMultiSigAcc.SignTx(netMagic, tx)
			if err != nil {
				prm.logger.Error("failed to sign transaction designating Notary role to the committee by committee multi-signature account, will try again later",
					zap.Error(err))
				return
			}

			prm.logger.Info("new transaction designating Notary role to the committee successfully made")
		} else {
			prm.logger.Info("previously made transaction designating Notary role to the committee is still relevant, continue with it")
		}

		needRemoteSignatures := committeeMultiSigM - 1 // -1 local, we always have it

		if len(mCommitteeIndexToSignature) < needRemoteSignatures {
			if mCommitteeIndexToSignature == nil {
				mCommitteeIndexToSignature = make(map[int][]byte, needRemoteSignatures)
			}

			prm.logger.Info("collecting signatures of the transaction designating notary role to the committee from other members using NNS...")

			var invalidSignatureCounter int

			for i := range prm.committee[1:] {
				domain := designateNotarySignatureDomainForMember(i)

				rec, err := lookupNNSDomainRecord(invkr, prm.nnsOnChainAddress, domain)
				if err != nil {
					if errors.Is(err, errMissingDomain) || errors.Is(err, errMissingDomainRecord) {
						prm.logger.Info("missing NNS domain record with committee member's signature of the transaction designating Notary role to the committee, will wait",
							zap.Stringer("member", prm.committee[i]),
							zap.String("domain", domain))
					} else {
						prm.logger.Error("failed to read NNS domain record with committee member's signature of the transaction designating Notary role to the committee, will try again later",
							zap.Stringer("member", prm.committee[i]),
							zap.String("domain", domain),
							zap.Error(err))
					}
					continue
				}

				bRec, err := base64.StdEncoding.DecodeString(rec)
				if err != nil {
					prm.logger.Info("failed to decode NNS domain record with committee member's signature of the transaction designating Notary role to the committee from base64, will wait for a background fix",
						zap.Stringer("member", prm.committee[i]),
						zap.String("domain", domain),
						zap.Error(err))
					continue
				}

				checksumMatches, bSignature := sharedTxData.shiftChecksum(bRec)
				if !checksumMatches {
					prm.logger.Info("checksum of shared data of the transaction designating Notary role to the committee submitted by committee member mismatches, skip signature",
						zap.Stringer("member", prm.committee[i]),
						zap.String("domain", domain))
					continue
				}

				txCp := *tx // to safely call Hash method below
				if !prm.committee[i].VerifyHashable(bSignature, uint32(localActor.GetNetwork()), &txCp) {
					prm.logger.Info("invalid signature of the transaction designating Notary role to the committee submitted by committee member",
						zap.Stringer("member", prm.committee[i]),
						zap.String("domain", domain))

					invalidSignatureCounter++

					if invalidSignatureCounter+committeeMultiSigM > len(prm.committee) {
						prm.logger.Info("number of invalid signatures of the transaction designating Notary role to the committee submitted by remote members exceeded the threshold, will recreate the transaction",
							zap.Int("invalid", invalidSignatureCounter), zap.Int("need", committeeMultiSigM),
							zap.Int("total members", len(prm.committee)))
						generateAndShareTxData(true)
						return
					}

					continue
				}

				prm.logger.Info("received valid signature of the transaction designating Notary role to the committee submitted by committee member",
					zap.Stringer("member", prm.committee[i]),
					zap.String("domain", domain))

				mCommitteeIndexToSignature[i] = bSignature
				if len(mCommitteeIndexToSignature) == needRemoteSignatures {
					break
				}
			}

			if len(mCommitteeIndexToSignature) < needRemoteSignatures {
				prm.logger.Info("there are still not enough signatures of the transaction designating Notary role to the committee in the NNS, will wait",
					zap.Int("need", needRemoteSignatures), zap.Int("got", len(mCommitteeIndexToSignature)))
				return
			}
		}

		prm.logger.Info("gathered enough signatures of the transaction designating Notary role to the committee")

		if designateRoleTxValidUntilBlock > 0 {
			prm.logger.Info("transaction designating Notary role to the committee was sent earlier, checking relevance...")

			if cur := prm.monitor.currentHeight(); cur <= designateRoleTxValidUntilBlock {
				prm.logger.Info("previously sent transaction designating Notary role to the committee may still be relevant, will wait for the outcome",
					zap.Uint32("current height", cur), zap.Uint32("retry after height", designateRoleTxValidUntilBlock))
				return
			}

			prm.logger.Info("previously sent transaction designating Notary role to the committee expired without side-effect, will recreate")
			generateAndShareTxData(true)
			return
		}

		if !txFullySigned {
			prm.logger.Info("finalizing the transaction designating Notary role to the committee...")

			initialLen := len(tx.Scripts[1].InvocationScript)
			var extraLen int

			for _, sig := range mCommitteeIndexToSignature {
				extraLen += 1 + 1 + len(sig) // opcode + length + value
			}

			tx.Scripts[1].InvocationScript = append(tx.Scripts[1].InvocationScript,
				make([]byte, extraLen)...)
			buf := tx.Scripts[1].InvocationScript[initialLen:]

			for _, sig := range mCommitteeIndexToSignature {
				buf[0] = byte(opcode.PUSHDATA1)
				buf[1] = byte(len(sig))
				buf = buf[2:]
				buf = buf[copy(buf, sig):]
			}

			txFullySigned = true
		}

		prm.logger.Info("sending the transaction designating Notary role to the committee...")

		_, vub, err := localActor.Send(tx)
		if err != nil {
			designateRoleTxValidUntilBlock = 0
			switch {
			default:
				prm.logger.Error("failed to send transaction designating Notary role to the committee, will try again later",
					zap.Error(err))
			case isErrNotEnoughGAS(err):
				prm.logger.Info("not enough GAS for transaction designating Notary role to the committee, will try again later")
			case isErrInvalidTransaction(err):
				prm.logger.Warn("composed transaction designating Notary role to the committee is invalid and can't be sent, will recreate",
					zap.Error(err))
				generateAndShareTxData(true)
			}
			return
		}

		designateRoleTxValidUntilBlock = vub

		prm.logger.Info("transaction designating Notary role to the committee has been successfully sent, will wait for the outcome")
	}, nil
}

// initDesignateNotaryRoleAsSignerTick returns a function that preserves context
// of the Notary role designation to the multi-acc committee between calls. The
// operation is performed by the non-leading committee member which is assigned to
// sign transaction submitted by the leader.
func initDesignateNotaryRoleAsSignerTick(prm enableNotaryPrm) (func(), error) {
	committeeMultiSigM := smartcontract.GetMajorityHonestNodeCount(len(prm.committee))
	committeeMultiSigAcc := wallet.NewAccountFromPrivateKey(prm.localAcc.PrivateKey())

	err := committeeMultiSigAcc.ConvertMultisig(committeeMultiSigM, prm.committee)
	if err != nil {
		return nil, fmt.Errorf("compose committee multi-signature account: %w", err)
	}

	localActor, err := actor.NewSimple(prm.blockchain, prm.localAcc)
	if err != nil {
		return nil, fmt.Errorf("init transaction sender from local account: %w", err)
	}

	committeeSigners := []actor.SignerAccount{
		{
			Signer: transaction.Signer{
				Account: prm.localAcc.ScriptHash(),
				Scopes:  transaction.None,
			},
			Account: prm.localAcc,
		},
		{
			Signer: transaction.Signer{
				Account: committeeMultiSigAcc.ScriptHash(),
				Scopes:  transaction.CalledByEntry,
			},
			Account: committeeMultiSigAcc,
		},
	}

	committeeActor, err := actor.New(prm.blockchain, committeeSigners)
	if err != nil {
		return nil, fmt.Errorf("init transaction sender with committee signers: %w", err)
	}

	invkr := invoker.New(prm.blockchain, nil)
	roleContract := rolemgmt.New(committeeActor)

	// multi-tick context
	var tx *transaction.Transaction
	var registerDomainTxValidUntilBlock uint32
	var setDomainRecordTxValidUntilBlock uint32

	resetTx := func() {
		tx = nil
		setDomainRecordTxValidUntilBlock = 0
	}

	return func() {
		l := prm.logger.With(zap.String("domain", domainDesignateNotaryTx))

		prm.logger.Info("synchronizing shared data of the transaction designating Notary role to the committee with NNS domain record...")

		strSharedTxData, err := lookupNNSDomainRecord(invkr, prm.nnsOnChainAddress, domainDesignateNotaryTx)
		if err != nil {
			switch {
			default:
				l.Error("failed to lookup NNS domain record, will try again later", zap.Error(err))
			case errors.Is(err, errMissingDomain):
				l.Info("NNS domain is missing, will wait for a leader")
			case errors.Is(err, errMissingDomainRecord):
				l.Info("missing record in the NNS domain, will wait for a leader")
			}
			return
		}

		var sharedTxData sharedTransactionData

		err = sharedTxData.decodeString(strSharedTxData)
		if err != nil {
			l.Error("failed to decode shared data of the transaction got from the NNS domain record, will wait for a background fix",
				zap.Error(err))
			return
		}

		if cur := prm.monitor.currentHeight(); cur > sharedTxData.validUntilBlock {
			l.Error("previously used shared data of the transaction expired, will wait for update by leader",
				zap.Uint32("expires after height", sharedTxData.validUntilBlock), zap.Uint32("current height", cur))
			resetTx()
			return
		}

		l.Info("shared data of the transaction designating Notary role to the committee synchronized successfully",
			zap.Uint32("nonce", sharedTxData.nonce), zap.Uint32("expires after height", sharedTxData.validUntilBlock),
			zap.Stringer("sender", sharedTxData.sender),
		)

		if tx == nil || !sharedTxDataMatches(tx, sharedTxData) {
			prm.logger.Info("recreating the transaction designating Notary role to the committee...")

			tx, err = makeUnsignedDesignateCommitteeNotaryTx(roleContract, prm.committee, sharedTxData)
			if err != nil {
				prm.logger.Error("failed to make unsigned transaction designating Notary role to the committee, will try again later",
					zap.Error(err))
				return
			}

			prm.logger.Info("transaction designating Notary role to the committee successfully recreated")
		} else {
			prm.logger.Info("previously made transaction designating Notary role to the committee is still relevant, continue with it")
		}

		domain := designateNotarySignatureDomainForMember(prm.localAccCommitteeIndex)

		l = prm.logger.With(zap.String("domain", domain))

		var recordExists bool
		var needReset bool

		rec, err := lookupNNSDomainRecord(invkr, prm.nnsOnChainAddress, domain)
		if err != nil {
			if errors.Is(err, errMissingDomain) {
				l.Info("NNS domain is missing, registration is needed")

				if registerDomainTxValidUntilBlock > 0 {
					l.Info("transaction registering NNS domain was sent earlier, checking relevance...")

					if cur := prm.monitor.currentHeight(); cur <= registerDomainTxValidUntilBlock {
						l.Info("previously sent transaction registering NNS domain may still be relevant, will wait for the outcome",
							zap.Uint32("current height", cur), zap.Uint32("retry after height", registerDomainTxValidUntilBlock))
						return
					}

					l.Info("previously sent transaction registering NNS domain expired without side-effect")
				}

				l.Info("sending new transaction registering domain in the NNS...")

				_, vub, err := localActor.SendCall(prm.nnsOnChainAddress, methodNNSRegister,
					domain, localActor.Sender(), prm.systemEmail, nnsRefresh, nnsRetry, nnsExpire, nnsMinimum)
				if err != nil {
					registerDomainTxValidUntilBlock = 0
					if isErrNotEnoughGAS(err) {
						prm.logger.Info("not enough GAS to register domain in the NNS, will try again later")
					} else {
						prm.logger.Error("failed to send transaction registering domain in the NNS, will try again later", zap.Error(err))
					}
					return
				}

				registerDomainTxValidUntilBlock = vub

				l.Info("transaction registering domain in the NNS has been successfully sent, will wait for the outcome")

				return
			} else if !errors.Is(err, errMissingDomainRecord) {
				l.Error("failed to lookup NNS domain record, will try again later", zap.Error(err))
				return
			}

			l.Info("missing record of the NNS domain, needed to be set")

			if setDomainRecordTxValidUntilBlock > 0 {
				l.Info("transaction setting NNS domain record was sent earlier, checking relevance...")

				if cur := prm.monitor.currentHeight(); cur <= setDomainRecordTxValidUntilBlock {
					l.Info("previously sent transaction setting NNS domain record may still be relevant, will wait for the outcome",
						zap.Uint32("current height", cur), zap.Uint32("retry after height", setDomainRecordTxValidUntilBlock))
					return
				}

				l.Info("previously sent transaction setting NNS domain record expired without side-effect")
			}

			needReset = true
		} else {
			bRec, err := base64.StdEncoding.DecodeString(rec)
			if err != nil {
				l.Info("failed to decode NNS domain record with local account's signature of the transaction designating Notary role to the committee from base64, will wait for a background fix",
					zap.String("domain", domain), zap.Error(err))
				return
			}

			checksumMatches, bSignature := sharedTxData.shiftChecksum(bRec)
			if !checksumMatches {
				l.Info("checksum of shared data of the transaction designating Notary role to the committee submitted by committee member mismatches, need to be recalculated")
				needReset = true
			} else {
				txCp := *tx // to safely call Hash method below
				if !prm.localAcc.PublicKey().VerifyHashable(bSignature, uint32(localActor.GetNetwork()), &txCp) {
					l.Info("invalid signature of the transaction designating Notary role to the committee submitted by local account, need to be recalculated")
					needReset = true
				}
			}

			recordExists = true
		}

		if needReset {
			prm.logger.Info("calculating signature of the transaction designating Notary role to the committee using local account...")

			sig := prm.localAcc.SignHashable(localActor.GetNetwork(), tx)
			sig = sharedTxData.unshiftChecksum(sig)

			rec = base64.StdEncoding.EncodeToString(sig)

			l.Info("sending new transaction setting domain record in the NNS...")

			var vub uint32
			if recordExists {
				_, vub, err = localActor.SendCall(prm.nnsOnChainAddress, methodNNSSetRecord,
					domain, int64(nns.TXT), 0, rec)
			} else {
				_, vub, err = localActor.SendCall(prm.nnsOnChainAddress, methodNNSAddRecord,
					domain, int64(nns.TXT), rec)
			}
			if err != nil {
				setDomainRecordTxValidUntilBlock = 0
				if isErrNotEnoughGAS(err) {
					prm.logger.Info("not enough GAS to set NNS domain record, will try again later")
				} else {
					prm.logger.Error("failed to send transaction setting NNS domain record, will try again later", zap.Error(err))
				}
				return
			}

			setDomainRecordTxValidUntilBlock = vub

			l.Info("transaction setting NNS domain record has been successfully sent, will wait for the outcome")

			return
		}
	}, nil
}

// sharedTransactionData groups transaction parameters that cannot be predicted
// in a decentralized way and need to be sent out.
type sharedTransactionData struct {
	sender          util.Uint160
	validUntilBlock uint32
	nonce           uint32
}

// bytes serializes sharedTransactionData.
func (x sharedTransactionData) bytes() []byte {
	b := make([]byte, sharedTransactionDataLen)
	// fixed size is more convenient for potential format changes in the future
	copy(b, x.sender.BytesBE())
	binary.BigEndian.PutUint32(b[util.Uint160Size:], x.validUntilBlock)
	binary.BigEndian.PutUint32(b[util.Uint160Size+4:], x.nonce)
	return b
}

// encodeToString returns serialized sharedTransactionData in base64.
func (x sharedTransactionData) encodeToString() string {
	return base64.StdEncoding.EncodeToString(x.bytes())
}

// decodeString decodes serialized sharedTransactionData from base64.
func (x *sharedTransactionData) decodeString(s string) (err error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("decode shared transaction data from base64: %w", err)
	}

	if len(b) != sharedTransactionDataLen {
		return fmt.Errorf("invalid/unsupported length of shared transaction data: expected %d, got %d",
			sharedTransactionDataLen, len(b))
	}

	x.sender, err = util.Uint160DecodeBytesBE(b[:util.Uint160Size])
	if err != nil {
		return fmt.Errorf("decode sender account binary: %w", err)
	}

	x.validUntilBlock = binary.BigEndian.Uint32(b[util.Uint160Size:])
	x.nonce = binary.BigEndian.Uint32(b[util.Uint160Size+4:])

	return nil
}

const (
	sharedTransactionDataLen         = util.Uint160Size + 4 + 4
	sharedTransactionDataChecksumLen = 4
)

// unshiftChecksum prepends given payload with first 4 bytes of the
// sharedTransactionData SHA-256 checksum. Inverse operation to shiftChecksum.
func (x sharedTransactionData) unshiftChecksum(data []byte) []byte {
	h := sha256.Sum256(x.bytes())
	return append(h[:sharedTransactionDataChecksumLen], data...)
}

// shiftChecksum matches checksum of the sharedTransactionData and returns
// payload. Inverse operation to unshiftChecksum.
func (x sharedTransactionData) shiftChecksum(data []byte) (bool, []byte) {
	if len(data) < sharedTransactionDataChecksumLen {
		return false, data
	}

	h := sha256.Sum256(x.bytes())
	if !bytes.HasPrefix(data, h[:sharedTransactionDataChecksumLen]) {
		return false, nil
	}

	return true, data[sharedTransactionDataChecksumLen:]
}

// sharedTxDataMatches checks if given transaction is constructed using provided
// shared parameters.
func sharedTxDataMatches(tx *transaction.Transaction, sharedTxData sharedTransactionData) bool {
	return sharedTxData.nonce == tx.Nonce &&
		sharedTxData.validUntilBlock == tx.ValidUntilBlock &&
		len(tx.Signers) > 0 && tx.Signers[0].Account.Equals(sharedTxData.sender)
}

// makeUnsignedDesignateCommitteeNotaryTx constructs unsigned transaction that
// designates Notary role to the specified committee members using shared
// parameters.
//
// Note: RoleManagement contract client must be initialized with two signers:
//  1. simple account with transaction.None witness scope
//  2. committee multi-signature account with transaction.CalledByEntry witness scope
func makeUnsignedDesignateCommitteeNotaryTx(roleContract *rolemgmt.Contract, committee keys.PublicKeys, sharedTxData sharedTransactionData) (*transaction.Transaction, error) {
	tx, err := roleContract.DesignateAsRoleUnsigned(noderoles.P2PNotary, committee)
	if err != nil {
		return nil, err
	}

	tx.ValidUntilBlock = sharedTxData.validUntilBlock
	tx.Nonce = sharedTxData.nonce
	tx.Signers[0].Account = sharedTxData.sender

	return tx, nil
}

// newCommitteeNotaryActor returns notary.Actor that builds and sends Notary
// service requests witnessed by the specified committee members to the provided
// Blockchain. Given local account pays for transactions.
func newCommitteeNotaryActor(b Blockchain, localAcc *wallet.Account, committee keys.PublicKeys) (*notary.Actor, error) {
	committeeMultiSigM := smartcontract.GetMajorityHonestNodeCount(len(committee))
	committeeMultiSigAcc := wallet.NewAccountFromPrivateKey(localAcc.PrivateKey())

	err := committeeMultiSigAcc.ConvertMultisig(committeeMultiSigM, committee)
	if err != nil {
		return nil, fmt.Errorf("compose committee multi-signature account: %w", err)
	}

	return notary.NewActor(b, []actor.SignerAccount{
		{
			Signer: transaction.Signer{
				Account: localAcc.ScriptHash(),
				Scopes:  transaction.None,
			},
			Account: localAcc,
		},
		{
			Signer: transaction.Signer{
				Account: committeeMultiSigAcc.ScriptHash(),
				Scopes:  transaction.CalledByEntry,
			},
			Account: committeeMultiSigAcc,
		},
	}, localAcc)
}

// notaryDepositDeficiencyHandler is a function returned by initNotaryDepositDeficiencyHandler.
// True argument is passed when there is not enough GAS on local account's
// balance in the Notary contract, false - when local account's Notary deposit
// expires before particular fallback transaction.
//
// The function is intended to be called multiple times on each deposit problem
// encounter. On each call, It attempts to fix Notary deposit problem without
// waiting for success. Caller should by default wait for the problem to be
// fixed, and if not, retry.
//
// notaryDepositDeficiencyHandler must not be called from multiple routines in
// parallel.
type notaryDepositDeficiencyHandler = func(lackOfGAS bool)

// Amount of GAS for the single local account's GAS->Notary transfer. Relatively
// small value for fallback transactions' fees.
var singleNotaryDepositAmount = big.NewInt(1_0000_0000) // 1 GAS

// constructs notaryDepositDeficiencyHandler working with the specified
// Blockchain and GAS/Notary balance of the given account.
func initNotaryDepositDeficiencyHandler(l *zap.Logger, b Blockchain, monitor *blockchainMonitor, localAcc *wallet.Account) (notaryDepositDeficiencyHandler, error) {
	localActor, err := actor.NewSimple(b, localAcc)
	if err != nil {
		return nil, fmt.Errorf("init transaction sender from local account: %w", err)
	}

	notaryContract := notary.New(localActor)
	gasContract := gas.New(localActor)
	localAccID := localAcc.ScriptHash()

	// multi-tick context
	var transferTxValidUntilBlock uint32
	var expirationTxValidUntilBlock uint32

	return func(lackOfGAS bool) {
		notaryBalance, err := notaryContract.BalanceOf(localAccID)
		if err != nil {
			l.Error("failed to read Notary balance of the local account, will try again later", zap.Error(err))
			return
		}

		gasBalance, err := gasContract.BalanceOf(localAccID)
		if err != nil {
			l.Error("failed to read GAS token balance of the local account, will try again later", zap.Error(err))
			return
		}

		// simple deposit scheme: transfer 1GAS (at most 2% of GAS token balance) for
		// 100 blocks after the latest deposit's expiration height (if first, then from
		// current height).
		//
		// If we encounter deposit expiration and current Notary balance >=20% of single
		// transfer, we just increase the expiration time of the deposit, otherwise, we
		// make transfer.

		const (
			// GAS:Notary proportion, see scheme above
			gasProportion = 50
			// even there is no lack of GAS at the moment, when the balance falls below 1/5
			// of the supported value - replenish
			refillProportion = 5
			// for simplicity, we just make Notary deposit "infinite" not to prolong
			till = math.MaxUint32
		)

		if !lackOfGAS { // deposit expired
			if new(big.Int).Mul(notaryBalance, big.NewInt(refillProportion)).Cmp(singleNotaryDepositAmount) >= 0 {
				if expirationTxValidUntilBlock > 0 {
					l.Info("transaction increasing expiration time of the Notary deposit was sent earlier, checking relevance...")

					if cur := monitor.currentHeight(); cur <= expirationTxValidUntilBlock {
						l.Info("previously sent transaction increasing expiration time of the Notary deposit may still be relevant, will wait for the outcome",
							zap.Uint32("current height", cur), zap.Uint32("retry after height", expirationTxValidUntilBlock))
						return
					}

					l.Info("previously sent transaction increasing expiration time of the Notary deposit expired without side-effect ")
				}

				l.Info("sending new transaction increasing expiration time of the Notary deposit...", zap.Uint32("till", till))

				_, vub, err := notaryContract.LockDepositUntil(localAccID, till)
				if err != nil {
					l.Error("failed to send transaction increasing expiration time of the Notary deposit, will try again later", zap.Error(err))
					return
				}

				expirationTxValidUntilBlock = vub

				l.Info("transaction increasing expiration time of the Notary deposit has been successfully sent, will wait for the outcome")

				return
			}
		}

		if transferTxValidUntilBlock > 0 {
			l.Info("transaction transferring local account's GAS to the Notary contract was sent earlier, checking relevance...")

			// for simplicity, we track ValidUntilBlock. In this particular case, it'd be
			// more efficient to monitor a transaction by ID, because side effect is
			// inconsistent (funds can be spent in background).

			if cur := monitor.currentHeight(); cur <= transferTxValidUntilBlock {
				l.Info("previously sent transaction transferring local account's GAS to the Notary contract may still be relevant, will wait for the outcome",
					zap.Uint32("current height", cur), zap.Uint32("retry after height", transferTxValidUntilBlock))
				return
			}

			l.Info("previously sent transaction transferring local account's GAS to the Notary contract expired without side-effect")
		}

		needAtLeast := new(big.Int).Mul(singleNotaryDepositAmount, big.NewInt(gasProportion))
		if gasBalance.Cmp(needAtLeast) < 0 {
			l.Info("minimum threshold for GAS transfer from local account to the Notary contract not reached, will wait for replenishment",
				zap.Stringer("need at least", needAtLeast), zap.Stringer("have", gasBalance))
			return
		}

		var transferData notary.OnNEP17PaymentData
		transferData.Account = &localAccID
		transferData.Till = till

		l.Info("sending new transaction transferring local account's GAS to the Notary contract...",
			zap.Stringer("amount", singleNotaryDepositAmount), zap.Uint32("till", transferData.Till))

		// nep17.TokenWriter.Transfer doesn't support notary.OnNEP17PaymentData
		// directly, so split the args
		// Track https://github.com/nspcc-dev/neofs-node/issues/2429
		_, vub, err := gasContract.Transfer(localAccID, notary.Hash, singleNotaryDepositAmount, []interface{}{transferData.Account, transferData.Till})
		if err != nil {
			l.Error("failed to send transaction transferring local account's GAS to the Notary contract, will try again later", zap.Error(err))
			return
		}

		transferTxValidUntilBlock = vub

		l.Info("transaction transferring local account's GAS to the Notary contract has been successfully sent, will wait for the outcome")
	}, nil
}

// listenCommitteeNotaryRequestsPrm groups parameters of listenCommitteeNotaryRequests.
type listenCommitteeNotaryRequestsPrm struct {
	logger *zap.Logger

	blockchain Blockchain

	localAcc *wallet.Account

	committee keys.PublicKeys

	onNotaryDepositDeficiency notaryDepositDeficiencyHandler
}

// listenCommitteeNotaryRequests starts background process listening to incoming
// Notary service requests. The process filters transactions witnessed by the
// committee and signs them on behalf of the local account (representing
// committee member). Routine handles only requests sent by the remote accounts.
// The process is stopped by context or internal Blockchain signal.
func listenCommitteeNotaryRequests(ctx context.Context, prm listenCommitteeNotaryRequestsPrm) error {
	committeeMultiSigM := smartcontract.GetMajorityHonestNodeCount(len(prm.committee))
	committeeMultiSigAcc := wallet.NewAccountFromPrivateKey(prm.localAcc.PrivateKey())

	err := committeeMultiSigAcc.ConvertMultisig(committeeMultiSigM, prm.committee)
	if err != nil {
		return fmt.Errorf("compose committee multi-signature account: %w", err)
	}

	committeeMultiSigAccID := committeeMultiSigAcc.ScriptHash()
	chNotaryRequests := make(chan *result.NotaryRequestEvent, 100) // secure from blocking
	// cache processed operations: when main transaction from received notary
	// request is signed and sent by the local account, we receive the request from
	// the channel again
	mProcessedMainTxs := make(map[util.Uint256]struct{})

	subID, err := prm.blockchain.ReceiveNotaryRequests(&neorpc.TxFilter{
		Signer: &committeeMultiSigAccID,
	}, chNotaryRequests)
	if err != nil {
		return fmt.Errorf("subscribe to notary requests from committee: %w", err)
	}

	go func() {
		defer func() {
			err := prm.blockchain.Unsubscribe(subID)
			if err != nil {
				prm.logger.Warn("failed to cancel subscription to notary requests", zap.Error(err))
			}
		}()

		prm.logger.Info("listening to committee notary requests...")

		for {
			select {
			case <-ctx.Done():
				prm.logger.Info("stop listening to committee notary requests (context is done)", zap.Error(ctx.Err()))
				return
			case notaryEvent, ok := <-chNotaryRequests:
				if !ok {
					prm.logger.Info("stop listening to committee notary requests (subscription channel closed)")
					return
				}

				// for simplicity, requests are handled one-by one. We could process them in parallel
				// using worker pool, but actions seem to be relatively lightweight

				const expectedSignersCount = 3 // sender + committee + Notary
				mainTx := notaryEvent.NotaryRequest.MainTransaction
				// note: instruction above can throw NPE and it's ok to panic: we confidently
				// expect that only non-nil pointers will come from the channel (NeoGo
				// guarantees)

				srcMainTxHash := mainTx.Hash()
				_, processed := mProcessedMainTxs[srcMainTxHash]

				// revise severity level of the messages
				// https://github.com/nspcc-dev/neofs-node/issues/2419
				switch {
				case processed:
					prm.logger.Info("main transaction of the notary request has already been processed, skip",
						zap.Stringer("ID", srcMainTxHash))
					continue
				case notaryEvent.Type != mempoolevent.TransactionAdded:
					prm.logger.Info("unsupported type of the notary request event, skip",
						zap.Stringer("got", notaryEvent.Type), zap.Stringer("expect", mempoolevent.TransactionAdded))
					continue
				case len(mainTx.Signers) != expectedSignersCount:
					prm.logger.Info("unsupported number of signers of main transaction from the received notary request, skip",
						zap.Int("expected", expectedSignersCount), zap.Int("got", len(mainTx.Signers)))
					continue
				case !mainTx.HasSigner(committeeMultiSigAccID):
					prm.logger.Info("committee is not a signer of main transaction from the received notary request, skip")
					continue
				case mainTx.HasSigner(prm.localAcc.ScriptHash()):
					prm.logger.Info("main transaction from the received notary request is signed by a local account, skip")
					continue
				case len(mainTx.Scripts) == 0:
					prm.logger.Info("missing scripts of main transaction from the received notary request, skip")
					continue
				}

				bSenderKey, ok := vm.ParseSignatureContract(mainTx.Scripts[0].VerificationScript)
				if !ok {
					prm.logger.Info("first verification script in main transaction of the received notary request is not a signature one, skip", zap.Error(err))
					continue
				}

				senderKey, err := keys.NewPublicKeyFromBytes(bSenderKey, elliptic.P256())
				if err != nil {
					prm.logger.Info("failed to decode sender's public key from first script of main transaction from the received notary request, skip", zap.Error(err))
					continue
				}

				// copy transaction to avoid pointer mutation
				mainTxCp := *mainTx
				mainTxCp.Scripts = nil

				mainTx = &mainTxCp // source one isn't needed anymore

				// it'd be safer to get into the transaction and analyze what it is trying to do.
				// For simplicity, now we blindly sign it. Track https://github.com/nspcc-dev/neofs-node/issues/2430

				prm.logger.Info("signing main transaction from the received notary request by the local account...")

				// create new actor for current signers. As a slight optimization, we could also
				// compare with signers of previously created actor and deduplicate.
				// See also https://github.com/nspcc-dev/neofs-node/issues/2314
				notaryActor, err := notary.NewActor(prm.blockchain, []actor.SignerAccount{
					{
						Signer:  mainTx.Signers[0],
						Account: notary.FakeSimpleAccount(senderKey),
					},
					{
						Signer:  mainTx.Signers[1],
						Account: committeeMultiSigAcc,
					},
				}, prm.localAcc)
				if err != nil {
					// not really expected
					prm.logger.Error("failed to init Notary request sender with signers from the main transaction of the received notary request", zap.Error(err))
					continue
				}

				err = notaryActor.Sign(mainTx)
				if err != nil {
					prm.logger.Error("failed to sign main transaction from the received notary request by the local account, skip", zap.Error(err))
					continue
				}

				prm.logger.Info("sending new notary request with the main transaction signed by the local account...")

				_, _, _, err = notaryActor.Notarize(mainTx, nil)
				if err != nil {
					lackOfGAS := isErrNotEnoughGAS(err)
					// here lackOfGAS=true always means lack of Notary balance and not related to
					// the main transaction itself
					if !lackOfGAS {
						if !isErrNotaryDepositExpires(err) {
							prm.logger.Error("failed to send transaction deploying NNS contract, will try again later", zap.Error(err))
							continue
						}
					}

					prm.onNotaryDepositDeficiency(lackOfGAS)

					continue
				}

				prm.logger.Info("main transaction from the received notary request has been successfully signed and sent by the local account")
			}
		}
	}()

	return nil
}
