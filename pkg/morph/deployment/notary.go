package deployment

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"go.uber.org/zap"
)

type initCommitteeNotaryPrm struct {
	logger            *zap.Logger
	committeeService  *committeeService
	nnsOnChainAddress util.Uint160
	// committee               keys.PublicKeys
	// localNodeCommitteeIndex int
	// localNodeLeads          bool
	// waitInterval            time.Duration
	// networkMagic            netmode.Magic
	// nnsOnChainAddress       util.Uint160
	// committeeMultiSigM      int
	// committeeMultiSigAcc    *wallet.Account
	// localNodeActor          *actor.Actor
	// committeeActor          *actor.Actor
}

//
// func initNotaryService(ctx context.Context, prm initCommitteeNotaryPrm) error {
// 	l := prm.logger.With(zap.String("context", "init Notary service"))
//
// 	var leaderPrm designateNotaryRoleAsLeaderPrm
// 	var leaderCtx *designateNotaryRoleAsLeaderContext
//
// 	for recycle := false; ; recycle = true {
// 		if recycle {
// 			prm.committeeService.waitForPossibleChainStateChange(ctx)
// 		}
//
// 		select {
// 		case <-ctx.Done():
// 			return fmt.Errorf("wait for designation of Notary role to the committee: %w", ctx.Err())
// 		default:
// 		}
//
// 		committeeWithNotaryRole, err := prm.committeeService.isCommitteeWithNotaryRole()
// 		if err != nil {
// 			l.Error("failed to check role of the committee, will try again later", zap.Error(err))
// 			continue
// 		} else if committeeWithNotaryRole {
// 			l.Info("all committee members have notary role")
// 			return nil
// 		}
//
// 		l.Info("designating notary role to the committee...")
//
// 		if prm.committeeService.isLocalNodeFirstInCommittee() {
// 			if leaderCtx == nil {
// 				leaderCtx = &designateNotaryRoleAsLeaderContext{
// 					Context: ctx,
// 				}
//
// 				leaderPrm.logger = prm.logger
// 				leaderPrm.committeeService = prm.committeeService
// 			}
//
// 			designateNotaryRoleAsLeader(leaderCtx, leaderPrm)
//
// 			continue
// 		}
//
// 	}
//
// 	var ctxLeader *designateCommitteeNotaryLeaderContext
// 	var ctxAnticipant *designateCommitteeNotaryAnticipantContext
//
// 	if prm.localNodeLeads {
// 		ctxLeader = &designateCommitteeNotaryLeaderContext{Context: ctx}
// 	} else {
// 		ctxAnticipant = &designateCommitteeNotaryAnticipantContext{Context: ctx}
// 	}
//
// 	for attemptMade := false; ; {
// 		select {
// 		case <-ctx.Done():
// 			return fmt.Errorf("wait notary role designation to the committee: %w",
// 				ctx.Err())
// 		default:
// 		}
//
// 		nBlock, err := prm.localNodeActor.GetBlockCount()
// 		if err != nil {
// 			// without this, we won't recognize the roles. We could try to naively rely on
// 			// the fact that everything worked out, but it's better to leave it to a full
// 			// reboot.
// 			return fmt.Errorf("get block index: %w", err)
// 		}
//
// 		membersWithNotaryRole, err := roleManagementContract.GetDesignatedByRole(noderoles.P2PNotary, nBlock)
// 		if err == nil {
// 			missingSomebody := len(membersWithNotaryRole) < len(prm.committee)
// 			if !missingSomebody {
// 				for i := range prm.committee {
// 					if !membersWithNotaryRole.Contains(prm.committee[i]) {
// 						missingSomebody = true
// 						break
// 					}
// 				}
// 			}
//
// 			if !missingSomebody {
// 				// theoretically it's enough to have a consensus minimum of them,
// 				// but, in practice, it is more reliable to wait for the full lineup
// 				l.Info("notary role is designated to all committee members")
// 				return nil
// 			}
//
// 			if attemptMade {
// 				l.Info("an attempt to designate notary role to the committee has already been made but this still hasn't settled to the chain, waiting...")
// 			} else {
// 				if prm.localNodeLeads {
// 					l.Info("attempting to designate notary role to the committee as leader...")
// 					attemptMade = c.tryDesignateNotaryRoleToCommitteeAsLeader(ctxLeader, designateCommitteeNotaryLeaderPrm{
// 						initCommitteeNotaryPrm: prm,
// 						roleManagementContract: roleManagementContract,
// 					})
// 				} else {
// 					l.Info("attempting to designate notary role to the committee as anticipant...")
// 					c.tryDesignateNotaryRoleAsAnticipant(ctxAnticipant, designateCommitteeNotaryAnticipantPrm{
// 						initCommitteeNotaryPrm: prm,
// 						roleManagementContract: roleManagementContract,
// 					})
// 				}
// 			}
// 		} else {
// 			l.Error("problem with reading members designated to the notary role, waiting for a possible background fix...",
// 				zap.Any("role", noderoles.P2PNotary), zap.Uint32("block", nBlock), zap.Error(err))
// 		}
//
// 		time.Sleep(prm.waitInterval)
// 	}
// }
//
// type designateNotaryRoleAsLeaderPrm struct {
// 	logger            *zap.Logger
// 	committeeService  *committeeService
// 	nnsOnChainAddress util.Uint160
// }
//
// type designateNotaryRoleAsLeaderContext struct {
// 	context.Context
// 	singleNodeRoleDesignationCtx transactionContext
// }
//
// func designateNotaryRoleAsLeader(ctx *designateNotaryRoleAsLeaderContext, prm designateNotaryRoleAsLeaderPrm) {
// 	if prm.committeeService.isSingleNode() {
// 		prm.logger.Info("committee is single-node, simple transaction is enough")
//
// 		if ctx.singleNodeTxValidUntilBlock > 0 {
// 			prm.logger.Info("request was sent earlier, checking relevance...")
//
// 			if prm.committeeService.isStillValid(ctx.singleNodeTxValidUntilBlock) {
// 				prm.logger.Info("previously sent request has not expired yet, will wait for the outcome",
// 					zap.Uint32("retry after block", ctx.singleNodeTxValidUntilBlock))
// 				return
// 			}
//
// 			prm.logger.Info("previously sent request expired")
// 		}
//
// 		prm.logger.Info("sending new request...")
//
// 		var err error
//
// 		ctx.singleNodeTxValidUntilBlock, err = prm.committeeService.sendDesignateNotaryRoleToCommitteeRequest()
// 		if err != nil {
// 			if errors.Is(err, errNotEnoughGAS) {
// 				prm.logger.Info("not enough GAS to send request, will wait for a background replenishment")
// 			} else {
// 				prm.logger.Error("failed to send request, will try again later", zap.Error(err))
// 			}
// 		} else {
// 			prm.logger.Info("request has been successfully sent, will check again later")
// 		}
//
// 		return
// 	}
//
// 	prm.logger.Info("committee is multi-node, going to gather signatures on the NNS contract")
//
// 	bootstrapDomainExists, err := prm.committeeService.domainExists(prm.nnsOnChainAddress, domainBootstrap)
// 	if err != nil {
// 		prm.logger.Error("failed to check presence of the system NNS domain, will try again later",
// 			zap.String("domain", domainBootstrap), zap.Error(err))
// 		return
// 	} else if !bootstrapDomainExists {
// 		prm.logger.Error("missing required system NNS domain, will wait for a background register",
// 			zap.String("domain", domainBootstrap), zap.Error(err))
// 		return
// 	}
//
// 	if !ctx.txDomainInSync {
// 		err = c.syncNNSDomainWithChain(ctx, prm.nnsOnChainAddress, nnsDomainDesignateCommitteeNotaryTx, prm.localNodeActor, prm.waitInterval)
// 		if err != nil {
// 			c.logger.Error("failed to sync NNS domain with the chain, waiting for a possible background fix...",
// 				zap.String("domain", nnsDomainDesignateCommitteeNotaryTx), zap.Error(err))
// 			return false
// 		}
//
// 		ctx.txDomainInSync = true
// 		c.logger.Info("required NNS domain is on the chain, we can continue", zap.String("domain", nnsDomainDesignateCommitteeNotaryTx))
// 	}
//
// 	if ctx.txSharedData == nil {
// 		c.logger.Info("synchronizing shared transaction data (designate notary role to the committee) with NNS domain record...",
// 			zap.String("domain", nnsDomainDesignateCommitteeNotaryTx))
//
// 		setRecord := c.syncNNSDomainRecordWithChain
// 		if ctx.txExpired {
// 			setRecord = c.setNNSDomainRecordOnChain
// 			ctx.txExpired = false
// 		}
//
// 		strTxSharedData, err := setRecord(ctx, prm.nnsOnChainAddress,
// 			nnsDomainDesignateCommitteeNotaryTx, prm.localNodeActor, prm.waitInterval, func() (string, error) {
// 				c.logger.Info("generating shared transaction data to designate notary role to the committee...")
//
// 				vub, err := prm.localNodeActor.CalculateValidUntilBlock() // TODO: fits?
// 				if err != nil {
// 					return "", fmt.Errorf("generate ValidUntilBlock for the transaction (designate notary role to the committee): %w", err)
// 				}
//
// 				bSharedTxData := encodeSharedTransactionData(sharedTransactionData{
// 					sender:          c.accAddr,
// 					validUntilBlock: vub,
// 					nonce:           randutil.Uint32(),
// 				})
//
// 				return base64.StdEncoding.EncodeToString(bSharedTxData), nil
// 			})
// 		if err != nil {
// 			c.logger.Error("failed to sync NNS domain record (shared data of the transaction designating notary role to the committee) with the chain, waiting for a possible background fix...",
// 				zap.String("domain", nnsDomainDesignateCommitteeNotaryTx), zap.Error(err))
// 			return false
// 		}
//
// 		ctx.txSharedData, err = base64.StdEncoding.DecodeString(strTxSharedData)
// 		if err != nil {
// 			c.logger.Error("failed to decode shared data of the transaction designating notary role to the committee from base-64, waiting for a possible background fix...",
// 				zap.String("domain", nnsDomainDesignateCommitteeNotaryTx), zap.Error(err))
// 			return false
// 		}
// 	}
//
// 	handleTxExpiration := func() {
// 		ctx.txExpired = true
// 		ctx.mCommitteeIndexToSignature = nil
// 		ctx.txSharedData = nil
// 		ctx.tx = nil
// 		ctx.txFin = false
//
// 		c.logger.Info("previously made transaction designating notary role to the committee expired, recreating...")
// 	}
//
// 	// transaction could expire because we are collecting signatures too long,
// 	// check manually to prevent endless waiting
// 	if ctx.tx != nil {
// 		nBlock, err := c.rpcActor.GetBlockCount()
// 		if err != nil {
// 			c.logger.Warn("failed to get chain height", zap.Error(err))
// 			return false
// 		}
//
// 		if nBlock > ctx.tx.ValidUntilBlock {
// 			handleTxExpiration()
// 			return false
// 		}
// 	}
//
// 	if ctx.tx == nil {
// 		c.logger.Info("making transaction (designate notary role to the committee) using shared data")
//
// 		ctx.tx, err = makeUnsignedDesignateCommitteeNotaryTx(prm.roleManagementContract, prm.committee, prm.committeeMultiSigAcc, ctx.txSharedData)
// 		if err != nil {
// 			if errors.Is(err, errDecodeSharedTxData) {
// 				c.logger.Error("failed to decode shared data of the transaction designating notary role to the committee, waiting for a possible background fix...",
// 					zap.String("domain", nnsDomainDesignateCommitteeNotaryTx), zap.Error(err))
// 				ctx.txSharedData = nil
// 			} else {
// 				c.logger.Info("failed to make transaction designating notary role to the committee, will try again later...", zap.Error(err))
// 			}
// 			return false
// 		}
//
// 		c.logger.Info("transaction designating notary role to the committee has been successfully made, signing...",
// 			zap.Stringer("sender", ctx.tx.Signers[0].Account),
// 			zap.Uint32("vub", ctx.tx.ValidUntilBlock),
// 			zap.Uint32("nonce", ctx.tx.Nonce),
// 		)
//
// 		err = c.acc.SignTx(prm.networkMagic, ctx.tx)
// 		if err != nil {
// 			c.logger.Error("failed to sign transaction (designate notary role to the committee) by local node's account, waiting for a possible background fix...",
// 				zap.Error(err))
// 			// this internal node error can't be fixed (not really expected)
// 			return true
// 		}
//
// 		err = prm.committeeMultiSigAcc.SignTx(prm.networkMagic, ctx.tx)
// 		if err != nil {
// 			c.logger.Error("failed to sign transaction (designate notary role to the committee) by committee multi-signature account, waiting for a possible background fix...",
// 				zap.Error(err))
// 			// this internal node error can't be fixed (not really expected)
// 			return true
// 		}
// 	}
//
// 	needSignatures := prm.committeeMultiSigM - 1 // -1 local, we always have it
//
// 	if len(ctx.mCommitteeIndexToSignature) < needSignatures {
// 		if ctx.mCommitteeIndexToSignature == nil {
// 			ctx.mCommitteeIndexToSignature = make(map[int][]byte, needSignatures)
// 		}
//
// 		c.logger.Info("collecting transaction signatures (designate notary role to the committee) of the committee members from the NNS...")
//
// 		for i := range prm.committee {
// 			select {
// 			case <-ctx.Done():
// 				c.logger.Info("stop collecting transaction signatures (designate notary role to the committee) from committee members by context",
// 					zap.Error(ctx.Err()))
// 				return false
// 			default:
// 			}
//
// 			if i == prm.localNodeCommitteeIndex {
// 				continue
// 			} else if _, ok := ctx.mCommitteeIndexToSignature[i]; ok {
// 				continue
// 			}
//
// 			domain := designateCommitteeNotaryTxSignatureDomainForMember(i)
//
// 			rec, err := lookupNNSDomainRecord(c.client, prm.nnsOnChainAddress, domain)
// 			if err != nil {
// 				if errors.Is(err, ErrNNSRecordNotFound) {
// 					c.logger.Info("transaction signature (designate notary role to the committee) of the committee member is still missing in the NNS",
// 						zap.Stringer("member", prm.committee[i]),
// 						zap.String("domain", domain))
// 				} else {
// 					c.logger.Error("failed to read NNS domain record with transaction signature (designate notary role to the committee) of the committee member",
// 						zap.Stringer("member", prm.committee[i]),
// 						zap.String("domain", domain),
// 						zap.Error(err))
// 				}
//
// 				continue
// 			}
//
// 			bRec, err := base64.StdEncoding.DecodeString(rec)
// 			if err != nil {
// 				c.logger.Info("failed to decode NNS record with committee member's transaction signature (designate notary role to the committee) from base-64, waiting for a possible background fix...",
// 					zap.Stringer("member", prm.committee[i]),
// 					zap.String("domain", domain),
// 					zap.Error(err))
// 				continue
// 			}
//
// 			checksumMatches, sig := verifyAndShiftChecksum(bRec, ctx.txSharedData)
// 			if !checksumMatches {
// 				c.logger.Info("checksum of shared transaction data (designate notary role to the committee) submitted by the committee member mismatches, skip signature...",
// 					zap.Stringer("member", prm.committee[i]),
// 					zap.String("domain", domain))
// 				continue
// 			}
//
// 			// FIXME: check if it's really a signature, e.g. check len and/or verify in-place
//
// 			ctx.mCommitteeIndexToSignature[i] = sig
// 			if len(ctx.mCommitteeIndexToSignature) == needSignatures {
// 				break
// 			}
// 		}
//
// 		if len(ctx.mCommitteeIndexToSignature) < needSignatures {
// 			// TODO: maybe it's enough to reach consensus threshold?
// 			c.logger.Info("there are still not enough transaction signatures (designate notary role to the committee) in the NNS, waiting...",
// 				zap.Int("need", needSignatures),
// 				zap.Int("got", len(ctx.mCommitteeIndexToSignature)))
// 			return false
// 		}
// 	}
//
// 	c.logger.Info("gathered enough signatures of transaction designating notary role to the committee")
//
// 	if !ctx.txFin {
// 		c.logger.Info("finalizing the transaction designating notary role to the committee...")
//
// 		initialLen := len(ctx.tx.Scripts[1].InvocationScript)
// 		var extraLen int
//
// 		for _, sig := range ctx.mCommitteeIndexToSignature {
// 			extraLen += 1 + 1 + len(sig) // opcode + length + value
// 		}
//
// 		ctx.tx.Scripts[1].InvocationScript = append(ctx.tx.Scripts[1].InvocationScript,
// 			make([]byte, extraLen)...)
// 		buf := ctx.tx.Scripts[1].InvocationScript[initialLen:]
//
// 		for _, sig := range ctx.mCommitteeIndexToSignature {
// 			buf[0] = byte(opcode.PUSHDATA1)
// 			buf[1] = byte(len(sig))
// 			buf = buf[2:]
// 			buf = buf[copy(buf, sig):]
// 		}
//
// 		ctx.txFin = true
// 	}
//
// 	if !ctx.txSent {
// 		c.logger.Info("sending the transaction designating notary role to the committee...")
//
// 		_, _, err = prm.localNodeActor.Send(ctx.tx)
// 		if err != nil && !isErrTransactionAlreadyExists(err) {
// 			switch {
// 			default:
// 				c.logger.Error("failed to send transaction designating notary role to the committee, waiting for a possible background fix...",
// 					zap.Error(err))
// 			case isErrNotEnoughGAS(err):
// 				c.logger.Info("not enough GAS for transaction designating notary role to the committee, waiting for replenishment...")
// 			case isErrTransactionExpired(err):
// 				handleTxExpiration()
// 			}
//
// 			return false
// 		}
//
// 		ctx.txSent = true
// 	}
//
// 	return true
// }
