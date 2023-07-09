package deploy

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nns"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"go.uber.org/zap"
)

// initCommitteeGroupPrm groups parameters of committee group initialization.
type initCommitteeGroupPrm struct {
	logger *zap.Logger

	blockchain Blockchain

	// based on blockchain
	monitor *blockchainMonitor

	nnsOnChainAddress util.Uint160
	systemEmail       string

	committee              keys.PublicKeys
	localAcc               *wallet.Account
	localAccCommitteeIndex int

	keyStorage KeyStorage
}

// initCommitteeGroup initializes committee group and returns corresponding private key.
func initCommitteeGroup(ctx context.Context, prm initCommitteeGroupPrm) (*keys.PrivateKey, error) {
	// wrap the parent context into the context of the current function so that
	// transaction wait routines do not leak
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	inv := invoker.New(prm.blockchain, nil)
	const leaderCommitteeIndex = 0
	var committeeGroupKey *keys.PrivateKey
	var leaderTick func()

upperLoop:
	for ; ; prm.monitor.waitForNextBlock(ctx) {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for committee group key to be distributed: %w", ctx.Err())
		default:
		}

		prm.logger.Info("checking domains with shared committee group key...")

		nShared := 0

		for i := range prm.committee {
			domain := committeeGroupDomainForMember(i)

			rec, err := lookupNNSDomainRecord(inv, prm.nnsOnChainAddress, domain)
			if err != nil {
				if errors.Is(err, errMissingDomain) || errors.Is(err, errMissingDomainRecord) {
					prm.logger.Info("NNS record with committee group key shared with the committee member is still missing, waiting...",
						zap.String("domain", domain))
				} else {
					prm.logger.Error("failed to lookup NNS domain record, will try again later",
						zap.String("domain", domain), zap.Error(err))
				}

				continue
			}

			if committeeGroupKey == nil && i == prm.localAccCommitteeIndex {
				committeeGroupKey, err = decryptSharedPrivateKey(rec, prm.committee[leaderCommitteeIndex], prm.localAcc.PrivateKey())
				if err != nil {
					prm.logger.Error("failed to decrypt shared committee group key, will wait for a background fix",
						zap.String("domain", domain), zap.Error(err))
					continue upperLoop
				}
			}

			nShared++
		}

		if nShared == len(prm.committee) {
			prm.logger.Info("committee group key is distributed between all committee members")
			return committeeGroupKey, nil
		}

		prm.logger.Info("not all committee members received the committee group key, distribution is needed",
			zap.Int("need", len(prm.committee)), zap.Int("shared", nShared))

		if prm.localAccCommitteeIndex != leaderCommitteeIndex {
			prm.logger.Info("will wait for distribution from the leader")
			continue
		}

		var err error

		if committeeGroupKey == nil {
			committeeGroupKey, err = prm.keyStorage.GetPersistedPrivateKey()
			if err != nil {
				prm.logger.Error("failed to init committee group key, will try again later", zap.Error(err))
				continue
			}
		}

		if leaderTick == nil {
			leaderTick, err = initShareCommitteeGroupKeyAsLeaderTick(ctx, prm, committeeGroupKey)
			if err != nil {
				prm.logger.Error("failed to construct action sharing committee group key between committee members as leader, will try again later",
					zap.Error(err))
				continue
			}
		}

		leaderTick()
	}
}

// initShareCommitteeGroupKeyAsLeaderTick returns a function that preserves
// context of the committee group key distribution by leading committee member
// between calls.
func initShareCommitteeGroupKeyAsLeaderTick(ctx context.Context, prm initCommitteeGroupPrm, committeeGroupKey *keys.PrivateKey) (func(), error) {
	localActor, err := actor.NewSimple(prm.blockchain, prm.localAcc)
	if err != nil {
		return nil, fmt.Errorf("init transaction sender from local account: %w", err)
	}

	invkr := invoker.New(prm.blockchain, nil)

	// multi-tick context
	mDomainsToRegisterTxs := make(map[string]*transactionGroupMonitor, len(prm.committee))
	mDomainsToSetRecordTxs := make(map[string]*transactionGroupMonitor, len(prm.committee))

	return func() {
		prm.logger.Info("distributing committee group key between committee members using NNS...")

		for i := range prm.committee {
			domain := committeeGroupDomainForMember(i)
			l := prm.logger.With(zap.String("domain", domain), zap.Stringer("member", prm.committee[i]))

			l.Info("synchronizing committee group key with NNS domain record...")

			_, err := lookupNNSDomainRecord(invkr, prm.nnsOnChainAddress, domain)
			if err != nil {
				if errors.Is(err, errMissingDomain) {
					l.Info("NNS domain is missing, registration is needed")

					txMonitor, ok := mDomainsToRegisterTxs[domain]
					if ok {
						if txMonitor.isPending() {
							l.Info("previously sent transaction registering NNS domain is still pending, will wait for the outcome")
							continue
						}
					} else {
						txMonitor = newTransactionGroupMonitor(localActor)
						mDomainsToRegisterTxs[domain] = txMonitor
					}

					l.Info("sending new transaction registering domain in the NNS...")

					txID, vub, err := localActor.SendCall(prm.nnsOnChainAddress, methodNNSRegister,
						domain, localActor.Sender(), prm.systemEmail, nnsRefresh, nnsRetry, nnsExpire, nnsMinimum)
					if err != nil {
						if isErrNotEnoughGAS(err) {
							l.Info("not enough GAS to register domain in the NNS, will try again later")
						} else {
							l.Error("failed to send transaction registering domain in the NNS, will try again later", zap.Error(err))
						}
						continue
					}

					l.Info("transaction registering domain in the NNS has been successfully sent, will wait for the outcome",
						zap.Stringer("tx", txID), zap.Uint32("vub", vub))

					txMonitor.trackPendingTransactionsAsync(ctx, vub, txID)

					continue
				} else if !errors.Is(err, errMissingDomainRecord) {
					l.Error("failed to lookup NNS domain record, will try again later", zap.Error(err))
					continue
				}

				l.Info("missing record of the NNS domain, needed to be set")

				txMonitor, ok := mDomainsToSetRecordTxs[domain]
				if ok {
					if txMonitor.isPending() {
						l.Info("previously sent transaction setting NNS domain record is still pending, will wait for the outcome")
						continue
					}
				} else {
					txMonitor = newTransactionGroupMonitor(localActor)
					mDomainsToSetRecordTxs[domain] = txMonitor
				}

				l.Info("sharing encrypted committee group key with the committee member...")

				keyCipher, err := encryptSharedPrivateKey(committeeGroupKey, prm.localAcc.PrivateKey(), prm.committee[i])
				if err != nil {
					l.Error("failed to encrypt committee group key to share with the committee member, will try again later",
						zap.Error(err))
					continue
				}

				l.Info("sending new transaction setting domain record in the NNS...")

				txID, vub, err := localActor.SendCall(prm.nnsOnChainAddress, methodNNSAddRecord,
					domain, int64(nns.TXT), keyCipher)
				if err != nil {
					if isErrNotEnoughGAS(err) {
						l.Info("not enough GAS to set NNS domain record, will try again later")
					} else {
						l.Error("failed to send transaction setting NNS domain record, will try again later", zap.Error(err))
					}
					continue
				}

				l.Info("transaction setting NNS domain record has been successfully sent, will wait for the outcome",
					zap.Stringer("tx", txID), zap.Uint32("vub", vub))

				txMonitor.trackPendingTransactionsAsync(ctx, vub, txID)

				continue
			}

			l.Info("committee group key is shared with the committee member in NNS domain record")
		}
	}, nil
}

// encryptSharedPrivateKey encrypts private key using provided coder's private
// key to be decrypted using decoder's private key. Inverse operation to
// decryptSharedPrivateKey.
func encryptSharedPrivateKey(sharedPrivKey, coderPrivKey *keys.PrivateKey, decoderPubKey *keys.PublicKey) (string, error) {
	sharedSecret, err := calculateSharedSecret(coderPrivKey, decoderPubKey)
	if err != nil {
		return "", fmt.Errorf("calculate shared secret: %w", err)
	}

	cipherBlock, err := aes.NewCipher(sharedSecret)
	if err != nil {
		return "", fmt.Errorf("create AES cipher block: %w", err)
	}

	cipherMode, err := cipher.NewGCM(cipherBlock)
	if err != nil {
		return "", fmt.Errorf("wrap cipher block in GCM: %w", err)
	}

	nonce := make([]byte, cipherMode.NonceSize())

	_, err = rand.Reader.Read(nonce)
	if err != nil {
		return "", fmt.Errorf("generate nonce using crypto randomizer: %w", err)
	}

	bKeyCipher, err := cipherMode.Seal(nonce, nonce, sharedPrivKey.Bytes(), nil), nil
	if err != nil {
		return "", fmt.Errorf("encrypt key binary: %w", err)
	}

	return base64.StdEncoding.EncodeToString(bKeyCipher), nil
}

// decryptSharedPrivateKey decrypts cipher of the private key encrypted by
// coder's private key. Inverse operation to encryptSharedPrivateKey.
func decryptSharedPrivateKey(sharedPrivKeyCipher string, coderPubKey *keys.PublicKey, decoderPrivKey *keys.PrivateKey) (*keys.PrivateKey, error) {
	bKeyCipher, err := base64.StdEncoding.DecodeString(sharedPrivKeyCipher)
	if err != nil {
		return nil, fmt.Errorf("decode key cipher from base64: %w", err)
	}

	sharedSecret, err := calculateSharedSecret(decoderPrivKey, coderPubKey)
	if err != nil {
		return nil, fmt.Errorf("calculate shared secret: %w", err)
	}

	cipherBlock, err := aes.NewCipher(sharedSecret)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher block: %w", err)
	}

	cipherMode, err := cipher.NewGCM(cipherBlock)
	if err != nil {
		return nil, fmt.Errorf("wrap cipher block in GCM: %w", err)
	}

	nonceSize := cipherMode.NonceSize()
	if len(sharedPrivKeyCipher) < nonceSize {
		return nil, fmt.Errorf("too short cipher %d", len(sharedPrivKeyCipher))
	}

	bSharedPrivKey, err := cipherMode.Open(nil, bKeyCipher[:nonceSize], bKeyCipher[nonceSize:], nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt cipher: %w", err)
	}

	sharedPrivKey, err := keys.NewPrivateKeyFromBytes(bSharedPrivKey)
	if err != nil {
		return nil, fmt.Errorf("decode key binary: %w", err)
	}

	return sharedPrivKey, nil
}

func calculateSharedSecret(localPrivKey *keys.PrivateKey, remotePubKey *keys.PublicKey) ([]byte, error) {
	// this commented code will start working from go1.20 (it's fully compatible
	// with current implementation)
	//
	// localPrivKeyECDH, err := localPrivKey.ECDH()
	// if err != nil {
	// 	return nil, fmt.Errorf("local private key to ECDH key: %w", err)
	// }
	//
	// remotePubKeyECDH, err := (*ecdsa.PublicKey)(remotePubKey).ECDH()
	// if err != nil {
	// 	return nil, fmt.Errorf("remote public key to ECDH key: %w", err)
	// }
	//
	// sharedSecret, err := localPrivKeyECDH.ECDH(remotePubKeyECDH)
	// if err != nil {
	// 	return nil, fmt.Errorf("ECDH exchange: %w", err)
	// }
	//
	// return sharedSecret, nil

	x, _ := localPrivKey.ScalarMult(remotePubKey.X, remotePubKey.Y, localPrivKey.D.Bytes())
	return x.Bytes(), nil
}
