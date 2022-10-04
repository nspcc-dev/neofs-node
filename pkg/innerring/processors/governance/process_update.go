package governance

import (
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	neofscontract "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs"
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

const (
	alphabetUpdateIDPrefix = "AlphabetUpdate"
)

func (gp *Processor) processAlphabetSync(txHash util.Uint256) {
	if !gp.alphabetState.IsAlphabet() {
		gp.log.Info("non alphabet mode, ignore alphabet sync")
		return
	}

	mainnetAlphabet, err := gp.mainnetClient.NeoFSAlphabetList()
	if err != nil {
		gp.log.Error("can't fetch alphabet list from main net",
			logger.FieldError(err),
		)
		return
	}

	sidechainAlphabet, err := gp.morphClient.Committee()
	if err != nil {
		gp.log.Error("can't fetch alphabet list from side chain",
			logger.FieldError(err),
		)
		return
	}

	newAlphabet, err := newAlphabetList(sidechainAlphabet, mainnetAlphabet)
	if err != nil {
		gp.log.Error("can't merge alphabet lists from main net and side chain",
			logger.FieldError(err),
		)
		return
	}

	if newAlphabet == nil {
		gp.log.Info("no governance update, alphabet list has not been changed")
		return
	}

	gp.log.Info("alphabet list has been changed, starting update",
		logger.FieldString("side_chain_alphabet", prettyKeys(sidechainAlphabet)),
		logger.FieldString("new_alphabet", prettyKeys(newAlphabet)),
	)

	votePrm := VoteValidatorPrm{
		Validators: newAlphabet,
		Hash:       &txHash,
	}

	// 1. Vote to sidechain committee via alphabet contracts.
	err = gp.voter.VoteForSidechainValidator(votePrm)
	if err != nil {
		gp.log.Error("can't vote for side chain committee",
			logger.FieldError(err),
		)
	}

	// 2. Update NeoFSAlphabet role in the sidechain.
	innerRing, err := gp.irFetcher.InnerRingKeys()
	if err != nil {
		gp.log.Error("can't fetch inner ring list from side chain",
			logger.FieldError(err),
		)
	} else {
		newInnerRing, err := updateInnerRing(innerRing, sidechainAlphabet, newAlphabet)
		if err != nil {
			gp.log.Error("can't create new inner ring list with new alphabet keys",
				logger.FieldError(err),
			)
		} else {
			sort.Sort(newInnerRing)

			gp.log.Info("update of the inner ring list",
				logger.FieldString("before", prettyKeys(innerRing)),
				logger.FieldString("after", prettyKeys(newInnerRing)),
			)

			if gp.notaryDisabled {
				updPrm := nmClient.UpdateIRPrm{}

				updPrm.SetKeys(newInnerRing)
				updPrm.SetHash(txHash)

				err = gp.netmapClient.UpdateInnerRing(updPrm)
			} else {
				updPrm := client.UpdateAlphabetListPrm{}

				updPrm.SetList(newInnerRing)
				updPrm.SetHash(txHash)

				err = gp.morphClient.UpdateNeoFSAlphabetList(updPrm)
			}

			if err != nil {
				gp.log.Error("can't update inner ring list with new alphabet keys",
					logger.FieldError(err))
			}
		}
	}

	if !gp.notaryDisabled {
		// 3. Update notary role in the sidechain.

		updPrm := client.UpdateNotaryListPrm{}

		updPrm.SetList(newAlphabet)
		updPrm.SetHash(txHash)

		err = gp.morphClient.UpdateNotaryList(updPrm)
		if err != nil {
			gp.log.Error("can't update list of notary nodes in side chain",
				logger.FieldError(err))
		}
	}

	// 4. Update NeoFS contract in the mainnet.
	epoch := gp.epochState.EpochCounter()

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, epoch)

	id := append([]byte(alphabetUpdateIDPrefix), buf...)

	prm := neofscontract.AlphabetUpdatePrm{}

	prm.SetID(id)
	prm.SetPubs(newAlphabet)

	err = gp.neofsClient.AlphabetUpdate(prm)
	if err != nil {
		gp.log.Error("can't update list of alphabet nodes in neofs contract",
			logger.FieldError(err))
	}

	gp.log.Info("finished alphabet list update")
}

func prettyKeys(keys keys.PublicKeys) string {
	const delimiter = ","

	sb := strings.Builder{}
	for _, key := range keys {
		sb.WriteString(hex.EncodeToString(key.Bytes()))
		sb.WriteString(delimiter)
	}

	return strings.TrimRight(sb.String(), delimiter)
}
