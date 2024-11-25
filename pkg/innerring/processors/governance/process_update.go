package governance

import (
	"encoding/binary"
	"sort"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"go.uber.org/zap"
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
			zap.Error(err))
		return
	}

	sidechainAlphabet, err := gp.morphClient.Committee()
	if err != nil {
		gp.log.Error("can't fetch alphabet list from side chain",
			zap.Error(err))
		return
	}

	newAlphabet, err := newAlphabetList(sidechainAlphabet, mainnetAlphabet)
	if err != nil {
		gp.log.Error("can't merge alphabet lists from main net and side chain",
			zap.Error(err))
		return
	}

	if newAlphabet == nil {
		gp.log.Info("no governance update, alphabet list has not been changed")
		return
	}

	gp.log.Info("alphabet list has been changed, starting update",
		zap.String("side_chain_alphabet", prettyKeys(sidechainAlphabet)),
		zap.String("new_alphabet", prettyKeys(newAlphabet)),
	)

	// 1. Vote to sidechain committee via alphabet contracts.
	err = gp.voter.VoteForSidechainValidator(newAlphabet, &txHash)
	if err != nil {
		gp.log.Error("can't vote for side chain committee",
			zap.Error(err))
	}

	// 2. Update NeoFSAlphabet role in the sidechain.
	innerRing, err := gp.irFetcher.InnerRingKeys()
	if err != nil {
		gp.log.Error("can't fetch inner ring list from side chain",
			zap.Error(err))
	} else {
		newInnerRing, err := updateInnerRing(innerRing, sidechainAlphabet, newAlphabet)
		if err != nil {
			gp.log.Error("can't create new inner ring list with new alphabet keys",
				zap.Error(err))
		} else {
			sort.Sort(newInnerRing)

			gp.log.Info("update of the inner ring list",
				zap.String("before", prettyKeys(innerRing)),
				zap.String("after", prettyKeys(newInnerRing)),
			)

			err = gp.morphClient.UpdateNeoFSAlphabetList(newInnerRing, txHash)

			if err != nil {
				gp.log.Error("can't update inner ring list with new alphabet keys",
					zap.Error(err))
			}
		}
	}

	// 3. Update notary role in the sidechain.
	err = gp.morphClient.UpdateNotaryList(newAlphabet, txHash)
	if err != nil {
		gp.log.Error("can't update list of notary nodes in side chain",
			zap.Error(err))
	}

	// 4. Update NeoFS contract in the mainnet.
	id := binary.LittleEndian.AppendUint64([]byte(alphabetUpdateIDPrefix), gp.epochState.EpochCounter())

	err = gp.neofsClient.AlphabetUpdate(id, newAlphabet)
	if err != nil {
		gp.log.Error("can't update list of alphabet nodes in neofs contract",
			zap.Error(err))
	}

	gp.log.Info("finished alphabet list update")
}

func prettyKeys(keys keys.PublicKeys) string {
	const delimiter = ","

	sb := strings.Builder{}
	for _, key := range keys {
		sb.WriteString(key.StringCompressed())
		sb.WriteString(delimiter)
	}

	return strings.TrimRight(sb.String(), delimiter)
}
