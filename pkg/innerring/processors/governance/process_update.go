package governance

import (
	"encoding/binary"
	"sort"

	"go.uber.org/zap"
)

const (
	alphabetUpdateIDPrefix = "AlphabetUpdate"
)

func (gp *Processor) processAlphabetSync() {
	if !gp.alphabetState.IsAlphabet() {
		gp.log.Info("non alphabet mode, ignore alphabet sync")
		return
	}

	mainnetAlphabet, err := gp.mainnetClient.NeoFSAlphabetList()
	if err != nil {
		gp.log.Error("can't fetch alphabet list from main net",
			zap.String("error", err.Error()))
		return
	}

	sidechainAlphabet, err := gp.morphClient.Committee()
	if err != nil {
		gp.log.Error("can't fetch alphabet list from side chain",
			zap.String("error", err.Error()))
		return
	}

	newAlphabet, err := newAlphabetList(sidechainAlphabet, mainnetAlphabet)
	if err != nil {
		gp.log.Error("can't merge alphabet lists from main net and side chain",
			zap.String("error", err.Error()))
		return
	}

	if newAlphabet == nil {
		gp.log.Info("no governance update, alphabet list has not been changed")
		return
	}

	gp.log.Info("alphabet list has been changed, starting update")

	// 1. Vote to side chain committee via alphabet contracts.
	err = gp.voter.VoteForSidechainValidator(newAlphabet)
	if err != nil {
		gp.log.Error("can't vote for side chain committee",
			zap.String("error", err.Error()))
	}

	// 2. Update NeoFSAlphabet role in side chain.
	innerRing, err := gp.irFetcher.InnerRingKeys()
	if err != nil {
		gp.log.Error("can't fetch inner ring list from side chain",
			zap.String("error", err.Error()))
	} else {
		newInnerRing, err := updateInnerRing(innerRing, sidechainAlphabet, newAlphabet)
		if err != nil {
			gp.log.Error("can't create new inner ring list with new alphabet keys",
				zap.String("error", err.Error()))
		} else {
			sort.Sort(newInnerRing)

			if gp.notaryDisabled {
				err = gp.netmapClient.UpdateInnerRing(newInnerRing)
			} else {
				err = gp.morphClient.UpdateNeoFSAlphabetList(newInnerRing)
			}

			if err != nil {
				gp.log.Error("can't update inner ring list with new alphabet keys",
					zap.String("error", err.Error()))
			}
		}
	}

	if !gp.notaryDisabled {
		// 3. Update notary role in side chain.
		err = gp.morphClient.UpdateNotaryList(newAlphabet)
		if err != nil {
			gp.log.Error("can't update list of notary nodes in side chain",
				zap.String("error", err.Error()))
		}
	}

	// 4. Update NeoFS contract in main net.
	epoch := gp.epochState.EpochCounter()

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, epoch)

	id := append([]byte(alphabetUpdateIDPrefix), buf...)

	err = gp.neofsClient.AlphabetUpdate(id, newAlphabet)
	if err != nil {
		gp.log.Error("can't update list of alphabet nodes in neofs contract",
			zap.String("error", err.Error()))
	}

	gp.log.Info("finished alphabet list update")
}
