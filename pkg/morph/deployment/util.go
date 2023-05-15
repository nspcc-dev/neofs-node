package deployment

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

type contractVersion struct{ major, minor, patch uint64 }

func newContractVersion(major, minor, patch uint64) contractVersion {
	return contractVersion{
		major: major,
		minor: minor,
		patch: patch,
	}
}

func newContractVersionFromNumber(n uint64) contractVersion {
	const majorSpace, minorSpace = 1e6, 1e3
	mjr := n / majorSpace
	mnr := (n - mjr*majorSpace) / minorSpace
	return newContractVersion(mjr, mnr, n%minorSpace)
}

func (x contractVersion) equals(major, minor, patch uint64) bool {
	return x.major == major && x.minor == minor && x.patch == patch
}

func (x contractVersion) String() string {
	const sep = "."
	return fmt.Sprintf("%d%s%d%s%d", x.major, sep, x.minor, sep, x.patch)
}

// FIXME: functions below use unstable approach with sub-stringing, but currently
//  there is no other way
//  Track https://github.com/nspcc-dev/neofs-node/issues/2285

var (
	errNotEnoughGAS = errors.New("not enough GAS")
)

func isErrContractNotFound(err error) bool {
	return strings.Contains(err.Error(), "Unknown contract")
}

func isErrNotEnoughGAS(err error) bool {
	return errors.Is(err, neorpc.ErrValidationFailed) && strings.Contains(err.Error(), "insufficient funds")
}

func setGroupInManifest(_manifest *manifest.Manifest, _nef nef.File, groupPrivKey *keys.PrivateKey, deployerAcc util.Uint160) {
	contractAddress := state.CreateContractHash(deployerAcc, _nef.Checksum, _manifest.Name)
	sig := groupPrivKey.Sign(contractAddress.BytesBE())
	groupPubKey := groupPrivKey.PublicKey()

	ind := groupIndexInManifest(*_manifest, groupPubKey)
	if ind >= 0 {
		_manifest.Groups[ind].Signature = sig
		return
	}

	_manifest.Groups = append(_manifest.Groups, manifest.Group{
		PublicKey: groupPubKey,
		Signature: sig,
	})
}

func groupIndexInManifest(_manifest manifest.Manifest, groupPubKey *keys.PublicKey) int {
	for i := range _manifest.Groups {
		if _manifest.Groups[i].PublicKey.Equal(groupPubKey) {
			return i
		}
	}
	return -1
}
