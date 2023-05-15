package contracts

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/stretchr/testify/require"
)

func TestGetDir(t *testing.T) {
	cs, err := Get()
	require.NoError(t, err)

	// names are taken from neofs-contract repository
	require.Equal(t, "NameService", cs.NNS.Manifest.Name)
	require.Equal(t, "NeoFS Alphabet", cs.Alphabet.Manifest.Name)
	require.Len(t, cs.Others, 2)
	require.Equal(t, "NeoFS Audit", cs.Others[0].Manifest.Name)
	require.Equal(t, "NeoFS Balance", cs.Others[1].Manifest.Name)
}

func TestGet(t *testing.T) {
	_fs := fstest.MapFS{}

	// missing or invalid NNS
	_, err := get(_fs)
	require.ErrorIs(t, err, fs.ErrNotExist)

	setInvalidContractFiles(t, _fs, "nns")
	_, err = get(_fs)
	require.Error(t, err)

	// add NNS
	nnsContract := setValidContractFiles(t, _fs, "nns")

	// missing or invalid Alphabet
	_, err = get(_fs)
	require.ErrorIs(t, err, fs.ErrNotExist)

	setInvalidContractFiles(t, _fs, "alphabet")
	_, err = get(_fs)
	require.Error(t, err)

	alphabetContract := setValidContractFiles(t, _fs, "alphabet")

	// missing or invalid indexed contracts
	_, err = get(_fs)
	require.Error(t, err)

	_fs["any-contract.nef"] = &fstest.MapFile{Data: []byte("any script")}

	_, err = get(_fs)
	require.Error(t, err)

	delete(_fs, "any-contract.nef")

	// add indexed contracts
	setInvalidContractFiles(t, _fs, "0")
	_, err = get(_fs)
	require.Error(t, err)

	firstContract := setValidContractFiles(t, _fs, "0")

	_, err = get(_fs)
	require.NoError(t, err)

	// add hole
	thirdContract := setValidContractFiles(t, _fs, "2")
	_, err = get(_fs)
	require.Error(t, err)

	// fill hole
	secondContract := setValidContractFiles(t, _fs, "1")
	c, err := get(_fs)
	require.NoError(t, err)

	// check correctness
	require.Equal(t, c.NNS, nnsContract)
	require.Equal(t, c.Alphabet, alphabetContract)
	require.Len(t, c.Others, 3)
	require.Equal(t, c.Others[0], firstContract)
	require.Equal(t, c.Others[1], secondContract)
	require.Equal(t, c.Others[2], thirdContract)
}

func setValidContractFiles(tb testing.TB, _fs fstest.MapFS, prefix string) Contract {
	_setContractFiles(tb, _fs, prefix, true)

	var c Contract

	err := readContractFromFilesWithPrefix(&c, _fs, prefix)
	require.NoError(tb, err)

	return c
}

func setInvalidContractFiles(tb testing.TB, _fs fstest.MapFS, prefix string) {
	_setContractFiles(tb, _fs, prefix, false)
}

func _setContractFiles(tb testing.TB, _fs fstest.MapFS, prefix string, valid bool) {
	var bNEF, jManifest []byte
	if valid {
		_nef, err := nef.NewFile([]byte(fmt.Sprintf("any %s script", prefix)))
		require.NoError(tb, err)

		bNEF, err = _nef.Bytes()
		require.NoError(tb, err)

		jManifest, err = json.Marshal(manifest.NewManifest(prefix + " contract"))
		require.NoError(tb, err)
	} else {
		bNEF = []byte("invalid NEF")
		jManifest = []byte("invalid manifest")
	}

	_fs[prefix+"-contract.nef"] = &fstest.MapFile{Data: bNEF}
	_fs[prefix+"-contract.manifest.json"] = &fstest.MapFile{Data: jManifest}
}
