package contracts

import (
	"crypto/rand"
	"encoding/json"
	"testing"
	"testing/fstest"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/stretchr/testify/require"
)

func TestReadRepo(t *testing.T) {
	_, err := Read()
	require.NoError(t, err)
}

func TestReadOrder(t *testing.T) {
	_nef0, bNEF0 := anyValidNEF(t)
	_manifest0, jManifest0 := anyValidManifest(t, "first")
	_nef1, bNEF1 := anyValidNEF(t)
	_manifest1, jManifest1 := anyValidManifest(t, "second")
	_nef11, bNEF11 := anyValidNEF(t)
	_manifest11, jManifest11 := anyValidManifest(t, "twelfth")

	_fs := fstest.MapFS{
		"00-hello.nef":           {Data: bNEF0},
		"00-hello.manifest.json": {Data: jManifest0},
		"01-world.nef":           {Data: bNEF1},
		"01-world.manifest.json": {Data: jManifest1},
		"11-bye.nef":             {Data: bNEF11},
		"11-bye.manifest.json":   {Data: jManifest11},
	}

	cs, err := read(_fs)
	require.NoError(t, err)
	require.Len(t, cs, 3)

	require.Equal(t, _nef0, cs[0].NEF)
	require.Equal(t, _manifest0, cs[0].Manifest)
	require.Equal(t, _nef1, cs[1].NEF)
	require.Equal(t, _manifest1, cs[1].Manifest)
	require.Equal(t, _nef11, cs[2].NEF)
	require.Equal(t, _manifest11, cs[2].Manifest)
}

func TestReadInvalidFilenames(t *testing.T) {
	_fs := fstest.MapFS{}

	_, err := read(_fs)
	require.NoError(t, err)

	for _, invalidName := range []string{
		"hello.nef",
		"-.nef",
		"-0.nef",
		"0-.nef",
		"0-_.nef",
		"0-1.nef",
		".nef",
	} {
		_fs[invalidName] = &fstest.MapFile{}
		_, err = read(_fs)
		require.ErrorIs(t, err, errInvalidFilename, invalidName)
		delete(_fs, invalidName)
	}
}

func TestReadDuplicatedContract(t *testing.T) {
	_, bNEF := anyValidNEF(t)
	_, jManifest := anyValidManifest(t, "some name")

	_fs := fstest.MapFS{
		"01-hello.nef":            {Data: bNEF},
		"01-hello.manifest.json":  {Data: jManifest},
		"001-hello.nef":           {Data: bNEF},
		"001-hello.manifest.json": {Data: jManifest},
	}

	_, err := read(_fs)
	require.ErrorIs(t, err, errDuplicatedContract)
}

func TestReadInvalidFormat(t *testing.T) {
	_fs := fstest.MapFS{}

	_, validNEF := anyValidNEF(t)
	_, validManifest := anyValidManifest(t, "zero")

	_fs["00-hello.nef"] = &fstest.MapFile{Data: validNEF}
	_fs["00-hello.manifest.json"] = &fstest.MapFile{Data: validManifest}

	_, err := read(_fs)
	require.NoError(t, err, errInvalidNEF)

	_fs["00-hello.nef"] = &fstest.MapFile{Data: []byte("not a NEF")}
	_fs["00-hello.manifest.json"] = &fstest.MapFile{Data: validManifest}

	_, err = read(_fs)
	require.ErrorIs(t, err, errInvalidNEF)

	_fs["00-hello.nef"] = &fstest.MapFile{Data: validNEF}
	_fs["00-hello.manifest.json"] = &fstest.MapFile{Data: []byte("not a manifest")}

	_, err = read(_fs)
	require.ErrorIs(t, err, errInvalidManifest)
}

func anyValidNEF(tb testing.TB) (nef.File, []byte) {
	script := make([]byte, 32)
	rand.Read(script)

	_nef, err := nef.NewFile(script)
	require.NoError(tb, err)

	bNEF, err := _nef.Bytes()
	require.NoError(tb, err)

	return *_nef, bNEF
}

func anyValidManifest(tb testing.TB, name string) (manifest.Manifest, []byte) {
	_manifest := manifest.NewManifest(name)

	jManifest, err := json.Marshal(_manifest)
	require.NoError(tb, err)

	return *_manifest, jManifest
}
