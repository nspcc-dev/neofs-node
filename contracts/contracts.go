/*
Package contracts embeds compiled Neo contracts and provides access to them.
*/
package contracts

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
)

// Contract groups information about Neo contract stored in the current package.
type Contract struct {
	NEF      nef.File
	Manifest manifest.Manifest
}

//go:embed *.nef *.manifest.json
var _fs embed.FS

// Read reads compiled contracts stored in the package sorted numerically.
// File schema:
//   - compiled executables (NEF) are named by pattern 'N-C.nef'
//   - JSON-encoded manifests are named by pattern 'N-C.manifest.json'
//
// where C is the contract name (a-z) and N is the serial number of the contract
// starting from 0. Leading zeros are ignored (except zero sequence
// corresponding to N=0).
//
// If NEF file exists, corresponding manifest file must exist. If manifest
// file is presented without corresponding NEF file, the contract is ignored.
//
// Read fails if contract files has invalid name or format.
func Read() ([]Contract, error) {
	return read(_fs)
}

const nefFileSuffix = ".nef"

var (
	errInvalidFilename    = errors.New("invalid file name")
	errDuplicatedContract = errors.New("duplicated contract")
	errInvalidNEF         = errors.New("invalid NEF")
	errInvalidManifest    = errors.New("invalid manifest")
)

type numberedContract struct {
	i int
	c Contract
}

type numberedContracts []numberedContract

func (x numberedContracts) Len() int           { return len(x) }
func (x numberedContracts) Less(i, j int) bool { return x[i].i < x[j].i }
func (x numberedContracts) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// read same as Read by allows to override source fs.FS.
func read(_fs fs.FS) ([]Contract, error) {
	nefFiles, err := fs.Glob(_fs, "*"+nefFileSuffix)
	if err != nil {
		return nil, fmt.Errorf("match files with suffix %s", nefFileSuffix)
	}

	cs := make(numberedContracts, 0, len(nefFiles))

	for i := range nefFiles {
		prefix := strings.TrimSuffix(nefFiles[i], nefFileSuffix)
		if prefix == "" {
			return nil, fmt.Errorf("%w: missing prefix '%s'", errInvalidFilename, nefFiles[i])
		}

		hyphenInd := strings.IndexByte(prefix, '-')
		if hyphenInd < 0 {
			return nil, fmt.Errorf("%w: missing hyphen '%s'", errInvalidFilename, nefFiles[i])
		}

		name := prefix[hyphenInd+1:]
		if len(name) == 0 {
			return nil, fmt.Errorf("%w: missing name '%s'", errInvalidFilename, nefFiles[i])
		}

		for i := range name {
			if name[i] < 'a' || name[i] > 'z' {
				return nil, fmt.Errorf("%w: unsupported char in name %c", errInvalidFilename, name[i])
			}
		}

		var ind int

		if noZerosPrefix := strings.TrimLeft(prefix[:hyphenInd], "0"); len(noZerosPrefix) > 0 {
			ind, err = strconv.Atoi(noZerosPrefix)
			if err != nil {
				return nil, fmt.Errorf("%w: invalid prefix of file name '%s' (expected serial number)", errInvalidFilename, nefFiles[i])
			} else if ind < 0 {
				return nil, fmt.Errorf("%w: negative serial number in file name '%s'", errInvalidFilename, nefFiles[i])
			}
		}

		for i := range cs {
			if cs[i].i == ind {
				return nil, fmt.Errorf("%w: more than one file with serial number #%d", errDuplicatedContract, ind)
			}
		}

		c, err := readContractFromFiles(_fs, prefix)
		if err != nil {
			return nil, fmt.Errorf("read contract #%d: %w", ind, err)
		}

		cs = append(cs, numberedContract{
			i: ind,
			c: c,
		})
	}

	sort.Sort(cs)

	res := make([]Contract, len(cs))

	for i := range cs {
		res[i] = cs[i].c
	}

	return res, nil
}

func readContractFromFiles(_fs fs.FS, filePrefix string) (c Contract, err error) {
	fNEF, err := _fs.Open(filePrefix + nefFileSuffix)
	if err != nil {
		return c, fmt.Errorf("open file containing contract NEF: %w", err)
	}
	defer fNEF.Close()

	fManifest, err := _fs.Open(filePrefix + ".manifest.json")
	if err != nil {
		return c, fmt.Errorf("open file containing contract NEF: %w", err)
	}
	defer fManifest.Close()

	bReader := io.NewBinReaderFromIO(fNEF)
	c.NEF.DecodeBinary(bReader)
	if bReader.Err != nil {
		return c, fmt.Errorf("%w: %w", errInvalidNEF, bReader.Err)
	}

	err = json.NewDecoder(fManifest).Decode(&c.Manifest)
	if err != nil {
		return c, fmt.Errorf("%w: %w", errInvalidManifest, err)
	}

	return
}
