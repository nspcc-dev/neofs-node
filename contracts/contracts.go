package contracts

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
)

type Contract struct {
	NEF      nef.File
	Manifest manifest.Manifest
}

type Contracts struct {
	NNS      Contract
	Alphabet Contract
	Others   []Contract
}

//go:embed *-contract.nef *-contract.manifest.json
var _fs embed.FS

func Get() (Contracts, error) {
	return get(_fs)
}

const fileSuffixNEF = "-contract.nef"

func get(_fs fs.FS) (Contracts, error) {
	var res Contracts

	const nnsPrefix = "nns"
	err := readContractFromFilesWithPrefix(&res.NNS, _fs, nnsPrefix)
	if err != nil {
		return res, fmt.Errorf("read NNS contract: %w", err)
	}

	const alphabetPrefix = "alphabet"
	err = readContractFromFilesWithPrefix(&res.Alphabet, _fs, alphabetPrefix)
	if err != nil {
		return res, fmt.Errorf("read Alphabet contract: %w", err)
	}

	nefFiles, err := fs.Glob(_fs, "*"+fileSuffixNEF)
	if err != nil {
		return res, fmt.Errorf("match files with suffix %s", fileSuffixNEF)
	}

	mOrdered := make(map[int]Contract, len(nefFiles))

	for i := range nefFiles {
		prefix := strings.TrimSuffix(nefFiles[i], fileSuffixNEF)
		if prefix == nnsPrefix || prefix == alphabetPrefix {
			continue
		}

		index, err := strconv.Atoi(prefix)
		if err != nil {
			return res, fmt.Errorf("invalid prefix of file '%s', expected index number", nefFiles[i])
		}

		var c Contract

		err = readContractFromFilesWithPrefix(&c, _fs, prefix)
		if err != nil {
			return res, fmt.Errorf("read contract #%d: %w", index, err)
		}

		mOrdered[index] = c
	}

	if len(mOrdered) == 0 {
		return res, errors.New("missing indexed contracts")
	}

	res.Others = make([]Contract, len(mOrdered))
	var ok bool

	for i := range res.Others {
		res.Others[i], ok = mOrdered[i]
		if !ok {
			return res, fmt.Errorf("missing index number %d", i)
		}
	}

	return res, nil
}

func readContractFromFilesWithPrefix(c *Contract, _fs fs.FS, filePrefix string) error {
	fNEF, err := _fs.Open(filePrefix + fileSuffixNEF)
	if err != nil {
		return fmt.Errorf("open file containing contract NEF: %w", err)
	}
	defer fNEF.Close()

	fManifest, err := _fs.Open(filePrefix + "-contract.manifest.json")
	if err != nil {
		return fmt.Errorf("open file containing contract NEF: %w", err)
	}
	defer fManifest.Close()

	bReader := io.NewBinReaderFromIO(fNEF)
	c.NEF.DecodeBinary(bReader)
	if bReader.Err != nil {
		return fmt.Errorf("decode contract NEF from binary: %w", bReader.Err)
	}

	err = json.NewDecoder(fManifest).Decode(&c.Manifest)
	if err != nil {
		return fmt.Errorf("decode contract manifest from JSON")
	}

	return nil
}
