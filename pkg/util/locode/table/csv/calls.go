package csvlocode

import (
	"encoding/csv"
	"io"
	"os"
	"strings"

	"github.com/nspcc-dev/neofs-node/pkg/util/locode"
	locodedb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db"
	"github.com/pkg/errors"
)

var errInvalidRecord = errors.New("invalid table record")

// IterateAll scans table record one-by-one, parses UN/LOCODE record
// from it and passes it to f.
//
// Returns f's errors directly.
func (t *Table) IterateAll(f func(locode.Record) error) error {
	const wordsPerRecord = 12

	return t.scanWords(t.paths, wordsPerRecord, func(words []string) error {
		lc, err := locode.FromString(strings.Join(words[1:3], " "))
		if err != nil {
			return err
		}

		record := locode.Record{
			Ch:               words[0],
			LOCODE:           *lc,
			Name:             words[3],
			NameWoDiacritics: words[4],
			SubDiv:           words[5],
			Function:         words[6],
			Status:           words[7],
			Date:             words[8],
			IATA:             words[9],
			Coordinates:      words[10],
			Remarks:          words[11],
		}

		return f(record)
	})
}

// SubDivName scans table record one-by-one, and returns subdivision name
// on country and subdivision codes match.
//
// Returns locodedb.ErrSubDivNotFound if no entry matches.
func (t *Table) SubDivName(countryCode *locodedb.CountryCode, name string) (subDiv string, err error) {
	const wordsPerRecord = 4

	err = t.scanWords([]string{t.subDivPath}, wordsPerRecord, func(words []string) error {
		if words[0] == countryCode.String() && words[1] == name {
			subDiv = words[2]
			return errScanInt
		}

		return nil
	})

	if err == nil && subDiv == "" {
		err = locodedb.ErrSubDivNotFound
	}

	return
}

var errScanInt = errors.New("interrupt scan")

func (t *Table) scanWords(paths []string, fpr int, wordsHandler func([]string) error) error {
	var (
		rdrs    = make([]io.Reader, 0, len(t.paths))
		closers = make([]io.Closer, 0, len(t.paths))
	)

	for i := range paths {
		file, err := os.OpenFile(paths[i], os.O_RDONLY, t.mode)
		if err != nil {
			return err
		}

		rdrs = append(rdrs, file)
		closers = append(closers, file)
	}

	defer func() {
		for i := range closers {
			_ = closers[i].Close()
		}
	}()

	r := csv.NewReader(io.MultiReader(rdrs...))
	r.ReuseRecord = true
	r.FieldsPerRecord = fpr

	for {
		words, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return err
		} else if len(words) != fpr {
			return errInvalidRecord
		}

		if err := wordsHandler(words); err != nil {
			if errors.Is(err, errScanInt) {
				break
			}

			return err
		}
	}

	return nil
}
