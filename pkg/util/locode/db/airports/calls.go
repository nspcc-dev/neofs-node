package airportsdb

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/nspcc-dev/neofs-node/pkg/util/locode"
	locodedb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db"
)

const (
	_ = iota - 1

	_ // Airport ID
	_ // Name
	airportCity
	airportCountry
	airportIATA
	_ // ICAO
	airportLatitude
	airportLongitude
	_ // Altitude
	_ // Timezone
	_ // DST
	_ // Tz database time zone
	_ // Type
	_ // Source

	airportFldNum
)

type record struct {
	city,
	country,
	iata,
	lat,
	lng string
}

// Get scans records of the OpenFlights Airport to in-memory table (once),
// and returns entry that matches passed UN/LOCODE record.
//
// Records are matched if they have the same country code and either
// same IATA code or same city name (location name in UN/LOCODE).
//
// Returns locodedb.ErrAirportNotFound if no entry matches.
func (db *DB) Get(locodeRecord locode.Record) (*locodedb.AirportRecord, error) {
	if err := db.initAirports(); err != nil {
		return nil, err
	}

	records := db.mAirports[locodeRecord.LOCODE.CountryCode()]

	for i := range records {
		if locodeRecord.LOCODE.LocationCode() != records[i].iata &&
			locodeRecord.NameWoDiacritics != records[i].city {
			continue
		}

		lat, err := strconv.ParseFloat(records[i].lat, 64)
		if err != nil {
			return nil, err
		}

		lng, err := strconv.ParseFloat(records[i].lng, 64)
		if err != nil {
			return nil, err
		}

		return &locodedb.AirportRecord{
			CountryName: records[i].country,
			Point:       locodedb.NewPoint(lat, lng),
		}, nil
	}

	return nil, locodedb.ErrAirportNotFound
}

const (
	_ = iota - 1

	countryName
	countryISOCode
	_ // dafif_code

	countryFldNum
)

// CountryName scans records of the OpenFlights Country table to in-memory table (once),
// and returns name of the country by code.
//
// Returns locodedb.ErrCountryNotFound if no entry matches.
func (db *DB) CountryName(code *locodedb.CountryCode) (name string, err error) {
	if err = db.initCountries(); err != nil {
		return
	}

	argCode := code.String()

	for cName, cCode := range db.mCountries {
		if cCode == argCode {
			name = cName
			break
		}
	}

	if name == "" {
		err = locodedb.ErrCountryNotFound
	}

	return
}

func (db *DB) initAirports() (err error) {
	db.airportsOnce.Do(func() {
		db.mAirports = make(map[string][]record)

		if err = db.initCountries(); err != nil {
			return
		}

		err = db.scanWords(db.airports, airportFldNum, func(words []string) error {
			countryCode := db.mCountries[words[airportCountry]]
			if countryCode != "" {
				db.mAirports[countryCode] = append(db.mAirports[countryCode], record{
					city:    words[airportCity],
					country: words[airportCountry],
					iata:    words[airportIATA],
					lat:     words[airportLatitude],
					lng:     words[airportLongitude],
				})
			}

			return nil
		})
	})

	return
}

func (db *DB) initCountries() (err error) {
	db.countriesOnce.Do(func() {
		db.mCountries = make(map[string]string)

		err = db.scanWords(db.countries, countryFldNum, func(words []string) error {
			db.mCountries[words[countryName]] = words[countryISOCode]

			return nil
		})
	})

	return
}

var errScanInt = errors.New("interrupt scan")

func (db *DB) scanWords(pm pathMode, num int, wordsHandler func([]string) error) error {
	tableFile, err := os.OpenFile(pm.path, os.O_RDONLY, pm.mode)
	if err != nil {
		return err
	}

	defer tableFile.Close()

	r := csv.NewReader(tableFile)
	r.ReuseRecord = true

	for {
		words, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return err
		} else if ln := len(words); ln != num {
			return fmt.Errorf("unexpected number of words %d", ln)
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
