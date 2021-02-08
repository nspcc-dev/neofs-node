package airportsdb

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"

	"github.com/nspcc-dev/neofs-node/pkg/util/locode"
	locodedb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db"
	"github.com/pkg/errors"
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

// Get scans records of the OpenFlights Airport table one-by-one, and
// returns entry that matches passed UN/LOCODE record.
//
// Records are matched if they have the same country code and either
// same IATA code or same city name (location name in UN/LOCODE).
//
// Returns locodedb.ErrAirportNotFound if no entry matches.
func (db *DB) Get(locodeRecord locode.Record) (rec *locodedb.AirportRecord, err error) {
	err = db.scanWords(db.airports, airportFldNum, func(words []string) error {
		airportRecord := record{
			city:    words[airportCity],
			country: words[airportCountry],
			iata:    words[airportIATA],
			lat:     words[airportLatitude],
			lng:     words[airportLongitude],
		}

		if related, err := db.isRelated(locodeRecord, airportRecord); err != nil || !related {
			return err
		}

		lat, err := strconv.ParseFloat(airportRecord.lat, 64)
		if err != nil {
			return err
		}

		lng, err := strconv.ParseFloat(airportRecord.lng, 64)
		if err != nil {
			return err
		}

		rec = &locodedb.AirportRecord{
			CountryName: airportRecord.country,
			Point:       locodedb.NewPoint(lat, lng),
		}

		return errScanInt
	})

	if err == nil && rec == nil {
		err = locodedb.ErrAirportNotFound
	}

	return
}

func (db *DB) isRelated(locodeRecord locode.Record, airportRecord record) (bool, error) {
	countryCode, err := db.countryCode(airportRecord.country)
	if err != nil {
		return false, errors.Wrap(err, "could not read country code of airport")
	}

	sameCountry := locodeRecord.LOCODE.CountryCode() == countryCode
	sameIATA := locodeRecord.LOCODE.LocationCode() == airportRecord.iata

	if sameCountry && sameIATA {
		return true, nil
	}

	sameCity := locodeRecord.Name == airportRecord.city

	return sameCountry && sameCity, nil
}

const (
	_ = iota - 1

	countryName
	countryISOCode
	_ // dafif_code

	countryFldNum
)

// CountryName scans records of the OpenFlights Country table, and returns
// name of the country by code.
//
// Returns locodedb.ErrCountryNotFound if no entry matches.
func (db *DB) CountryName(code *locodedb.CountryCode) (name string, err error) {
	err = db.scanWords(db.countries, countryFldNum, func(words []string) error {
		if words[countryISOCode] == code.String() {
			name = words[countryName]
			return errScanInt
		}

		return nil
	})

	if err == nil && name == "" {
		err = locodedb.ErrCountryNotFound
	}

	return
}

func (db *DB) countryCode(country string) (code string, err error) {
	err = db.scanWords(db.countries, countryFldNum, func(words []string) error {
		if words[countryName] == country {
			code = words[countryISOCode]
			return errScanInt
		}

		return nil
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
			return errors.Errorf("unexpected number of words %d", ln)
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
