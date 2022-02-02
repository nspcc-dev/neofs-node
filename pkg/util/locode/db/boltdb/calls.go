package locodebolt

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/util"
	locodedb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db"
	"go.etcd.io/bbolt"
)

// Open opens underlying BoltDB instance.
//
// Timeout of BoltDB opening is 3s (only for Linux or Darwin).
//
// Opens BoltDB in read-only mode if DB is read-only.
func (db *DB) Open() error {
	// copy-paste from metabase:
	// consider universal Open/Close for BoltDB wrappers

	err := util.MkdirAllX(filepath.Dir(db.path), db.mode)
	if err != nil {
		return fmt.Errorf("could not create dir for BoltDB: %w", err)
	}

	db.bolt, err = bbolt.Open(db.path, db.mode, db.boltOpts)
	if err != nil {
		return fmt.Errorf("could not open BoltDB: %w", err)
	}

	return nil
}

// Close closes underlying BoltDB instance.
//
// Must not be called before successful Open call.
func (db *DB) Close() error {
	return db.bolt.Close()
}

func countryBucketKey(cc *locodedb.CountryCode) ([]byte, error) {
	return []byte(cc.String()), nil
}

func locationBucketKey(lc *locodedb.LocationCode) ([]byte, error) {
	return []byte(lc.String()), nil
}

type recordJSON struct {
	CountryName  string
	LocationName string
	SubDivName   string
	SubDivCode   string
	Latitude     float64
	Longitude    float64
	Continent    string
}

func recordValue(r locodedb.Record) ([]byte, error) {
	p := r.GeoPoint()

	rj := &recordJSON{
		CountryName:  r.CountryName(),
		LocationName: r.LocationName(),
		SubDivName:   r.SubDivName(),
		SubDivCode:   r.SubDivCode(),
		Latitude:     p.Latitude(),
		Longitude:    p.Longitude(),
		Continent:    r.Continent().String(),
	}

	return json.Marshal(rj)
}

func recordFromValue(data []byte) (*locodedb.Record, error) {
	rj := new(recordJSON)

	if err := json.Unmarshal(data, rj); err != nil {
		return nil, err
	}

	r := new(locodedb.Record)
	r.SetCountryName(rj.CountryName)
	r.SetLocationName(rj.LocationName)
	r.SetSubDivName(rj.SubDivName)
	r.SetSubDivCode(rj.SubDivCode)
	r.SetGeoPoint(locodedb.NewPoint(rj.Latitude, rj.Longitude))

	cont := locodedb.ContinentFromString(rj.Continent)
	r.SetContinent(&cont)

	return r, nil
}

// Put saves the record by key in underlying BoltDB instance.
//
// Country code from key is used for allocating the 1st level buckets.
// Records are stored in country buckets by location code from key.
// The records are stored in internal binary JSON format.
//
// Must not be called before successful Open call.
// Must not be called in read-only mode: behavior is undefined.
func (db *DB) Put(key locodedb.Key, rec locodedb.Record) error {
	return db.bolt.Update(func(tx *bbolt.Tx) error {
		countryKey, err := countryBucketKey(key.CountryCode())
		if err != nil {
			return err
		}

		bktCountry, err := tx.CreateBucketIfNotExists(countryKey)
		if err != nil {
			return fmt.Errorf("could not create country bucket: %w", err)
		}

		// TODO: write country name once in Country bucket

		locationKey, err := locationBucketKey(key.LocationCode())
		if err != nil {
			return err
		}

		cont, err := recordValue(rec)
		if err != nil {
			return err
		}

		return bktCountry.Put(locationKey, cont)
	})
}

var errRecordNotFound = errors.New("record not found")

// Get reads the record by key from underlying BoltDB instance.
//
// Returns an error if no record is presented by key in DB.
//
// Must not be called before successful Open call.
func (db *DB) Get(key locodedb.Key) (rec *locodedb.Record, err error) {
	err = db.bolt.View(func(tx *bbolt.Tx) error {
		countryKey, err := countryBucketKey(key.CountryCode())
		if err != nil {
			return err
		}

		bktCountry := tx.Bucket(countryKey)
		if bktCountry == nil {
			return errRecordNotFound
		}

		locationKey, err := locationBucketKey(key.LocationCode())
		if err != nil {
			return err
		}

		data := bktCountry.Get(locationKey)
		if data == nil {
			return errRecordNotFound
		}

		rec, err = recordFromValue(data)

		return err
	})

	return
}
