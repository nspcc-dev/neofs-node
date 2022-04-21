package locodedb

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/util/locode"
	locodecolumn "github.com/nspcc-dev/neofs-node/pkg/util/locode/column"
)

// Key represents the key in NeoFS location database.
type Key struct {
	cc *CountryCode

	lc *LocationCode
}

// NewKey calculates Key from LOCODE.
func NewKey(lc locode.LOCODE) (*Key, error) {
	country, err := CountryCodeFromString(lc.CountryCode())
	if err != nil {
		return nil, fmt.Errorf("could not parse country: %w", err)
	}

	location, err := LocationCodeFromString(lc.LocationCode())
	if err != nil {
		return nil, fmt.Errorf("could not parse location: %w", err)
	}

	return &Key{
		cc: country,
		lc: location,
	}, nil
}

// CountryCode returns the location's country code.
func (k *Key) CountryCode() *CountryCode {
	return k.cc
}

// LocationCode returns the location code.
func (k *Key) LocationCode() *LocationCode {
	return k.lc
}

// Record represents the entry in NeoFS location database.
type Record struct {
	countryName string

	locationName string

	subDivName string

	subDivCode string

	p *Point

	cont *Continent
}

var errParseCoordinates = errors.New("invalid coordinates")

// NewRecord calculates the Record from the UN/LOCODE table record.
func NewRecord(r locode.Record) (*Record, error) {
	crd, err := locodecolumn.CoordinatesFromString(r.Coordinates)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errParseCoordinates, err)
	}

	point, err := PointFromCoordinates(crd)
	if err != nil {
		return nil, fmt.Errorf("could not parse geo point: %w", err)
	}

	return &Record{
		locationName: r.NameWoDiacritics,
		subDivCode:   r.SubDiv,
		p:            point,
	}, nil
}

// CountryName returns the country name.
func (r *Record) CountryName() string {
	return r.countryName
}

// SetCountryName sets the country name.
func (r *Record) SetCountryName(name string) {
	r.countryName = name
}

// LocationName returns the location name.
func (r *Record) LocationName() string {
	return r.locationName
}

// SetLocationName sets the location name.
func (r *Record) SetLocationName(name string) {
	r.locationName = name
}

// SubDivCode returns the subdivision code.
func (r *Record) SubDivCode() string {
	return r.subDivCode
}

// SetSubDivCode sets the subdivision code.
func (r *Record) SetSubDivCode(name string) {
	r.subDivCode = name
}

// SubDivName returns the subdivision name.
func (r *Record) SubDivName() string {
	return r.subDivName
}

// SetSubDivName sets the subdivision name.
func (r *Record) SetSubDivName(name string) {
	r.subDivName = name
}

// GeoPoint returns geo point of the location.
func (r *Record) GeoPoint() *Point {
	return r.p
}

// SetGeoPoint sets geo point of the location.
func (r *Record) SetGeoPoint(p *Point) {
	r.p = p
}

// Continent returns the location continent.
func (r *Record) Continent() *Continent {
	return r.cont
}

// SetContinent sets the location continent.
func (r *Record) SetContinent(c *Continent) {
	r.cont = c
}
