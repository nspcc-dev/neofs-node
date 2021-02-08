package locodedb

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/locode"
	locodecolumn "github.com/nspcc-dev/neofs-node/pkg/util/locode/column"
	"github.com/pkg/errors"
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
		return nil, errors.Wrap(err, "could not parse country")
	}

	location, err := LocationCodeFromString(lc.LocationCode())
	if err != nil {
		return nil, errors.Wrap(err, "could not parse location")
	}

	return &Key{
		cc: country,
		lc: location,
	}, nil
}

// CountryCode returns location's country code.
func (k *Key) CountryCode() *CountryCode {
	return k.cc
}

// LocationCode returns location code.
func (k *Key) LocationCode() *LocationCode {
	return k.lc
}

// Record represents the entry in NeoFS location database.
type Record struct {
	countryName string

	cityName string

	subDivName string

	subDivCode string

	p *Point

	cont *Continent
}

var errParseCoordinates = errors.New("invalid coordinates")

// NewRecord calculates Record from UN/LOCODE table record.
func NewRecord(r locode.Record) (*Record, error) {
	crd, err := locodecolumn.CoordinatesFromString(r.Coordinates)
	if err != nil {
		return nil, errors.Wrap(errParseCoordinates, err.Error())
	}

	point, err := PointFromCoordinates(crd)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse geo point")
	}

	return &Record{
		cityName:   r.NameWoDiacritics,
		subDivCode: r.SubDiv,
		p:          point,
	}, nil
}

// CountryName returns country name.
func (r *Record) CountryName() string {
	return r.countryName
}

// SetCountryName sets country name.
func (r *Record) SetCountryName(name string) {
	r.countryName = name
}

// CityName returns city name.
func (r *Record) CityName() string {
	return r.cityName
}

// SetCityName sets city name.
func (r *Record) SetCityName(name string) {
	r.cityName = name
}

// SubDivCode returns subdivision code.
func (r *Record) SubDivCode() string {
	return r.subDivCode
}

// SetSubDivCode sets subdivision code.
func (r *Record) SetSubDivCode(name string) {
	r.subDivCode = name
}

// SubDivName returns subdivision name.
func (r *Record) SubDivName() string {
	return r.subDivName
}

// SetSubDivName sets subdivision name.
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

// Continent returns location continent.
func (r *Record) Continent() *Continent {
	return r.cont
}

// SetContinent sets location continent.
func (r *Record) SetContinent(c *Continent) {
	r.cont = c
}
