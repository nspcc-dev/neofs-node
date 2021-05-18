package locode

import (
	"errors"
	"strings"
)

// LOCODE represents code from UN/LOCODE coding scheme.
type LOCODE [2]string

// Record represents a single record of the UN/LOCODE table.
type Record struct {
	// Change Indicator.
	Ch string

	// Combination of a 2-character country code and a 3-character location code.
	LOCODE LOCODE

	// Name of the locations which has been allocated a UN/LOCODE.
	Name string

	// Names of the locations which have been allocated a UN/LOCODE without diacritic signs.
	NameWoDiacritics string

	// ISO 1-3 character alphabetic and/or numeric code for the administrative division of the country concerned.
	SubDiv string

	// 8-digit function classifier code for the location.
	Function string

	// Status of the entry by a 2-character code.
	Status string

	// Last date when the location was updated/entered.
	Date string

	// The IATA code for the location if different from location code in column LOCODE.
	IATA string

	// Geographical coordinates (latitude/longitude) of the location, if there is any.
	Coordinates string

	// Some general remarks regarding the UN/LOCODE in question.
	Remarks string
}

// ErrInvalidString is the error of incorrect string format of the LOCODE.
var ErrInvalidString = errors.New("invalid string format in UN/Locode")

// FromString parses string and returns LOCODE.
//
// If string has incorrect format, ErrInvalidString returns.
func FromString(s string) (*LOCODE, error) {
	const locationSeparator = " "

	words := strings.Split(s, locationSeparator)
	if ln := len(words); ln != 1 && ln != 2 {
		return nil, ErrInvalidString
	}

	l := new(LOCODE)
	copy(l[:], words)

	return l, nil
}

// CountryCode returns a string representation of country code.
func (l *LOCODE) CountryCode() string {
	return l[0]
}

// LocationCode returns a string representation of location code.
func (l *LOCODE) LocationCode() string {
	return l[1]
}
