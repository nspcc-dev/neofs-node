package locodedb

import (
	"fmt"
	"strconv"

	locodecolumn "github.com/nspcc-dev/neofs-node/pkg/util/locode/column"
)

// Point represents 2D geographic point.
type Point struct {
	lat, lng float64
}

// NewPoint creates, initializes and returns new Point.
func NewPoint(lat, lng float64) *Point {
	return &Point{
		lat: lat,
		lng: lng,
	}
}

// Latitude returns Point's latitude.
func (p Point) Latitude() float64 {
	return p.lat
}

// Longitude returns Point's longitude.
func (p Point) Longitude() float64 {
	return p.lng
}

// PointFromCoordinates converts UN/LOCODE coordinates to Point.
func PointFromCoordinates(crd *locodecolumn.Coordinates) (*Point, error) {
	if crd == nil {
		return nil, nil
	}

	cLat := crd.Latitude()
	cLatDeg := cLat.Degrees()
	cLatMnt := cLat.Minutes()

	lat, err := toDecimal(cLatDeg[:], cLatMnt[:])
	if err != nil {
		return nil, fmt.Errorf("could not parse latitude: %w", err)
	}

	if !cLat.Hemisphere().North() {
		lat = -lat
	}

	cLng := crd.Longitude()
	cLngDeg := cLng.Degrees()
	cLngMnt := cLng.Minutes()

	lng, err := toDecimal(cLngDeg[:], cLngMnt[:])
	if err != nil {
		return nil, fmt.Errorf("could not parse longitude: %w", err)
	}

	if !cLng.Hemisphere().East() {
		lng = -lng
	}

	return &Point{
		lat: lat,
		lng: lng,
	}, nil
}

func toDecimal(intRaw, minutesRaw []byte) (float64, error) {
	integer, err := strconv.ParseFloat(string(intRaw), 64)
	if err != nil {
		return 0, fmt.Errorf("could not parse integer part: %w", err)
	}

	decimal, err := minutesToDegrees(minutesRaw)
	if err != nil {
		return 0, fmt.Errorf("could not parse decimal part: %w", err)
	}

	return integer + decimal, nil
}

// minutesToDegrees converts minutes to decimal part of a degree
func minutesToDegrees(raw []byte) (float64, error) {
	minutes, err := strconv.ParseFloat(string(raw), 64)
	if err != nil {
		return 0, err
	}

	return minutes / 60, nil
}
