package locodedb

import (
	"strconv"
	"strings"

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

	lat, err := strconv.ParseFloat(strings.Join([]string{
		string(cLatDeg[:]),
		string(cLatMnt[:]),
	}, "."), 64)
	if err != nil {
		return nil, err
	}

	if !cLat.Hemisphere().North() {
		lat = -lat
	}

	cLng := crd.Longitude()
	cLngDeg := cLng.Degrees()
	cLngMnt := cLng.Minutes()

	lng, err := strconv.ParseFloat(strings.Join([]string{
		string(cLngDeg[:]),
		string(cLngMnt[:]),
	}, "."), 64)
	if err != nil {
		return nil, err
	}

	if !cLng.Hemisphere().East() {
		lng = -lng
	}

	return &Point{
		lat: lat,
		lng: lng,
	}, nil
}
