package continentsdb

import (
	"fmt"
	"os"

	locodedb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/orb/planar"
)

const continentProperty = "Continent"

// PointContinent goes through all polygons, and returns continent
// in which point is located.
//
// Returns locodedb.ContinentUnknown if no entry matches.
//
// All GeoJSON feature are parsed from file once and stored in memory.
func (db *DB) PointContinent(point *locodedb.Point) (*locodedb.Continent, error) {
	var err error

	db.once.Do(func() {
		err = db.init()
	})

	if err != nil {
		return nil, err
	}

	planarPoint := orb.Point{point.Longitude(), point.Latitude()}

	var continent string

	for _, feature := range db.features {
		if multiPolygon, ok := feature.Geometry.(orb.MultiPolygon); ok {
			if planar.MultiPolygonContains(multiPolygon, planarPoint) {
				continent = feature.Properties.MustString(continentProperty)
				break
			}
		} else if polygon, ok := feature.Geometry.(orb.Polygon); ok {
			if planar.PolygonContains(polygon, planarPoint) {
				continent = feature.Properties.MustString(continentProperty)
				break
			}
		}
	}

	c := continentFromString(continent)

	return &c, nil
}

func (db *DB) init() error {
	data, err := os.ReadFile(db.path)
	if err != nil {
		return fmt.Errorf("could not read data file: %w", err)
	}

	features, err := geojson.UnmarshalFeatureCollection(data)
	if err != nil {
		return fmt.Errorf("could not unmarshal GeoJSON feature collection: %w", err)
	}

	db.features = features.Features

	return nil
}

func continentFromString(c string) locodedb.Continent {
	switch c {
	default:
		return locodedb.ContinentUnknown
	case "Africa":
		return locodedb.ContinentAfrica
	case "Asia":
		return locodedb.ContinentAsia
	case "Europe":
		return locodedb.ContinentEurope
	case "North America":
		return locodedb.ContinentNorthAmerica
	case "South America":
		return locodedb.ContinentSouthAmerica
	case "Antarctica":
		return locodedb.ContinentAntarctica
	case "Australia", "Oceania":
		return locodedb.ContinentOceania
	}
}
