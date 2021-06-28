package airportsdb

import (
	"io/fs"
)

// Option sets an optional parameter of DB.
type Option func(*options)

type options struct {
	airportMode, countryMode fs.FileMode
}

func defaultOpts() *options {
	return &options{
		airportMode: fs.ModePerm, // 0777
		countryMode: fs.ModePerm, // 0777
	}
}
