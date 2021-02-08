package airportsdb

import (
	"os"
)

// Option sets an optional parameter of DB.
type Option func(*options)

type options struct {
	airportMode, countryMode os.FileMode
}

func defaultOpts() *options {
	return &options{
		airportMode: os.ModePerm, // 0777
		countryMode: os.ModePerm, // 0777
	}
}
