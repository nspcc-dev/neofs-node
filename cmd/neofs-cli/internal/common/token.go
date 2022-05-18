package common

import (
	"encoding/json"
	"os"

	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/spf13/cobra"
)

// ReadBearerToken reads bearer token from the path provided in a specified flag.
func ReadBearerToken(cmd *cobra.Command, flagname string) *bearer.Token {
	path, err := cmd.Flags().GetString(flagname)
	ExitOnErr(cmd, "", err)

	if len(path) == 0 {
		return nil
	}

	data, err := os.ReadFile(path)
	ExitOnErr(cmd, "can't read bearer token file: %w", err)

	var tok bearer.Token
	if err := tok.UnmarshalJSON(data); err != nil {
		err = tok.Unmarshal(data)
		ExitOnErr(cmd, "can't decode bearer token: %w", err)

		PrintVerbose("Using binary encoded bearer token")
	} else {
		PrintVerbose("Using JSON encoded bearer token")
	}

	return &tok
}

// ReadSessionToken reads session token as JSON file with session token
// from path provided in a specified flag.
func ReadSessionToken(cmd *cobra.Command, dst json.Unmarshaler, fPath string) {
	// try to read session token from file
	data, err := os.ReadFile(fPath)
	ExitOnErr(cmd, "could not open file with session token: %w", err)

	err = dst.UnmarshalJSON(data)
	ExitOnErr(cmd, "could not unmarshal session token from file: %w", err)
}
