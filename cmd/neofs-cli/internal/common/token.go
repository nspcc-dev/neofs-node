package common

import (
	"os"

	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/session"
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
func ReadSessionToken(cmd *cobra.Command, flag string) *session.Token {
	// try to read session token from file
	var tok *session.Token

	path, err := cmd.Flags().GetString(flag)
	ExitOnErr(cmd, "", err)

	if path == "" {
		return tok
	}

	data, err := os.ReadFile(path)
	ExitOnErr(cmd, "could not open file with session token: %w", err)

	tok = session.NewToken()
	err = tok.UnmarshalJSON(data)
	ExitOnErr(cmd, "could not ummarshal session token from file: %w", err)

	return tok
}
