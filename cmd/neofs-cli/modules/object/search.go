package object

import (
	"errors"
	"fmt"
	"os"
	"strings"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var (
	searchFilters []string

	objectSearchCmd = &cobra.Command{
		Use:   "search",
		Short: "Search object",
		Long:  "Search object",
		Args:  cobra.NoArgs,
		RunE:  searchObject,
	}
)

func initObjectSearchCmd() {
	commonflags.Init(objectSearchCmd)
	initFlagSession(objectSearchCmd, "SEARCH")

	flags := objectSearchCmd.Flags()

	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = objectSearchCmd.MarkFlagRequired(commonflags.CIDFlag)

	flags.StringSliceVarP(&searchFilters, "filters", "f", nil,
		"Repeated filter expressions or files with protobuf JSON")

	flags.Bool("root", false, "Search for user objects")
	flags.Bool("phy", false, "Search physically stored objects")
	flags.String(commonflags.OIDFlag, "", "Search object by identifier")
}

func searchObject(cmd *cobra.Command, _ []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	var cnr cid.ID
	err := readCID(cmd, &cnr)
	if err != nil {
		return err
	}

	sf, err := parseSearchFilters(cmd)
	if err != nil {
		return err
	}

	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}

	cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
	if err != nil {
		return err
	}

	var prm internalclient.SearchObjectsPrm
	prm.SetClient(cli)
	prm.SetPrivateKey(*pk)
	err = Prepare(cmd, &prm)
	if err != nil {
		return err
	}

	err = readSessionGlobal(cmd, &prm, pk, cnr)
	if err != nil {
		return err
	}

	prm.SetContainerID(cnr)
	prm.SetFilters(sf)

	res, err := internalclient.SearchObjects(ctx, prm)
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	ids := res.IDList()

	cmd.Printf("Found %d objects.\n", len(ids))
	for i := range ids {
		cmd.Println(ids[i].String())
	}

	return nil
}

var searchUnaryOpVocabulary = map[string]object.SearchMatchType{
	"NOPRESENT": object.MatchNotPresent,
}

var searchBinaryOpVocabulary = map[string]object.SearchMatchType{
	"EQ":            object.MatchStringEqual,
	"NE":            object.MatchStringNotEqual,
	"COMMON_PREFIX": object.MatchCommonPrefix,
	"GT":            object.MatchNumGT,
	"GE":            object.MatchNumGE,
	"LE":            object.MatchNumLE,
	"LT":            object.MatchNumLT,
}

func parseSearchFilters(cmd *cobra.Command) (object.SearchFilters, error) {
	var fs object.SearchFilters

	for i := range searchFilters {
		words := strings.Fields(searchFilters[i])

		switch len(words) {
		default:
			return nil, fmt.Errorf("invalid field number: %d", len(words))
		case 1:
			data, err := os.ReadFile(words[0])
			if err != nil {
				return nil, fmt.Errorf("could not read attributes filter from file: %w", err)
			}

			subFs := object.NewSearchFilters()

			if err := subFs.UnmarshalJSON(data); err != nil {
				return nil, fmt.Errorf("could not unmarshal attributes filter from file: %w", err)
			}

			fs = append(fs, subFs...)
		case 2:
			m, ok := searchUnaryOpVocabulary[words[1]]
			if !ok {
				return nil, fmt.Errorf("%w: unary op: %s", errors.ErrUnsupported, words[1])
			}

			fs.AddFilter(words[0], "", m)
		case 3:
			m, ok := searchBinaryOpVocabulary[words[1]]
			if !ok {
				return nil, fmt.Errorf("%w: binary op: %s", errors.ErrUnsupported, words[1])
			}

			fs.AddFilter(words[0], words[2], m)
		}
	}

	root, _ := cmd.Flags().GetBool("root")
	if root {
		fs.AddRootFilter()
	}

	phy, _ := cmd.Flags().GetBool("phy")
	if phy {
		fs.AddPhyFilter()
	}

	oid, _ := cmd.Flags().GetString(commonflags.OIDFlag)
	if oid != "" {
		var id oidSDK.ID
		if err := id.DecodeString(oid); err != nil {
			return nil, fmt.Errorf("could not parse object ID: %w", err)
		}

		fs.AddObjectIDFilter(object.MatchStringEqual, id)
	}

	return fs, nil
}
