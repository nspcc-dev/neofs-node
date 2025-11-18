package object

import (
	"errors"
	"fmt"
	"os"
	"strings"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// searchV2Cmd flags.
var (
	searchAttributesFlag = flag[[]string]{f: "attributes"}
	searchCountFlag      = flag[uint16]{f: "count"}
	searchCursorFlag     = flag[string]{f: "cursor"}
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
	searchV2Cmd = &cobra.Command{
		Use:   objectSearchCmd.Use + "v2",
		Short: objectSearchCmd.Short + " (new)", // TODO: drop suffix on old search deprecation
		Long:  objectSearchCmd.Long + " (new)",  // TODO: desc in details
		Args:  objectSearchCmd.Args,
		RunE:  searchV2,
	}
)

func initObjectSearchCmd() {
	commonflags.Init(objectSearchCmd)
	commonflags.Init(searchV2Cmd)
	initFlagSession(objectSearchCmd, "SEARCH")
	initFlagSession(searchV2Cmd, "SEARCH")

	flags := objectSearchCmd.Flags()
	flags2 := searchV2Cmd.Flags()

	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = objectSearchCmd.MarkFlagRequired(commonflags.CIDFlag)
	flags2.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = searchV2Cmd.MarkFlagRequired(commonflags.CIDFlag)

	flags.StringSliceVarP(&searchFilters, "filters", "f", nil,
		"Repeated filter expressions or files with protobuf JSON")
	flags2.StringSliceVarP(&searchFilters, "filters", "f", nil,
		"Repeated filter expressions or files with protobuf JSON")

	flags.Bool("root", false, "Search for user objects")
	flags2.Bool("root", false, "Search for user objects")
	flags.Bool("phy", false, "Search physically stored objects")
	flags2.Bool("phy", false, "Search physically stored objects")

	flags2.StringSliceVar(&searchAttributesFlag.v, searchAttributesFlag.f, nil, "Additional attributes to display for suitable objects")
	flags2.Uint16Var(&searchCountFlag.v, searchCountFlag.f, 0, "Max number of resulting items. Must not exceed 1000")
	flags2.StringVar(&searchCursorFlag.v, searchCursorFlag.f, "", "Cursor to continue previous search")
}

func searchObject(cmd *cobra.Command, _ []string) error {
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

	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
	if err != nil {
		return err
	}
	defer cli.Close()

	var prm client.PrmObjectSearch
	err = Prepare(cmd, &prm)
	if err != nil {
		return err
	}

	err = readSessionGlobal(cmd, &prm, pk, cnr)
	if err != nil {
		return err
	}

	prm.SetFilters(sf)

	rdr, err := cli.ObjectSearchInit(ctx, cnr, user.NewAutoIDSigner(*pk), prm)
	if err != nil {
		return fmt.Errorf("rpc error: init object search: %w", err)
	}

	var ids []oidSDK.ID

	err = rdr.Iterate(func(id oidSDK.ID) bool {
		ids = append(ids, id)
		return false
	})
	if err != nil {
		return fmt.Errorf("rpc error: search objects using NeoFS API: %w", err)
	}

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

	return fs, nil
}

func searchV2(cmd *cobra.Command, _ []string) error {
	var cnr cid.ID
	if err := readCID(cmd, &cnr); err != nil {
		return err
	}
	fs, err := parseSearchFilters(cmd)
	if err != nil {
		return err
	}
	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}
	bt, err := common.ReadBearerToken(cmd, BearerTokenFlag)
	if err != nil {
		return err
	}
	st, err := getVerifiedSession(cmd, session.VerbObjectSearch, pk, cnr)
	if err != nil {
		return err
	}

	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()
	cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
	if err != nil {
		return err
	}
	defer cli.Close()

	var opts client.SearchObjectsOptions
	opts.SetCount(uint32(searchCountFlag.v))
	opts.WithXHeaders(ParseXHeaders(cmd)...)
	if viper.GetUint32(commonflags.TTL) == 1 {
		opts.DisableForwarding()
	}
	if bt != nil {
		opts.WithBearerToken(*bt)
	}
	if st != nil {
		opts.WithSessionToken(*st)
	}
	res, cursor, err := cli.SearchObjects(ctx, cnr, fs, searchAttributesFlag.v, searchCursorFlag.v, neofsecdsa.Signer(*pk), opts)
	if err != nil && !errors.Is(err, apistatus.ErrIncomplete) {
		return fmt.Errorf("rpc error: %w", err)
	}

	var incompleteString string
	if errors.Is(err, apistatus.ErrIncomplete) {
		incompleteString = ", result may be incomplete (not all nodes responded)"
	}

	cmd.Printf("Found %d objects%s.\n", len(res), incompleteString)
	for i := range res {
		cmd.Println(res[i].ID)
		for j := range searchAttributesFlag.v {
			val := res[i].Attributes[j]
			if searchAttributesFlag.v[j] == object.AttributeTimestamp {
				val = common.PrettyPrintUnixTime(val)
			}
			fmt.Printf("\t%s: %s\n", searchAttributesFlag.v[j], val)
		}
	}
	if cursor != "" {
		cmd.Printf("Cursor: %s\n", cursor)
	}
	return err
}
