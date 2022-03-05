package cmd

import (
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/token"
	"github.com/spf13/cobra"
)

const (
	getRangeCmdUse = "range"

	getRangeCmdShortDesc = "Get payload range data of an object"

	getRangeCmdLongDesc = "Get payload range data of an object"
)

const (
	getRangeHashSaltFlag = "salt"
)

const bearerTokenFlag = "bearer"

const sessionTokenLifetime = 10 // in epochs

var (
	// objectCmd represents the object command
	objectCmd = &cobra.Command{
		Use:   "object",
		Short: "Operations with Objects",
		Long:  `Operations with Objects`,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// bind exactly that cmd's flags to
			// the viper before execution
			bindCommonFlags(cmd)
			bindAPIFlags(cmd)
		},
	}

	objectPutCmd = &cobra.Command{
		Use:   "put",
		Short: "Put object to NeoFS",
		Long:  "Put object to NeoFS",
		Run:   putObject,
	}

	objectGetCmd = &cobra.Command{
		Use:   "get",
		Short: "Get object from NeoFS",
		Long:  "Get object from NeoFS",
		Run:   getObject,
	}

	objectDelCmd = &cobra.Command{
		Use:     "delete",
		Aliases: []string{"del"},
		Short:   "Delete object from NeoFS",
		Long:    "Delete object from NeoFS",
		Run:     deleteObject,
	}

	searchFilters []string

	objectSearchCmd = &cobra.Command{
		Use:   "search",
		Short: "Search object",
		Long:  "Search object",
		Run:   searchObject,
	}

	objectHeadCmd = &cobra.Command{
		Use:   "head",
		Short: "Get object header",
		Long:  "Get object header",
		Run:   getObjectHeader,
	}

	objectHashCmd = &cobra.Command{
		Use:   "hash",
		Short: "Get object hash",
		Long:  "Get object hash",
		Run:   getObjectHash,
	}

	objectRangeCmd = &cobra.Command{
		Use:   getRangeCmdUse,
		Short: getRangeCmdShortDesc,
		Long:  getRangeCmdLongDesc,
		Run:   getObjectRange,
	}
)

const (
	hashSha256 = "sha256"
	hashTz     = "tz"
	rangeSep   = ":"
)

const searchOIDFlag = "oid"

const (
	rawFlag     = "raw"
	rawFlagDesc = "Set raw request option"
)

const putExpiresOnFlag = "expires-on"

var putExpiredOn uint64

func initObjectPutCmd() {
	initCommonFlags(objectPutCmd)

	flags := objectPutCmd.Flags()

	flags.String("file", "", "File with object payload")
	_ = objectPutCmd.MarkFlagFilename("file")
	_ = objectPutCmd.MarkFlagRequired("file")

	flags.String("cid", "", "Container ID")
	_ = objectPutCmd.MarkFlagRequired("cid")

	flags.String("attributes", "", "User attributes in form of Key1=Value1,Key2=Value2")
	flags.Bool("disable-filename", false, "Do not set well-known filename attribute")
	flags.Bool("disable-timestamp", false, "Do not set well-known timestamp attribute")
	flags.Uint64VarP(&putExpiredOn, putExpiresOnFlag, "e", 0, "Last epoch in the life of the object")
}

func initObjectDeleteCmd() {
	initCommonFlags(objectDelCmd)

	flags := objectDelCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = objectDelCmd.MarkFlagRequired("cid")

	flags.String("oid", "", "Object ID")
	_ = objectDelCmd.MarkFlagRequired("oid")
}

func initObjectGetCmd() {
	initCommonFlags(objectGetCmd)

	flags := objectGetCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = objectGetCmd.MarkFlagRequired("cid")

	flags.String("oid", "", "Object ID")
	_ = objectGetCmd.MarkFlagRequired("oid")

	flags.String("file", "", "File to write object payload to. Default: stdout.")
	flags.String("header", "", "File to write header to. Default: stdout.")
	flags.Bool(rawFlag, false, rawFlagDesc)
}

func initObjectSearchCmd() {
	initCommonFlags(objectSearchCmd)

	flags := objectSearchCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = objectSearchCmd.MarkFlagRequired("cid")

	flags.StringSliceVarP(&searchFilters, "filters", "f", nil,
		"Repeated filter expressions or files with protobuf JSON")

	flags.Bool("root", false, "Search for user objects")
	flags.Bool("phy", false, "Search physically stored objects")
	flags.String(searchOIDFlag, "", "Search object by identifier")
}

func initObjectHeadCmd() {
	initCommonFlags(objectHeadCmd)

	flags := objectHeadCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = objectHeadCmd.MarkFlagRequired("cid")

	flags.String("oid", "", "Object ID")
	_ = objectHeadCmd.MarkFlagRequired("oid")

	flags.String("file", "", "File to write header to. Default: stdout.")
	flags.Bool("main-only", false, "Return only main fields")
	flags.Bool("json", false, "Marshal output in JSON")
	flags.Bool("proto", false, "Marshal output in Protobuf")
	flags.Bool(rawFlag, false, rawFlagDesc)
}

func initObjectHashCmd() {
	initCommonFlags(objectHashCmd)

	flags := objectHashCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = objectHashCmd.MarkFlagRequired("cid")

	flags.String("oid", "", "Object ID")
	_ = objectHashCmd.MarkFlagRequired("oid")

	flags.String("range", "", "Range to take hash from in the form offset1:length1,...")
	flags.String("type", hashSha256, "Hash type. Either 'sha256' or 'tz'")
	flags.String(getRangeHashSaltFlag, "", "Salt in hex format")
}

func initObjectRangeCmd() {
	initCommonFlags(objectRangeCmd)

	flags := objectRangeCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = objectRangeCmd.MarkFlagRequired("cid")

	flags.String("oid", "", "Object ID")
	_ = objectRangeCmd.MarkFlagRequired("oid")

	flags.String("range", "", "Range to take data from in the form offset:length")
	flags.String("file", "", "File to write object payload to. Default: stdout.")
	flags.Bool(rawFlag, false, rawFlagDesc)
}

func init() {
	objectChildCommands := []*cobra.Command{
		objectPutCmd,
		objectDelCmd,
		objectGetCmd,
		objectSearchCmd,
		objectHeadCmd,
		objectHashCmd,
		objectRangeCmd,
	}

	rootCmd.AddCommand(objectCmd)
	objectCmd.AddCommand(objectChildCommands...)

	for _, objCommand := range objectChildCommands {
		flags := objCommand.Flags()

		flags.String(bearerTokenFlag, "", "File with signed JSON or binary encoded bearer token")
		flags.StringSliceVarP(&xHeaders, xHeadersKey, xHeadersShorthand, xHeadersDefault, xHeadersUsage)
		flags.Uint32P(ttl, ttlShorthand, ttlDefault, ttlUsage)
	}

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// objectCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// objectCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	initObjectPutCmd()
	initObjectDeleteCmd()
	initObjectGetCmd()
	initObjectSearchCmd()
	initObjectHeadCmd()
	initObjectHashCmd()
	initObjectRangeCmd()
}

type clientKeySession interface {
	clientWithKey
	SetSessionToken(*session.Token)
}

func prepareSessionPrm(cmd *cobra.Command, prms ...clientKeySession) {
	key, err := getKey()
	exitOnErr(cmd, errf("get private key: %w", err))

	prepareSessionPrmWithKey(cmd, key, prms...)
}

func prepareSessionPrmWithKey(cmd *cobra.Command, key *ecdsa.PrivateKey, prms ...clientKeySession) {
	ownerID, err := getOwnerID(key)
	exitOnErr(cmd, errf("owner ID from key: %w", err))

	prepareSessionPrmWithOwner(cmd, key, ownerID, prms...)
}

func prepareSessionPrmWithOwner(
	cmd *cobra.Command,
	key *ecdsa.PrivateKey,
	ownerID *owner.ID,
	prms ...clientKeySession,
) {
	var (
		sessionPrm internalclient.CreateSessionPrm
		netInfoPrm internalclient.NetworkInfoPrm
	)

	cws := make([]clientWithKey, 2, len(prms)+2)
	cws[0] = &sessionPrm
	cws[1] = &netInfoPrm

	for i := range prms {
		cws = append(cws, prms[i])
	}

	prepareAPIClientWithKey(cmd, key, cws...)

	ni, err := internalclient.NetworkInfo(netInfoPrm)
	exitOnErr(cmd, errf("read network info: %w", err))

	cur := ni.NetworkInfo().CurrentEpoch()
	exp := cur + sessionTokenLifetime
	sessionPrm.SetExp(exp)

	sessionRes, err := internalclient.CreateSession(sessionPrm)
	exitOnErr(cmd, errf("open session: %w", err))

	for i := range prms {
		objectContext := session.NewObjectContext()
		switch prms[i].(type) {
		case *internalclient.GetObjectPrm:
			objectContext.ForGet()
		case *internalclient.HeadObjectPrm:
			objectContext.ForHead()
		case *internalclient.PutObjectPrm:
			objectContext.ForPut()
		case *internalclient.DeleteObjectPrm:
			objectContext.ForDelete()
		case *internalclient.SearchObjectsPrm:
			objectContext.ForSearch()
		case *internalclient.PayloadRangePrm:
			objectContext.ForRange()
		case *internalclient.HashPayloadRangesPrm:
			objectContext.ForRangeHash()
		default:
			panic("invalid client parameter type")
		}

		tok := session.NewToken()
		tok.SetID(sessionRes.ID())
		tok.SetSessionKey(sessionRes.SessionKey())
		tok.SetOwnerID(ownerID)
		tok.SetContext(objectContext)
		tok.SetExp(exp)
		tok.SetIat(cur)
		tok.SetNbf(cur)

		err = tok.Sign(key)
		exitOnErr(cmd, errf("session token signing: %w", err))

		prms[i].SetSessionToken(tok)
	}
}

type objectPrm interface {
	bearerPrm
	SetTTL(uint32)
	SetXHeaders([]*session.XHeader)
}

func prepareObjectPrm(cmd *cobra.Command, prms ...objectPrm) {
	for i := range prms {
		prepareBearerPrm(cmd, prms[i])

		prms[i].SetTTL(getTTL())
		prms[i].SetXHeaders(parseXHeaders())
	}
}

func prepareObjectPrmRaw(cmd *cobra.Command, prm interface {
	objectPrm
	SetRawFlag(bool)
}) {
	prepareObjectPrm(cmd, prm)

	raw, _ := cmd.Flags().GetBool(rawFlag)
	prm.SetRawFlag(raw)
}

func putObject(cmd *cobra.Command, _ []string) {
	key, err := getKey()
	exitOnErr(cmd, errf("can't fetch private key: %w", err))

	ownerID, err := getOwnerID(key)
	exitOnErr(cmd, err)
	cid, err := getCID(cmd)
	exitOnErr(cmd, err)

	filename := cmd.Flag("file").Value.String()
	f, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		exitOnErr(cmd, fmt.Errorf("can't open file '%s': %w", filename, err))
	}

	attrs, err := parseObjectAttrs(cmd)
	exitOnErr(cmd, errf("can't parse object attributes: %w", err))

	expiresOn, _ := cmd.Flags().GetUint64(putExpiresOnFlag)
	if expiresOn > 0 {
		var expAttr *object.Attribute

		for _, a := range attrs {
			if a.Key() == objectV2.SysAttributeExpEpoch {
				expAttr = a
				break
			}
		}

		if expAttr == nil {
			expAttr = object.NewAttribute()
			expAttr.SetKey(objectV2.SysAttributeExpEpoch)
			attrs = append(attrs, expAttr)
		}

		expAttr.SetValue(strconv.FormatUint(expiresOn, 10))
	}

	obj := object.NewRaw()
	obj.SetContainerID(cid)
	obj.SetOwnerID(ownerID)
	obj.SetAttributes(attrs...)

	var prm internalclient.PutObjectPrm

	prepareSessionPrmWithOwner(cmd, key, ownerID, &prm)
	prepareObjectPrm(cmd, &prm)
	prm.SetHeader(obj.Object())
	prm.SetPayloadReader(f)

	res, err := internalclient.PutObject(prm)
	exitOnErr(cmd, errf("rpc error: %w", err))

	cmd.Printf("[%s] Object successfully stored\n", filename)
	cmd.Printf("  ID: %s\n  CID: %s\n", res.ID(), cid)
}

func deleteObject(cmd *cobra.Command, _ []string) {
	objAddr, err := getObjectAddress(cmd)
	exitOnErr(cmd, err)

	var prm internalclient.DeleteObjectPrm

	prepareSessionPrm(cmd, &prm)
	prepareObjectPrm(cmd, &prm)
	prm.SetAddress(objAddr)

	res, err := internalclient.DeleteObject(prm)
	exitOnErr(cmd, errf("rpc error: %w", err))

	tombstoneAddr := res.TombstoneAddress()

	cmd.Println("Object removed successfully.")
	cmd.Printf("  ID: %s\n  CID: %s\n", tombstoneAddr.ObjectID(), tombstoneAddr.ContainerID())
}

func getObject(cmd *cobra.Command, _ []string) {
	objAddr, err := getObjectAddress(cmd)
	exitOnErr(cmd, err)

	var out io.Writer
	filename := cmd.Flag("file").Value.String()
	if filename == "" {
		out = os.Stdout
	} else {
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			exitOnErr(cmd, fmt.Errorf("can't open file '%s': %w", filename, err))
		}

		defer f.Close()

		out = f
	}

	var prm internalclient.GetObjectPrm

	prepareSessionPrm(cmd, &prm)
	prepareObjectPrmRaw(cmd, &prm)
	prm.SetAddress(objAddr)
	prm.SetPayloadWriter(out)

	res, err := internalclient.GetObject(prm)
	if err != nil {
		if ok := printSplitInfoErr(cmd, err); ok {
			return
		}

		exitOnErr(cmd, errf("rpc error: %w", err))
	}

	if filename != "" {
		cmd.Printf("[%s] Object successfully saved\n", filename)
	}

	// Print header only if file is not streamed to stdout.
	hdrFile := cmd.Flag("header").Value.String()
	if filename != "" || hdrFile != "" {
		err = saveAndPrintHeader(cmd, res.Header(), hdrFile)
		exitOnErr(cmd, err)
	}
}

func getObjectHeader(cmd *cobra.Command, _ []string) {
	objAddr, err := getObjectAddress(cmd)
	exitOnErr(cmd, err)

	mainOnly, _ := cmd.Flags().GetBool("main-only")

	var prm internalclient.HeadObjectPrm

	prepareSessionPrm(cmd, &prm)
	prepareObjectPrmRaw(cmd, &prm)
	prm.SetAddress(objAddr)
	prm.SetMainOnlyFlag(mainOnly)

	res, err := internalclient.HeadObject(prm)
	if err != nil {
		if ok := printSplitInfoErr(cmd, err); ok {
			return
		}

		exitOnErr(cmd, errf("rpc error: %w", err))
	}

	err = saveAndPrintHeader(cmd, res.Header(), cmd.Flag("file").Value.String())
	exitOnErr(cmd, err)
}

func searchObject(cmd *cobra.Command, _ []string) {
	cid, err := getCID(cmd)
	exitOnErr(cmd, err)

	sf, err := parseSearchFilters(cmd)
	exitOnErr(cmd, err)

	var prm internalclient.SearchObjectsPrm

	prepareSessionPrm(cmd, &prm)
	prepareObjectPrm(cmd, &prm)
	prm.SetContainerID(cid)
	prm.SetFilters(sf)

	res, err := internalclient.SearchObjects(prm)
	exitOnErr(cmd, errf("rpc error: %w", err))

	ids := res.IDList()

	cmd.Printf("Found %d objects.\n", len(ids))
	for _, id := range ids {
		cmd.Println(id)
	}
}

func getObjectHash(cmd *cobra.Command, _ []string) {
	objAddr, err := getObjectAddress(cmd)
	exitOnErr(cmd, err)
	ranges, err := getRangeList(cmd)
	exitOnErr(cmd, err)
	typ, err := getHashType(cmd)
	exitOnErr(cmd, err)

	strSalt := strings.TrimPrefix(cmd.Flag(getRangeHashSaltFlag).Value.String(), "0x")

	salt, err := hex.DecodeString(strSalt)
	exitOnErr(cmd, errf("could not decode salt: %w", err))

	var (
		hashPrm internalclient.HashPayloadRangesPrm
		headPrm internalclient.HeadObjectPrm

		sesPrms = []clientKeySession{&hashPrm}
		objPrms = []objectPrm{&hashPrm}
	)

	fullHash := len(ranges) == 0
	if fullHash {
		sesPrms = append(sesPrms, &headPrm)
		objPrms = append(objPrms, &headPrm)
	}

	prepareSessionPrm(cmd, sesPrms...)
	prepareObjectPrm(cmd, objPrms...)

	tz := typ == hashTz

	if fullHash {
		headPrm.SetAddress(objAddr)

		// get hash of full payload through HEAD (may be user can do it through dedicated command?)
		res, err := internalclient.HeadObject(headPrm)
		exitOnErr(cmd, errf("rpc error: %w", err))

		var cs *checksum.Checksum

		if tz {
			cs = res.Header().PayloadHomomorphicHash()
		} else {
			cs = res.Header().PayloadChecksum()
		}

		cmd.Println(hex.EncodeToString(cs.Sum()))

		return
	}

	hashPrm.SetAddress(objAddr)
	hashPrm.SetSalt(salt)
	hashPrm.SetRanges(ranges)

	if tz {
		hashPrm.TZ()
	}

	res, err := internalclient.HashPayloadRanges(hashPrm)
	exitOnErr(cmd, errf("rpc error: %w", err))

	hs := res.HashList()

	for i := range hs {
		cmd.Printf("Offset=%d (Length=%d)\t: %s\n", ranges[i].GetOffset(), ranges[i].GetLength(),
			hex.EncodeToString(hs[i]))
	}
}

func getOwnerID(key *ecdsa.PrivateKey) (*owner.ID, error) {
	return owner.NewIDFromPublicKey(&key.PublicKey), nil
}

var searchUnaryOpVocabulary = map[string]object.SearchMatchType{
	"NOPRESENT": object.MatchNotPresent,
}

var searchBinaryOpVocabulary = map[string]object.SearchMatchType{
	"EQ":            object.MatchStringEqual,
	"NE":            object.MatchStringNotEqual,
	"COMMON_PREFIX": object.MatchCommonPrefix,
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
				return nil, fmt.Errorf("unsupported unary op: %s", words[1])
			}

			fs.AddFilter(words[0], "", m)
		case 3:
			m, ok := searchBinaryOpVocabulary[words[1]]
			if !ok {
				return nil, fmt.Errorf("unsupported binary op: %s", words[1])
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

	oid, _ := cmd.Flags().GetString(searchOIDFlag)
	if oid != "" {
		id := oidSDK.NewID()
		if err := id.Parse(oid); err != nil {
			return nil, fmt.Errorf("could not parse object ID: %w", err)
		}

		fs.AddObjectIDFilter(object.MatchStringEqual, id)
	}

	return fs, nil
}

func parseObjectAttrs(cmd *cobra.Command) ([]*object.Attribute, error) {
	var rawAttrs []string

	raw := cmd.Flag("attributes").Value.String()
	if len(raw) != 0 {
		rawAttrs = strings.Split(raw, ",")
	}

	attrs := make([]*object.Attribute, 0, len(rawAttrs)+2) // name + timestamp attributes
	for i := range rawAttrs {
		kv := strings.SplitN(rawAttrs[i], "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid attribute format: %s", rawAttrs[i])
		}
		attr := object.NewAttribute()
		attr.SetKey(kv[0])
		attr.SetValue(kv[1])
		attrs = append(attrs, attr)
	}

	disableFilename, _ := cmd.Flags().GetBool("disable-filename")
	if !disableFilename {
		filename := filepath.Base(cmd.Flag("file").Value.String())
		attr := object.NewAttribute()
		attr.SetKey(object.AttributeFileName)
		attr.SetValue(filename)
		attrs = append(attrs, attr)
	}

	disableTime, _ := cmd.Flags().GetBool("disable-timestamp")
	if !disableTime {
		attr := object.NewAttribute()
		attr.SetKey(object.AttributeTimestamp)
		attr.SetValue(strconv.FormatInt(time.Now().Unix(), 10))
		attrs = append(attrs, attr)
	}

	return attrs, nil
}

func getCID(cmd *cobra.Command) (*cid.ID, error) {
	id := cid.New()

	err := id.Parse(cmd.Flag("cid").Value.String())
	if err != nil {
		return nil, fmt.Errorf("could not parse container ID: %w", err)
	}

	return id, nil
}

func getOID(cmd *cobra.Command) (*oidSDK.ID, error) {
	oid := oidSDK.NewID()

	err := oid.Parse(cmd.Flag("oid").Value.String())
	if err != nil {
		return nil, fmt.Errorf("could not parse object ID: %w", err)
	}

	return oid, nil
}

func getObjectAddress(cmd *cobra.Command) (*addressSDK.Address, error) {
	cid, err := getCID(cmd)
	if err != nil {
		return nil, err
	}
	oid, err := getOID(cmd)
	if err != nil {
		return nil, err
	}

	objAddr := addressSDK.NewAddress()
	objAddr.SetContainerID(cid)
	objAddr.SetObjectID(oid)
	return objAddr, nil
}

func getRangeList(cmd *cobra.Command) ([]*object.Range, error) {
	v := cmd.Flag("range").Value.String()
	if len(v) == 0 {
		return nil, nil
	}
	vs := strings.Split(v, ",")
	rs := make([]*object.Range, len(vs))
	for i := range vs {
		r := strings.Split(vs[i], rangeSep)
		if len(r) != 2 {
			return nil, fmt.Errorf("invalid range specifier: %s", vs[i])
		}

		offset, err := strconv.ParseUint(r[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid range specifier: %s", vs[i])
		}
		length, err := strconv.ParseUint(r[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid range specifier: %s", vs[i])
		}
		rs[i] = object.NewRange()
		rs[i].SetOffset(offset)
		rs[i].SetLength(length)
	}
	return rs, nil
}

func getHashType(cmd *cobra.Command) (string, error) {
	rawType := cmd.Flag("type").Value.String()
	switch typ := strings.ToLower(rawType); typ {
	case hashSha256, hashTz:
		return typ, nil
	default:
		return "", fmt.Errorf("invalid hash type: %s", typ)
	}
}

func saveAndPrintHeader(cmd *cobra.Command, obj *object.Object, filename string) error {
	bs, err := marshalHeader(cmd, obj)
	if err != nil {
		return fmt.Errorf("could not marshal header: %w", err)
	}
	if len(bs) != 0 {
		if filename == "" {
			cmd.Println(string(bs))
			return nil
		}
		err = os.WriteFile(filename, bs, os.ModePerm)
		if err != nil {
			return fmt.Errorf("could not write header to file: %w", err)
		}
		cmd.Printf("[%s] Header successfully saved.", filename)
	}

	return printHeader(cmd, obj)
}

func printHeader(cmd *cobra.Command, obj *object.Object) error {
	cmd.Printf("ID: %s\n", obj.ID())
	cmd.Printf("CID: %s\n", obj.ContainerID())
	cmd.Printf("Owner: %s\n", obj.OwnerID())
	cmd.Printf("CreatedAt: %d\n", obj.CreationEpoch())
	cmd.Printf("Size: %d\n", obj.PayloadSize())
	cmd.Printf("HomoHash: %s\n", hex.EncodeToString(obj.PayloadHomomorphicHash().Sum()))
	cmd.Printf("Checksum: %s\n", hex.EncodeToString(obj.PayloadChecksum().Sum()))
	switch obj.Type() {
	case object.TypeRegular:
		cmd.Println("Type: regular")
	case object.TypeTombstone:
		cmd.Println("Type: tombstone")
	case object.TypeStorageGroup:
		cmd.Println("Type: storage group")
	default:
		cmd.Println("Type: unknown")
	}

	cmd.Println("Attributes:")
	for _, attr := range obj.Attributes() {
		if attr.Key() == object.AttributeTimestamp {
			cmd.Printf("  %s=%s (%s)\n",
				attr.Key(),
				attr.Value(),
				prettyPrintUnixTime(attr.Value()))
			continue
		}
		cmd.Printf("  %s=%s\n", attr.Key(), attr.Value())
	}

	return printSplitHeader(cmd, obj)
}

func printSplitHeader(cmd *cobra.Command, obj *object.Object) error {
	if splitID := obj.SplitID(); splitID != nil {
		cmd.Printf("Split ID: %s\n", splitID)
	}

	if oid := obj.ParentID(); oid != nil {
		cmd.Printf("Split ParentID: %s\n", oid)
	}

	if prev := obj.PreviousID(); prev != nil {
		cmd.Printf("Split PreviousID: %s\n", prev)
	}

	for _, child := range obj.Children() {
		cmd.Printf("Split ChildID: %s\n", child)
	}

	if signature := obj.Signature(); signature != nil {
		cmd.Print("Split Header Signature:\n")
		cmd.Printf("  public key: %s\n", hex.EncodeToString(signature.Key()))
		cmd.Printf("  signature: %s\n", hex.EncodeToString(signature.Sign()))
	}

	parent := obj.Parent()
	if parent != nil {
		cmd.Print("\nSplit Parent Header:\n")

		return printHeader(cmd, parent)
	}

	return nil
}

func marshalHeader(cmd *cobra.Command, hdr *object.Object) ([]byte, error) {
	toJSON, _ := cmd.Flags().GetBool("json")
	toProto, _ := cmd.Flags().GetBool("proto")
	switch {
	case toJSON && toProto:
		return nil, errors.New("'--json' and '--proto' flags are mutually exclusive")
	case toJSON:
		return hdr.MarshalJSON()
	case toProto:
		return hdr.Marshal()
	default:
		return nil, nil
	}
}

func getBearerToken(cmd *cobra.Command, flagname string) (*token.BearerToken, error) {
	path, err := cmd.Flags().GetString(flagname)
	if err != nil || len(path) == 0 {
		return nil, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("can't read bearer token file: %w", err)
	}

	tok := token.NewBearerToken()
	if err := tok.UnmarshalJSON(data); err != nil {
		if err = tok.Unmarshal(data); err != nil {
			return nil, fmt.Errorf("can't decode bearer token: %w", err)
		}

		printVerbose("Using binary encoded bearer token")
	} else {
		printVerbose("Using JSON encoded bearer token")
	}

	return tok, nil
}

func getObjectRange(cmd *cobra.Command, _ []string) {
	objAddr, err := getObjectAddress(cmd)
	exitOnErr(cmd, err)

	ranges, err := getRangeList(cmd)
	exitOnErr(cmd, err)

	if len(ranges) != 1 {
		exitOnErr(cmd, fmt.Errorf("exactly one range must be specified, got: %d", len(ranges)))
	}

	var out io.Writer

	filename := cmd.Flag("file").Value.String()
	if filename == "" {
		out = os.Stdout
	} else {
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			exitOnErr(cmd, fmt.Errorf("can't open file '%s': %w", filename, err))
		}

		defer f.Close()

		out = f
	}

	var prm internalclient.PayloadRangePrm

	prepareSessionPrm(cmd, &prm)
	prepareObjectPrmRaw(cmd, &prm)
	prm.SetAddress(objAddr)
	prm.SetRange(ranges[0])
	prm.SetPayloadWriter(out)

	_, err = internalclient.PayloadRange(prm)
	if err != nil {
		if ok := printSplitInfoErr(cmd, err); ok {
			return
		}

		exitOnErr(cmd, fmt.Errorf("can't get object payload range: %w", err))
	}

	if filename != "" {
		cmd.Printf("[%s] Payload successfully saved\n", filename)
	}
}

func printSplitInfoErr(cmd *cobra.Command, err error) bool {
	var errSplitInfo *object.SplitInfoError

	ok := errors.As(err, &errSplitInfo)

	if ok {
		cmd.Println("Object is complex, split information received.")
		printSplitInfo(cmd, errSplitInfo.SplitInfo())
	}

	return ok
}

func printSplitInfo(cmd *cobra.Command, info *object.SplitInfo) {
	if splitID := info.SplitID(); splitID != nil {
		cmd.Println("Split ID:", splitID)
	}

	if link := info.Link(); link != nil {
		cmd.Println("Linking object:", link)
	}

	if last := info.LastPart(); last != nil {
		cmd.Println("Last object:", last)
	}
}
