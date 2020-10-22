package cmd

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	v2ACL "github.com/nspcc-dev/neofs-api-go/v2/acl"
	grpcACL "github.com/nspcc-dev/neofs-api-go/v2/acl/grpc"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
)

var (
	// objectCmd represents the object command
	objectCmd = &cobra.Command{
		Use:   "object",
		Short: "Operations with Objects",
		Long:  `Operations with Objects`,
	}

	objectPutCmd = &cobra.Command{
		Use:   "put",
		Short: "Put object to NeoFS",
		Long:  "Put object to NeoFS",
		RunE:  putObject,
	}

	objectGetCmd = &cobra.Command{
		Use:   "get",
		Short: "Get object from NeoFS",
		Long:  "Get object from NeoFS",
		RunE:  getObject,
	}

	objectDelCmd = &cobra.Command{
		Use:     "delete",
		Aliases: []string{"del"},
		Short:   "Delete object from NeoFS",
		Long:    "Delete object from NeoFS",
		RunE:    deleteObject,
	}

	objectSearchCmd = &cobra.Command{
		Use:   "search",
		Short: "Search object",
		Long:  "Search object",
		RunE:  searchObject,
	}

	objectHeadCmd = &cobra.Command{
		Use:   "head",
		Short: "Get object header",
		Long:  "Get object header",
		RunE:  getObjectHeader,
	}

	objectHashCmd = &cobra.Command{
		Use:   "hash",
		Short: "Get object hash",
		Long:  "Get object hash",
		RunE:  getObjectHash,
	}
)

const (
	hashSha256 = "sha256"
	hashTz     = "tz"
	rangeSep   = ":"
)

func init() {
	rootCmd.AddCommand(objectCmd)
	objectCmd.PersistentFlags().String("bearer", "", "File with signed JSON or binary encoded bearer token")

	objectCmd.AddCommand(objectPutCmd)
	objectPutCmd.Flags().String("file", "", "File with object payload")
	_ = objectPutCmd.MarkFlagFilename("file")
	_ = objectPutCmd.MarkFlagRequired("file")
	objectPutCmd.Flags().String("cid", "", "Container ID")
	_ = objectPutCmd.MarkFlagRequired("cid")
	objectPutCmd.Flags().String("attributes", "", "User attributes in form of Key1=Value1,Key2=Value2")

	objectCmd.AddCommand(objectDelCmd)
	objectDelCmd.Flags().String("cid", "", "Container ID")
	_ = objectDelCmd.MarkFlagRequired("cid")
	objectDelCmd.Flags().String("oid", "", "Object ID")
	_ = objectDelCmd.MarkFlagRequired("oid")

	objectCmd.AddCommand(objectGetCmd)
	objectGetCmd.Flags().String("file", "", "File to write object payload to. Default: stdout.")
	objectGetCmd.Flags().String("header", "", "File to write header to. Default: stdout.")
	objectGetCmd.Flags().String("cid", "", "Container ID")
	_ = objectGetCmd.MarkFlagRequired("cid")
	objectGetCmd.Flags().String("oid", "", "Object ID")
	_ = objectGetCmd.MarkFlagRequired("oid")

	objectCmd.AddCommand(objectSearchCmd)
	objectSearchCmd.Flags().String("cid", "", "Container ID")
	_ = objectSearchCmd.MarkFlagRequired("cid")
	objectSearchCmd.Flags().String("filters", "", "Filters in the form hdrName=value,...")
	objectSearchCmd.Flags().Bool("root", false, "Search for user objects")
	objectSearchCmd.Flags().Bool("no-root", false, "Search for service objects")
	objectSearchCmd.Flags().Bool("leaf", false, "Search physically stored objects")
	objectSearchCmd.Flags().Bool("no-leaf", false, "Search for virtual objects")

	objectCmd.AddCommand(objectHeadCmd)
	objectHeadCmd.Flags().String("file", "", "File to write header to. Default: stdout.")
	objectHeadCmd.Flags().String("cid", "", "Container ID")
	_ = objectHeadCmd.MarkFlagRequired("cid")
	objectHeadCmd.Flags().String("oid", "", "Object ID")
	_ = objectHeadCmd.MarkFlagRequired("oid")
	objectHeadCmd.Flags().Bool("main-only", false, "Return only main fields")
	objectHeadCmd.Flags().Bool("json", false, "Marshal output in JSON")
	objectHeadCmd.Flags().Bool("proto", false, "Marshal output in Protobuf")

	objectCmd.AddCommand(objectHashCmd)
	objectHashCmd.Flags().String("cid", "", "Container ID")
	_ = objectHashCmd.MarkFlagRequired("cid")
	objectHashCmd.Flags().String("oid", "", "Object ID")
	_ = objectHashCmd.MarkFlagRequired("oid")
	objectHashCmd.Flags().String("range", "", "Range to take hash from in the form offset1:length1,...")
	objectHashCmd.Flags().String("type", hashSha256, "Hash type. Either 'sha256' or 'tz'")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// objectCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// objectCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func initSession(ctx context.Context) (*client.Client, *token.SessionToken, error) {
	cli, err := getSDKClient()
	if err != nil {
		return nil, nil, fmt.Errorf("can't create client: %w", err)
	}
	tok, err := cli.CreateSession(ctx, math.MaxUint64)
	if err != nil {
		return nil, nil, fmt.Errorf("can't create session: %w", err)
	}
	return cli, tok, nil
}

func putObject(cmd *cobra.Command, _ []string) error {
	ownerID, err := getOwnerID()
	if err != nil {
		return err
	}
	cid, err := getCID(cmd)
	if err != nil {
		return err
	}

	filename := cmd.Flag("file").Value.String()
	f, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("can't open file '%s': %w", filename, err)
	}

	attrs, err := parseObjectAttrs(cmd)
	if err != nil {
		return fmt.Errorf("can't parse object attributes: %w", err)
	}

	obj := object.NewRaw()
	obj.SetContainerID(cid)
	obj.SetOwnerID(ownerID)
	obj.SetAttributes(attrs...)

	ctx := context.Background()
	cli, tok, err := initSession(ctx)
	if err != nil {
		return err
	}
	btok, err := getBearerToken(cmd, "bearer")
	if err != nil {
		return err
	}
	oid, err := cli.PutObject(ctx,
		new(client.PutObjectParams).
			WithObject(obj.Object()).
			WithPayloadReader(f),
		client.WithSession(tok),
		client.WithBearer(btok))
	if err != nil {
		return fmt.Errorf("can't put object: %w", err)
	}

	cmd.Printf("[%s] Object successfully stored\n", filename)
	cmd.Printf("  ID: %s\n  CID: %s\n", oid, cid)
	return nil
}

func deleteObject(cmd *cobra.Command, _ []string) error {
	objAddr, err := getObjectAddress(cmd)
	if err != nil {
		return err
	}

	ctx := context.Background()
	cli, tok, err := initSession(ctx)
	if err != nil {
		return err
	}
	btok, err := getBearerToken(cmd, "bearer")
	if err != nil {
		return err
	}
	err = cli.DeleteObject(ctx,
		new(client.DeleteObjectParams).WithAddress(objAddr),
		client.WithSession(tok),
		client.WithBearer(btok))
	if err != nil {
		return err
	}

	cmd.Println("Object removed successfully.")
	cmd.Printf("  ID: %s\n  CID: %s\n", objAddr.GetObjectID(), objAddr.GetContainerID())
	return nil
}

func getObject(cmd *cobra.Command, _ []string) error {
	objAddr, err := getObjectAddress(cmd)
	if err != nil {
		return err
	}

	var out io.Writer
	filename := cmd.Flag("file").Value.String()
	if filename == "" {
		out = os.Stdout
	} else {
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			return fmt.Errorf("can't open file '%s': %w", filename, err)
		}
		defer f.Close()
		out = f
	}

	ctx := context.Background()
	cli, tok, err := initSession(ctx)
	if err != nil {
		return err
	}
	btok, err := getBearerToken(cmd, "bearer")
	if err != nil {
		return err
	}
	obj, err := cli.GetObject(ctx,
		new(client.GetObjectParams).
			WithAddress(objAddr).
			WithPayloadWriter(out),
		client.WithSession(tok),
		client.WithBearer(btok))
	if err != nil {
		return fmt.Errorf("can't put object: %w", err)
	}

	if filename != "" {
		cmd.Printf("[%s] Object successfully saved\n", filename)
	}

	// Print header only if file is not streamed to stdout.
	hdrFile := cmd.Flag("header").Value.String()
	if filename != "" || hdrFile != "" {
		return printHeader(cmd, obj, hdrFile)
	}
	return nil
}

func getObjectHeader(cmd *cobra.Command, _ []string) error {
	objAddr, err := getObjectAddress(cmd)
	if err != nil {
		return err
	}

	ctx := context.Background()
	cli, tok, err := initSession(ctx)
	if err != nil {
		return err
	}
	btok, err := getBearerToken(cmd, "bearer")
	if err != nil {
		return err
	}
	ps := new(client.ObjectHeaderParams).WithAddress(objAddr)
	if ok, _ := cmd.Flags().GetBool("main-only"); ok {
		ps = ps.WithMainFields()
	}
	obj, err := cli.GetObjectHeader(ctx, ps,
		client.WithSession(tok),
		client.WithBearer(btok))
	if err != nil {
		return fmt.Errorf("can't put object: %w", err)
	}

	return printHeader(cmd, obj, cmd.Flag("file").Value.String())
}

func searchObject(cmd *cobra.Command, _ []string) error {
	cid, err := getCID(cmd)
	if err != nil {
		return err
	}

	sf, err := parseSearchFilters(cmd)
	if err != nil {
		return err
	}

	ctx := context.Background()
	cli, tok, err := initSession(ctx)
	if err != nil {
		return err
	}
	btok, err := getBearerToken(cmd, "bearer")
	if err != nil {
		return err
	}
	ps := new(client.SearchObjectParams).WithContainerID(cid).WithSearchFilters(sf)
	ids, err := cli.SearchObject(ctx, ps,
		client.WithSession(tok),
		client.WithBearer(btok))
	if err != nil {
		return fmt.Errorf("can't put object: %w", err)
	}
	cmd.Printf("Found %d objects.\n", len(ids))
	for _, id := range ids {
		cmd.Println(id)
	}
	return nil
}

func getObjectHash(cmd *cobra.Command, _ []string) error {
	objAddr, err := getObjectAddress(cmd)
	if err != nil {
		return err
	}
	ranges, err := getRangeList(cmd)
	if err != nil {
		return err
	}
	typ, err := getHashType(cmd)
	if err != nil {
		return err
	}

	ctx := context.Background()
	cli, tok, err := initSession(ctx)
	if err != nil {
		return err
	}
	btok, err := getBearerToken(cmd, "bearer")
	if err != nil {
		return err
	}
	if len(ranges) == 0 { // hash of full payload
		obj, err := cli.GetObjectHeader(ctx,
			new(client.ObjectHeaderParams).WithAddress(objAddr),
			client.WithSession(tok),
			client.WithBearer(btok))
		if err != nil {
			return fmt.Errorf("can't get object: %w", err)
		}
		switch typ {
		case hashSha256:
			cmd.Println(hex.EncodeToString(obj.GetPayloadChecksum().GetSum()))
		case hashTz:
			cmd.Println(hex.EncodeToString(obj.GetPayloadHomomorphicHash().GetSum()))
		}
		return nil
	}

	ps := new(client.RangeChecksumParams).WithAddress(objAddr).WithRangeList(ranges...)
	switch typ {
	case hashSha256:
		res, err := cli.ObjectPayloadRangeSHA256(ctx, ps,
			client.WithSession(tok),
			client.WithBearer(btok))
		if err != nil {
			return err
		}
		for i := range res {
			cmd.Printf("Offset=%d (Length=%d)\t: %s\n", ranges[i].GetOffset(), ranges[i].GetLength(),
				hex.EncodeToString(res[i][:]))
		}
	case hashTz:
		res, err := cli.ObjectPayloadRangeTZ(ctx, ps,
			client.WithSession(tok),
			client.WithBearer(btok))
		if err != nil {
			return err
		}
		for i := range res {
			cmd.Printf("Offset=%d (Length=%d)\t: %s\n", ranges[i].GetOffset(), ranges[i].GetLength(),
				hex.EncodeToString(res[i][:]))
		}
	}
	return nil
}

func getOwnerID() (*owner.ID, error) {
	key, err := getKey()
	if err != nil {
		return nil, err
	}
	w, err := owner.NEO3WalletFromPublicKey(&key.PublicKey)
	if err != nil {
		return nil, err
	}
	ownerID := owner.NewID()
	ownerID.SetNeo3Wallet(w)
	return ownerID, nil
}

func parseSearchFilters(cmd *cobra.Command) (object.SearchFilters, error) {
	var fs object.SearchFilters
	if raw := cmd.Flag("filters").Value.String(); len(raw) != 0 {
		rawFs := strings.Split(raw, ",")
		for i := range rawFs {
			kv := strings.SplitN(rawFs[i], "=", 2)
			if len(kv) != 2 {
				return nil, fmt.Errorf("invalid filter format: %s", rawFs[i])
			}
			fs.AddFilter(kv[0], kv[1], object.MatchStringEqual)
		}
	}

	root, _ := cmd.Flags().GetBool("root")
	noroot, _ := cmd.Flags().GetBool("no-root")
	switch {
	case root && noroot:
		return nil, errors.New("'--root' and '--no-root' flags can't be set together")
	case root:
		fs.AddRootFilter()
	case noroot:
		fs.AddNonRootFilter()
	}

	leaf, _ := cmd.Flags().GetBool("leaf")
	noleaf, _ := cmd.Flags().GetBool("no-leaf")
	switch {
	case leaf && noleaf:
		return nil, errors.New("'--leaf' and '--no-leaf' flags can't be set together")
	case leaf:
		fs.AddLeafFilter()
	case noleaf:
		fs.AddNonLeafFilter()
	}

	return fs, nil
}

func parseObjectAttrs(cmd *cobra.Command) ([]*object.Attribute, error) {
	raw := cmd.Flag("attributes").Value.String()
	if len(raw) == 0 {
		return nil, nil
	}
	rawAttrs := strings.Split(raw, ",")
	attrs := make([]*object.Attribute, len(rawAttrs))
	for i := range rawAttrs {
		kv := strings.SplitN(rawAttrs[i], "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid attribute format: %s", rawAttrs[i])
		}
		attrs[i] = object.NewAttribute()
		attrs[i].SetKey(kv[0])
		attrs[i].SetValue(kv[1])
	}
	return attrs, nil
}

func getCID(cmd *cobra.Command) (*container.ID, error) {
	cid := container.NewID()
	err := cid.Parse(cmd.Flag("cid").Value.String())

	return cid, err
}

func getOID(cmd *cobra.Command) (*object.ID, error) {
	oid := object.NewID()
	err := oid.Parse(cmd.Flag("oid").Value.String())

	return oid, err
}

func getObjectAddress(cmd *cobra.Command) (*object.Address, error) {
	cid, err := getCID(cmd)
	if err != nil {
		return nil, err
	}
	oid, err := getOID(cmd)
	if err != nil {
		return nil, err
	}

	objAddr := object.NewAddress()
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

func printHeader(cmd *cobra.Command, obj *object.Object, filename string) error {
	bs, err := marshalHeader(cmd, obj)
	if err != nil {
		return err
	}
	if len(bs) != 0 {
		if filename == "" {
			cmd.Println(string(bs))
			return nil
		}
		err := ioutil.WriteFile(filename, bs, os.ModePerm)
		if err != nil {
			return err
		}
		cmd.Printf("[%s] Header successfully saved.", filename)
	}

	cmd.Printf("ID: %s\n", obj.GetID())
	cmd.Printf("CID: %s\n", obj.GetContainerID())
	cmd.Printf("Owner: %s\n", obj.GetOwnerID())
	cmd.Printf("CreatedAt: %d\n", obj.GetCreationEpoch())
	cmd.Printf("Size: %d\n", obj.GetPayloadSize())
	cmd.Printf("HomoHash: %s\n", hex.EncodeToString(obj.GetPayloadHomomorphicHash().GetSum()))
	cmd.Printf("Checksum: %s\n", hex.EncodeToString(obj.GetPayloadChecksum().GetSum()))
	switch obj.GetType() {
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
	for _, attr := range obj.GetAttributes() {
		cmd.Printf("  %s=%s\n", attr.GetKey(), attr.GetValue())
	}
	return nil
}

func marshalHeader(cmd *cobra.Command, hdr *object.Object) ([]byte, error) {
	toJson, _ := cmd.Flags().GetBool("json")
	toProto, _ := cmd.Flags().GetBool("proto")
	switch {
	case toJson && toProto:
		return nil, errors.New("'--json' and '--proto' flags are mutually exclusive")
	case toJson:
		return json.Marshal(hdr) // TODO currently not supported by neofs-api-go
	case toProto:
		return hdr.ToV2().StableMarshal(nil)
	default:
		return nil, nil
	}
}

func getBearerToken(cmd *cobra.Command, flagname string) (*token.BearerToken, error) {
	path, err := cmd.Flags().GetString(flagname)
	if err != nil || len(path) == 0 {
		return nil, nil
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("can't read bearer token file: %w", err)
	}

	v2token := v2ACL.BearerTokenFromJSON(data)
	if v2token == nil {
		msg := new(grpcACL.BearerToken)
		if proto.Unmarshal(data, msg) != nil {
			return nil, errors.New("can't decode bearer token")
		}

		v2token = v2ACL.BearerTokenFromGRPCMessage(msg)

		printVerbose("Using binary encoded bearer token")
	} else {
		printVerbose("Using JSON encoded bearer token")
	}

	return token.NewBearerTokenFromV2(v2token), nil
}
