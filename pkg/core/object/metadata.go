package object

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/tzhash/tz"
)

// IsIntegerSearchOp reports whether given op matches integer attributes.
func IsIntegerSearchOp(op object.SearchMatchType) bool {
	return op == object.MatchNumGT || op == object.MatchNumGE || op == object.MatchNumLT || op == object.MatchNumLE
}

// MergeSearchResults merges up to lim elements from sorted search result sets
// into the one sorted set. Items are compared by the 1st attribute (if
// withAttr), and then by IDs when equal. If cmpInt is set, attributes are
// compared numerically. Otherwise, lexicographically. Additional booleans show
// whether corresponding sets can be continued. If the merged set can be
// continued itself, true is returned.
func MergeSearchResults(lim uint16, firstAttr string, cmpInt bool, sets [][]client.SearchResultItem, mores []bool) ([]client.SearchResultItem, bool, error) {
	if lim == 0 || len(sets) == 0 {
		return nil, false, nil
	}
	if len(sets) == 1 {
		ul := uint16(len(sets[0]))
		return sets[0][:min(ul, lim)], ul > lim || ul == lim && slices.Contains(mores, true), nil
	}
	lim = calcMaxUniqueSearchResults(lim, sets)
	res := make([]client.SearchResultItem, 0, lim)
	var more bool
	var minInt, curInt *big.Int
	if cmpInt {
		minInt, curInt = new(big.Int), new(big.Int)
	}
	var minOID, curOID oid.ID
	var minUsr, curUsr user.ID
	var err error
	for minInd := -1; ; minInd = -1 {
		for i := range sets {
			if len(sets[i]) == 0 {
				continue
			}
			if minInd < 0 {
				minInd = i
				if cmpInt {
					if _, ok := minInt.SetString(sets[i][0].Attributes[0], 10); !ok {
						return nil, false, fmt.Errorf("non-int attribute in result #%d", i)
					}
				}
				continue
			}
			cmpID := bytes.Compare(sets[i][0].ID[:], sets[minInd][0].ID[:])
			if cmpID == 0 {
				continue
			}
			if firstAttr != "" {
				var cmpAttr int
				if cmpInt {
					if _, ok := curInt.SetString(sets[i][0].Attributes[0], 10); !ok {
						return nil, false, fmt.Errorf("non-int attribute in result #%d", i)
					}
					cmpAttr = curInt.Cmp(minInt)
				} else {
					switch firstAttr {
					default:
						cmpAttr = strings.Compare(sets[i][0].Attributes[0], sets[minInd][0].Attributes[0])
					case object.FilterParentID, object.FilterFirstSplitObject:
						if err = curOID.DecodeString(sets[i][0].Attributes[0]); err == nil {
							err = minOID.DecodeString(sets[minInd][0].Attributes[0])
						}
						if err != nil {
							return nil, false, fmt.Errorf("invalid %q attribute value: %w", firstAttr, err)
						}
						cmpAttr = bytes.Compare(curOID[:], minOID[:])
					case object.FilterOwnerID:
						if err = curUsr.DecodeString(sets[i][0].Attributes[0]); err == nil {
							err = minUsr.DecodeString(sets[minInd][0].Attributes[0])
						}
						if err != nil {
							return nil, false, fmt.Errorf("invalid %q attribute value: %w", firstAttr, err)
						}
						cmpAttr = bytes.Compare(curUsr[:], minUsr[:])
					}
				}
				if cmpAttr != 0 {
					if cmpAttr < 0 {
						minInd = i
						if cmpInt {
							minInt, curInt = curInt, new(big.Int)
						}
					}
					continue
				}
			}
			if cmpID < 0 {
				minInd = i
				if cmpInt {
					minInt, curInt = curInt, new(big.Int)
				}
			}
		}
		if minInd < 0 {
			break
		}
		res = append(res, sets[minInd][0])
		if uint16(len(res)) == lim {
			if more = len(sets[minInd]) > 1 || slices.Contains(mores, true); !more {
			loop:
				for i := range sets {
					if i == minInd {
						continue
					}
					for j := range sets[i] {
						if more = sets[i][j].ID != sets[minInd][0].ID; more {
							break loop
						}
					}
				}
			}
			break
		}
		for i := range sets {
			if i == minInd {
				continue
			}
			for j := range sets[i] {
				if sets[i][j].ID == sets[minInd][0].ID {
					sets[i] = sets[i][j+1:]
					break
				}
			}
		}
		sets[minInd] = sets[minInd][1:]
	}
	return res, more, nil
}

func calcMaxUniqueSearchResults(lim uint16, sets [][]client.SearchResultItem) uint16 {
	n := uint16(len(sets[0]))
	if n >= lim {
		return lim
	}
	for i := 1; i < len(sets); i++ {
	nextItem:
		for j := range sets[i] {
			for k := range i {
				for l := range sets[k] {
					if sets[k][l].ID == sets[i][j].ID {
						continue nextItem
					}
				}
			}
			n++
			if n == lim {
				return n
			}
		}
	}
	return n
}

// VerifyHeaderForMetadata checks whether given header corresponds to metadata
// bucket requirements and limits.
func VerifyHeaderForMetadata(hdr object.Object) error {
	if ln := hdr.HeaderLen(); ln > object.MaxHeaderLen {
		return fmt.Errorf("header len %d exceeds the limit", ln)
	}
	if hdr.GetContainerID().IsZero() {
		return fmt.Errorf("invalid container: %w", cid.ErrZero)
	}
	if hdr.Owner().IsZero() {
		return fmt.Errorf("invalid owner: %w", user.ErrZeroID)
	}
	if _, ok := hdr.PayloadChecksum(); !ok {
		return errors.New("missing payload checksum")
	}
	attrs := hdr.Attributes()
	for i := range attrs {
		if strings.IndexByte(attrs[i].Key(), MetaAttributeDelimiter[0]) >= 0 {
			return fmt.Errorf("attribute #%d key contains 0x%02X byte used in sep", i, MetaAttributeDelimiter[0])
		}
		if strings.IndexByte(attrs[i].Value(), MetaAttributeDelimiter[0]) >= 0 {
			return fmt.Errorf("attribute #%d value contains 0x%02X byte used in sep", i, MetaAttributeDelimiter[0])
		}
	}
	return nil
}

var (
	// ErrUnreachableQuery is returned when no object ever matches a particular
	// search query.
	ErrUnreachableQuery = errors.New("unreachable query")

	errInvalidCursor         = errors.New("invalid cursor")
	errWrongValOIDDelim      = errors.New("wrong value-OID delimiter")
	errInvalidPrimaryFilter  = errors.New("invalid primary filter")
	errWrongPrimaryAttribute = errors.New("wrong primary attribute")
	errWrongKeyValDelim      = errors.New("wrong key-value delimiter")

	// MetaAttributeDelimiter is attribute key and value separator used in metadata DB.
	MetaAttributeDelimiter = []byte{0x00}
	metaOIDPrefix          = []byte{metaPrefixID}

	maxUint256 = new(big.Int).SetBytes([]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
		255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255})
	maxUint256Neg = new(big.Int).Neg(maxUint256)
)

// SearchCursor is a cursor used for continuous search in the meta indexes
// database.
type SearchCursor struct {
	// PrimarySeekKey defines the last key that was already handled.
	PrimarySeekKey []byte
	// PrimaryKeysPrefix defines what part of primary key is prefix.
	PrimaryKeysPrefix []byte
}

// ParsedIntFilter is returned by [PreprocessSearchQuery] to pass into the
// [MetaDataKVHandler].
type ParsedIntFilter struct {
	// AutoMatch means every existing value is acceptable for filters.
	AutoMatch bool
	// Parsed is parsed integer value from filter.
	Parsed *big.Int
	// RawValue is raw attribute value in the original form.
	Raw []byte
}

// AttributeGetter provides access to attributes based on database's indexes.
type AttributeGetter interface {
	// Get must return attribute value for a pair of object ID and attribute
	// value. (nil, nil) must be returned if value is missing. err should mean
	// only data base errors.
	Get(oID []byte, attributeKey string) (attributeValue []byte, err error)
}

// AdditionalObjectChecker is an additional check that may be provided to
// [MetaDataKVHandler]. `false` return value stops object from being recorded
// to [SearchResult], `true` keeps searching without additional filtering.
type AdditionalObjectChecker func(oid.ID) (match bool)

// SearchResult is a [MetaDataKVHandler] operation's results holder.
type SearchResult struct {
	Objects             []client.SearchResultItem
	UpdatedSearchCursor []byte
	Err                 error
}

// MetaDataKVHandler returns a handler that filters out and writes search results
// for Search NeoFS operation (see [client.Client.SearchObjects] for details) based on
// the provided arguments. resHolder must not be nil. additionalCheck may or may
// not be nil, it will be called once on every object that matches all the previous
// (filters-based) checks. fInt must have preparsed (from string attribute form)
// integer values from the fs.
// Returned handler must be called on a sorted list of key-value pairs without
// any pair omission and/or duplication. Pairs must be common NeoFS indexes for
// object header's fields. See [meta] (storage engine's package) for details.
func MetaDataKVHandler(resHolder *SearchResult, attrGetter AttributeGetter, additionalCheck AdditionalObjectChecker, fs object.SearchFilters, fInt map[int]ParsedIntFilter, attrs []string, cursor *SearchCursor, count uint16) func(k, v []byte) bool {
	primMatcher, _ := convertFilterValue(fs[0])
	intPrimMatcher := IsIntegerSearchOp(primMatcher)
	idIter := len(attrs) == 0 || primMatcher == object.MatchNotPresent
	var lastMatchedPrimKey []byte
	var n uint16
	var more bool
	var id, dbVal, primDBVal []byte
	var wasPrimMatch bool
	dbValInt := new(big.Int)

	return func(k, v []byte) bool {
		defer func() {
			if more {
				resHolder.UpdatedSearchCursor = slices.Clone(lastMatchedPrimKey[1:])
			}
		}()

		if idIter {
			if id = k[1:]; len(id) != oid.Size {
				resHolder.Err = invalidMetaBucketKeyErr(k, fmt.Errorf("invalid OID len %d", len(id)))
				return false
			}
		} else { // apply primary filter
			valID := k[len(cursor.PrimaryKeysPrefix):] // VAL_OID
			if intPrimMatcher {
				if len(valID) <= oid.Size {
					resHolder.Err = invalidMetaBucketKeyErr(k, fmt.Errorf("too small VAL_OID len %d", len(valID)))
					return false
				}
				primDBVal, id = valID[:len(valID)-oid.Size], valID[len(valID)-oid.Size:]
			} else {
				var err error
				if primDBVal, id, err = splitValOID(valID); err != nil {
					resHolder.Err = invalidMetaBucketKeyErr(k, fmt.Errorf("invalid VAL_OID: %w", err))
					return false
				}
			}
			for i := range fs {
				// there may be several filters by primary key, e.g. N >= 10 && N <= 20. We
				// check them immediately before moving through the DB.
				attr := fs[i].Header()
				if i > 0 && attr != fs[0].Header() {
					continue
				}
				mch, val := convertFilterValue(fs[i])
				var matches bool
				if IsIntegerSearchOp(mch) {
					f := fInt[i]
					matches = f.AutoMatch || intBytesMatch(primDBVal, mch, f.Raw)
				} else {
					checkedDBVal, fltVal, err := combineValues(attr, primDBVal, val) // TODO: deduplicate DB value preparation
					if err != nil {
						resHolder.Err = fmt.Errorf("invalid key in meta bucket: invalid attribute %s value: %w", attr, err)
						return false
					}
					matches = matchValues(checkedDBVal, mch, fltVal)
				}
				if !matches {
					if mch != object.MatchStringNotEqual && (wasPrimMatch || mch != object.MatchNumGT) {
						return false
					}
					return true
				}
				wasPrimMatch = true
				// TODO: attribute value can be requested, it can be collected here, or we can
				//  detect earlier when an object goes beyond the already collected result. The
				//  code can become even more complex. Same below
			}
		}
		// apply other filters
		for i := range fs {
			if !idIter && (i == 0 || fs[i].Header() == fs[0].Header()) { // 1st already checked
				continue
			}
			attr := fs[i].Header() // emptiness already prevented
			for j := 1; j < i; j++ {
				if fs[j].Header() == attr { // already match, checked in loop below
					continue
				}
			}
			var err error
			if dbVal, err = attrGetter.Get(id, attr); err != nil {
				resHolder.Err = err
				return false
			}
			var dbValIsInt bool
			for j := i; j < len(fs); j++ {
				if j > 0 && fs[j].Header() != attr {
					continue
				}
				m, val := convertFilterValue(fs[j])
				if dbVal == nil {
					if m == object.MatchNotPresent {
						continue
					}
					return true
				}
				if m == object.MatchNotPresent {
					return true
				}
				var matches bool
				if IsIntegerSearchOp(m) {
					if !dbValIsInt {
						_, dbValIsInt = dbValInt.SetString(string(dbVal), 10)
					}
					if dbValIsInt {
						f := fInt[j]
						matches = f.AutoMatch || intMatches(dbValInt, m, f.Parsed)
					}
				} else {
					checkedDBVal, fltVal, err := combineValues(attr, dbVal, val) // TODO: deduplicate DB value preparation
					if err != nil {
						resHolder.Err = invalidMetaBucketKeyErr(k, fmt.Errorf("invalid attribute %s value: %w", attr, err))
						return false
					}
					matches = matchValues(checkedDBVal, m, fltVal)
				}
				if !matches {
					return true
				}
			}
		}
		if additionalCheck != nil && !additionalCheck(oid.ID(id)) {
			return true
		}
		// object matches
		if n == count {
			more = true
			return false
		}
		// collect attributes
		collected := make([]string, len(attrs))
		if len(attrs) > 0 {
			var err error
			if intPrimMatcher {
				if collected[0], err = RestoreIntAttribute(primDBVal); err != nil {
					resHolder.Err = invalidMetaBucketKeyErr(k, fmt.Errorf("invalid integer value: %w", err))
					return false
				}
			} else {
				if collected[0], resHolder.Err = restoreAttributeValue(fs[0].Header(), primDBVal); err != nil {
					return false
				}
			}
		}
		for i := 1; i < len(attrs); i++ {
			val, err := attrGetter.Get(id, attrs[i])
			if err != nil {
				resHolder.Err = err
				return false
			}
			if collected[i], err = restoreAttributeValue(attrs[i], val); err != nil {
				resHolder.Err = err
				return false
			}
		}
		resHolder.Objects = append(resHolder.Objects, client.SearchResultItem{
			ID:         oid.ID(id),
			Attributes: collected,
		})
		lastMatchedPrimKey = k
		n++
		// note that even when n == count meaning there will be no more attachments, we
		// still need to determine whether there more matching elements (see condition above)

		return true
	}
}

func convertFilterValue(f object.SearchFilter) (object.SearchMatchType, string) {
	if attr := f.Header(); attr == object.FilterRoot || attr == object.FilterPhysical {
		return object.MatchStringEqual, binPropMarker
	}
	return f.Operation(), f.Value()
}

func invalidMetaBucketKeyErr(key []byte, cause error) error {
	return fmt.Errorf("invalid meta bucket key (prefix 0x%X): %w", key[0], cause)
}

const (
	metaPrefixID = byte(iota)
	metaPrefixAttrIDInt
	metaPrefixAttrIDPlain
)

const (
	attributeDelimiterLen = 1
	binPropMarker         = "1" // ROOT, PHY, etc.
	intValLen             = 33  // prefix byte for sign + fixed256 in metaPrefixAttrIDInt
)

// splits VAL_DELIM_OID.
func splitValOID(b []byte) ([]byte, []byte, error) {
	if len(b) < attributeDelimiterLen+oid.Size+1 { // +1 because VAL cannot be empty
		return nil, nil, fmt.Errorf("too short len %d", len(b))
	}
	idOff := len(b) - oid.Size
	valLn := idOff - attributeDelimiterLen
	if !bytes.Equal(b[valLn:idOff], MetaAttributeDelimiter) {
		return nil, nil, errWrongValOIDDelim
	}
	return b[:valLn], b[idOff:], nil
}

func intBytesMatch(dbVal []byte, matcher object.SearchMatchType, fltValBytes []byte) bool {
	switch matcher {
	default:
		panic(fmt.Errorf("unexpected integer matcher %d", matcher))
	case object.MatchNumGT:
		return bytes.Compare(dbVal, fltValBytes) > 0
	case object.MatchNumGE:
		return bytes.Compare(dbVal, fltValBytes) >= 0
	case object.MatchNumLT:
		return bytes.Compare(dbVal, fltValBytes) < 0
	case object.MatchNumLE:
		return bytes.Compare(dbVal, fltValBytes) <= 0
	}
}

// combines attribute's DB and NeoFS API SearchV2 values to the matchable
// format. Returns DB errors only.
func combineValues(attr string, dbVal []byte, fltVal string) ([]byte, []byte, error) {
	switch attr {
	case object.FilterOwnerID:
		if len(dbVal) != user.IDSize {
			return nil, nil, fmt.Errorf("invalid owner len %d != %d", len(dbVal), user.IDSize)
		}
		if b, _ := base58.Decode(fltVal); len(b) == user.IDSize {
			return dbVal, b, nil
		}
		// consider filter 'owner PREFIX N':
		//  - any object matches it
		//  - decoded filter byte is always 21 while the DB one is always 53
		// so we'd get false mismatch. To avoid this, we have to decode each DB val.
		dbVal = []byte(base58.Encode(dbVal))
	case object.FilterFirstSplitObject, object.FilterParentID:
		if len(dbVal) != oid.Size {
			return nil, nil, fmt.Errorf("invalid OID len %d != %d", len(dbVal), oid.Size)
		}
		if b, _ := base58.Decode(fltVal); len(b) == oid.Size {
			return dbVal, b, nil
		}
		// same as owner
		dbVal = []byte(base58.Encode(dbVal))
	case object.FilterPayloadChecksum:
		if len(dbVal) != sha256.Size {
			return nil, nil, fmt.Errorf("invalid payload checksum len %d != %d", len(dbVal), sha256.Size)
		}
		if b, err := hex.DecodeString(fltVal); err == nil {
			return dbVal, b, nil
		}
		dbVal = []byte(hex.EncodeToString(dbVal))
	case object.FilterPayloadHomomorphicHash:
		if len(dbVal) != tz.Size {
			return nil, nil, fmt.Errorf("invalid payload homomorphic hash len %d != %d", len(dbVal), tz.Size)
		}
		if b, err := hex.DecodeString(fltVal); err == nil {
			return dbVal, b, nil
		}
		dbVal = []byte(hex.EncodeToString(dbVal))
	case object.FilterSplitID:
		if len(dbVal) != 16 {
			return nil, nil, fmt.Errorf("invalid split ID len %d != 16", len(dbVal))
		}
		uid, err := uuid.Parse(fltVal)
		if err == nil {
			return dbVal, uid[:], nil
		}
		copy(uid[:], dbVal)
		dbVal = []byte(uid.String())
	}
	return dbVal, []byte(fltVal), nil
}

// matches object attribute's search query value to the DB-stored one. Matcher
// must be supported but not [object.MatchNotPresent] or numeric.
func matchValues(dbVal []byte, matcher object.SearchMatchType, fltVal []byte) bool {
	switch {
	default:
		return false // TODO: check whether supported in blindlyProcess. Then panic here
	case matcher == object.MatchNotPresent || IsIntegerSearchOp(matcher):
		panic(fmt.Sprintf("unexpected matcher %s", matcher))
	case matcher == object.MatchStringEqual:
		return bytes.Equal(dbVal, fltVal)
	case matcher == object.MatchStringNotEqual:
		return !bytes.Equal(dbVal, fltVal)
	case matcher == object.MatchCommonPrefix:
		return bytes.HasPrefix(dbVal, fltVal)
	}
}

// RestoreIntAttribute restores from raw signed uint256 format its string
// representation.
func RestoreIntAttribute(b []byte) (string, error) {
	if len(b) != intValLen {
		return "", fmt.Errorf("invalid len %d", len(b))
	}
	switch b[0] {
	default:
		return "", fmt.Errorf("invalid sign byte %d", b[0])
	case 1:
		return new(big.Int).SetBytes(b[1:]).String(), nil
	case 0:
		cp := slices.Clone(b[1:])
		for i := range cp {
			cp[i] = ^cp[i]
		}
		n := new(big.Int).SetBytes(cp)
		return n.Neg(n).String(), nil
	}
}

func restoreAttributeValue(attr string, stored []byte) (string, error) {
	switch attr {
	case object.FilterOwnerID, object.FilterFirstSplitObject, object.FilterParentID:
		return base58.Encode(stored), nil
	case object.FilterPayloadChecksum, object.FilterPayloadHomomorphicHash:
		return hex.EncodeToString(stored), nil
	case object.FilterSplitID:
		uid, err := uuid.FromBytes(stored)
		if err != nil {
			return "", invalidMetaBucketKeyErr([]byte{metaPrefixAttrIDPlain}, fmt.Errorf("decode split ID: decode UUID: %w", err))
		}
		return uid.String(), nil
	}
	return string(stored), nil
}

func intMatches(dbVal *big.Int, matcher object.SearchMatchType, fltVal *big.Int) bool {
	switch matcher {
	default:
		panic(fmt.Errorf("unexpected integer matcher %d", matcher))
	case object.MatchNumGT:
		return dbVal.Cmp(fltVal) > 0
	case object.MatchNumGE:
		return dbVal.Cmp(fltVal) >= 0
	case object.MatchNumLT:
		return dbVal.Cmp(fltVal) < 0
	case object.MatchNumLE:
		return dbVal.Cmp(fltVal) <= 0
	}
}

// PreprocessSearchQuery accepts verified search filters, requested attributes
// along and continuation cursor, verifies the cursor and returns additional
// arguments to pass into [DB.Search]. If the query is valid but unreachable,
// [ErrUnreachableQuery] is returned.
func PreprocessSearchQuery(fs object.SearchFilters, attrs []string, cursor string) (*SearchCursor, map[int]ParsedIntFilter, error) {
	if len(fs) == 0 {
		if cursor != "" {
			primSeekKey, err := decodeOIDFromCursor(cursor)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: %w", errInvalidCursor, err)
			}
			return &SearchCursor{PrimaryKeysPrefix: metaOIDPrefix, PrimarySeekKey: primSeekKey}, nil, nil
		}
		return &SearchCursor{PrimaryKeysPrefix: metaOIDPrefix, PrimarySeekKey: metaOIDPrefix}, nil, nil
	}

	primMatcher, primVal := convertFilterValue(fs[0])
	oidSorted := len(attrs) == 0 || primMatcher == object.MatchNotPresent
	var primValDB []byte
	if !oidSorted && cursor == "" && primMatcher != object.MatchStringNotEqual && !IsIntegerSearchOp(primMatcher) {
		switch attr := fs[0].Header(); attr {
		default:
			primValDB = []byte(primVal)
		case object.FilterOwnerID, object.FilterFirstSplitObject, object.FilterParentID:
			var err error
			if primValDB, err = base58.Decode(primVal); err != nil {
				return nil, nil, fmt.Errorf("%w: decode %q attribute value from Base58: %w", errInvalidPrimaryFilter, attr, err)
			}
		case object.FilterPayloadChecksum, object.FilterPayloadHomomorphicHash:
			var err error
			if primValDB, err = hex.DecodeString(primVal); err != nil {
				return nil, nil, fmt.Errorf("%w: decode %q attribute value from HEX: %w", errInvalidPrimaryFilter, attr, err)
			}
		case object.FilterSplitID:
			uid, err := uuid.Parse(primVal)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: code %q UUID attribute: %w", errInvalidPrimaryFilter, attr, err)
			}
			primValDB = uid[:]
		}
	}

	var primKeysPrefix, primSeekKey []byte
	if oidSorted {
		if cursor != "" {
			var err error
			if primSeekKey, err = decodeOIDFromCursor(cursor); err != nil {
				return nil, nil, fmt.Errorf("%w: %w", errInvalidCursor, err)
			}
		}
	} else if cursor != "" {
		// TODO: wrap into "invalid cursor" error
		primSeekKey = make([]byte, 1+base64.StdEncoding.DecodedLen(len(cursor)))
		n, err := base64.StdEncoding.Decode(primSeekKey[1:], []byte(cursor))
		if err != nil {
			return nil, nil, fmt.Errorf("%w: decode Base64: %w", errInvalidCursor, err)
		}
		if n > object.MaxHeaderLen {
			return nil, nil, fmt.Errorf("%w: len %d exceeds the limit %d", errInvalidCursor, n, object.MaxHeaderLen)
		}
		primSeekKey = primSeekKey[:1+n]
		if IsIntegerSearchOp(primMatcher) {
			if n != len(attrs[0])+attributeDelimiterLen+intValLen+oid.Size {
				return nil, nil, fmt.Errorf("%w: wrong len %d for int query", errInvalidCursor, n)
			}
			primKeysPrefix = primSeekKey[:1+len(attrs[0])+attributeDelimiterLen]
			if !bytes.Equal(primKeysPrefix[1:1+len(attrs[0])], []byte(attrs[0])) {
				return nil, nil, fmt.Errorf("%w: %w", errInvalidCursor, errWrongPrimaryAttribute)
			}
			if !bytes.Equal(primKeysPrefix[1+len(attrs[0]):], MetaAttributeDelimiter) {
				return nil, nil, fmt.Errorf("%w: %w", errInvalidCursor, errWrongKeyValDelim)
			}
			if primSeekKey[len(primKeysPrefix)] > 1 {
				return nil, nil, fmt.Errorf("%w: invalid sign byte 0x%02X", errInvalidCursor, primSeekKey[len(primKeysPrefix)])
			}
		} else {
			if n < len(attrs[0])+attributeDelimiterLen+1+attributeDelimiterLen+oid.Size { // +1 because VAL cannot be empty
				return nil, nil, fmt.Errorf("%w: too short len %d", errInvalidCursor, n)
			}
			primKeysPrefix = primSeekKey[:1+len(attrs[0])+attributeDelimiterLen]
			if !bytes.Equal(primKeysPrefix[1:1+len(attrs[0])], []byte(attrs[0])) {
				return nil, nil, fmt.Errorf("%w: %w", errInvalidCursor, errWrongPrimaryAttribute)
			}
			if !bytes.Equal(primKeysPrefix[1+len(attrs[0]):], MetaAttributeDelimiter) {
				return nil, nil, fmt.Errorf("%w: %w", errInvalidCursor, errWrongKeyValDelim)
			}
			if !bytes.Equal(primSeekKey[len(primSeekKey)-oid.Size-attributeDelimiterLen:][:attributeDelimiterLen], MetaAttributeDelimiter) {
				return nil, nil, fmt.Errorf("%w: %w", errInvalidCursor, errWrongValOIDDelim)
			}
		}
	}

	if blindlyProcess(fs) {
		return nil, nil, ErrUnreachableQuery
	}
	fInt, ok := parseIntFilters(fs)
	if !ok {
		return nil, nil, ErrUnreachableQuery
	}

	if oidSorted {
		if cursor == "" {
			primSeekKey = metaOIDPrefix
		}
		primKeysPrefix = metaOIDPrefix
	} else {
		if cursor != "" {
			if IsIntegerSearchOp(primMatcher) {
				primSeekKey[0] = metaPrefixAttrIDInt // fins primKeysPrefix also
			} else {
				primSeekKey[0] = metaPrefixAttrIDPlain // fins primKeysPrefix also
			}
		} else {
			if IsIntegerSearchOp(primMatcher) {
				f := fInt[0]
				if !f.AutoMatch && (primMatcher == object.MatchNumGE || primMatcher == object.MatchNumGT) {
					primSeekKey = slices.Concat([]byte{metaPrefixAttrIDInt}, []byte(attrs[0]), MetaAttributeDelimiter, f.Raw)
					primKeysPrefix = primSeekKey[:1+len(attrs[0])+attributeDelimiterLen]
				} else {
					primSeekKey = slices.Concat([]byte{metaPrefixAttrIDInt}, []byte(attrs[0]), MetaAttributeDelimiter)
					primKeysPrefix = primSeekKey
				}
			} else {
				// according to the condition above, primValDB is empty for '!=' matcher as it should be
				primSeekKey = slices.Concat([]byte{metaPrefixAttrIDPlain}, []byte(attrs[0]), MetaAttributeDelimiter, primValDB)
				primKeysPrefix = primSeekKey[:1+len(attrs[0])+attributeDelimiterLen]
			}
		}
	}
	return &SearchCursor{PrimaryKeysPrefix: primKeysPrefix, PrimarySeekKey: primSeekKey}, fInt, nil
}

func decodeOIDFromCursor(cursor string) ([]byte, error) {
	key := make([]byte, 1+base64.StdEncoding.DecodedLen(len(cursor)))
	n, err := base64.StdEncoding.Decode(key[1:], []byte(cursor))
	if err != nil {
		return nil, fmt.Errorf("decode Base64: %w", err)
	}
	if n != oid.Size {
		return nil, fmt.Errorf("wrong len %d for listing query", n)
	}
	key[0] = metaPrefixID
	return key[:1+n], nil
}

// returns true if query leads to a deliberately empty result.
func blindlyProcess(fs object.SearchFilters) bool {
	for i := range fs {
		if fs[i].Operation() == object.MatchNotPresent && fs[i].IsNonAttribute() {
			return true
		}

		// TODO: #1148 check other cases
		//  e.g. (a == b) && (a != b)
	}

	return false
}

func parseIntFilters(fs object.SearchFilters) (map[int]ParsedIntFilter, bool) {
	fInt := make(map[int]ParsedIntFilter, len(fs)) // number of filters is limited by pretty small value, so we can afford it
	for i := range fs {
		m, val := convertFilterValue(fs[i])
		if !IsIntegerSearchOp(m) {
			continue
		}
		n, ok := new(big.Int).SetString(val, 10)
		if !ok {
			return nil, false
		}
		var f ParsedIntFilter
		if c := n.Cmp(maxUint256); c >= 0 {
			if c > 0 || m == object.MatchNumGT {
				return nil, false
			}
			f.AutoMatch = m == object.MatchNumLE
		} else if c = n.Cmp(maxUint256Neg); c <= 0 {
			if c < 0 || m == object.MatchNumLT {
				return nil, false
			}
			f.AutoMatch = m == object.MatchNumGE
		}
		if !f.AutoMatch {
			if i == 0 || IsIntegerSearchOp(fs[0].Operation()) && fs[i].Header() == fs[0].Header() {
				f.Raw = BigIntBytes(n)
			}
			f.Parsed = n
		}
		// TODO: #1148 there are more auto-cases (like <=X AND >=X, <X AND >X), cover more here
		fInt[i] = f
	}
	return fInt, true
}

// BigIntBytes returns integer's raw representation. Int must belong to
// [-maxUint256; maxUint256] interval. Result's length is fixed: 33 bytes,
// first describes sign.
func BigIntBytes(n *big.Int) []byte {
	b := make([]byte, intValLen)
	putInt(b, n)
	return b
}

func putInt(b []byte, n *big.Int) {
	if len(b) < intValLen {
		panic(fmt.Errorf("insufficient buffer len %d", len(b)))
	}
	neg := n.Sign() < 0
	if neg {
		b[0] = 0
	} else {
		b[0] = 1
	}
	n.FillBytes(b[1:intValLen])
	if neg {
		for i := range b[1:] {
			b[1+i] = ^b[1+i]
		}
	}
}
