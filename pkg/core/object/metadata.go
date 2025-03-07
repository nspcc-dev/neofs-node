package object

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
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

// AttributeDelimiter is attribute key and value separator used in metadata DB.
var AttributeDelimiter = []byte{0x00}

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
		if strings.IndexByte(attrs[i].Key(), AttributeDelimiter[0]) >= 0 {
			return fmt.Errorf("attribute #%d key contains 0x%02X byte used in sep", i, AttributeDelimiter[0])
		}
		if strings.IndexByte(attrs[i].Value(), AttributeDelimiter[0]) >= 0 {
			return fmt.Errorf("attribute #%d value contains 0x%02X byte used in sep", i, AttributeDelimiter[0])
		}
	}
	return nil
}
