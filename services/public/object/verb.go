package object

import (
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
)

// Verb is a type alias of
// Token_Info_Verb from service package of neofs-api-go.
type Verb = service.Token_Info_Verb

const (
	undefinedVerbDesc uint32 = 1 << iota
	putVerbDesc
	getVerbDesc
	headVerbDesc
	deleteVerbDesc
	searchVerbDesc
	rangeVerbDesc
	rangeHashVerbDesc
)

const (
	headSpawnMask      = headVerbDesc | getVerbDesc | putVerbDesc | rangeVerbDesc | rangeHashVerbDesc
	rangeHashSpawnMask = rangeHashVerbDesc
	rangeSpawnMask     = rangeVerbDesc | getVerbDesc
	getSpawnMask       = getVerbDesc
	putSpawnMask       = putVerbDesc | deleteVerbDesc
	deleteSpawnMask    = deleteVerbDesc
	searchSpawnMask    = searchVerbDesc | getVerbDesc | putVerbDesc | headVerbDesc | rangeVerbDesc | rangeHashVerbDesc | deleteVerbDesc
)

func toVerbDesc(verb Verb) uint32 {
	switch verb {
	case service.Token_Info_Put:
		return putVerbDesc
	case service.Token_Info_Get:
		return getVerbDesc
	case service.Token_Info_Head:
		return headVerbDesc
	case service.Token_Info_Delete:
		return deleteVerbDesc
	case service.Token_Info_Search:
		return searchVerbDesc
	case service.Token_Info_Range:
		return rangeVerbDesc
	case service.Token_Info_RangeHash:
		return rangeHashVerbDesc
	default:
		return undefinedVerbDesc
	}
}

func toSpawnMask(rt object.RequestType) uint32 {
	switch rt {
	case object.RequestPut:
		return putSpawnMask
	case object.RequestGet:
		return getSpawnMask
	case object.RequestHead:
		return headSpawnMask
	case object.RequestDelete:
		return deleteSpawnMask
	case object.RequestSearch:
		return searchSpawnMask
	case object.RequestRange:
		return rangeSpawnMask
	case object.RequestRangeHash:
		return rangeHashSpawnMask
	default:
		return undefinedVerbDesc
	}
}

func allowedSpawn(from Verb, to object.RequestType) bool {
	desc := toVerbDesc(from)

	return toSpawnMask(to)&desc == desc
}
