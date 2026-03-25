package protoscan

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/internal/protobuf"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"google.golang.org/protobuf/encoding/protowire"
)

type simpleField struct {
	num protowire.Number
	namedField
}

func newSimpleField(num protowire.Number, name string, typ fieldType) simpleField {
	return simpleField{num: num, namedField: newNamedField(name, typ)}
}

func newSimpleFieldsScheme(fs ...simpleField) MessageScheme {
	m := make(map[protowire.Number]namedField, len(fs))

	for _, f := range fs {
		if _, ok := m[f.num]; ok {
			panic(fmt.Sprintf("duplicated field with number %d", f.num))
		}

		m[f.num] = f.namedField
	}

	return MessageScheme{fields: m}
}

// Simple messages.
var (
	versionScheme = newSimpleFieldsScheme(
		newSimpleField(protorefs.FieldVersionMajor, "major", fieldTypeUint32),
		newSimpleField(protorefs.FieldVersionMinor, "minor", fieldTypeUint32),
	)
	ChecksumScheme = newSimpleFieldsScheme(
		newSimpleField(protorefs.FieldChecksumType, "type", fieldTypeEnum),
		newSimpleField(protorefs.FieldChecksumValue, "value", fieldTypeBytes),
	)
	signatureScheme = newSimpleFieldsScheme(
		newSimpleField(protorefs.FieldSignatureKey, "key", fieldTypeBytes),
		newSimpleField(protorefs.FieldSignatureValue, "value", fieldTypeBytes),
		newSimpleField(protorefs.FieldSignatureScheme, "scheme", fieldTypeEnum),
	)
	attributeScheme = newSimpleFieldsScheme(
		newSimpleField(1, "key", fieldTypeString),
		newSimpleField(2, "value", fieldTypeString),
	)
	tokenLifetimeScheme = newSimpleFieldsScheme(
		newSimpleField(1, "exp", fieldTypeUint64),
		newSimpleField(2, "nbf", fieldTypeUint64),
		newSimpleField(3, "iat", fieldTypeUint64),
	)
)

func newIDScheme(kind binaryFieldKind) MessageScheme {
	return MessageScheme{
		fields:           map[protowire.Number]namedField{1: {name: "value", typ: fieldTypeBytes}},
		binaryKindFields: map[protowire.Number]binaryFieldKind{1: kind},
	}
}

// IDs.
var (
	containerIDScheme = newIDScheme(binaryFieldSHA256)
	ObjectIDScheme    = newIDScheme(binaryFieldSHA256)
	userIDScheme      = newIDScheme(binaryFieldN3Address)
)

// Session token.
var (
	sessionSubjectScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protosession.FieldTargetOwnerID: newNamedField("owner", fieldTypeNestedMessage),
			protosession.FieldTargetNNSName: newNamedField("NNS name", fieldTypeString),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protosession.FieldTargetOwnerID: userIDScheme,
		},
	}
	sessionContextScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protosession.FieldSessionContextV2Container: newNamedField("container", fieldTypeNestedMessage),
			protosession.FieldSessionContextV2Verbs:     newNamedField("verbs", fieldTypeRepeatedEnum),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protosession.FieldSessionContextV2Container: containerIDScheme,
		},
	}
	sessionTokenBodyScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protosession.FieldSessionTokenV2BodyVersion:  newNamedField("version", fieldTypeUint32),
			protosession.FieldSessionTokenV2BodyAppdata:  newNamedField("appdata", fieldTypeBytes),
			protosession.FieldSessionTokenV2BodyIssuer:   newNamedField("issuer", fieldTypeNestedMessage),
			protosession.FieldSessionTokenV2BodySubjects: newNamedField("subject", fieldTypeNestedMessage),
			protosession.FieldSessionTokenV2BodyLifetime: newNamedField("lifetime", fieldTypeNestedMessage),
			protosession.FieldSessionTokenV2BodyContexts: newNamedField("contexts", fieldTypeNestedMessage),
			protosession.FieldSessionTokenV2BodyFinal:    newNamedField("final", fieldTypeBool),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protosession.FieldSessionTokenV2BodyIssuer:   userIDScheme,
			protosession.FieldSessionTokenV2BodySubjects: sessionSubjectScheme,
			protosession.FieldSessionTokenV2BodyLifetime: tokenLifetimeScheme,
			protosession.FieldSessionTokenV2BodyContexts: sessionContextScheme,
		},
	}
	sessionTokenScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protosession.FieldSessionTokenV2Body:      newNamedField("body", fieldTypeNestedMessage),
			protosession.FieldSessionTokenV2Signature: newNamedField("signature", fieldTypeNestedMessage),
			protosession.FieldSessionTokenV2Origin:    newNamedField("origin", fieldTypeNestedMessage),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protosession.FieldSessionTokenV2Body:      sessionTokenBodyScheme,
			protosession.FieldSessionTokenV2Signature: signatureScheme,
		},
		recursionFields: []protowire.Number{protosession.FieldSessionTokenV2Origin},
	}
)

// Session V1.
var (
	sessionV1ObjectTargetScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protosession.FieldObjectSessionContextTargetContainer: newNamedField("container", fieldTypeNestedMessage),
			protosession.FieldObjectSessionContextTargetObjects:   newNamedField("objects", fieldTypeNestedMessage),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protosession.FieldObjectSessionContextTargetContainer: containerIDScheme,
			protosession.FieldObjectSessionContextTargetObjects:   ObjectIDScheme,
		},
	}
	sessionV1ObjectContextScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protosession.FieldObjectSessionContextVerb:   newNamedField("verb", fieldTypeEnum),
			protosession.FieldObjectSessionContextTarget: newNamedField("target", fieldTypeNestedMessage),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protosession.FieldObjectSessionContextTargetObjects: sessionV1ObjectTargetScheme,
		},
	}
	sessionV1ContainerContextScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protosession.FieldContainerSessionContextVerb:        newNamedField("verb", fieldTypeEnum),
			protosession.FieldContainerSessionContextWildcard:    newNamedField("wildcard", fieldTypeBool),
			protosession.FieldContainerSessionContextContainerID: newNamedField("container", fieldTypeNestedMessage),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protosession.FieldContainerSessionContextContainerID: containerIDScheme,
		},
	}
	sessionV1TokenBodyScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protosession.FieldSessionTokenBodyID:         newNamedField("id", fieldTypeBytes),
			protosession.FieldSessionTokenBodyOwnerID:    newNamedField("owner", fieldTypeNestedMessage),
			protosession.FieldSessionTokenBodyLifetime:   newNamedField("lifetime", fieldTypeNestedMessage),
			protosession.FieldSessionTokenBodySessionKey: newNamedField("session key", fieldTypeBytes),
			protosession.FieldSessionTokenBodyObject:     newNamedField("object context", fieldTypeNestedMessage),
			protosession.FieldSessionTokenBodyContainer:  newNamedField("container context", fieldTypeNestedMessage),
		},
		binaryKindFields: map[protowire.Number]binaryFieldKind{
			protosession.FieldSessionTokenBodyID: binaryFieldKindUUIDV4,
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protosession.FieldSessionTokenBodyOwnerID:   userIDScheme,
			protosession.FieldSessionTokenBodyLifetime:  tokenLifetimeScheme,
			protosession.FieldSessionTokenBodyObject:    sessionV1ObjectContextScheme,
			protosession.FieldSessionTokenBodyContainer: sessionV1ContainerContextScheme,
		},
	}
	sessionV1TokenScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protosession.FieldSessionTokenBody:      newNamedField("body", fieldTypeNestedMessage),
			protosession.FieldSessionTokenSignature: newNamedField("signature", fieldTypeNestedMessage),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protosession.FieldSessionTokenBody:      sessionV1TokenBodyScheme,
			protosession.FieldSessionTokenSignature: signatureScheme,
		},
	}
)

// Object.
var (
	objectSplitHeaderScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protoobject.FieldHeaderSplitParent:          newNamedField("parent", fieldTypeNestedMessage),
			protoobject.FieldHeaderSplitPrevious:        newNamedField("previous", fieldTypeNestedMessage),
			protoobject.FieldHeaderSplitParentSignature: newNamedField("parent signature", fieldTypeNestedMessage),
			protoobject.FieldHeaderSplitParentHeader:    newNamedField("parent header", fieldTypeNestedMessage),
			protoobject.FieldHeaderSplitChildren:        newNamedField("children", fieldTypeNestedMessage),
			protoobject.FieldHeaderSplitSplitID:         newNamedField("split ID", fieldTypeBytes),
			protoobject.FieldHeaderSplitFirst:           newNamedField("first", fieldTypeNestedMessage),
		},
		binaryKindFields: map[protowire.Number]binaryFieldKind{
			protoobject.FieldHeaderSplitSplitID: binaryFieldKindUUIDV4,
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protoobject.FieldHeaderSplitParent:          ObjectIDScheme,
			protoobject.FieldHeaderSplitPrevious:        ObjectIDScheme,
			protoobject.FieldHeaderSplitParentSignature: signatureScheme,
			protoobject.FieldHeaderSplitChildren:        ObjectIDScheme,
			protoobject.FieldHeaderSplitFirst:           ObjectIDScheme,
		},
		nestedAliases: map[protowire.Number]schemeAlias{
			protoobject.FieldHeaderSplitParentHeader: schemeAliasObjectHeader,
		},
	}
	ObjectHeaderScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protoobject.FieldHeaderVersion:         newNamedField("version", fieldTypeNestedMessage),
			protoobject.FieldHeaderContainerID:     newNamedField("container ID", fieldTypeNestedMessage),
			protoobject.FieldHeaderOwnerID:         newNamedField("owner ID", fieldTypeNestedMessage),
			protoobject.FieldHeaderCreationEpoch:   newNamedField("creation epoch", fieldTypeUint64),
			protoobject.FieldHeaderPayloadLength:   newNamedField("payload length", fieldTypeUint64),
			protoobject.FieldHeaderPayloadHash:     newNamedField("payload hash", fieldTypeNestedMessage),
			protoobject.FieldHeaderObjectType:      newNamedField("type", fieldTypeEnum),
			protoobject.FieldHeaderHomomorphicHash: newNamedField("homomorphic hash", fieldTypeNestedMessage),
			protoobject.FieldHeaderSessionToken:    newNamedField("session V1 token", fieldTypeNestedMessage),
			protoobject.FieldHeaderAttributes:      newNamedField("attribute", fieldTypeNestedMessage),
			protoobject.FieldHeaderSplit:           newNamedField("split", fieldTypeNestedMessage),
			protoobject.FieldHeaderSessionV2:       newNamedField("session token", fieldTypeNestedMessage),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protoobject.FieldHeaderVersion:         versionScheme,
			protoobject.FieldHeaderContainerID:     containerIDScheme,
			protoobject.FieldHeaderOwnerID:         userIDScheme,
			protoobject.FieldHeaderPayloadHash:     ChecksumScheme,
			protoobject.FieldHeaderHomomorphicHash: ChecksumScheme,
			protoobject.FieldHeaderAttributes:      attributeScheme,
			protoobject.FieldHeaderSessionToken:    sessionV1TokenScheme,
			protoobject.FieldHeaderSplit:           objectSplitHeaderScheme,
			protoobject.FieldHeaderSessionV2:       sessionTokenScheme,
		},
	}
	ObjectHeaderWithSignatureScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protoobject.FieldHeaderWithSignatureHeader:    newNamedField("header", fieldTypeNestedMessage),
			protoobject.FieldHeaderWithSignatureSignature: newNamedField("signature", fieldTypeNestedMessage),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protoobject.FieldHeaderWithSignatureHeader:    ObjectHeaderScheme,
			protoobject.FieldHeaderWithSignatureSignature: signatureScheme,
		},
	}
	ObjectSplitInfoScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protoobject.FieldSplitInfoSplitID:   newNamedField("split ID", fieldTypeBytes),
			protoobject.FieldSplitInfoLastPart:  newNamedField("last part", fieldTypeNestedMessage),
			protoobject.FieldSplitInfoLink:      newNamedField("link", fieldTypeNestedMessage),
			protoobject.FieldSplitInfoFirstPart: newNamedField("first part", fieldTypeNestedMessage),
		},
		binaryKindFields: map[protowire.Number]binaryFieldKind{
			protoobject.FieldSplitInfoSplitID: binaryFieldKindUUIDV4,
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protoobject.FieldSplitInfoLastPart:  ObjectIDScheme,
			protoobject.FieldSplitInfoLink:      ObjectIDScheme,
			protoobject.FieldSplitInfoFirstPart: ObjectIDScheme,
		},
	}
)

// Responses.
var (
	responseStatusDetailScheme = newSimpleFieldsScheme(
		newSimpleField(protostatus.FieldStatusDetailID, "ID", fieldTypeUint32),
		newSimpleField(protostatus.FieldStatusDetailValue, "value", fieldTypeBytes),
	)
	ResponseStatusScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protostatus.FieldStatusCode:    newNamedField("code", fieldTypeUint32),
			protostatus.FieldStatusDetails: newNamedField("details", fieldTypeNestedMessage),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protostatus.FieldStatusDetails: responseStatusDetailScheme,
		},
	}
	ResponseMetaHeaderScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protosession.FieldResponseMetaHeaderVersion:  newNamedField("version", fieldTypeNestedMessage),
			protosession.FieldResponseMetaHeaderEpoch:    newNamedField("epoch", fieldTypeUint64),
			protosession.FieldResponseMetaHeaderTTL:      newNamedField("TTL", fieldTypeUint32),
			protosession.FieldResponseMetaHeaderXHeaders: newNamedField("X-headers", fieldTypeNestedMessage),
			protosession.FieldResponseMetaHeaderOrigin:   newNamedField("origin", fieldTypeNestedMessage),
			protosession.FieldResponseMetaHeaderStatus:   newNamedField("status", fieldTypeNestedMessage),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protosession.FieldResponseMetaHeaderVersion:  versionScheme,
			protosession.FieldResponseMetaHeaderXHeaders: attributeScheme,
			protosession.FieldResponseMetaHeaderStatus:   ResponseStatusScheme,
		},
		recursionFields: []protowire.Number{protosession.FieldResponseMetaHeaderOrigin},
	}
	ResponseScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protobuf.FieldResponseBody:       newNamedField("body", fieldTypeNestedMessage),
			protobuf.FieldResponseMetaHeader: newNamedField("meta header", fieldTypeNestedMessage),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protobuf.FieldResponseMetaHeader: ResponseMetaHeaderScheme,
		},
	}
	ObjectHeadResponseBodyScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protoobject.FieldHeadResponseBodyHeader:      newNamedField("header", fieldTypeNestedMessage),
			protoobject.FieldHeadResponseBodyShortHeader: newNamedField("short header", fieldTypeNestedMessage),
			protoobject.FieldHeadResponseBodySplitInfo:   newNamedField("split info", fieldTypeNestedMessage),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protoobject.FieldHeadResponseBodyHeader:    ObjectHeaderWithSignatureScheme,
			protoobject.FieldHeadResponseBodySplitInfo: ObjectSplitInfoScheme,
		},
	}
	ObjectGetResponseInitScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protoobject.FieldGetResponseBodyInitObjectID:  newNamedField("ID", fieldTypeNestedMessage),
			protoobject.FieldGetResponseBodyInitSignature: newNamedField("signature", fieldTypeNestedMessage),
			protoobject.FieldGetResponseBodyInitHeader:    newNamedField("header", fieldTypeNestedMessage),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protoobject.FieldGetResponseBodyInitObjectID:  ObjectIDScheme,
			protoobject.FieldGetResponseBodyInitSignature: signatureScheme,
			protoobject.FieldGetResponseBodyInitHeader:    ObjectHeaderScheme,
		},
	}
	ObjectGetResponseBodyScheme = MessageScheme{
		fields: map[protowire.Number]namedField{
			protoobject.FieldGetResponseBodyInit:      newNamedField("init", fieldTypeNestedMessage),
			protoobject.FieldGetResponseBodyChunk:     newNamedField("chunk", fieldTypeBytes),
			protoobject.FieldGetResponseBodySplitInfo: newNamedField("split info", fieldTypeNestedMessage),
		},
		nestedFields: map[protowire.Number]MessageScheme{
			protoobject.FieldGetResponseBodyInit:      ObjectGetResponseInitScheme,
			protoobject.FieldGetResponseBodySplitInfo: ObjectSplitInfoScheme,
		},
	}
)
