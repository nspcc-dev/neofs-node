package protoscan

import (
	"fmt"

	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"google.golang.org/protobuf/encoding/protowire"
)

type simpleField struct {
	num protowire.Number
	MessageField
}

func newSimpleField(num protowire.Number, name string, typ FieldType) simpleField {
	return simpleField{num: num, MessageField: NewMessageField(name, typ)}
}

func newSimpleFieldsScheme(fs ...simpleField) MessageScheme {
	m := make(map[protowire.Number]MessageField, len(fs))

	for _, f := range fs {
		if _, ok := m[f.num]; ok {
			panic(fmt.Sprintf("duplicated field with number %d", f.num))
		}

		m[f.num] = f.MessageField
	}

	return MessageScheme{Fields: m}
}

// Simple messages.
var (
	VersionScheme = newSimpleFieldsScheme(
		newSimpleField(protorefs.FieldVersionMajor, "major", FieldTypeUint32),
		newSimpleField(protorefs.FieldVersionMinor, "minor", FieldTypeUint32),
	)
	ChecksumScheme = newSimpleFieldsScheme(
		newSimpleField(protorefs.FieldChecksumType, "type", FieldTypeEnum),
		newSimpleField(protorefs.FieldChecksumValue, "value", FieldTypeBytes),
	)
	SignatureScheme = newSimpleFieldsScheme(
		newSimpleField(protorefs.FieldSignatureKey, "key", FieldTypeBytes),
		newSimpleField(protorefs.FieldSignatureValue, "value", FieldTypeBytes),
		newSimpleField(protorefs.FieldSignatureScheme, "scheme", FieldTypeEnum),
	)
	AttributeScheme = newSimpleFieldsScheme(
		newSimpleField(1, "key", FieldTypeString),
		newSimpleField(2, "value", FieldTypeString),
	)
	TokenLifetimeScheme = newSimpleFieldsScheme(
		newSimpleField(1, "exp", FieldTypeUint64),
		newSimpleField(2, "nbf", FieldTypeUint64),
		newSimpleField(3, "iat", FieldTypeUint64),
	)
)

func newIDScheme(kind BinaryFieldKind) MessageScheme {
	return MessageScheme{
		Fields:       map[protowire.Number]MessageField{1: {name: "value", typ: FieldTypeBytes}},
		BinaryFields: map[protowire.Number]BinaryFieldKind{1: kind},
	}
}

// IDs.
var (
	ContainerIDScheme = newIDScheme(BinaryFieldSHA256)
	ObjectIDScheme    = newIDScheme(BinaryFieldSHA256)
	UserIDScheme      = newIDScheme(BinaryFieldN3Address)
)

// Session token.
var (
	sessionSubjectScheme = MessageScheme{
		Fields: map[protowire.Number]MessageField{
			protosession.FieldTargetOwnerID: NewMessageField("owner", FieldTypeNestedMessage),
			protosession.FieldTargetNNSName: NewMessageField("NNS name", FieldTypeString),
		},
		NestedMessageFields: map[protowire.Number]MessageScheme{
			protosession.FieldTargetOwnerID: UserIDScheme,
		},
	}
	sessionContextScheme = MessageScheme{
		Fields: map[protowire.Number]MessageField{
			protosession.FieldSessionContextV2Container: NewMessageField("container", FieldTypeNestedMessage),
			protosession.FieldSessionContextV2Verbs:     NewMessageField("verbs", FieldTypeRepeatedEnum),
		},
		NestedMessageFields: map[protowire.Number]MessageScheme{
			protosession.FieldSessionContextV2Container: ContainerIDScheme,
		},
	}
	sessionTokenBodyScheme = MessageScheme{
		Fields: map[protowire.Number]MessageField{
			protosession.FieldSessionTokenV2BodyVersion:  NewMessageField("version", FieldTypeUint32),
			protosession.FieldSessionTokenV2BodyAppdata:  NewMessageField("appdata", FieldTypeBytes),
			protosession.FieldSessionTokenV2BodyIssuer:   NewMessageField("issuer", FieldTypeNestedMessage),
			protosession.FieldSessionTokenV2BodySubjects: NewMessageField("subject", FieldTypeNestedMessage),
			protosession.FieldSessionTokenV2BodyLifetime: NewMessageField("lifetime", FieldTypeNestedMessage),
			protosession.FieldSessionTokenV2BodyContexts: NewMessageField("contexts", FieldTypeNestedMessage),
			protosession.FieldSessionTokenV2BodyFinal:    NewMessageField("final", FieldTypeBool),
		},
		NestedMessageFields: map[protowire.Number]MessageScheme{
			protosession.FieldSessionTokenV2BodyIssuer:   UserIDScheme,
			protosession.FieldSessionTokenV2BodySubjects: sessionSubjectScheme,
			protosession.FieldSessionTokenV2BodyLifetime: TokenLifetimeScheme,
			protosession.FieldSessionTokenV2BodyContexts: sessionContextScheme,
		},
	}
	SessionTokenScheme = MessageScheme{
		Fields: map[protowire.Number]MessageField{
			protosession.FieldSessionTokenV2Body:      NewMessageField("body", FieldTypeNestedMessage),
			protosession.FieldSessionTokenV2Signature: NewMessageField("signature", FieldTypeNestedMessage),
			protosession.FieldSessionTokenV2Origin:    NewMessageField("origin", FieldTypeNestedMessage),
		},
		NestedMessageFields: map[protowire.Number]MessageScheme{
			protosession.FieldSessionTokenV2Body:      sessionTokenBodyScheme,
			protosession.FieldSessionTokenV2Signature: SignatureScheme,
		},
		RecursiveField: protosession.FieldSessionTokenV2Origin,
	}
)

// Session V1.
var (
	sessionV1ObjectTargetScheme = MessageScheme{
		Fields: map[protowire.Number]MessageField{
			protosession.FieldObjectSessionContextTargetContainer: NewMessageField("container", FieldTypeNestedMessage),
			protosession.FieldObjectSessionContextTargetObjects:   NewMessageField("objects", FieldTypeNestedMessage),
		},
		NestedMessageFields: map[protowire.Number]MessageScheme{
			protosession.FieldObjectSessionContextTargetContainer: ContainerIDScheme,
			protosession.FieldObjectSessionContextTargetObjects:   ObjectIDScheme,
		},
	}
	sessionV1ObjectContextScheme = MessageScheme{
		Fields: map[protowire.Number]MessageField{
			protosession.FieldObjectSessionContextVerb:   NewMessageField("verb", FieldTypeEnum),
			protosession.FieldObjectSessionContextTarget: NewMessageField("target", FieldTypeNestedMessage),
		},
		NestedMessageFields: map[protowire.Number]MessageScheme{
			protosession.FieldObjectSessionContextTargetObjects: sessionV1ObjectTargetScheme,
		},
	}
	sessionV1ContainerContextScheme = MessageScheme{
		Fields: map[protowire.Number]MessageField{
			protosession.FieldContainerSessionContextVerb:        NewMessageField("verb", FieldTypeEnum),
			protosession.FieldContainerSessionContextWildcard:    NewMessageField("wildcard", FieldTypeBool),
			protosession.FieldContainerSessionContextContainerID: NewMessageField("container", FieldTypeNestedMessage),
		},
		NestedMessageFields: map[protowire.Number]MessageScheme{
			protosession.FieldContainerSessionContextContainerID: ContainerIDScheme,
		},
	}
	sessionV1TokenBodyScheme = MessageScheme{
		Fields: map[protowire.Number]MessageField{
			protosession.FieldSessionTokenBodyID:         NewMessageField("id", FieldTypeBytes),
			protosession.FieldSessionTokenBodyOwnerID:    NewMessageField("owner", FieldTypeNestedMessage),
			protosession.FieldSessionTokenBodyLifetime:   NewMessageField("lifetime", FieldTypeNestedMessage),
			protosession.FieldSessionTokenBodySessionKey: NewMessageField("session key", FieldTypeBytes),
			protosession.FieldSessionTokenBodyObject:     NewMessageField("object context", FieldTypeNestedMessage),
			protosession.FieldSessionTokenBodyContainer:  NewMessageField("container context", FieldTypeNestedMessage),
		},
		BinaryFields: map[protowire.Number]BinaryFieldKind{
			protosession.FieldSessionTokenBodyID: BinaryFieldKindUUIDV4,
		},
		NestedMessageFields: map[protowire.Number]MessageScheme{
			protosession.FieldSessionTokenBodyOwnerID:   UserIDScheme,
			protosession.FieldSessionTokenBodyLifetime:  TokenLifetimeScheme,
			protosession.FieldSessionTokenBodyObject:    sessionV1ObjectContextScheme,
			protosession.FieldSessionTokenBodyContainer: sessionV1ContainerContextScheme,
		},
	}
	SessionV1TokenScheme = MessageScheme{
		Fields: map[protowire.Number]MessageField{
			protosession.FieldSessionTokenBody:      NewMessageField("body", FieldTypeNestedMessage),
			protosession.FieldSessionTokenSignature: NewMessageField("signature", FieldTypeNestedMessage),
		},
		NestedMessageFields: map[protowire.Number]MessageScheme{
			protosession.FieldSessionTokenBody:      sessionV1TokenBodyScheme,
			protosession.FieldSessionTokenSignature: SignatureScheme,
		},
	}
)

// Object.
var (
	ObjectSplitHeaderScheme = MessageScheme{
		Fields: map[protowire.Number]MessageField{
			protoobject.FieldHeaderSplitParent:          NewMessageField("parent", FieldTypeNestedMessage),
			protoobject.FieldHeaderSplitPrevious:        NewMessageField("previous", FieldTypeNestedMessage),
			protoobject.FieldHeaderSplitParentSignature: NewMessageField("parent signature", FieldTypeNestedMessage),
			protoobject.FieldHeaderSplitParentHeader:    NewMessageField("parent header", FieldTypeNestedMessage),
			protoobject.FieldHeaderSplitChildren:        NewMessageField("children", FieldTypeNestedMessage),
			protoobject.FieldHeaderSplitSplitID:         NewMessageField("split ID", FieldTypeBytes),
			protoobject.FieldHeaderSplitFirst:           NewMessageField("first", FieldTypeNestedMessage),
		},
		BinaryFields: map[protowire.Number]BinaryFieldKind{
			protoobject.FieldHeaderSplitSplitID: BinaryFieldKindUUIDV4,
		},
		NestedMessageFields: map[protowire.Number]MessageScheme{
			protoobject.FieldHeaderSplitParent:          ObjectIDScheme,
			protoobject.FieldHeaderSplitPrevious:        ObjectIDScheme,
			protoobject.FieldHeaderSplitParentSignature: SignatureScheme,
			protoobject.FieldHeaderSplitChildren:        ObjectIDScheme,
			protoobject.FieldHeaderSplitFirst:           ObjectIDScheme,
		},
		NestedMessageAliases: map[protowire.Number]SchemeAlias{
			protoobject.FieldHeaderSplitParentHeader: SchemeAliasObjectHeader,
		},
	}
	ObjectHeaderScheme = MessageScheme{
		Fields: map[protowire.Number]MessageField{
			protoobject.FieldHeaderVersion:         NewMessageField("version", FieldTypeNestedMessage),
			protoobject.FieldHeaderContainerID:     NewMessageField("container ID", FieldTypeNestedMessage),
			protoobject.FieldHeaderOwnerID:         NewMessageField("owner ID", FieldTypeNestedMessage),
			protoobject.FieldHeaderCreationEpoch:   NewMessageField("creation epoch", FieldTypeUint64),
			protoobject.FieldHeaderPayloadLength:   NewMessageField("payload length", FieldTypeUint64),
			protoobject.FieldHeaderPayloadHash:     NewMessageField("payload hash", FieldTypeNestedMessage),
			protoobject.FieldHeaderObjectType:      NewMessageField("type", FieldTypeEnum),
			protoobject.FieldHeaderHomomorphicHash: NewMessageField("homomorphic hash", FieldTypeNestedMessage),
			protoobject.FieldHeaderSessionToken:    NewMessageField("session V1 token", FieldTypeNestedMessage),
			protoobject.FieldHeaderAttributes:      NewMessageField("attribute", FieldTypeNestedMessage),
			protoobject.FieldHeaderSplit:           NewMessageField("split", FieldTypeNestedMessage),
			protoobject.FieldHeaderSessionV2:       NewMessageField("session token", FieldTypeNestedMessage),
		},
		NestedMessageFields: map[protowire.Number]MessageScheme{
			protoobject.FieldHeaderVersion:         VersionScheme,
			protoobject.FieldHeaderContainerID:     ContainerIDScheme,
			protoobject.FieldHeaderOwnerID:         UserIDScheme,
			protoobject.FieldHeaderPayloadHash:     ChecksumScheme,
			protoobject.FieldHeaderHomomorphicHash: ChecksumScheme,
			protoobject.FieldHeaderAttributes:      AttributeScheme,
			protoobject.FieldHeaderSessionToken:    SessionV1TokenScheme,
			protoobject.FieldHeaderSplit:           ObjectSplitHeaderScheme,
			protoobject.FieldHeaderSessionV2:       SessionTokenScheme,
		},
	}
	ObjectScheme = MessageScheme{
		Fields: map[protowire.Number]MessageField{
			protoobject.FieldObjectID:        NewMessageField("ID", FieldTypeNestedMessage),
			protoobject.FieldObjectSignature: NewMessageField("signature", FieldTypeNestedMessage),
			protoobject.FieldObjectHeader:    NewMessageField("header", FieldTypeNestedMessage),
			protoobject.FieldObjectPayload:   NewMessageField("payload", FieldTypeBytes),
		},
		NestedMessageFields: map[protowire.Number]MessageScheme{
			protoobject.FieldObjectID:        ObjectIDScheme,
			protoobject.FieldObjectSignature: SignatureScheme,
			protoobject.FieldObjectHeader:    ObjectHeaderScheme,
		},
	}
)
