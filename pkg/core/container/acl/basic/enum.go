package basic

const (
	// OpGetRangeHash is an index of GetRangeHash operation in basic ACL bitmask order.
	OpGetRangeHash uint8 = iota

	// OpGetRange is an index of GetRange operation in basic ACL bitmask order.
	OpGetRange

	// OpSearch is an index of Search operation in basic ACL bitmask order.
	OpSearch

	// OpDelete is an index of Delete operation in basic ACL bitmask order.
	OpDelete

	// OpPut is an index of Put operation in basic ACL bitmask order.
	OpPut

	// OpHead is an index of Head operation in basic ACL bitmask order.
	OpHead

	// OpGet is an index of Get operation in basic ACL bitmask order.
	OpGet
)
