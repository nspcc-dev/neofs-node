package headers

// UserHeader is a value of object extended header
// that carries user string key-value pairs.
//
// All user headers must be type of TypeUser.
// All user header must have UserHeader pointer value.
type UserHeader struct {
	key, val string
}

// NewUserHeader creates, initialized and returns
// the user extended header.
func NewUserHeader(key, val string) *Header {
	res := new(Header)

	res.SetType(TypeUser)

	res.SetValue(&UserHeader{
		key: key,
		val: val,
	})

	return res
}

// Key returns the user header key.
func (u UserHeader) Key() string {
	return u.key
}

// SetKey sets the user header key.
func (u *UserHeader) SetKey(key string) {
	u.key = key
}

// Value returns the user header value.
func (u UserHeader) Value() string {
	return u.val
}

// SetValue sets the user header value.
func (u *UserHeader) SetValue(val string) {
	u.val = val
}
