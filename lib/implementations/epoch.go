package implementations

// EpochReceiver is an interface of the container
// of NeoFS epoch number with read access.
type EpochReceiver interface {
	Epoch() uint64
}
