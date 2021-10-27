// Package neofsapiclient provides functionality for IR application communication with NeoFS network.
//
// The basic client for accessing remote nodes via NeoFS API is a NeoFS SDK Go API client.
// However, although it encapsulates a useful piece of business logic (e.g. the signature mechanism),
// the IR application does not fully use the client's flexible interface.
//
// In this regard, this package represents an abstraction - a type-wrapper over the base client.
// The type provides the minimum interface necessary for the application, and also allows you to concentrate
// the entire spectrum of the client's use in one place (this will be convenient both when updating the base client
// and for evaluating the UX of SDK library). So it is expected that all application packages will be limited
// to this package for the development of functionality requiring NeoFS API communication.
package neofsapiclient
