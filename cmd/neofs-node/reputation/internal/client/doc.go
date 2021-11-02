// Package internal provides functionality for NeoFS Node Reputation system communication with NeoFS network.
// The base client for accessing remote nodes via NeoFS API is a NeoFS SDK Go API client.
// However, although it encapsulates a useful piece of business logic (e.g. the signature mechanism),
// the Reputation service does not fully use the client's flexible interface.
//
// In this regard, this package provides functions over base API client necessary for the application.
// This allows you to concentrate the entire spectrum of the client's use in one place  (this will be convenient
// both when updating the base client and for evaluating the UX of SDK library). So it is expected that all
// Reputation service packages will be limited to this package for the development of  functionality requiring
// NeoFS API communication.
package internal
