package settlement

// AuditProcessor is an interface of data audit fee processor.
type AuditProcessor interface {
	// Must process data audit conducted in epoch.
	ProcessAuditSettlements(epoch uint64)
}
