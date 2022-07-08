package fstree

// Open implements common.Storage.
func (*FSTree) Open(bool) error { return nil }

// Init implements common.Storage.
func (*FSTree) Init() error { return nil }

// Close implements common.Storage.
func (*FSTree) Close() error { return nil }
