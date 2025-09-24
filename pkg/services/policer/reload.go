package policer

// Reload allows runtime reconfiguration for the following parameters:
// - [WithHeadTimeout];
// - [WithObjectCacheTime];
// - [WithReplicationCooldown];
// - [WithMaxCapacity];
// - [WithObjectBatchSize];
// - [WithObjectCacheSize].
func (p *Policer) Reload(opts ...Option) {
	cfg := new(cfg)
	for _, o := range opts {
		o(cfg)
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.headTimeout = cfg.headTimeout
	p.repCooldown = cfg.repCooldown
	p.maxCapacity = cfg.maxCapacity
	p.batchSize = cfg.batchSize
}
