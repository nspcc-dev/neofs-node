package policer

// Reload allows runtime reconfiguration for the following parameters:
// - [WithHeadTimeout];
// - [WithObjectCacheTime];
// - [WithReplicationCooldown];
// - [WithObjectBatchSize];
// - [WithObjectCacheSize];
// - [WithBoostMultiplier].
func (p *Policer) Reload(opts ...Option) {
	cfg := new(cfg)
	for _, o := range opts {
		o(cfg)
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.headTimeout = cfg.headTimeout
	p.repCooldown = cfg.repCooldown
	p.batchSize = cfg.batchSize
	p.boostMultiplier = cfg.boostMultiplier
}
