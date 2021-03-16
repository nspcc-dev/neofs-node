package metrics

const namespace = "neofs_node"

type StorageMetrics struct {
	objectServiceMetrics
	engineMetrics
}

func NewStorageMetrics() *StorageMetrics {
	objectService := newObjectServiceMetrics()
	objectService.register()

	engine := newEngineMetrics()
	engine.register()

	return &StorageMetrics{
		objectServiceMetrics: objectService,
		engineMetrics:        engine,
	}
}
