package targets

type Processor interface {
	Init(workerNum int, doLoad, hashWorkers bool)
	ProcessBatch(b Batch, doLoad bool) (metricCount, rowCount uint64)
}

type ProcessorCloser interface {
	Processor
	Close(doLoad bool)
}
