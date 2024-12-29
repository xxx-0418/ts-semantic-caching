package load

import "github.com/timescale/tsbs/pkg/targets"

type duplexChannel struct {
	toWorker  chan targets.Batch
	toScanner chan bool
}

func newDuplexChannel(queueLen int) *duplexChannel {
	return &duplexChannel{
		toWorker:  make(chan targets.Batch, queueLen),
		toScanner: make(chan bool, queueLen),
	}
}

func (dc *duplexChannel) sendToWorker(b targets.Batch) {
	dc.toWorker <- b
}

func (dc *duplexChannel) sendToScanner() {
	dc.toScanner <- true
}

func (dc *duplexChannel) close() {
	close(dc.toWorker)
	close(dc.toScanner)
}
