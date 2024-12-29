package load

import (
	"reflect"

	"github.com/timescale/tsbs/pkg/targets"
)

func ackAndMaybeSend(ch *duplexChannel, count *int, unsent []targets.Batch) []targets.Batch {
	*count--
	if len(unsent) > 0 {
		ch.sendToWorker(unsent[0])
		if len(unsent) > 1 {
			return unsent[1:]
		}
		return unsent[:0]
	}
	return unsent
}

func sendOrQueueBatch(ch *duplexChannel, count *int, batch targets.Batch, unsent []targets.Batch) []targets.Batch {
	*count++
	if len(unsent) == 0 && len(ch.toWorker) < cap(ch.toWorker) {
		ch.sendToWorker(batch)
	} else {
		return append(unsent, batch)
	}
	return unsent
}

func scanWithFlowControl(
	channels []*duplexChannel, batchSize uint, limit uint64,
	ds targets.DataSource, factory targets.BatchFactory, indexer targets.PointIndexer,
) uint64 {
	var itemsRead uint64
	numChannels := len(channels)

	if batchSize < 1 {
		panic("--batch-size cannot be less than 1")
	}

	fillingBatches := make([]targets.Batch, numChannels)
	for i := range fillingBatches {
		fillingBatches[i] = factory.New()
	}

	unsentBatches := make([][]targets.Batch, numChannels)
	for i := range unsentBatches {
		unsentBatches[i] = []targets.Batch{}
	}

	cases := make([]reflect.SelectCase, numChannels+1)
	for i, ch := range channels {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch.toScanner),
		}
	}
	cases[numChannels] = reflect.SelectCase{
		Dir: reflect.SelectDefault,
	}

	ocnt := 0
	olimit := numChannels * cap(channels[0].toWorker) * 3
	for {
		if limit > 0 && itemsRead == limit {
			break
		}

		caseLimit := len(cases)
		if ocnt >= olimit {
			caseLimit--
		}

		chosen, _, ok := reflect.Select(cases[:caseLimit])
		if ok {
			unsentBatches[chosen] = ackAndMaybeSend(channels[chosen], &ocnt, unsentBatches[chosen])
		}

		item := ds.NextItem()
		if item.Data == nil {
			break
		}
		itemsRead++

		idx := indexer.GetIndex(item)
		fillingBatches[idx].Append(item)

		if fillingBatches[idx].Len() >= batchSize {
			unsentBatches[idx] = sendOrQueueBatch(channels[idx], &ocnt, fillingBatches[idx], unsentBatches[idx])
			fillingBatches[idx] = factory.New()
		}
	}

	for idx, b := range fillingBatches {
		if b.Len() > 0 {
			unsentBatches[idx] = sendOrQueueBatch(channels[idx], &ocnt, fillingBatches[idx], unsentBatches[idx])
		}
	}

	for {
		if ocnt == 0 {
			break
		}

		chosen, _, ok := reflect.Select(cases[:len(cases)-1])
		if ok {
			unsentBatches[chosen] = ackAndMaybeSend(channels[chosen], &ocnt, unsentBatches[chosen])
		}
	}

	return itemsRead
}
