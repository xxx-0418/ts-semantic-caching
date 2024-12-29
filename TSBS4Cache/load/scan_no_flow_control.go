package load

import (
	"github.com/timescale/tsbs/pkg/targets"
)

func scanWithoutFlowControl(
	ds targets.DataSource, indexer targets.PointIndexer, factory targets.BatchFactory, channels []chan targets.Batch,
	batchSize uint, limit uint64,
) uint64 {
	if batchSize == 0 {
		panic("batch size can't be 0")
	}
	numChannels := len(channels)
	batches := make([]targets.Batch, numChannels)
	for i := 0; i < numChannels; i++ {
		batches[i] = factory.New()
	}
	var itemsRead uint64
	for {
		if limit > 0 && itemsRead >= limit {
			break
		}
		item := ds.NextItem()
		if item.Data == nil {
			break
		}
		itemsRead++

		idx := indexer.GetIndex(item)
		batches[idx].Append(item)

		if batches[idx].Len() >= batchSize {
			channels[idx] <- batches[idx]
			batches[idx] = factory.New()
		}
	}

	for idx, unfilledBatch := range batches {
		if unfilledBatch.Len() > 0 {
			channels[idx] <- unfilledBatch
		}
	}
	return itemsRead
}
