package report

import (
	"context"
	"log"
	"sync/atomic"
	"time"
)

type Count struct {
	PendingTx     uint64
	TxFeched      uint64
	MempoolStored uint64
}

var M = &Count{}

func IncPendginRecieved() {
	atomic.AddUint64(&M.PendingTx, 1)
}

func IncTxFetched(cnt uint64) {
	atomic.AddUint64(&M.TxFeched, cnt)
}

func IncMempoolStored() {
	atomic.AddUint64(&M.MempoolStored, 1)
}

func StartReporter(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				pendingTx := atomic.SwapUint64(&M.PendingTx, 0)
				txFeched := atomic.SwapUint64(&M.TxFeched, 0)
				mempoolStored := atomic.SwapUint64(&M.MempoolStored, 0)

				log.Printf(
					"[TPS] pending=%d/sec fetched=%d/sec stored=%d/sec", pendingTx, txFeched, mempoolStored,
				)
			}
		}
	}()
}
