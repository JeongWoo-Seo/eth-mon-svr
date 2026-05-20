package mempool

type minTipHeap []PendingTxInfo

func (h minTipHeap) Len() int      { return len(h) }
func (h minTipHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h minTipHeap) Less(i, j int) bool {
	tipI := h[i].GasTipCap
	tipJ := h[j].GasTipCap

	if tipI == nil {
		return true
	}
	if tipJ == nil {
		return false
	}

	return tipI.Cmp(tipJ) < 0
}

// "container/heap"을 사용하기 위해 push와 pop은 interface로 처리해야함
func (h *minTipHeap) Push(tx interface{}) {
	*h = append(*h, tx.(PendingTxInfo))
}

func (h *minTipHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]

	return x
}

func (h minTipHeap) Peak() PendingTxInfo {
	if len(h) == 0 {
		return PendingTxInfo{}
	}

	return h[0]
}
