package main

import "container/heap"

// GameTribeOutput is the JSON structure for the toptribes list
type GameTribeOutput struct {
	TribeID   uint64 `json:"tribeID"`
	TribeName string `json:"tribeName"`
	Index     int    `json:"index"`
}

// TribeCount holds the per tribe number of markers
type TribeCount struct {
	tribeID uint64
	count   uint32
}

// TribeCountHeap heap wrapper
type TribeCountHeap []*TribeCount

func (h TribeCountHeap) Len() int { return len(h) }
func (h TribeCountHeap) Less(i, j int) bool {
	if h[i].count < h[j].count {
		return true
	} else if h[i].count == h[j].count {
		return h[i].tribeID < h[j].tribeID
	} else {
		return false
	}
}
func (h TribeCountHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *TribeCountHeap) Push(x interface{}) { *h = append(*h, x.(*TribeCount)) }
func (h *TribeCountHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func TopNTribes(n int, counts map[uint64]*TribeCount) []uint64 {
	pq := make(TribeCountHeap, 0)
	i := 0
	for _, v := range counts {
		// prime heap with n-items
		if i < n {
			pq = append(pq, v)
			i++
			continue
		} else if i == n {
			heap.Init(&pq)
		}

		// add n+1 items and fix up heap
		heap.Push(&pq, v)
		if pq.Len() > n {
			heap.Pop(&pq)
		}
		i++
	}
	if i <= n {
		heap.Init(&pq)
	}
	results := make([]uint64, 0)
	for i := Min(n, len(pq)) - 1; i >= 0; i-- {
		results = append(results, pq[i].tribeID)
	}
	return results
}
