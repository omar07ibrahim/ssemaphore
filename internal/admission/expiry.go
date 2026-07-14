package admission

import "container/heap"

type expiryQueue []*entry

func (q expiryQueue) Len() int { return len(q) }

func (q expiryQueue) Less(left, right int) bool {
	if q[left].deadline.Equal(q[right].deadline) {
		return q[left].sequence < q[right].sequence
	}
	return q[left].deadline.Before(q[right].deadline)
}

func (q expiryQueue) Swap(left, right int) {
	q[left], q[right] = q[right], q[left]
	q[left].heapIndex = left
	q[right].heapIndex = right
}

func (q *expiryQueue) Push(value any) {
	item := value.(*entry)
	item.heapIndex = len(*q)
	*q = append(*q, item)
}

func (q *expiryQueue) Pop() any {
	old := *q
	last := len(old) - 1
	item := old[last]
	old[last] = nil
	item.heapIndex = -1
	*q = old[:last]
	return item
}

func (q *expiryQueue) remove(item *entry) {
	if item.heapIndex >= 0 {
		heap.Remove(q, item.heapIndex)
	}
}
