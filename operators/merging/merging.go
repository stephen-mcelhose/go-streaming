// Package merging provides combinators for merging, zipping, and merge-sorting multiple streams.
package merging

import (
	"container/heap"
	"context"
	"sync"

	"github.com/stephen-mcelhose/go-streaming"
)

// Merge combines multiple Receiver[T] inputs concurrently into a single Sender[T].
// If any receiver returns an error, the operation cancels remaining receivers and returns the error.
func Merge[T any](
	ctx context.Context,
	sender streaming.Sender[T],
	receivers ...streaming.Receiver[T],
) error {
	if len(receivers) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(receivers))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, r := range receivers {
		wg.Add(1)
		go func(rx streaming.Receiver[T]) {
			defer wg.Done()
			err := rx.Receive(ctx, func(ctx context.Context, item T) error {
				return sender.Send(ctx, item)
			})
			if err != nil && err != context.Canceled {
				select {
				case errCh <- err:
					cancel()
				default:
				}
			}
		}(r)
	}

	wg.Wait()
	close(errCh)

	if len(errCh) > 0 {
		return <-errCh
	}
	return nil
}

// Zip pairs items 1-to-1 from two receivers and emits the combined result to out.
// Terminates as soon as either receiver exhausts its input.
func Zip[A any, B any, O any](
	ctx context.Context,
	zipper func(ctx context.Context, a A, b B) (O, error),
	aRx streaming.Receiver[A],
	bRx streaming.Receiver[B],
	out streaming.Sender[O],
) error {
	aCh := make(chan A)
	bCh := make(chan B)
	errCh := make(chan error, 2)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		defer close(aCh)
		err := aRx.Receive(ctx, func(ctx context.Context, item A) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case aCh <- item:
				return nil
			}
		})
		if err != nil && err != context.Canceled {
			select {
			case errCh <- err:
				cancel()
			default:
			}
		}
	}()

	go func() {
		defer close(bCh)
		err := bRx.Receive(ctx, func(ctx context.Context, item B) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case bCh <- item:
				return nil
			}
		})
		if err != nil && err != context.Canceled {
			select {
			case errCh <- err:
				cancel()
			default:
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case a, okA := <-aCh:
			if !okA {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case b, okB := <-bCh:
				if !okB {
					return nil
				}
				combined, err := zipper(ctx, a, b)
				if err != nil {
					return err
				}
				if err := out.Send(ctx, combined); err != nil {
					return err
				}
			}
		}
	}
}

type headNode[T any] struct {
	item  T
	index int
	ch    <-chan T
}

type minHeap[T any] struct {
	nodes []*headNode[T]
	cmp   func(a, b T) int
}

func (h minHeap[T]) Len() int           { return len(h.nodes) }
func (h minHeap[T]) Less(i, j int) bool { return h.cmp(h.nodes[i].item, h.nodes[j].item) < 0 }
func (h minHeap[T]) Swap(i, j int)      { h.nodes[i], h.nodes[j] = h.nodes[j], h.nodes[i] }
func (h *minHeap[T]) Push(x any)        { h.nodes = append(h.nodes, x.(*headNode[T])) }
func (h *minHeap[T]) Pop() any {
	old := h.nodes
	n := len(old)
	x := old[n-1]
	h.nodes = old[0 : n-1]
	return x
}

// MergeSort merges pre-sorted partitions into a single sorted stream using cmp comparator.
// The comparator cmp(a, b) should return a negative number if a < b, 0 if a == b, and positive if a > b.
func MergeSort[T any](
	ctx context.Context,
	cmp func(a, b T) int,
	sender streaming.Sender[T],
	partitions ...streaming.Receiver[T],
) error {
	if len(partitions) == 0 {
		return nil
	}

	channels := make([]chan T, len(partitions))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i, part := range partitions {
		ch := make(chan T)
		channels[i] = ch
		go func(p streaming.Receiver[T], c chan T) {
			defer close(c)
			_ = p.Receive(ctx, func(ctx context.Context, item T) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case c <- item:
					return nil
				}
			})
		}(part, ch)
	}

	h := &minHeap[T]{cmp: cmp}
	heap.Init(h)

	for i, ch := range channels {
		item, ok := <-ch
		if ok {
			heap.Push(h, &headNode[T]{item: item, index: i, ch: ch})
		}
	}

	for h.Len() > 0 {
		smallest := heap.Pop(h).(*headNode[T])
		if err := sender.Send(ctx, smallest.item); err != nil {
			return err
		}

		nextItem, ok := <-smallest.ch
		if ok {
			heap.Push(h, &headNode[T]{item: nextItem, index: smallest.index, ch: smallest.ch})
		}
	}

	return nil
}
