package plugin

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/uuid"
)

type killBatch struct {
	target killTarget
	ids    []uuid.UUID
}

type killRecorder struct {
	mu      sync.Mutex
	batches []killBatch
}

func (r *killRecorder) flush(_ context.Context, target killTarget, ids []uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Keep each call independent of the coalescer's backing array.
	r.batches = append(r.batches, killBatch{target: target, ids: append([]uuid.UUID(nil), ids...)})
}

func (r *killRecorder) snapshot() []killBatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]killBatch(nil), r.batches...)
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

// flushNow enqueues against a coalescer whose interval never elapses, then
// flushes synchronously, so assertions see exactly one deterministic flush.
func flushNow(enqueue func(kc *killCoalescer, flush killFlushFunc)) *killRecorder {
	rec := &killRecorder{}
	kc := &killCoalescer{interval: time.Hour}
	enqueue(kc, rec.flush)
	kc.flush(rec.flush)
	return rec
}

// The interval alone flushes, with nothing forcing it. One id keeps the
// assertion independent of how enqueues interleave with the interval;
// multi-id coalescing is covered by the synchronous flushNow tests below.
func TestKillCoalescerFlushesOnInterval(t *testing.T) {
	rec := &killRecorder{}
	kc := &killCoalescer{interval: 5 * time.Millisecond}

	id := uuid.NewUUID()
	kc.enqueue(rec.flush, id, killTarget{token: bearertoken.Token("t1")})

	waitForCondition(t, 2*time.Second, func() bool { return len(rec.snapshot()) == 1 })

	batches := rec.snapshot()
	if want := []uuid.UUID{id}; !slices.Equal(batches[0].ids, want) {
		t.Fatalf("expected ids %v, got %v", want, batches[0].ids)
	}
}

func TestKillCoalescerChunksAtLimit(t *testing.T) {
	ids := make([]uuid.UUID, killChunkSize+1)
	for i := range ids {
		ids[i] = uuid.NewUUID()
	}

	rec := flushNow(func(kc *killCoalescer, flush killFlushFunc) {
		for _, id := range ids {
			kc.enqueue(flush, id, killTarget{token: bearertoken.Token("t1")})
		}
	})

	batches := rec.snapshot()
	if len(batches) != 2 {
		t.Fatalf("expected 2 chunked kill calls, got %d", len(batches))
	}
	if !slices.Equal(batches[0].ids, ids[:killChunkSize]) {
		t.Fatalf("expected first chunk to be the first %d ids in order, got %v", killChunkSize, batches[0].ids)
	}
	if !slices.Equal(batches[1].ids, ids[killChunkSize:]) {
		t.Fatalf("expected second chunk to be the remaining id, got %v", batches[1].ids)
	}
}

func TestKillCoalescerDropsOnOverflow(t *testing.T) {
	rec := flushNow(func(kc *killCoalescer, flush killFlushFunc) {
		for i := 0; i < killBufferLimit+10; i++ {
			kc.enqueue(flush, uuid.NewUUID(), killTarget{token: bearertoken.Token("t1")})
		}
	})

	total := 0
	for _, b := range rec.snapshot() {
		total += len(b.ids)
	}
	if total != killBufferLimit {
		t.Fatalf("expected exactly %d ids flushed (overflow dropped), got %d", killBufferLimit, total)
	}
}

func TestKillCoalescerGroupsByTarget(t *testing.T) {
	id1, id2 := uuid.NewUUID(), uuid.NewUUID()
	target1 := killTarget{token: bearertoken.Token("t"), ua: userAgentComponents{PluginVersion: "1.0.0"}}
	target2 := killTarget{token: bearertoken.Token("t"), ua: userAgentComponents{PluginVersion: "2.0.0"}}

	rec := flushNow(func(kc *killCoalescer, flush killFlushFunc) {
		kc.enqueue(flush, id1, target1)
		kc.enqueue(flush, id2, target2)
	})

	batches := rec.snapshot()
	if len(batches) != 2 {
		t.Fatalf("expected one flush call per target, got %d", len(batches))
	}

	// Flush order across targets is map-iteration order, so key on target before comparing.
	byTarget := make(map[killTarget][]uuid.UUID, len(batches))
	for _, b := range batches {
		byTarget[b.target] = b.ids
	}
	if want := []uuid.UUID{id1}; !slices.Equal(byTarget[target1], want) {
		t.Fatalf("expected target %#v to carry ids %v, got %v", target1, want, byTarget[target1])
	}
	if want := []uuid.UUID{id2}; !slices.Equal(byTarget[target2], want) {
		t.Fatalf("expected target %#v to carry ids %v, got %v", target2, want, byTarget[target2])
	}
}

func TestKillFlushGivesEachCallAFreshDeadline(t *testing.T) {
	// flush runs synchronously on the test goroutine, so no locking is needed.
	var deadlines []time.Time
	flush := func(ctx context.Context, _ killTarget, _ []uuid.UUID) {
		d, ok := ctx.Deadline()
		if !ok {
			t.Error("flush ctx has no deadline")
		}
		deadlines = append(deadlines, d)
		time.Sleep(20 * time.Millisecond)
	}
	kc := &killCoalescer{interval: time.Hour}

	for i := 0; i < killChunkSize+1; i++ {
		kc.enqueue(flush, uuid.NewUUID(), killTarget{token: bearertoken.Token("t1")})
	}
	kc.flush(flush)

	if len(deadlines) != 2 {
		t.Fatalf("expected 2 chunked calls, got %d", len(deadlines))
	}
	if !deadlines[1].After(deadlines[0]) {
		t.Fatalf("expected a fresh deadline per call, got %v then %v", deadlines[0], deadlines[1])
	}
}

// A flush that empties the buffer must leave the next enqueue able to arm a
// flush of its own. The first interval elapses before the second enqueue, so
// only a fresh arming can deliver it.
func TestKillCoalescerRearmsAfterDrain(t *testing.T) {
	rec := &killRecorder{}
	kc := &killCoalescer{interval: 5 * time.Millisecond}
	target := killTarget{token: bearertoken.Token("t1")}

	kc.enqueue(rec.flush, uuid.NewUUID(), target)
	waitForCondition(t, 2*time.Second, func() bool { return len(rec.snapshot()) == 1 })

	second := uuid.NewUUID()
	kc.enqueue(rec.flush, second, target)
	waitForCondition(t, 2*time.Second, func() bool { return len(rec.snapshot()) == 2 })

	batches := rec.snapshot()
	if want := []uuid.UUID{second}; !slices.Equal(batches[1].ids, want) {
		t.Fatalf("expected the second flush to carry ids %v, got %v", want, batches[1].ids)
	}
}

// Every enqueued id reaches the sender exactly once, however enqueues, interval
// flushes, and explicit flushes interleave. The total stays under
// killBufferLimit so no enqueue can be dropped and the expected set is exact.
// Nothing here depends on how ids were batched or on which flush carried them,
// only on which came out.
func TestKillCoalescerDeliversEveryIDUnderConcurrency(t *testing.T) {
	const (
		writers   = 8
		perWriter = 100
		total     = writers * perWriter
	)

	for _, tc := range []struct {
		name    string
		disrupt bool
	}{
		// Delivery rests on the interval alone, so a coalescer that stops arming
		// after an earlier flush strands everything enqueued after it.
		{name: "interval only"},
		// Forced flushes race the interval, so taking the buffer has to stay
		// atomic with appending to it.
		{name: "racing flushes", disrupt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &killRecorder{}
			kc := &killCoalescer{interval: time.Millisecond}

			// Two targets so grouping runs on every flush without changing the id set.
			targets := []killTarget{
				{token: bearertoken.Token("t1")},
				{token: bearertoken.Token("t2")},
			}
			ids := make([][]uuid.UUID, writers)
			for w := range ids {
				ids[w] = make([]uuid.UUID, perWriter)
				for i := range ids[w] {
					ids[w][i] = uuid.NewUUID()
				}
			}

			var wg sync.WaitGroup
			for w := range writers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					target := targets[w%len(targets)]
					for i, id := range ids[w] {
						kc.enqueue(rec.flush, id, target)
						if !tc.disrupt {
							continue
						}
						// Coprime strides keep the forced flushes from settling into
						// lockstep with each other or with the interval.
						if w%2 == 0 && i%7 == 0 {
							go kc.flush(rec.flush)
						} else if i%11 == 0 {
							kc.flush(rec.flush)
						}
					}
				}()
			}
			wg.Wait()

			delivered := func() []uuid.UUID {
				var out []uuid.UUID
				for _, b := range rec.snapshot() {
					out = append(out, b.ids...)
				}
				return out
			}
			waitForCondition(t, 2*time.Second, func() bool { return len(delivered()) >= total })
			// Anything still buffered comes out here, so a shortfall cannot hide
			// behind a duplicate that padded the count.
			kc.flush(rec.flush)

			counts := make(map[uuid.UUID]int, total)
			for _, id := range delivered() {
				counts[id]++
			}
			for _, batch := range ids {
				for _, id := range batch {
					switch n := counts[id]; n {
					case 1:
						delete(counts, id)
					case 0:
						t.Fatalf("id %v was enqueued but never flushed", id)
					default:
						t.Fatalf("id %v was flushed %d times", id, n)
					}
				}
			}
			if len(counts) != 0 {
				t.Fatalf("flush sent %d ids that were never enqueued", len(counts))
			}
		})
	}
}
