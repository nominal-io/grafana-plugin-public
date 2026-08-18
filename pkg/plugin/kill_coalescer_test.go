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
func flushNow(enqueue func(kc *killCoalescer)) *killRecorder {
	rec := &killRecorder{}
	kc := newKillCoalescer(rec.flush, time.Hour)
	enqueue(kc)
	kc.flush()
	return rec
}

// The interval alone flushes, with no dispose to force it. One id keeps the
// assertion independent of how enqueues interleave with the interval;
// multi-id coalescing is covered by the dispose-driven tests below.
func TestKillCoalescerFlushesOnInterval(t *testing.T) {
	rec := &killRecorder{}
	kc := newKillCoalescer(rec.flush, 5*time.Millisecond)

	id := uuid.NewUUID()
	kc.enqueue(id, killTarget{token: bearertoken.Token("t1")})

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

	rec := flushNow(func(kc *killCoalescer) {
		for _, id := range ids {
			kc.enqueue(id, killTarget{token: bearertoken.Token("t1")})
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
	rec := flushNow(func(kc *killCoalescer) {
		for i := 0; i < killBufferLimit+10; i++ {
			kc.enqueue(uuid.NewUUID(), killTarget{token: bearertoken.Token("t1")})
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

	rec := flushNow(func(kc *killCoalescer) {
		kc.enqueue(id1, target1)
		kc.enqueue(id2, target2)
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

func TestKillCoalescerFlushAsyncDoesNotWaitForFlush(t *testing.T) {
	flushStarted := make(chan struct{})
	unblockFlush := make(chan struct{})
	flushDone := make(chan struct{})
	kc := newKillCoalescer(func(_ context.Context, _ killTarget, _ []uuid.UUID) {
		close(flushStarted)
		<-unblockFlush
		close(flushDone)
	}, time.Hour)
	kc.enqueue(uuid.NewUUID(), killTarget{token: bearertoken.Token("t")})

	returned := make(chan struct{})
	go func() {
		kc.flushAsync()
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		close(unblockFlush)
		t.Fatal("flushAsync waited for the flush")
	}
	select {
	case <-flushStarted:
	case <-time.After(2 * time.Second):
		close(unblockFlush)
		t.Fatal("expected the async flush to start")
	}
	close(unblockFlush)
	select {
	case <-flushDone:
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not finish after being unblocked")
	}
}

func TestKillFlushGivesEachCallAFreshDeadline(t *testing.T) {
	var mu sync.Mutex
	var deadlines []time.Time
	kc := newKillCoalescer(func(ctx context.Context, _ killTarget, _ []uuid.UUID) {
		d, ok := ctx.Deadline()
		if !ok {
			t.Error("flush ctx has no deadline")
		}
		mu.Lock()
		deadlines = append(deadlines, d)
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}, time.Hour)

	for i := 0; i < killChunkSize+1; i++ {
		kc.enqueue(uuid.NewUUID(), killTarget{token: bearertoken.Token("t1")})
	}
	kc.flush()

	if len(deadlines) != 2 {
		t.Fatalf("expected 2 chunked calls, got %d", len(deadlines))
	}
	if !deadlines[1].After(deadlines[0]) {
		t.Fatalf("expected a fresh deadline per call, got %v then %v", deadlines[0], deadlines[1])
	}
}
