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

func testKillTarget(token bearertoken.Token) killTarget {
	return killTarget{token: token}
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

// flushOnDispose enqueues against a coalescer whose ticker never fires, so the
// dispose flush is the only flush.
func flushOnDispose(enqueue func(kc *killCoalescer)) (*killCoalescer, *killRecorder) {
	rec := &killRecorder{}
	kc := newKillCoalescer(rec.flush, time.Hour)
	enqueue(kc)
	kc.dispose()
	select {
	case <-kc.done:
	case <-time.After(2 * time.Second):
		panic("kill coalescer did not finish its final flush")
	}
	return kc, rec
}

func TestKillCoalescerFlushesOnInterval(t *testing.T) {
	rec := &killRecorder{}
	kc := newKillCoalescer(rec.flush, 5*time.Millisecond)
	t.Cleanup(func() {
		kc.dispose()
		<-kc.done
	})

	id1, id2 := uuid.NewUUID(), uuid.NewUUID()
	tok := bearertoken.Token("t1")
	kc.enqueue(id1, testKillTarget(tok))
	kc.enqueue(id2, testKillTarget(tok))

	waitForCondition(t, 2*time.Second, func() bool { return len(rec.snapshot()) >= 1 })

	batches := rec.snapshot()
	if len(batches) != 1 {
		t.Fatalf("expected 1 coalesced flush call, got %d", len(batches))
	}
	if want := []uuid.UUID{id1, id2}; !slices.Equal(batches[0].ids, want) {
		t.Fatalf("expected ids %v, got %v", want, batches[0].ids)
	}
}

func TestKillCoalescerChunksAtLimit(t *testing.T) {
	ids := make([]uuid.UUID, killChunkSize+1)
	for i := range ids {
		ids[i] = uuid.NewUUID()
	}

	_, rec := flushOnDispose(func(kc *killCoalescer) {
		for _, id := range ids {
			kc.enqueue(id, testKillTarget(bearertoken.Token("t1")))
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
	_, rec := flushOnDispose(func(kc *killCoalescer) {
		for i := 0; i < killBufferLimit+10; i++ {
			kc.enqueue(uuid.NewUUID(), testKillTarget(bearertoken.Token("t1")))
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

	_, rec := flushOnDispose(func(kc *killCoalescer) {
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

func TestKillCoalescerDisposeFlushesFinalBufferAndRejectsEnqueues(t *testing.T) {
	tok := bearertoken.Token("t1")
	kc, rec := flushOnDispose(func(kc *killCoalescer) { kc.enqueue(uuid.NewUUID(), testKillTarget(tok)) })

	if len(rec.snapshot()) != 1 {
		t.Fatalf("expected final flush of 1 call, got %d", len(rec.snapshot()))
	}

	kc.enqueue(uuid.NewUUID(), testKillTarget(tok))
	if total := len(rec.snapshot()); total != 1 {
		t.Fatalf("enqueue after dispose must be dropped, got %d calls", total)
	}

	// dispose is idempotent: a second close(kc.stop) would panic.
	kc.dispose()
}

func TestKillCoalescerDisposeDoesNotWaitForFlush(t *testing.T) {
	flushStarted := make(chan struct{})
	unblockFlush := make(chan struct{})
	kc := newKillCoalescer(func(_ context.Context, _ killTarget, _ []uuid.UUID) {
		close(flushStarted)
		<-unblockFlush
	}, time.Hour)
	kc.enqueue(uuid.NewUUID(), testKillTarget(bearertoken.Token("t")))
	disposed := make(chan struct{})
	go func() {
		kc.dispose()
		close(disposed)
	}()

	select {
	case <-disposed:
	case <-time.After(2 * time.Second):
		close(unblockFlush)
		t.Fatal("dispose waited for the final flush")
	}
	select {
	case <-flushStarted:
	case <-time.After(2 * time.Second):
		close(unblockFlush)
		t.Fatal("expected final flush")
	}
	close(unblockFlush)
	select {
	case <-kc.done:
	case <-time.After(2 * time.Second):
		t.Fatal("coalescer did not finish after its flush was unblocked")
	}
}
