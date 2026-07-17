package plugin

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/uuid"
)

type killRecorder struct {
	mu    sync.Mutex
	calls [][]uuid.UUID
	toks  []bearertoken.Token
}

func (r *killRecorder) flush(_ context.Context, token bearertoken.Token, ids []uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Keep each call independent of the coalescer's backing array.
	r.calls = append(r.calls, append([]uuid.UUID(nil), ids...))
	r.toks = append(r.toks, token)
}

func (r *killRecorder) snapshot() [][]uuid.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]uuid.UUID(nil), r.calls...)
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

func TestKillCoalescerFlushesOnInterval(t *testing.T) {
	rec := &killRecorder{}
	kc := newKillCoalescer(rec.flush, 5*time.Millisecond)
	defer kc.dispose()

	id1, id2 := uuid.NewUUID(), uuid.NewUUID()
	tok := bearertoken.Token("t1")
	kc.enqueue(id1, tok)
	kc.enqueue(id2, tok)

	waitForCondition(t, 2*time.Second, func() bool { return len(rec.snapshot()) >= 1 })

	calls := rec.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 coalesced flush call, got %d", len(calls))
	}
	if len(calls[0]) != 2 {
		t.Fatalf("expected 2 ids in one call, got %d", len(calls[0]))
	}
}

func TestKillCoalescerChunksAtLimit(t *testing.T) {
	rec := &killRecorder{}
	kc := newKillCoalescer(rec.flush, time.Hour)

	tok := bearertoken.Token("t1")
	for i := 0; i < killChunkSize+1; i++ {
		kc.enqueue(uuid.NewUUID(), tok)
	}
	kc.dispose()

	calls := rec.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected 2 chunked kill calls, got %d", len(calls))
	}
	if len(calls[0]) != killChunkSize || len(calls[1]) != 1 {
		t.Fatalf("expected chunks of %d and 1, got %d and %d", killChunkSize, len(calls[0]), len(calls[1]))
	}
}

func TestKillCoalescerDropsOnOverflow(t *testing.T) {
	rec := &killRecorder{}
	kc := newKillCoalescer(rec.flush, time.Hour)

	tok := bearertoken.Token("t1")
	for i := 0; i < killBufferLimit+10; i++ {
		kc.enqueue(uuid.NewUUID(), tok)
	}
	kc.dispose()

	total := 0
	for _, c := range rec.snapshot() {
		total += len(c)
	}
	if total != killBufferLimit {
		t.Fatalf("expected exactly %d ids flushed (overflow dropped), got %d", killBufferLimit, total)
	}
}

func TestKillCoalescerGroupsByToken(t *testing.T) {
	rec := &killRecorder{}
	kc := newKillCoalescer(rec.flush, time.Hour)

	kc.enqueue(uuid.NewUUID(), bearertoken.Token("t1"))
	kc.enqueue(uuid.NewUUID(), bearertoken.Token("t2"))
	kc.dispose()

	if len(rec.snapshot()) != 2 {
		t.Fatalf("expected one flush call per token, got %d", len(rec.snapshot()))
	}
}

func TestKillCoalescerDisposeStopsGoroutineAndRejectsEnqueues(t *testing.T) {
	rec := &killRecorder{}
	kc := newKillCoalescer(rec.flush, time.Hour)

	tok := bearertoken.Token("t1")
	kc.enqueue(uuid.NewUUID(), tok)
	kc.dispose()

	select {
	case <-kc.done:
	default:
		t.Fatal("coalescer goroutine still running after dispose")
	}

	if len(rec.snapshot()) != 1 {
		t.Fatalf("expected final flush of 1 call, got %d", len(rec.snapshot()))
	}

	kc.enqueue(uuid.NewUUID(), tok)
	if total := len(rec.snapshot()); total != 1 {
		t.Fatalf("enqueue after dispose must be dropped, got %d calls", total)
	}

	kc.dispose()
}
