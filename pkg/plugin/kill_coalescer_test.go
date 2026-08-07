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
	token bearertoken.Token
	ids   []uuid.UUID
}

type killRecorder struct {
	mu      sync.Mutex
	batches []killBatch
}

func (r *killRecorder) flush(_ context.Context, token bearertoken.Token, ids []uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Keep each call independent of the coalescer's backing array.
	r.batches = append(r.batches, killBatch{token: token, ids: append([]uuid.UUID(nil), ids...)})
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

func TestKillCoalescerFlushesOnInterval(t *testing.T) {
	rec := &killRecorder{}
	kc := newKillCoalescer(rec.flush, 5*time.Millisecond)
	defer kc.dispose()

	id1, id2 := uuid.NewUUID(), uuid.NewUUID()
	tok := bearertoken.Token("t1")
	kc.enqueue(id1, tok)
	kc.enqueue(id2, tok)

	waitForCondition(t, 2*time.Second, func() bool { return len(rec.snapshot()) >= 1 })

	batches := rec.snapshot()
	if len(batches) != 1 {
		t.Fatalf("expected 1 coalesced flush call, got %d", len(batches))
	}
	if batches[0].token != tok {
		t.Fatalf("expected flush tagged with token %q, got %q", tok, batches[0].token)
	}
	if want := []uuid.UUID{id1, id2}; !slices.Equal(batches[0].ids, want) {
		t.Fatalf("expected ids %v, got %v", want, batches[0].ids)
	}
}

func TestKillCoalescerChunksAtLimit(t *testing.T) {
	rec := &killRecorder{}
	kc := newKillCoalescer(rec.flush, time.Hour)

	tok := bearertoken.Token("t1")
	ids := make([]uuid.UUID, killChunkSize+1)
	for i := range ids {
		ids[i] = uuid.NewUUID()
		kc.enqueue(ids[i], tok)
	}
	kc.dispose()

	batches := rec.snapshot()
	if len(batches) != 2 {
		t.Fatalf("expected 2 chunked kill calls, got %d", len(batches))
	}
	if batches[0].token != tok || batches[1].token != tok {
		t.Fatalf("expected both chunks tagged with token %q, got %q and %q", tok, batches[0].token, batches[1].token)
	}
	if !slices.Equal(batches[0].ids, ids[:killChunkSize]) {
		t.Fatalf("expected first chunk to be the first %d ids in order, got %v", killChunkSize, batches[0].ids)
	}
	if !slices.Equal(batches[1].ids, ids[killChunkSize:]) {
		t.Fatalf("expected second chunk to be the remaining id, got %v", batches[1].ids)
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
	for _, b := range rec.snapshot() {
		total += len(b.ids)
	}
	if total != killBufferLimit {
		t.Fatalf("expected exactly %d ids flushed (overflow dropped), got %d", killBufferLimit, total)
	}
}

func TestKillCoalescerGroupsByToken(t *testing.T) {
	rec := &killRecorder{}
	kc := newKillCoalescer(rec.flush, time.Hour)

	id1, id2 := uuid.NewUUID(), uuid.NewUUID()
	tok1, tok2 := bearertoken.Token("t1"), bearertoken.Token("t2")
	kc.enqueue(id1, tok1)
	kc.enqueue(id2, tok2)
	kc.dispose()

	batches := rec.snapshot()
	if len(batches) != 2 {
		t.Fatalf("expected one flush call per token, got %d", len(batches))
	}

	// Flush order across tokens is map-iteration order, so key on token before comparing.
	byToken := make(map[bearertoken.Token][]uuid.UUID, len(batches))
	for _, b := range batches {
		byToken[b.token] = b.ids
	}
	if want := []uuid.UUID{id1}; !slices.Equal(byToken[tok1], want) {
		t.Fatalf("expected token %q to carry ids %v, got %v", tok1, want, byToken[tok1])
	}
	if want := []uuid.UUID{id2}; !slices.Equal(byToken[tok2], want) {
		t.Fatalf("expected token %q to carry ids %v, got %v", tok2, want, byToken[tok2])
	}
}

func TestKillCoalescerDisposeFlushesFinalBufferAndRejectsEnqueues(t *testing.T) {
	rec := &killRecorder{}
	kc := newKillCoalescer(rec.flush, time.Hour)

	tok := bearertoken.Token("t1")
	kc.enqueue(uuid.NewUUID(), tok)
	kc.dispose()

	if len(rec.snapshot()) != 1 {
		t.Fatalf("expected final flush of 1 call, got %d", len(rec.snapshot()))
	}

	kc.enqueue(uuid.NewUUID(), tok)
	if total := len(rec.snapshot()); total != 1 {
		t.Fatalf("enqueue after dispose must be dropped, got %d calls", total)
	}

	kc.dispose()
}
