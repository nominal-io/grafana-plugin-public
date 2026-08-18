package plugin

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/palantir/pkg/bearertoken"
	"github.com/palantir/pkg/uuid"
)

const (
	// Match the compute worker's kill coalescing window.
	killFlushInterval = 100 * time.Millisecond
	// Kill requests are best effort, so overflow is dropped.
	killBufferLimit = 1024
	// Match the BatchKillRequests limit.
	killChunkSize    = 300
	killFlushTimeout = 5 * time.Second
)

// killTarget identifies a detached batch-kill request.
type killTarget struct {
	token bearertoken.Token
	ua    userAgentComponents
}

// killFlushFunc performs one best-effort BatchKillRequests call.
type killFlushFunc func(ctx context.Context, target killTarget, ids []uuid.UUID)

type killEntry struct {
	id     uuid.UUID
	target killTarget
}

// killCoalescer batches kill ids for one datasource instance. A non-empty buffer
// always has a flush pending and a flush with nothing to send does nothing, so
// the buffer is the only state, and an idle coalescer costs nothing. At most one
// flush sends at a time, so a slow kill endpoint cannot be fanned out to.
//
// The zero value is ready to use. Callers pass the sender to every enqueue and
// flush, and an armed timer uses the sender from the enqueue that armed it, so
// every caller of one coalescer must pass the same sender. A zero interval means
// killFlushInterval.
type killCoalescer struct {
	interval time.Duration

	mu       sync.Mutex
	buf      []killEntry
	flushing bool
}

// armInterval resolves the flush delay so a zero-value coalescer arms on the
// production window.
func (kc *killCoalescer) armInterval() time.Duration {
	if kc.interval == 0 {
		return killFlushInterval
	}
	return kc.interval
}

func (kc *killCoalescer) enqueue(flush killFlushFunc, id uuid.UUID, target killTarget) {
	kc.mu.Lock()
	if len(kc.buf) >= killBufferLimit {
		kc.mu.Unlock()
		log.DefaultLogger.Debug("Dropping compute kill enqueue", "buffered", killBufferLimit)
		return
	}
	kc.buf = append(kc.buf, killEntry{id: id, target: target})
	if len(kc.buf) == 1 {
		// AfterFunc runs flush on its own goroutine, so arming under mu is safe.
		time.AfterFunc(kc.armInterval(), func() { kc.flush(flush) })
	}
	kc.mu.Unlock()
}

// flush sends every buffered id and leaves the buffer idle again. Flushing more
// often than the buffer fills is harmless: whichever flush takes the entries
// sends them, and the rest find nothing. A flush that arrives while another is
// sending returns at once and leaves its entries to that one, which re-drains
// until the buffer is empty, so kills queue behind a slow endpoint instead of
// piling onto it.
func (kc *killCoalescer) flush(flush killFlushFunc) {
	kc.mu.Lock()
	if kc.flushing {
		kc.mu.Unlock()
		return
	}
	kc.flushing = true
	for {
		entries := kc.buf
		kc.buf = nil
		if len(entries) == 0 {
			// Clearing the flag in the same critical section that finds the buffer
			// empty is what stops an entry from being stranded: any later enqueue
			// sees an empty buffer and arms its own timer.
			kc.flushing = false
			kc.mu.Unlock()
			return
		}
		kc.mu.Unlock()
		kc.send(flush, entries)
		kc.mu.Lock()
	}
}

// send delivers one drained batch, one call per target per chunk.
func (kc *killCoalescer) send(flush killFlushFunc, entries []killEntry) {
	byTarget := make(map[killTarget][]uuid.UUID)
	for _, e := range entries {
		byTarget[e.target] = append(byTarget[e.target], e.id)
	}
	for target, ids := range byTarget {
		for chunk := range slices.Chunk(ids, killChunkSize) {
			// A fresh deadline per call keeps one slow kill from starving the rest.
			ctx, cancel := context.WithTimeout(context.Background(), killFlushTimeout)
			flush(ctx, target, chunk)
			cancel()
		}
	}
}
