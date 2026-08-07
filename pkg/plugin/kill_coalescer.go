package plugin

import (
	"context"
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

// killFlushFunc performs one best-effort BatchKillRequests call without plugin-level retries.
type killFlushFunc func(ctx context.Context, token bearertoken.Token, ids []uuid.UUID)

type killEntry struct {
	id    uuid.UUID
	token bearertoken.Token
}

type killCoalescer struct {
	flushFn  killFlushFunc
	interval time.Duration

	mu     sync.Mutex
	buf    []killEntry
	closed bool

	stop chan struct{}
	done chan struct{}
}

func newKillCoalescer(flush killFlushFunc, interval time.Duration) *killCoalescer {
	kc := &killCoalescer{
		flushFn:  flush,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go kc.run()
	return kc
}

func (kc *killCoalescer) enqueue(id uuid.UUID, token bearertoken.Token) {
	kc.mu.Lock()
	defer kc.mu.Unlock()
	if kc.closed || len(kc.buf) >= killBufferLimit {
		log.DefaultLogger.Debug("Dropping compute kill enqueue", "closed", kc.closed, "buffered", len(kc.buf))
		return
	}
	kc.buf = append(kc.buf, killEntry{id: id, token: token})
}

func (kc *killCoalescer) run() {
	defer close(kc.done)
	ticker := time.NewTicker(kc.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			kc.flushBuffered()
		case <-kc.stop:
			// closed is set before stop closes, so this is the final buffer.
			kc.flushBuffered()
			return
		}
	}
}

func (kc *killCoalescer) flushBuffered() {
	kc.mu.Lock()
	entries := kc.buf
	kc.buf = nil
	kc.mu.Unlock()
	if len(entries) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), killFlushTimeout)
	defer cancel()

	byToken := make(map[bearertoken.Token][]uuid.UUID)
	for _, e := range entries {
		byToken[e.token] = append(byToken[e.token], e.id)
	}
	for token, ids := range byToken {
		for start := 0; start < len(ids); start += killChunkSize {
			end := min(start+killChunkSize, len(ids))
			kc.flushFn(ctx, token, ids[start:end])
		}
	}
}

// dispose stops the coalescer after flushing buffered IDs. It is idempotent.
func (kc *killCoalescer) dispose() {
	kc.mu.Lock()
	alreadyClosed := kc.closed
	kc.closed = true
	kc.mu.Unlock()
	if !alreadyClosed {
		close(kc.stop)
	}
	<-kc.done
}
