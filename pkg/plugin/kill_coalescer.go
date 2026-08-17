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

// killCoalescer batches kill ids for one datasource instance. Its owner
// serializes enqueue against dispose, so it carries no disposal state itself.
type killCoalescer struct {
	flushFn  killFlushFunc
	interval time.Duration

	mu  sync.Mutex
	buf []killEntry

	wake chan struct{}
	stop chan struct{}
	done chan struct{}
}

func newKillCoalescer(flush killFlushFunc, interval time.Duration) *killCoalescer {
	kc := &killCoalescer{
		flushFn:  flush,
		interval: interval,
		wake:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go kc.run()
	return kc
}

func (kc *killCoalescer) enqueue(id uuid.UUID, target killTarget) {
	kc.mu.Lock()
	if len(kc.buf) >= killBufferLimit {
		kc.mu.Unlock()
		log.DefaultLogger.Debug("Dropping compute kill enqueue", "buffered", killBufferLimit)
		return
	}
	kc.buf = append(kc.buf, killEntry{id: id, target: target})
	kc.mu.Unlock()

	select {
	case kc.wake <- struct{}{}:
	default:
	}
}

// run flushes one interval after the buffer becomes non-empty. Waiting on wake
// rather than a ticker keeps an instance the SDK never disposes from waking for
// the life of the process.
func (kc *killCoalescer) run() {
	defer close(kc.done)
	for {
		select {
		case <-kc.wake:
			select {
			case <-time.After(kc.interval):
				kc.flushBuffered()
			case <-kc.stop:
				kc.flushBuffered()
				return
			}
		case <-kc.stop:
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

	byTarget := make(map[killTarget][]uuid.UUID)
	for _, e := range entries {
		byTarget[e.target] = append(byTarget[e.target], e.id)
	}
	for target, ids := range byTarget {
		for start := 0; start < len(ids); start += killChunkSize {
			end := min(start+killChunkSize, len(ids))
			kc.flushFn(ctx, target, ids[start:end])
		}
	}
}

// dispose schedules a final flush without waiting for it. The owner calls it
// once; a second call panics.
func (kc *killCoalescer) dispose() {
	close(kc.stop)
}
