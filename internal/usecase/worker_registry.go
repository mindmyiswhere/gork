package usecase

import (
	"sync"
	"time"
)

// WorkerInfo — состояние подключённого воркера.
type WorkerInfo struct {
	ID             string
	SupportedTypes []string
	Concurrency    int32
	RunningTasks   map[string]struct{} // set из task_id
	LastSeen       time.Time
}

// WorkerRegistry — потокобезопасный реестр воркеров в памяти.
type WorkerRegistry struct {
	mu      sync.RWMutex
	workers map[string]*WorkerInfo
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{workers: make(map[string]*WorkerInfo)}
}

func (r *WorkerRegistry) Register(w *WorkerInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w.RunningTasks = make(map[string]struct{})
	w.LastSeen = time.Now()
	r.workers[w.ID] = w
}

func (r *WorkerRegistry) Unregister(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.workers, id)
}

func (r *WorkerRegistry) Heartbeat(id string, running []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, ok := r.workers[id]
	if !ok {
		return
	}
	w.LastSeen = time.Now()
	w.RunningTasks = make(map[string]struct{}, len(running))
	for _, tid := range running {
		w.RunningTasks[tid] = struct{}{}
	}
}

func (r *WorkerRegistry) Get(id string) (*WorkerInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w, ok := r.workers[id]
	return w, ok
}
