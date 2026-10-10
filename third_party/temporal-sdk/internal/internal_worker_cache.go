package internal

import (
	"errors"
	"runtime"
	"sync"

	"go.temporal.io/sdk/internal/common/cache"
)

var errWorkerCacheReleased = errors.New("workflow cache handle is released")

// WorkerCache keeps a stable cache generation and one idempotently released owner.
type WorkerCache struct {
	sharedCache      *sharedWorkerCache
	workflowCache    cache.Cache
	maxCacheSize     int
	lifecycleMu      sync.Mutex
	released         bool
	closeOnce        sync.Once
	fathomryLifetime *fathomryWorkerLifetime
	fathomryMu       sync.Mutex
	fathomryEntries  map[*workflowExecutionContextImpl]string
}

// A container for data workers in this process may want to share with eachother
type sharedWorkerCache struct {
	// Count of live workers
	workerRefcount int

	// A cache workers can use to store workflow state.
	workflowCache *cache.Cache
	// Max size for the cache
	maxWorkflowCacheSize int
}

// A shared cache workers can use to store state. The cache is expected to be initialized with the first worker to be
// instantiated. IE: All workers have a pointer to it. The pointer itself is never made nil, but when the refcount
// reaches zero, the shared caches inside of it will be nilled out. Do not manipulate without holding
// sharedWorkerCacheLock
var sharedWorkerCachePtr = &sharedWorkerCache{}
var sharedWorkerCacheLock sync.Mutex

// Must be set before spawning any workers
var desiredWorkflowCacheSize = defaultStickyCacheSize

// SetStickyWorkflowCacheSize sets the cache size for sticky workflow cache. Sticky workflow execution is the affinity
// between workflow tasks of a specific workflow execution to a specific worker. The benefit of sticky execution is that
// the workflow does not have to reconstruct state by replaying history from the beginning. The cache is shared between
// workers running within same process. This must be called before any worker is started. If not called, the default
// size of 10K (which may change) will be used.
func SetStickyWorkflowCacheSize(cacheSize int) {
	sharedWorkerCacheLock.Lock()
	defer sharedWorkerCacheLock.Unlock()
	desiredWorkflowCacheSize = cacheSize
}

// PurgeStickyWorkflowCache resets the sticky workflow cache. This must be called only when all workers are stopped.
func PurgeStickyWorkflowCache() {
	sharedWorkerCacheLock.Lock()
	defer sharedWorkerCacheLock.Unlock()

	if sharedWorkerCachePtr.workflowCache != nil {
		(*sharedWorkerCachePtr.workflowCache).Clear()
	}
}

// NewWorkerCache acquires one shared cache owner. Worker shutdown or replay
// completion releases it explicitly; the finalizer is only a fallback for
// unreachable handles and cannot collect handles retained by cached workflows.
func NewWorkerCache() *WorkerCache {
	sharedWorkerCacheLock.Lock()
	desiredWorkflowCacheSize := desiredWorkflowCacheSize
	sharedWorkerCacheLock.Unlock()

	return newWorkerCache(sharedWorkerCachePtr, &sharedWorkerCacheLock, desiredWorkflowCacheSize)
}

// newWorkerCache creates a cache backed by the provided store. Replayers use it to isolate
// one-shot execution state from live workers, and tests use it to avoid global cache state.
func newWorkerCache(storeIn *sharedWorkerCache, lock *sync.Mutex, cacheSize int) *WorkerCache {
	lock.Lock()
	defer lock.Unlock()

	if storeIn == nil {
		panic("Provided sharedWorkerCache pointer must not be nil")
	}

	if storeIn.workerRefcount == 0 {
		newcache := cache.New(cacheSize-1, &cache.Options{
			RemovedFunc: func(cachedEntity any) {
				wc := cachedEntity.(*workflowExecutionContextImpl)
				defer wc.wth.cache.fathomryEvicted(wc)
				wc.onEviction()
			},
		})
		*storeIn = sharedWorkerCache{workflowCache: &newcache, workerRefcount: 0, maxWorkflowCacheSize: cacheSize}
	}
	storeIn.workerRefcount++
	newWorkerCache := WorkerCache{
		sharedCache:   storeIn,
		workflowCache: *storeIn.workflowCache,
		maxCacheSize:  storeIn.maxWorkflowCacheSize,
	}
	runtime.SetFinalizer(&newWorkerCache, func(wc *WorkerCache) {
		wc.close(lock)
	})
	return &newWorkerCache
}

func (wc *WorkerCache) getWorkflowCache() cache.Cache {
	return wc.workflowCache
}

func (wc *WorkerCache) close(lock *sync.Mutex) {
	wc.closeOnce.Do(func() {
		wc.lifecycleMu.Lock()
		defer wc.lifecycleMu.Unlock()
		wc.released = true
		lock.Lock()

		wc.sharedCache.workerRefcount--
		var released cache.Cache
		if wc.sharedCache.workerRefcount == 0 {
			released = *wc.sharedCache.workflowCache
			wc.sharedCache.workflowCache = nil
		}
		lock.Unlock()
		if released != nil {
			released.Clear()
		}
	})
}

func (wc *WorkerCache) getWorkflowContext(runID string) *workflowExecutionContextImpl {
	o := wc.workflowCache.Get(runID)
	if o == nil {
		return nil
	}
	wec := o.(*workflowExecutionContextImpl)
	return wec
}

func (wc *WorkerCache) putWorkflowContext(runID string, wec *workflowExecutionContextImpl) (*workflowExecutionContextImpl, error) {
	wc.lifecycleMu.Lock()
	defer wc.lifecycleMu.Unlock()
	if wc.released {
		return wec, errWorkerCacheReleased
	}
	wc.fathomryRetain(runID, wec)
	existing, err := wc.workflowCache.PutIfNotExist(runID, wec)
	if err != nil || existing != wec {
		wc.fathomryEvicted(wec)
	}
	if err != nil {
		return nil, err
	}
	return existing.(*workflowExecutionContextImpl), nil
}

func (wc *WorkerCache) removeWorkflowContext(runID string, expected *workflowExecutionContextImpl) {
	wc.workflowCache.DeleteIf(runID, expected)
}

// MaxWorkflowCacheSize returns the maximum allowed size of the sticky cache
func (wc *WorkerCache) MaxWorkflowCacheSize() int {
	if wc == nil {
		return desiredWorkflowCacheSize
	}
	return wc.maxCacheSize
}
