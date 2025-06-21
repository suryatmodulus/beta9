package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/network"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	requestProcessingInterval time.Duration = 100 * time.Millisecond
	// Batch processing configuration
	maxBatchSize           = 10
	minBatchSize           = 1
	batchTimeout           = 50 * time.Millisecond
	workerSelectionTimeout = 500 * time.Millisecond
	// Adaptive batching thresholds
	highLoadThreshold = 50 // Consider high load when backlog > 50
	lowLoadThreshold  = 5  // Consider low load when backlog < 5
	// Multi-replica coordination
	workerCacheSyncInterval = 500 * time.Millisecond // How often to sync worker cache across replicas
)

type Scheduler struct {
	ctx                   context.Context
	backendRepo           repo.BackendRepository
	workerRepo            repo.WorkerRepository
	workerPoolManager     *WorkerPoolManager
	requestBacklog        *RequestBacklog
	containerRepo         repo.ContainerRepository
	workspaceRepo         repo.WorkspaceRepository
	eventRepo             repo.EventRepository
	schedulerUsageMetrics SchedulerUsageMetrics
	eventBus              *common.EventBus
	// Worker cache for batch processing
	workerCache       []*types.Worker
	workerCacheMutex  sync.RWMutex
	workerCacheExpiry time.Time
	workerCacheTTL    time.Duration
	// Multi-replica coordination
	replicaId             string
	workerCacheSyncTicker *time.Ticker
}

func NewScheduler(ctx context.Context, config types.AppConfig, redisClient *common.RedisClient, usageRepo repo.UsageMetricsRepository, backendRepo repo.BackendRepository, workspaceRepo repo.WorkspaceRepository, tailscale *network.Tailscale) (*Scheduler, error) {
	eventBus := common.NewEventBus(redisClient)
	workerRepo := repo.NewWorkerRedisRepository(redisClient, config.Worker)
	providerRepo := repo.NewProviderRedisRepository(redisClient)
	requestBacklog := NewRequestBacklog(redisClient)
	containerRepo := repo.NewContainerRedisRepository(redisClient)
	workerPoolRepo := repo.NewWorkerPoolRedisRepository(redisClient)

	schedulerUsage := NewSchedulerUsageMetrics(usageRepo)
	eventRepo := repo.NewTCPEventClientRepo(config.Monitoring.FluentBit.Events)

	// Generate unique replica ID for this scheduler instance
	replicaId := uuid.New().String()

	// Load worker pools
	workerPoolManager := NewWorkerPoolManager(config.Worker.Failover.Enabled)
	for name, pool := range config.Worker.Pools {
		var controller WorkerPoolController = nil
		var err error = nil

		switch pool.Mode {
		case types.PoolModeLocal:
			controller, err = NewLocalKubernetesWorkerPoolController(WorkerPoolControllerOptions{
				Context:        ctx,
				Name:           name,
				Config:         config,
				BackendRepo:    backendRepo,
				WorkerRepo:     workerRepo,
				ProviderRepo:   providerRepo,
				WorkerPoolRepo: workerPoolRepo,
				ContainerRepo:  containerRepo,
				EventRepo:      eventRepo,
			})
		case types.PoolModeExternal:
			controller, err = NewExternalWorkerPoolController(WorkerPoolControllerOptions{
				Context:        ctx,
				Name:           name,
				Config:         config,
				BackendRepo:    backendRepo,
				WorkerRepo:     workerRepo,
				ProviderRepo:   providerRepo,
				WorkerPoolRepo: workerPoolRepo,
				ContainerRepo:  containerRepo,
				ProviderName:   pool.Provider,
				Tailscale:      tailscale,
				EventRepo:      eventRepo,
			})
		default:
			log.Error().Str("pool_name", name).Str("mode", string(pool.Mode)).Msg("no valid controller found for pool")
			continue
		}

		if err != nil {
			log.Error().Str("pool_name", name).Err(err).Msg("unable to load controller")
			continue
		}

		workerPoolManager.SetPool(name, pool, controller)
		log.Info().Str("pool_name", name).Str("mode", string(pool.Mode)).Str("gpu_type", pool.GPUType).Msg("loaded controller")
	}

	scheduler := &Scheduler{
		ctx:                   ctx,
		eventBus:              eventBus,
		backendRepo:           backendRepo,
		workerRepo:            workerRepo,
		workerPoolManager:     workerPoolManager,
		requestBacklog:        requestBacklog,
		containerRepo:         containerRepo,
		schedulerUsageMetrics: schedulerUsage,
		eventRepo:             eventRepo,
		workspaceRepo:         workspaceRepo,
		workerCacheTTL:        500 * time.Millisecond, // Cache workers for 500ms to reduce stale data
		replicaId:             replicaId,
		workerCacheSyncTicker: time.NewTicker(workerCacheSyncInterval),
	}

	// Start worker cache synchronization for multi-replica coordination
	go scheduler.syncWorkerCache()

	log.Info().Str("replica_id", replicaId).Msg("scheduler initialized for multi-replica deployment")

	return scheduler, nil
}

func (s *Scheduler) Run(request *types.ContainerRequest) error {
	log.Info().Interface("request", request).Msg("received run request")

	request.Timestamp = time.Now()

	containerState, err := s.containerRepo.GetContainerState(request.ContainerId)
	if err == nil {
		switch types.ContainerStatus(containerState.Status) {
		case types.ContainerStatusPending, types.ContainerStatusRunning:
			return &types.ContainerAlreadyScheduledError{Msg: "a container with this id is already running or pending"}
		default:
			// Do nothing
		}
	}

	go s.schedulerUsageMetrics.CounterIncContainerRequested(request)
	go s.eventRepo.PushContainerRequestedEvent(request)

	quota, err := s.getConcurrencyLimit(request)
	if err != nil {
		return err
	}

	err = s.containerRepo.SetContainerStateWithConcurrencyLimit(quota, request)
	if err != nil {
		return err
	}

	return s.addRequestToBacklog(request)
}

func (s *Scheduler) getConcurrencyLimit(request *types.ContainerRequest) (*types.ConcurrencyLimit, error) {
	// First try to get the cached quota
	var quota *types.ConcurrencyLimit
	quota, err := s.workspaceRepo.GetConcurrencyLimitByWorkspaceId(request.WorkspaceId)
	if err != nil {
		return nil, err
	}

	if quota == nil {
		quota, err = s.backendRepo.GetConcurrencyLimitByWorkspaceId(s.ctx, request.WorkspaceId)
		if err != nil && err == sql.ErrNoRows {
			return nil, nil // No quota set for this workspace
		}
		if err != nil {
			return nil, err
		}

		err = s.workspaceRepo.SetConcurrencyLimitByWorkspaceId(request.WorkspaceId, quota)
		if err != nil {
			return nil, err
		}
	}

	return quota, nil
}

func (s *Scheduler) Stop(stopArgs *types.StopContainerArgs) error {
	log.Info().Interface("stop_args", stopArgs).Msg("received stop request")

	err := s.containerRepo.UpdateContainerStatus(stopArgs.ContainerId, types.ContainerStatusStopping, types.ContainerStateTtlSWhilePending)
	if err != nil {
		return err
	}

	eventArgs, err := stopArgs.ToMap()
	if err != nil {
		return err
	}

	_, err = s.eventBus.Send(&common.Event{
		Type:          common.EventTypeStopContainer,
		Args:          eventArgs,
		LockAndDelete: false,
	})
	if err != nil {
		log.Error().Err(err).Msg("could not stop container")
		return err
	}

	return nil
}

func (s *Scheduler) getControllers(request *types.ContainerRequest) ([]WorkerPoolController, error) {
	controllers := []WorkerPoolController{}

	if request.PoolSelector != "" {
		wp, ok := s.workerPoolManager.GetPool(request.PoolSelector)
		if !ok {
			return nil, errors.New("no controller found for request")
		}
		controllers = append(controllers, wp.Controller)

	} else if !request.RequiresGPU() {
		pools := s.workerPoolManager.GetPoolByFilters(poolFilters{
			GPUType: "",
		})
		for _, pool := range pools {
			controllers = append(controllers, pool.Controller)
		}
	} else {
		for _, gpu := range request.GpuRequest {
			pools := s.workerPoolManager.GetPoolByFilters(poolFilters{
				GPUType: gpu,
			})

			for _, pool := range pools {
				controllers = append(controllers, pool.Controller)
			}

			// If the request contains the "any" GPU selector, we've already retrieved all pools
			if gpu == string(types.GPU_ANY) {
				break
			}
		}
	}

	controllers = filterControllersByFlags(controllers, request)
	if len(controllers) == 0 {
		return nil, errors.New("no controller found for request")
	}

	return controllers, nil
}

func (s *Scheduler) collectBatch() []*types.ContainerRequest {
	backlogLen := s.requestBacklog.Len()

	// Adaptive batch sizing based on load
	batchSize := maxBatchSize
	if backlogLen > int64(highLoadThreshold) {
		// High load: use maximum batch size for efficiency
		batchSize = maxBatchSize
	} else if backlogLen < int64(lowLoadThreshold) {
		// Low load: use smaller batches for responsiveness
		batchSize = minBatchSize
	} else {
		// Medium load: scale batch size with backlog length
		batchSize = int(backlogLen / 5) // Roughly 20% of backlog
		if batchSize > maxBatchSize {
			batchSize = maxBatchSize
		}
		if batchSize < minBatchSize {
			batchSize = minBatchSize
		}
	}

	// Try to collect requests with distributed claiming for multi-replica safety
	requests, err := s.requestBacklog.PopBatchWithClaim(batchSize, s.replicaId)
	if err != nil {
		// If batch operation fails, fall back to individual pops
		batch := make([]*types.ContainerRequest, 0, batchSize)
		for i := 0; i < batchSize; i++ {
			request, err := s.requestBacklog.Pop()
			if err != nil {
				break // No more requests available
			}
			batch = append(batch, request)
		}
		return batch
	}

	return requests
}

func (s *Scheduler) processBatchConcurrently(requests []*types.ContainerRequest) {
	if len(requests) == 0 {
		return
	}

	startTime := time.Now()
	batchSize := len(requests)

	// Get all workers once for the batch to avoid repeated database calls
	allWorkers, err := s.getCachedWorkers()
	if err != nil {
		log.Error().Err(err).Msg("failed to get workers for batch processing")
		// Fall back to individual processing
		for _, request := range requests {
			s.addRequestToBacklog(request)
		}
		return
	}

	// Process requests concurrently with a semaphore to limit concurrency
	semaphore := make(chan struct{}, 5) // Limit to 5 concurrent operations
	var wg sync.WaitGroup

	for _, request := range requests {
		wg.Add(1)
		go func(req *types.ContainerRequest) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire semaphore
			defer func() { <-semaphore }() // Release semaphore

			s.processRequestWithWorkers(req, allWorkers)
		}(request)
	}

	wg.Wait()

	// Log batch processing performance
	processingTime := time.Since(startTime)
	log.Debug().
		Int("batch_size", batchSize).
		Dur("processing_time", processingTime).
		Float64("requests_per_second", float64(batchSize)/processingTime.Seconds()).
		Msg("batch processing completed")
}

func (s *Scheduler) processRequestWithWorkers(request *types.ContainerRequest, allWorkers []*types.Worker) {
	// Ensure we release the claim when we're done processing
	defer func() {
		if err := s.requestBacklog.ReleaseClaim(request.ContainerId); err != nil {
			log.Error().Str("container_id", request.ContainerId).Err(err).Msg("failed to release request claim")
		}
	}()

	// Find a worker using the pre-fetched worker list for initial filtering
	worker, err := s.selectWorkerFromList(request, allWorkers)
	if err != nil || worker == nil {
		// We didn't find a Worker that fit the ContainerRequest's requirements. Let's find a controller
		// so we can add a new worker.

		controllers, err := s.getControllers(request)
		if err != nil {
			log.Error().Interface("request", request).Err(err).Msg("no controller found for request")
			s.addRequestToBacklog(request)
			return
		}

		var err2 error
		for _, c := range controllers {
			// Iterates through controllers in the order of prioritized gpus to attempt to add a worker
			if c == nil {
				continue
			}

			var newWorker *types.Worker
			newWorker, err2 = c.AddWorker(request.Cpu, request.Memory, request.GpuCount)
			if err2 == nil {
				log.Info().Str("worker_id", newWorker.Id).Str("container_id", request.ContainerId).Msg("added new worker")

				err2 = s.scheduleRequest(newWorker, request)
				if err2 != nil {
					log.Error().Str("container_id", request.ContainerId).Err(err2).Msg("unable to schedule request")
					s.addRequestToBacklog(request)
				}

				return
			}
		}

		log.Error().Str("container_id", request.ContainerId).Err(err2).Msg("unable to add worker")
		s.addRequestToBacklog(request)
		return
	}

	// Validate that the worker still has capacity before scheduling
	// This prevents resource version conflicts by ensuring we have current data
	validWorker, err := s.validateWorkerCapacity(worker, request)
	if err != nil {
		log.Debug().Str("worker_id", worker.Id).Str("container_id", request.ContainerId).Msg("worker capacity validation failed, will retry")
		s.addRequestToBacklog(request)
		return
	}

	// We found a worker that met the ContainerRequest's requirements. Schedule the request
	// on that worker.
	err = s.scheduleRequest(validWorker, request)
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Err(err).Msg("unable to schedule request on existing worker")
		s.addRequestToBacklog(request)
		return
	}

	// Record the request processing duration
	schedulingDuration := time.Since(request.Timestamp)
	metrics.RecordRequestSchedulingDuration(schedulingDuration, request)
}

func (s *Scheduler) validateWorkerCapacity(worker *types.Worker, request *types.ContainerRequest) (*types.Worker, error) {
	// Get fresh worker data to validate current capacity
	freshWorker, err := s.workerRepo.GetWorkerById(worker.Id)
	if err != nil {
		return nil, err
	}

	// Check if the worker still has sufficient capacity
	if freshWorker.FreeCpu < int64(request.Cpu) || freshWorker.FreeMemory < int64(request.Memory) {
		return nil, errors.New("worker no longer has sufficient capacity")
	}

	if request.RequiresGPU() {
		if freshWorker.FreeGpuCount < request.GpuCount {
			return nil, errors.New("worker no longer has sufficient GPU capacity")
		}
	}

	// Check if worker is still available
	if freshWorker.Status == types.WorkerStatusDisabled {
		return nil, errors.New("worker is no longer available")
	}

	return freshWorker, nil
}

func (s *Scheduler) selectWorkerFromList(request *types.ContainerRequest, workers []*types.Worker) (*types.Worker, error) {
	filteredWorkers := filterWorkersByPoolSelector(workers, request)     // Filter workers by pool selector
	filteredWorkers = filterWorkersByResources(filteredWorkers, request) // Filter workers resource requirements
	filteredWorkers = filterWorkersByFlags(filteredWorkers, request)     // Filter workers by flags

	if len(filteredWorkers) == 0 {
		return nil, &types.ErrNoSuitableWorkerFound{}
	}

	// Score workers based on status and priority
	scoredWorkers := []scoredWorker{}
	for _, worker := range filteredWorkers {
		score := int32(0)

		if worker.Status == types.WorkerStatusAvailable {
			score += scoreAvailableWorker
		}

		score += worker.Priority
		scoredWorkers = append(scoredWorkers, scoredWorker{worker: worker, score: score})
	}

	// Select the worker with the highest score
	sort.Slice(scoredWorkers, func(i, j int) bool {
		// TODO: Figure out a short way to randomize order of workers with the same score
		return scoredWorkers[i].score > scoredWorkers[j].score
	})

	return scoredWorkers[0].worker, nil
}

func (s *Scheduler) StartProcessingRequests() {
	for {
		select {
		case <-s.ctx.Done():
			// Context has been cancelled
			return
		default:
			// Continue processing requests
		}

		if s.requestBacklog.Len() == 0 {
			time.Sleep(requestProcessingInterval)
			continue
		}

		requests := s.collectBatch()
		if len(requests) == 0 {
			time.Sleep(requestProcessingInterval)
			continue
		}

		s.processBatchConcurrently(requests)
	}
}

func (s *Scheduler) scheduleRequest(worker *types.Worker, request *types.ContainerRequest) error {
	if err := s.containerRepo.UpdateAssignedContainerGPU(request.ContainerId, worker.Gpu); err != nil {
		log.Error().Str("container_id", request.ContainerId).Err(err).Msg("failed to update assigned container gpu")
		return err
	}

	request.Gpu = worker.Gpu

	// Invalidate worker cache since worker capacity has changed
	s.invalidateWorkerCache()

	go s.schedulerUsageMetrics.CounterIncContainerScheduled(request)
	go s.eventRepo.PushContainerScheduledEvent(request.ContainerId, worker.Id, request)
	return s.workerRepo.ScheduleContainerRequest(worker, request)
}

func filterControllersByFlags(controllers []WorkerPoolController, request *types.ContainerRequest) []WorkerPoolController {
	filteredControllers := []WorkerPoolController{}
	for _, controller := range controllers {
		if !request.Preemptable && controller.IsPreemptable() {
			continue
		}

		if (request.PoolSelector != "" && controller.Name() != request.PoolSelector) ||
			(request.PoolSelector == "" && controller.RequiresPoolSelector()) {
			continue
		}

		filteredControllers = append(filteredControllers, controller)
	}

	return filteredControllers
}

func filterWorkersByPoolSelector(workers []*types.Worker, request *types.ContainerRequest) []*types.Worker {
	filteredWorkers := []*types.Worker{}
	for _, worker := range workers {
		if (request.PoolSelector != "" && worker.PoolName == request.PoolSelector) ||
			(request.PoolSelector == "" && !worker.RequiresPoolSelector) {
			filteredWorkers = append(filteredWorkers, worker)
		}
	}
	return filteredWorkers
}

func filterWorkersByResources(workers []*types.Worker, request *types.ContainerRequest) []*types.Worker {
	filteredWorkers := []*types.Worker{}
	gpuRequestsMap := map[string]int{}
	requiresGPU := request.RequiresGPU()

	for index, gpu := range request.GpuRequest {
		gpuRequestsMap[gpu] = index
	}

	// If the request contains the "any" GPU selector, we need to check all GPU types
	if slices.Contains(request.GpuRequest, string(types.GPU_ANY)) {
		gpuRequestsMap = types.GPUTypesToMap(types.AllGPUTypes())
	}

	for _, worker := range workers {
		isGpuWorker := worker.Gpu != ""

		// Check if the worker has enough free cpu and memory to run the container
		if worker.FreeCpu < int64(request.Cpu) || worker.FreeMemory < int64(request.Memory) {
			continue
		}

		// Check if the worker has been cordoned
		if worker.Status == types.WorkerStatusDisabled {
			continue
		}

		if (requiresGPU && !isGpuWorker) || (!requiresGPU && isGpuWorker) {
			// If the worker doesn't have a GPU and the request requires one, skip
			// Likewise, if the worker has a GPU and the request doesn't require one, skip
			continue
		}

		if requiresGPU {
			// Validate GPU resource availability
			priorityModifier, validGpu := gpuRequestsMap[worker.Gpu]
			if !validGpu || worker.FreeGpuCount < request.GpuCount {
				continue
			}

			// This will account for the preset priorities for the pool type as well as the order of the GPU requests
			// NOTE: will only work properly if all GPU types and their pools start from 0 and pool priority are incremental by changes of ±1
			worker.Priority -= int32(priorityModifier)
		}

		filteredWorkers = append(filteredWorkers, worker)
	}
	return filteredWorkers
}

func filterWorkersByFlags(workers []*types.Worker, request *types.ContainerRequest) []*types.Worker {
	filteredWorkers := []*types.Worker{}
	for _, worker := range workers {
		if !request.Preemptable && worker.Preemptable {
			continue
		}

		filteredWorkers = append(filteredWorkers, worker)
	}

	return filteredWorkers
}

type scoredWorker struct {
	worker *types.Worker
	score  int32
}

// Constants used for scoring workers
const (
	scoreAvailableWorker int32 = 10
)

func (s *Scheduler) selectWorker(request *types.ContainerRequest) (*types.Worker, error) {
	workers, err := s.workerRepo.GetAllWorkers()
	if err != nil {
		return nil, err
	}

	filteredWorkers := filterWorkersByPoolSelector(workers, request)     // Filter workers by pool selector
	filteredWorkers = filterWorkersByResources(filteredWorkers, request) // Filter workers resource requirements
	filteredWorkers = filterWorkersByFlags(filteredWorkers, request)     // Filter workers by flags

	if len(filteredWorkers) == 0 {
		return nil, &types.ErrNoSuitableWorkerFound{}
	}

	// Score workers based on status and priority
	scoredWorkers := []scoredWorker{}
	for _, worker := range filteredWorkers {
		score := int32(0)

		if worker.Status == types.WorkerStatusAvailable {
			score += scoreAvailableWorker
		}

		score += worker.Priority
		scoredWorkers = append(scoredWorkers, scoredWorker{worker: worker, score: score})
	}

	// Select the worker with the highest score
	sort.Slice(scoredWorkers, func(i, j int) bool {
		// TODO: Figure out a short way to randomize order of workers with the same score
		return scoredWorkers[i].score > scoredWorkers[j].score
	})

	return scoredWorkers[0].worker, nil
}

const maxScheduleRetryCount = 3
const maxScheduleRetryDuration = 10 * time.Minute

func (s *Scheduler) addRequestToBacklog(request *types.ContainerRequest) error {
	if request.RequiresGPU() && request.GpuCount <= 0 {
		request.GpuCount = 1
	}

	if request.RetryCount == 0 {
		request.RetryCount++
		return s.requestBacklog.Push(request)
	}

	go func() {
		if request.RetryCount < maxScheduleRetryCount && time.Since(request.Timestamp) < maxScheduleRetryDuration {
			delay := calculateBackoffDelay(request.RetryCount)
			time.Sleep(delay)
			request.RetryCount++
			s.requestBacklog.Push(request)
			return
		}

		log.Error().Str("container_id", request.ContainerId).Int("retry_count", request.RetryCount).Msg("giving up on request")
		s.containerRepo.DeleteContainerState(request.ContainerId)
		s.containerRepo.SetContainerRequestStatus(request.ContainerId, types.ContainerRequestStatusFailed)
		metrics.RecordRequestScheduleFailure(request)
	}()

	return nil
}

func calculateBackoffDelay(retryCount int) time.Duration {
	if retryCount == 0 {
		return 0
	}

	baseDelay := 1 * time.Second
	maxDelay := 30 * time.Second
	delay := time.Duration(math.Pow(2, float64(retryCount))) * baseDelay
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}

func (s *Scheduler) getCachedWorkers() ([]*types.Worker, error) {
	s.workerCacheMutex.RLock()
	if time.Now().Before(s.workerCacheExpiry) && len(s.workerCache) > 0 {
		workers := make([]*types.Worker, len(s.workerCache))
		copy(workers, s.workerCache)
		s.workerCacheMutex.RUnlock()
		return workers, nil
	}
	s.workerCacheMutex.RUnlock()

	// Cache miss or expired, refresh
	s.workerCacheMutex.Lock()
	defer s.workerCacheMutex.Unlock()

	// Double-check after acquiring write lock
	if time.Now().Before(s.workerCacheExpiry) && len(s.workerCache) > 0 {
		workers := make([]*types.Worker, len(s.workerCache))
		copy(workers, s.workerCache)
		return workers, nil
	}

	// Fetch fresh workers
	workers, err := s.workerRepo.GetAllWorkers()
	if err != nil {
		return nil, err
	}

	// Use a very short cache TTL to minimize stale data issues
	// The cache is mainly for reducing database load during batch processing
	cacheTTL := 500 * time.Millisecond

	// Update cache
	s.workerCache = make([]*types.Worker, len(workers))
	copy(s.workerCache, workers)
	s.workerCacheExpiry = time.Now().Add(cacheTTL)

	log.Debug().
		Int("worker_count", len(workers)).
		Dur("cache_ttl", cacheTTL).
		Msg("worker cache refreshed")

	return workers, nil
}

func (s *Scheduler) invalidateWorkerCache() {
	s.workerCacheMutex.Lock()
	defer s.workerCacheMutex.Unlock()
	s.workerCacheExpiry = time.Time{} // Force cache refresh
}

func (s *Scheduler) syncWorkerCache() {
	for {
		select {
		case <-s.ctx.Done():
			// Context has been cancelled
			return
		case <-s.workerCacheSyncTicker.C:
			// Continue processing requests
		}

		workers, err := s.getCachedWorkers()
		if err != nil {
			log.Error().Err(err).Msg("failed to sync worker cache")
			continue
		}

		s.workerCacheMutex.Lock()
		s.workerCache = workers
		s.workerCacheExpiry = time.Now().Add(s.workerCacheTTL)
		s.workerCacheMutex.Unlock()
	}
}

func (s *Scheduler) Cleanup() {
	if s.workerCacheSyncTicker != nil {
		s.workerCacheSyncTicker.Stop()
	}
	log.Info().Str("replica_id", s.replicaId).Msg("scheduler cleanup completed")
}
