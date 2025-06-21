package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

const (
	requestClaimTTL = 30 * time.Second // How long to hold a claimed request
)

type RequestBacklog struct {
	rdb *common.RedisClient
	mu  sync.Mutex
}

func NewRequestBacklog(rdb *common.RedisClient) *RequestBacklog {
	return &RequestBacklog{rdb: rdb}
}

// Pushes a new container request into the sorted set
func (rb *RequestBacklog) Push(request *types.ContainerRequest) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	jsonData, err := json.Marshal(request)
	if err != nil {
		return err
	}

	// Use the timestamp as the score for sorting
	timestamp := float64(request.Timestamp.UnixNano())
	return rb.rdb.ZAdd(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), redis.Z{Score: timestamp, Member: jsonData}).Err()
}

// Pops the oldest container request from the sorted set
func (rb *RequestBacklog) Pop() (*types.ContainerRequest, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	result, err := rb.rdb.ZPopMin(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), 1).Result()
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, errors.New("backlog empty")
	}

	var poppedItem types.ContainerRequest
	err = json.Unmarshal([]byte(result[0].Member.(string)), &poppedItem)
	if err != nil {
		return nil, err
	}

	return &poppedItem, nil
}

// PopBatch pops multiple container requests from the sorted set in a single operation
func (rb *RequestBacklog) PopBatch(count int) ([]*types.ContainerRequest, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	result, err := rb.rdb.ZPopMin(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), int64(count)).Result()
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, errors.New("backlog empty")
	}

	requests := make([]*types.ContainerRequest, 0, len(result))
	for _, item := range result {
		var poppedItem types.ContainerRequest
		err = json.Unmarshal([]byte(item.Member.(string)), &poppedItem)
		if err != nil {
			// Log error but continue processing other items
			continue
		}
		requests = append(requests, &poppedItem)
	}

	return requests, nil
}

// PopBatchWithClaim pops multiple container requests from the sorted set and claims them for this replica
func (rb *RequestBacklog) PopBatchWithClaim(count int, replicaId string) ([]*types.ContainerRequest, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	result, err := rb.rdb.ZPopMin(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), int64(count)).Result()
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, errors.New("backlog empty")
	}

	requests := make([]*types.ContainerRequest, 0, len(result))
	for _, item := range result {
		var poppedItem types.ContainerRequest
		err = json.Unmarshal([]byte(item.Member.(string)), &poppedItem)
		if err != nil {
			// Log error but continue processing other items
			continue
		}

		// Claim this request for this replica
		claimKey := fmt.Sprintf("scheduler:request_claim:%s", poppedItem.ContainerId)
		claimed, err := rb.rdb.SetNX(context.TODO(), claimKey, replicaId, requestClaimTTL).Result()
		if err != nil {
			// If claiming fails, skip this request
			continue
		}

		if !claimed {
			// Another replica already claimed this request, skip it
			continue
		}

		requests = append(requests, &poppedItem)
	}

	return requests, nil
}

// ReleaseClaim releases the claim on a request
func (rb *RequestBacklog) ReleaseClaim(containerId string) error {
	claimKey := fmt.Sprintf("scheduler:request_claim:%s", containerId)
	return rb.rdb.Del(context.TODO(), claimKey).Err()
}

// Gets the length of the sorted set
func (rb *RequestBacklog) Len() int64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	return rb.rdb.ZCard(context.TODO(), common.RedisKeys.SchedulerContainerRequests()).Val()
}
