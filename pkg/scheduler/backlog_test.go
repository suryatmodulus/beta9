package scheduler

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

func NewRequestBacklogForTest(rdb *common.RedisClient) *RequestBacklog {
	return &RequestBacklog{rdb: rdb}
}

func TestRequestBacklogOrdering(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.NoError(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.NoError(t, err)

	rb := NewRequestBacklogForTest(redisClient)

	// Create some container requests with increasing timestamps
	req1 := &types.ContainerRequest{Timestamp: time.Unix(1, 0)}
	req2 := &types.ContainerRequest{Timestamp: time.Unix(2, 0)}
	req3 := &types.ContainerRequest{Timestamp: time.Unix(3, 0)}

	// Push them in out of order
	if err := rb.Push(req2); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}
	if err := rb.Push(req1); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}
	if err := rb.Push(req3); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}

	// Pop them and verify the order based on timestamp
	var poppedReq *types.ContainerRequest

	poppedReq, err = rb.Pop()
	if err != nil {
		t.Fatalf("Could not pop request: %v", err)
	}
	if poppedReq.Timestamp.Unix() != req1.Timestamp.Unix() {
		t.Errorf("Expected timestamp %v, got %v", req1.Timestamp.Unix(), poppedReq.Timestamp.Unix())
	}

	poppedReq, err = rb.Pop()
	if err != nil {
		t.Fatalf("Could not pop request: %v", err)
	}
	if poppedReq.Timestamp.Unix() != req2.Timestamp.Unix() {
		t.Errorf("Expected timestamp %v, got %v", req2.Timestamp.Unix(), poppedReq.Timestamp.Unix())
	}

	poppedReq, err = rb.Pop()
	if err != nil {
		t.Fatalf("Could not pop request: %v", err)
	}
	if poppedReq.Timestamp.Unix() != req3.Timestamp.Unix() {
		t.Errorf("Expected timestamp %v, got %v", req3.Timestamp.Unix(), poppedReq.Timestamp.Unix())
	}
}

func TestRequestBacklogBatchPop(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.NoError(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.NoError(t, err)

	rb := NewRequestBacklogForTest(redisClient)

	// Create some container requests with increasing timestamps
	req1 := &types.ContainerRequest{Timestamp: time.Unix(1, 0)}
	req2 := &types.ContainerRequest{Timestamp: time.Unix(2, 0)}
	req3 := &types.ContainerRequest{Timestamp: time.Unix(3, 0)}
	req4 := &types.ContainerRequest{Timestamp: time.Unix(4, 0)}

	// Push them in out of order
	if err := rb.Push(req3); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}
	if err := rb.Push(req1); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}
	if err := rb.Push(req4); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}
	if err := rb.Push(req2); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}

	// Pop batch and verify the order based on timestamp
	poppedReqs, err := rb.PopBatch(3)
	if err != nil {
		t.Fatalf("Could not pop batch: %v", err)
	}

	assert.Equal(t, 3, len(poppedReqs))
	assert.Equal(t, req1.Timestamp.Unix(), poppedReqs[0].Timestamp.Unix())
	assert.Equal(t, req2.Timestamp.Unix(), poppedReqs[1].Timestamp.Unix())
	assert.Equal(t, req3.Timestamp.Unix(), poppedReqs[2].Timestamp.Unix())

	// Pop the remaining request
	poppedReqs, err = rb.PopBatch(1)
	if err != nil {
		t.Fatalf("Could not pop remaining request: %v", err)
	}

	assert.Equal(t, 1, len(poppedReqs))
	assert.Equal(t, req4.Timestamp.Unix(), poppedReqs[0].Timestamp.Unix())

	// Verify backlog is empty
	assert.Equal(t, int64(0), rb.Len())
}

func TestRequestBacklogDistributedClaiming(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.NoError(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.NoError(t, err)

	rb := NewRequestBacklogForTest(redisClient)

	// Create a container request
	req := &types.ContainerRequest{
		ContainerId: "test-container-1",
		Timestamp:   time.Now(),
	}

	// Push the request
	if err := rb.Push(req); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}

	// Simulate two replicas trying to claim the same request
	replica1 := "replica-1"
	replica2 := "replica-2"

	// Replica 1 claims the request
	claimed1, err := rb.PopBatchWithClaim(1, replica1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(claimed1))
	assert.Equal(t, req.ContainerId, claimed1[0].ContainerId)

	// Replica 2 tries to claim the same request (should fail)
	claimed2, err := rb.PopBatchWithClaim(1, replica2)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(claimed2)) // Should not get any requests

	// Release the claim from replica 1
	err = rb.ReleaseClaim(req.ContainerId)
	assert.NoError(t, err)

	// Now replica 2 should be able to claim it
	claimed2, err = rb.PopBatchWithClaim(1, replica2)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(claimed2))
	assert.Equal(t, req.ContainerId, claimed2[0].ContainerId)
}
