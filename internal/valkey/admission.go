/*
Copyright 2025 Valkey Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package valkey

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

// gossipPollInterval is the interval between gossip convergence polls.
const gossipPollInterval = 2 * time.Second

// gossipTimeout is the maximum time to wait for gossip convergence.
const gossipTimeout = 30 * time.Second

// gossipPollConcurrency is the max number of parallel gossip polls.
const gossipPollConcurrency = 10

// BatchResult summarizes a batch admission attempt.
type BatchResult struct {
	Admitted          int
	Failed            []AdmissionError
	Duration          time.Duration
	GossipConvergence time.Duration
}

// AdmissionError records a per-node failure during batch admission.
type AdmissionError struct {
	NodeAddress string
	Err         error
}

// SlotAssigner is called to assign slots to a new primary node.
// The implementation lives in the controller package and handles
// CLUSTER ADDSLOTSRANGE and event recording.
type SlotAssigner func(ctx context.Context, node *NodeState) error

// ReplicaAttacher is called to attach a replica to its shard's primary.
// primaryID is the Valkey node ID of the primary to replicate.
type ReplicaAttacher func(ctx context.Context, node *NodeState, primaryID string) error

// AdmissionManager handles batch introduction of pending nodes into the cluster.
// It partitions nodes by role (primaries first), MEETs them in parallel via
// SeedMeetStrategy, waits for gossip convergence, then assigns slots to
// primaries (Phase 3a) and attaches replicas (Phase 3b).
type AdmissionManager struct {
	config       valkeyiov1alpha1.AdmissionConfig
	seedStrategy *SeedMeetStrategy
}

// NewAdmissionManager creates an AdmissionManager with the given config and
// seed meet strategy.
func NewAdmissionManager(config valkeyiov1alpha1.AdmissionConfig, seedStrategy *SeedMeetStrategy) *AdmissionManager {
	return &AdmissionManager{
		config:       config,
		seedStrategy: seedStrategy,
	}
}

// ProcessPendingNodes admits up to config.Parallelism nodes in one reconcile.
// Primaries are admitted before replicas using a two-phase approach:
//   - Phase 1: MEET all nodes in the batch in parallel (seed-based)
//   - Phase 2: Wait for gossip propagation
//   - Phase 3a: Assign slots to primaries (sequential, updating batchPrimaryMap)
//   - Phase 3b: Attach replicas (sequential, checking batchPrimaryMap first)
//
// The assignSlots callback is called for each new primary that needs slots.
// The attachReplica callback is called for each replica with the primary's node ID.
// The findPrimary function looks up pre-existing primaries by shard index.
func (am *AdmissionManager) ProcessPendingNodes(
	ctx context.Context,
	state *ClusterState,
	pods *corev1.PodList,
	cluster *valkeyiov1alpha1.ValkeyCluster,
	assignSlots SlotAssigner,
	attachReplica ReplicaAttacher,
	findPrimary func(state *ClusterState, shardIndex int, pods *corev1.PodList) (nodeID string, ip string),
	podRoleAndShardFn func(address string, pods *corev1.PodList) (role string, shardIndex int),
	shardExistsFn func(state *ClusterState, shardIndex int, pods *corev1.PodList) bool,
) (*BatchResult, error) {
	log := logf.FromContext(ctx)
	result := &BatchResult{}
	start := time.Now()
	defer func() { result.Duration = time.Since(start) }()

	// Partition: primaries first, then replicas.
	primaries, replicas := partitionByRole(state.PendingNodes, pods, podRoleAndShardFn)
	pending := append(primaries, replicas...)

	// Cap at parallelism.
	batchSize := int(am.config.Parallelism)
	if len(pending) < batchSize {
		batchSize = len(pending)
	}
	if batchSize == 0 {
		return result, nil
	}
	batch := pending[:batchSize]

	log.V(1).Info("processing pending nodes batch",
		"total", len(pending), "batchSize", batchSize,
		"primaries", len(primaries), "replicas", len(replicas))

	// Phase 1: MEET all nodes in batch (parallel, seed-based).
	seeds := am.seedStrategy.SelectSeeds(state, pods)
	if len(seeds) == 0 {
		// Bootstrap path: cluster has no admitted primaries yet. Seed-based MEET
		// cannot run, so assign slots to exactly one primary and let subsequent
		// reconciles use it as the first seed.
		for _, node := range batch {
			role, shardIndex := podRoleAndShardFn(node.Address, pods)
			if role != "primary" {
				continue
			}
			if shardExistsFn(state, shardIndex, pods) {
				continue
			}
			if err := assignSlots(ctx, node); err != nil {
				result.Failed = append(result.Failed, AdmissionError{
					NodeAddress: node.Address,
					Err:         fmt.Errorf("bootstrap slot assignment failed: %w", err),
				})
				return result, nil
			}
			result.Admitted++
			log.V(1).Info("bootstrap primary admitted without seed MEET", "node", node.Address)
			return result, nil
		}
		return result, fmt.Errorf("no seed nodes available for CLUSTER MEET and no bootstrap primary found")
	}

	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(batchSize)

	for _, node := range batch {
		node := node
		g.Go(func() error {
			if err := am.seedStrategy.MeetSeeds(gctx, node, seeds); err != nil {
				mu.Lock()
				result.Failed = append(result.Failed, AdmissionError{
					NodeAddress: node.Address,
					Err:         fmt.Errorf("CLUSTER MEET failed: %w", err),
				})
				mu.Unlock()
				// Don't abort the batch for one failure.
				return nil
			}
			return nil
		})
	}
	_ = g.Wait()

	// Build the set of successfully met nodes (exclude failures).
	metBatch := filterSuccessful(batch, result.Failed)
	if len(metBatch) == 0 {
		return result, nil
	}

	// Phase 2: Wait for gossip propagation.
	gossipStart := time.Now()
	if err := am.waitForGossipConvergence(ctx, metBatch, state); err != nil {
		result.GossipConvergence = time.Since(gossipStart)
		return result, fmt.Errorf("gossip propagation timeout: %w", err)
	}
	result.GossipConvergence = time.Since(gossipStart)

	// Phase 3a: Assign slots to primaries (sequential to avoid slot overlap).
	// batchPrimaryMap tracks shardIndex → nodeId for primaries assigned in this batch.
	batchPrimaryMap := map[int]string{}
	for _, node := range metBatch {
		role, shardIndex := podRoleAndShardFn(node.Address, pods)
		if role != "primary" {
			continue
		}
		// If shard already exists in topology, this is usually a replacement
		// node-index=0 pod after failover. Attach it as a replica instead of
		// assigning new slots.
		if shardExistsFn(state, shardIndex, pods) {
			primaryID, _ := findPrimary(state, shardIndex, pods)
			if primaryID == "" {
				result.Failed = append(result.Failed, AdmissionError{
					NodeAddress: node.Address,
					Err:         fmt.Errorf("no primary found for existing shard %d", shardIndex),
				})
				continue
			}
			if err := attachReplica(ctx, node, primaryID); err != nil {
				result.Failed = append(result.Failed, AdmissionError{
					NodeAddress: node.Address,
					Err:         fmt.Errorf("replica attach failed: %w", err),
				})
				continue
			}
			log.V(1).Info("shard already exists in topology, attached replacement node as replica",
				"shardIndex", shardIndex, "node", node.Address)
			result.Admitted++
			continue
		}
		if err := assignSlots(ctx, node); err != nil {
			result.Failed = append(result.Failed, AdmissionError{
				NodeAddress: node.Address,
				Err:         fmt.Errorf("slot assignment failed: %w", err),
			})
			continue
		}
		batchPrimaryMap[shardIndex] = node.Id
		result.Admitted++
	}

	// Phase 3b: Attach replicas.
	// For each replica, first check batchPrimaryMap for primaries assigned in
	// this batch, then fall back to findPrimary for pre-existing primaries.
	for _, node := range metBatch {
		role, shardIndex := podRoleAndShardFn(node.Address, pods)
		if role != "replica" {
			continue
		}

		var primaryID string
		if id, ok := batchPrimaryMap[shardIndex]; ok {
			// Primary was just assigned in this batch.
			primaryID = id
		} else {
			// Fall back to pre-existing primary in cluster state.
			id, _ := findPrimary(state, shardIndex, pods)
			if id == "" {
				result.Failed = append(result.Failed, AdmissionError{
					NodeAddress: node.Address,
					Err:         fmt.Errorf("no primary found for shard %d", shardIndex),
				})
				continue
			}
			primaryID = id
		}

		if err := attachReplica(ctx, node, primaryID); err != nil {
			result.Failed = append(result.Failed, AdmissionError{
				NodeAddress: node.Address,
				Err:         fmt.Errorf("replica attach failed: %w", err),
			})
			continue
		}
		result.Admitted++
	}

	log.V(1).Info("batch admission complete",
		"admitted", result.Admitted, "failed", len(result.Failed),
		"gossipConvergence", result.GossipConvergence)

	return result, nil
}

// partitionByRole splits pending nodes into primaries and replicas.
// All primaries come before all replicas, ensuring primaries get slots
// assigned before any replica attempts CLUSTER REPLICATE.
func partitionByRole(
	nodes []*NodeState,
	pods *corev1.PodList,
	podRoleAndShardFn func(address string, pods *corev1.PodList) (string, int),
) (primaries, replicas []*NodeState) {
	for _, node := range nodes {
		role, _ := podRoleAndShardFn(node.Address, pods)
		if role == "primary" {
			primaries = append(primaries, node)
		} else {
			replicas = append(replicas, node)
		}
	}
	return primaries, replicas
}

// waitForGossipConvergence polls cluster_known_nodes on ALL nodes in the
// cluster (existing nodes from state.Shards plus newBatch) until every node
// sees the expected total count or a 30-second timeout is reached.
//
// This replicates the failover-tool's convergence verification pattern:
// poll all nodes in parallel with bounded concurrency.
func (am *AdmissionManager) waitForGossipConvergence(
	ctx context.Context,
	newBatch []*NodeState,
	state *ClusterState,
) error {
	log := logf.FromContext(ctx)

	// Collect all existing nodes from shards.
	var existingNodes []*NodeState
	for _, shard := range state.Shards {
		existingNodes = append(existingNodes, shard.Nodes...)
	}
	expectedCount := len(existingNodes) + len(newBatch)

	// Combine all nodes to poll.
	allNodes := make([]*NodeState, 0, expectedCount)
	allNodes = append(allNodes, existingNodes...)
	allNodes = append(allNodes, newBatch...)

	deadline := time.Now().Add(gossipTimeout)

	for time.Now().Before(deadline) {
		allConverged := true
		var mu sync.Mutex

		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(gossipPollConcurrency)

		for _, node := range allNodes {
			node := node
			g.Go(func() error {
				result := node.Client.Do(gctx, node.Client.B().ClusterInfo().Build())
				if err := result.Error(); err != nil {
					// Transient error, will retry on next poll.
					mu.Lock()
					allConverged = false
					mu.Unlock()
					return nil
				}
				infoStr, _ := result.ToString()
				knownNodes := parseClusterKnownNodes(infoStr)
				if knownNodes < expectedCount {
					mu.Lock()
					allConverged = false
					mu.Unlock()
				}
				return nil
			})
		}
		_ = g.Wait()

		if allConverged {
			log.V(1).Info("gossip convergence achieved",
				"expectedCount", expectedCount)
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(gossipPollInterval):
		}
	}

	return fmt.Errorf("gossip convergence timeout: not all nodes see %d nodes after %v",
		expectedCount, gossipTimeout)
}

// parseClusterKnownNodes extracts the cluster_known_nodes value from a
// CLUSTER INFO response string.
func parseClusterKnownNodes(info string) int {
	for _, line := range strings.Split(info, "\r\n") {
		if key, val, ok := strings.Cut(line, ":"); ok {
			if key == "cluster_known_nodes" {
				n, err := strconv.Atoi(strings.TrimSpace(val))
				if err != nil {
					return 0
				}
				return n
			}
		}
	}
	return 0
}

// filterSuccessful returns nodes from batch that are not in the failed list.
func filterSuccessful(batch []*NodeState, failed []AdmissionError) []*NodeState {
	failedAddrs := make(map[string]bool, len(failed))
	for _, f := range failed {
		failedAddrs[f.NodeAddress] = true
	}
	var successful []*NodeState
	for _, node := range batch {
		if !failedAddrs[node.Address] {
			successful = append(successful, node)
		}
	}
	return successful
}
