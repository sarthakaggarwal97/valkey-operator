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
	"sync"

	vclient "github.com/valkey-io/valkey-go"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

const (
	// migrateBatchSize is the number of keys to migrate per GETKEYSINSLOT call.
	migrateBatchSize = 100

	// migrateTimeoutMs is the timeout in milliseconds for the MIGRATE command.
	migrateTimeoutMs = 5000

	// ConditionResharding is the condition type for resharding progress.
	ConditionResharding = "Resharding"

	// ReasonReshardInProgress indicates resharding is underway.
	ReasonReshardInProgress = "ReshardInProgress"

	// ReasonReshardComplete indicates resharding finished successfully.
	ReasonReshardComplete = "ReshardComplete"

	// ReasonReshardFailed indicates resharding failed.
	ReasonReshardFailed = "ReshardFailed"
)

// SlotMigration describes a single slot to move between nodes.
type SlotMigration struct {
	Slot       int
	SourceID   string
	SourceAddr string
	SourcePort int
	DestID     string
	DestAddr   string
	DestPort   int
	// Runtime fields populated by ReshardController before execution.
	sourceClient vclient.Client
	destClient   vclient.Client
}

// MigrationPlan describes the full set of slot moves for a resharding operation.
type MigrationPlan struct {
	Migrations []SlotMigration
}

// ReshardController manages slot migration between primaries for scale-out/scale-in.
type ReshardController struct {
	client   client.Client
	recorder events.EventRecorder
}

// NewReshardController creates a ReshardController.
func NewReshardController(c client.Client, recorder events.EventRecorder) *ReshardController {
	return &ReshardController{
		client:   c,
		recorder: recorder,
	}
}

// ComputeMigrationPlan calculates the minimal set of slot moves to rebalance
// from the current shard layout to targetShards, keeping slot ranges contiguous
// where possible. It computes the target slot assignment, then diffs against
// the current assignment to produce only the necessary moves.
func (rc *ReshardController) ComputeMigrationPlan(
	state *ClusterState,
	targetShards int,
) (*MigrationPlan, error) {
	if targetShards < 1 {
		return nil, fmt.Errorf("targetShards must be >= 1, got %d", targetShards)
	}
	if len(state.Shards) == 0 {
		return nil, fmt.Errorf("cluster has no shards")
	}

	// Compute the target slot ranges for the desired shard count.
	targetRanges, err := ComputeSlotRanges(targetShards)
	if err != nil {
		return nil, fmt.Errorf("computing target slot ranges: %w", err)
	}

	// Build a map of current slot ownership: slot → (shardIndex, nodeState).
	currentOwner := make(map[int]int, TotalSlots) // slot → shard index
	for i, shard := range state.Shards {
		for _, sr := range shard.Slots {
			for slot := sr.Start; slot <= sr.End; slot++ {
				currentOwner[slot] = i
			}
		}
	}

	// Build a map of target slot ownership: slot → target shard index.
	targetOwner := make(map[int]int, TotalSlots)
	for i, sr := range targetRanges {
		for slot := sr.Start; slot <= sr.End; slot++ {
			targetOwner[slot] = i
		}
	}

	// For scale-out, new shards (index >= len(state.Shards)) need a destination
	// node. We'll map target shard indices to existing shard primaries where
	// possible, and leave new shard indices for the caller to handle after
	// new primaries are admitted.
	//
	// For scale-in, slots from removed shards move to remaining shards.

	plan := &MigrationPlan{}

	for slot := 0; slot < TotalSlots; slot++ {
		curShard, hasCurrent := currentOwner[slot]
		tgtShard := targetOwner[slot]

		if !hasCurrent {
			// Slot is unassigned — nothing to migrate.
			continue
		}

		if tgtShard == curShard {
			// Slot stays on the same shard — no move needed.
			continue
		}

		// The slot needs to move. Source is the current shard's primary.
		srcShard := state.Shards[curShard]
		srcPrimary := srcShard.GetPrimaryNode()
		if srcPrimary == nil {
			return nil, fmt.Errorf("shard %d has no primary for slot %d migration", curShard, slot)
		}

		// Destination: if the target shard index exists in the current state,
		// use its primary. Otherwise, skip — the caller must admit new primaries first.
		if tgtShard >= len(state.Shards) {
			continue
		}
		dstShard := state.Shards[tgtShard]
		dstPrimary := dstShard.GetPrimaryNode()
		if dstPrimary == nil {
			return nil, fmt.Errorf("target shard %d has no primary for slot %d migration", tgtShard, slot)
		}

		plan.Migrations = append(plan.Migrations, SlotMigration{
			Slot:       slot,
			SourceID:   srcPrimary.Id,
			SourceAddr: srcPrimary.Address,
			SourcePort: srcPrimary.Port,
			DestID:     dstPrimary.Id,
			DestAddr:   dstPrimary.Address,
			DestPort:   dstPrimary.Port,
		})
	}

	return plan, nil
}

// ExecuteMigration performs a single slot migration using the Valkey 8+ protocol:
//
//  1. CLUSTER SETSLOT <slot> IMPORTING <sourceID> on destination
//  2. CLUSTER SETSLOT <slot> MIGRATING <destID> on source
//  3. Loop: CLUSTER GETKEYSINSLOT <slot> <count> + MIGRATE with REPLACE
//  4. CLUSTER SETSLOT <slot> NODE <destID> on both nodes
//
// On failure at any step after IMPORTING, rollback is performed by calling
// CLUSTER SETSLOT STABLE on both source and destination nodes.
func (rc *ReshardController) ExecuteMigration(
	ctx context.Context,
	m SlotMigration,
) error {
	log := logf.FromContext(ctx)
	srcClient := m.sourceClient
	dstClient := m.destClient

	// Step 1: Set importing state on destination.
	if err := dstClient.Do(ctx, dstClient.B().ClusterSetslot().
		Slot(int64(m.Slot)).Importing().NodeId(m.SourceID).Build()).Error(); err != nil {
		return fmt.Errorf("SETSLOT %d IMPORTING on dest: %w", m.Slot, err)
	}

	// Step 2: Set migrating state on source.
	if err := srcClient.Do(ctx, srcClient.B().ClusterSetslot().
		Slot(int64(m.Slot)).Migrating().NodeId(m.DestID).Build()).Error(); err != nil {
		rc.rollbackSlot(ctx, m)
		return fmt.Errorf("SETSLOT %d MIGRATING on src: %w", m.Slot, err)
	}

	// Step 3: Migrate keys in batches.
	for {
		keys, err := srcClient.Do(ctx, srcClient.B().ClusterGetkeysinslot().
			Slot(int64(m.Slot)).Count(migrateBatchSize).Build()).AsStrSlice()
		if err != nil {
			rc.rollbackSlot(ctx, m)
			return fmt.Errorf("GETKEYSINSLOT %d: %w", m.Slot, err)
		}
		if len(keys) == 0 {
			break
		}

		// MIGRATE with REPLACE option for safe retries (avoids BUSYKEY errors).
		if err := srcClient.Do(ctx, srcClient.B().Migrate().
			Host(m.DestAddr).Port(int64(m.DestPort)).Key("").
			DestinationDb(0).Timeout(migrateTimeoutMs).Replace().
			Keys(keys...).Build()).Error(); err != nil {
			rc.rollbackSlot(ctx, m)
			return fmt.Errorf("MIGRATE slot %d keys: %w", m.Slot, err)
		}
	}

	// Step 4: Finalize slot ownership on destination.
	if err := dstClient.Do(ctx, dstClient.B().ClusterSetslot().
		Slot(int64(m.Slot)).Node().NodeId(m.DestID).Build()).Error(); err != nil {
		return fmt.Errorf("SETSLOT %d NODE on dest: %w", m.Slot, err)
	}

	// Finalize on source. Non-fatal: gossip will propagate ownership.
	if err := srcClient.Do(ctx, srcClient.B().ClusterSetslot().
		Slot(int64(m.Slot)).Node().NodeId(m.DestID).Build()).Error(); err != nil {
		log.Info("SETSLOT NODE on source failed, gossip will propagate",
			"slot", m.Slot, "err", err)
	}

	return nil
}

// rollbackSlot sends CLUSTER SETSLOT STABLE on both source and destination
// to revert a failed migration, restoring the slot to its pre-migration state.
func (rc *ReshardController) rollbackSlot(ctx context.Context, m SlotMigration) {
	log := logf.FromContext(ctx)

	if m.destClient != nil {
		if err := m.destClient.Do(ctx, m.destClient.B().ClusterSetslot().
			Slot(int64(m.Slot)).Stable().Build()).Error(); err != nil {
			log.Error(err, "rollback SETSLOT STABLE on dest failed", "slot", m.Slot)
		}
	}

	if m.sourceClient != nil {
		if err := m.sourceClient.Do(ctx, m.sourceClient.B().ClusterSetslot().
			Slot(int64(m.Slot)).Stable().Build()).Error(); err != nil {
			log.Error(err, "rollback SETSLOT STABLE on src failed", "slot", m.Slot)
		}
	}
}

// ReshardCluster orchestrates the full resharding workflow with bounded
// parallelism (one migration in progress per source node). It:
//  1. Computes the migration plan
//  2. Populates runtime client fields on each migration
//  3. Executes migrations grouped by source, one at a time per source
//  4. Reports progress via cluster status conditions
func (rc *ReshardController) ReshardCluster(
	ctx context.Context,
	state *ClusterState,
	cluster *valkeyiov1alpha1.ValkeyCluster,
) error {
	log := logf.FromContext(ctx)

	targetShards := int(cluster.Spec.Shards)
	plan, err := rc.ComputeMigrationPlan(state, targetShards)
	if err != nil {
		rc.setReshardCondition(ctx, cluster, metav1.ConditionFalse, ReasonReshardFailed,
			fmt.Sprintf("Failed to compute migration plan: %v", err))
		return fmt.Errorf("computing migration plan: %w", err)
	}

	if len(plan.Migrations) == 0 {
		log.Info("no slot migrations needed")
		return nil
	}

	log.Info("resharding cluster",
		"totalMigrations", len(plan.Migrations),
		"currentShards", len(state.Shards),
		"targetShards", targetShards)

	// Set resharding in-progress condition.
	rc.setReshardCondition(ctx, cluster, metav1.ConditionTrue, ReasonReshardInProgress,
		fmt.Sprintf("Migrating %d slots", len(plan.Migrations)))

	// Build a client cache keyed by node ID to avoid creating duplicate connections.
	clientCache := make(map[string]vclient.Client)
	for _, shard := range state.Shards {
		for _, node := range shard.Nodes {
			if node.Client != nil {
				clientCache[node.Id] = node.Client
			}
		}
	}

	// Populate runtime client fields on each migration.
	for i := range plan.Migrations {
		m := &plan.Migrations[i]
		srcClient, ok := clientCache[m.SourceID]
		if !ok {
			return fmt.Errorf("no client for source node %s", m.SourceID)
		}
		dstClient, ok := clientCache[m.DestID]
		if !ok {
			return fmt.Errorf("no client for dest node %s", m.DestID)
		}
		m.sourceClient = srcClient
		m.destClient = dstClient
	}

	// Group migrations by source node for bounded parallelism.
	bySource := make(map[string][]SlotMigration)
	for _, m := range plan.Migrations {
		bySource[m.SourceID] = append(bySource[m.SourceID], m)
	}

	// Execute migrations: one goroutine per source node (bounded parallelism).
	var (
		mu       sync.Mutex
		errCount int
		migrated int
	)

	var wg sync.WaitGroup
	for sourceID, migrations := range bySource {
		wg.Add(1)
		go func(srcID string, migs []SlotMigration) {
			defer wg.Done()
			for _, m := range migs {
				if ctx.Err() != nil {
					return
				}
				if err := rc.ExecuteMigration(ctx, m); err != nil {
					log.Error(err, "slot migration failed",
						"slot", m.Slot, "source", srcID, "dest", m.DestID)
					mu.Lock()
					errCount++
					mu.Unlock()
					// Continue with remaining slots from this source.
					continue
				}
				mu.Lock()
				migrated++
				mu.Unlock()
			}
		}(sourceID, migrations)
	}
	wg.Wait()

	// Report final status.
	if errCount > 0 {
		rc.setReshardCondition(ctx, cluster, metav1.ConditionFalse, ReasonReshardFailed,
			fmt.Sprintf("Resharding completed with %d errors (%d/%d slots migrated)",
				errCount, migrated, len(plan.Migrations)))
		return fmt.Errorf("resharding completed with %d errors", errCount)
	}

	log.Info("resharding complete", "migratedSlots", migrated)
	rc.setReshardCondition(ctx, cluster, metav1.ConditionFalse, ReasonReshardComplete,
		fmt.Sprintf("Successfully migrated %d slots", migrated))

	return nil
}

// setReshardCondition updates the Resharding condition on the cluster status.
func (rc *ReshardController) setReshardCondition(
	ctx context.Context,
	cluster *valkeyiov1alpha1.ValkeyCluster,
	status metav1.ConditionStatus,
	reason, message string,
) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               ConditionResharding,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: cluster.Generation,
		LastTransitionTime: metav1.Now(),
	})

	if err := rc.client.Status().Update(ctx, cluster); err != nil {
		log := logf.FromContext(ctx)
		log.Error(err, "failed to update resharding condition")
	}
}
