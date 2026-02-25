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
	"slices"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// zoneLabel is the well-known Kubernetes topology label for availability zones.
const zoneLabel = "topology.kubernetes.io/zone"

// meetMaxRetries is the number of retry attempts for a transient CLUSTER MEET failure.
const meetMaxRetries = 3

// meetRetryDelay is the delay between CLUSTER MEET retry attempts.
const meetRetryDelay = 500 * time.Millisecond

// SeedMeetStrategy introduces a new node to the cluster by MEETing a small
// number of seed nodes. Gossip propagation handles the rest.
type SeedMeetStrategy struct {
	SeedCount int
}

// NewSeedMeetStrategy creates a SeedMeetStrategy with the given seed count.
func NewSeedMeetStrategy(seedCount int) *SeedMeetStrategy {
	return &SeedMeetStrategy{SeedCount: seedCount}
}

// SelectSeeds picks up to seedCount primaries distributed across zones.
// When zone topology labels are available on pods, seeds are distributed
// across as many distinct zones as possible using round-robin zone selection.
// When zone labels are absent, falls back to round-robin across primaries.
//
// Returns exactly min(seedCount, totalPrimaries) seed nodes.
func (s *SeedMeetStrategy) SelectSeeds(
	state *ClusterState,
	pods *corev1.PodList,
) []*NodeState {
	// Collect all primaries from shards.
	primaries := collectPrimaries(state)
	if len(primaries) == 0 {
		return nil
	}

	// Group primaries by zone.
	zoneMap := groupPrimariesByZone(primaries, pods)

	// Determine the target count: min(seedCount, totalPrimaries).
	target := s.SeedCount
	if len(primaries) < target {
		target = len(primaries)
	}

	// Get sorted zone keys for deterministic round-robin.
	zones := sortedZoneKeys(zoneMap)

	// Round-robin across zones to pick seeds.
	seeds := make([]*NodeState, 0, target)
	zoneOffset := make(map[string]int, len(zones))

	for len(seeds) < target {
		added := false
		for _, zone := range zones {
			if len(seeds) >= target {
				break
			}
			offset := zoneOffset[zone]
			if offset < len(zoneMap[zone]) {
				seeds = append(seeds, zoneMap[zone][offset])
				zoneOffset[zone] = offset + 1
				added = true
			}
		}
		// Safety: if no zone had remaining nodes, break to avoid infinite loop.
		if !added {
			break
		}
	}

	return seeds
}

// MeetSeeds issues CLUSTER MEET from newNode to each seed node.
// Retries on transient failure up to meetMaxRetries times per seed.
func (s *SeedMeetStrategy) MeetSeeds(
	ctx context.Context,
	newNode *NodeState,
	seeds []*NodeState,
) error {
	log := logf.FromContext(ctx)

	for _, seed := range seeds {
		if err := meetWithRetry(ctx, newNode, seed); err != nil {
			log.Error(err, "CLUSTER MEET failed after retries",
				"from", newNode.Address, "to", seed.Address)
			return fmt.Errorf("CLUSTER MEET from %s to %s failed: %w",
				newNode.Address, seed.Address, err)
		}
		log.V(1).Info("CLUSTER MEET succeeded",
			"from", newNode.Address, "to", seed.Address)
	}
	return nil
}

// meetWithRetry attempts CLUSTER MEET from src to dst, retrying on transient
// failures up to meetMaxRetries times.
func meetWithRetry(ctx context.Context, src, dst *NodeState) error {
	var lastErr error
	for attempt := 0; attempt <= meetMaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(meetRetryDelay):
			}
		}

		err := src.Client.Do(ctx,
			src.Client.B().ClusterMeet().
				Ip(dst.Address).
				Port(int64(dst.Port)).
				Build(),
		).Error()
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

// collectPrimaries returns all primary nodes from the cluster state's shards.
func collectPrimaries(state *ClusterState) []*NodeState {
	var primaries []*NodeState
	for _, shard := range state.Shards {
		primary := shard.GetPrimaryNode()
		if primary != nil {
			primaries = append(primaries, primary)
		}
	}
	return primaries
}

// groupPrimariesByZone groups primaries by their zone label from the pod list.
// Pods without a zone label are grouped under an empty string key.
func groupPrimariesByZone(primaries []*NodeState, pods *corev1.PodList) map[string][]*NodeState {
	zoneMap := make(map[string][]*NodeState)
	for _, primary := range primaries {
		zone := podZone(primary.Address, pods)
		zoneMap[zone] = append(zoneMap[zone], primary)
	}
	return zoneMap
}

// podZone looks up the zone label for the pod matching the given IP address.
// Returns an empty string if the pod is not found or has no zone label.
func podZone(address string, pods *corev1.PodList) string {
	idx := slices.IndexFunc(pods.Items, func(p corev1.Pod) bool {
		return p.Status.PodIP == address
	})
	if idx == -1 {
		return ""
	}
	return pods.Items[idx].Labels[zoneLabel]
}

// sortedZoneKeys returns the keys of a zone map in sorted order for
// deterministic round-robin selection.
func sortedZoneKeys(zoneMap map[string][]*NodeState) []string {
	zones := make([]string, 0, len(zoneMap))
	for z := range zoneMap {
		zones = append(zones, z)
	}
	sort.Strings(zones)
	return zones
}
