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

package controller

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

// Default values for ZoneConfig.
const (
	DefaultTopologyKey = "topology.kubernetes.io/zone"
	DefaultMaxSkew     int32 = 1
)

// ApplyZoneDefaults fills in defaults for ZoneConfig if it is non-nil but
// has zero-value fields. It does NOT create a ZoneConfig if omitted — zone
// awareness is opt-in via zoneAwareness.enabled.
func ApplyZoneDefaults(spec *valkeyiov1alpha1.ValkeyClusterSpec) {
	if spec.ZoneAwareness == nil {
		return
	}
	if spec.ZoneAwareness.TopologyKey == "" {
		spec.ZoneAwareness.TopologyKey = DefaultTopologyKey
	}
	if spec.ZoneAwareness.MaxSkew == 0 {
		spec.ZoneAwareness.MaxSkew = DefaultMaxSkew
	}
}

// injectTopologySpreadConstraints builds TopologySpreadConstraints for a
// Valkey node's pod template when zone awareness is enabled. The constraint
// uses DoNotSchedule to enforce strict zone spreading for the shard's pods.
//
// The label selector matches pods belonging to the same shard (using
// valkey.io/shard-index), so that the primary and replicas of each shard
// are spread across different zones.
func injectTopologySpreadConstraints(
	cluster *valkeyiov1alpha1.ValkeyCluster,
	shardIndex int,
) []corev1.TopologySpreadConstraint {
	zone := cluster.Spec.ZoneAwareness
	if zone == nil || !zone.Enabled {
		return nil
	}

	ApplyZoneDefaults(&cluster.Spec)

	return []corev1.TopologySpreadConstraint{
		{
			MaxSkew:           zone.MaxSkew,
			TopologyKey:       zone.TopologyKey,
			WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/instance": cluster.Name,
					LabelShardIndex:              strconv.Itoa(shardIndex),
				},
			},
		},
	}
}

// checkZoneSpread checks whether the cluster has enough zones to fully
// spread each shard's primary and replicas across different zones. If the
// number of available zones is less than replicas + 1, it emits a warning
// event and sets the ZoneSpreadDegraded condition to True. Otherwise, it
// sets the condition to False.
//
// The zone count is determined by inspecting the topology labels on the
// provided pod list.
func checkZoneSpread(
	recorder events.EventRecorder,
	cluster *valkeyiov1alpha1.ValkeyCluster,
	pods *corev1.PodList,
) {
	zone := cluster.Spec.ZoneAwareness
	if zone == nil || !zone.Enabled {
		return
	}

	ApplyZoneDefaults(&cluster.Spec)

	zoneCount := countDistinctZones(pods, zone.TopologyKey)
	requiredZones := int(cluster.Spec.Replicas) + 1

	if zoneCount < requiredZones {
		recorder.Eventf(
			cluster, nil, corev1.EventTypeWarning,
			"InsufficientZones", "ZoneAwareness",
			fmt.Sprintf("Available zones (%d) < replicas+1 (%d); zone spread is degraded, using best-effort placement",
				zoneCount, requiredZones),
		)
		setCondition(cluster,
			valkeyiov1alpha1.ConditionZoneSpreadDegraded,
			valkeyiov1alpha1.ReasonInsufficientZones,
			fmt.Sprintf("Only %d zones available, need %d for full zone isolation", zoneCount, requiredZones),
			metav1.ConditionTrue,
		)
	} else {
		setCondition(cluster,
			valkeyiov1alpha1.ConditionZoneSpreadDegraded,
			valkeyiov1alpha1.ReasonZoneSpreadOK,
			fmt.Sprintf("%d zones available, sufficient for %d replicas+1", zoneCount, cluster.Spec.Replicas),
			metav1.ConditionFalse,
		)
	}
}

// countDistinctZones counts the number of distinct zone values across all
// pods using the given topology key label.
func countDistinctZones(pods *corev1.PodList, topologyKey string) int {
	zones := make(map[string]struct{})
	for i := range pods.Items {
		if z, ok := pods.Items[i].Labels[topologyKey]; ok && z != "" {
			zones[z] = struct{}{}
		}
		// Also check the node labels via the pod's spec.nodeName — but since
		// we don't have node objects here, we rely on the topology being
		// propagated to pod labels by the scheduler or node affinity.
		// As a fallback, check the well-known topology spread label on the
		// pod's node selector or the pod's node name label.
		if z, ok := pods.Items[i].Spec.NodeSelector[topologyKey]; ok && z != "" {
			zones[z] = struct{}{}
		}
	}
	return len(zones)
}

// pdbName returns the deterministic name for a shard's PodDisruptionBudget.
func pdbName(clusterName string, shardIndex int) string {
	return fmt.Sprintf("%s-shard-%d-pdb", clusterName, shardIndex)
}

// createShardPDB builds a PodDisruptionBudget for a single shard with
// minAvailable=1. The PDB's label selector matches pods belonging to the
// same shard (using valkey.io/shard-index and app.kubernetes.io/instance),
// ensuring at least one pod per shard survives voluntary disruptions.
func createShardPDB(cluster *valkeyiov1alpha1.ValkeyCluster, shardIndex int) *policyv1.PodDisruptionBudget {
	minAvailable := intstr.FromInt(1)
	pdbLabels := labels(cluster)
	pdbLabels[LabelShardIndex] = strconv.Itoa(shardIndex)

	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pdbName(cluster.Name, shardIndex),
			Namespace: cluster.Namespace,
			Labels:    pdbLabels,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/instance": cluster.Name,
					LabelShardIndex:              strconv.Itoa(shardIndex),
				},
			},
		},
	}
}

