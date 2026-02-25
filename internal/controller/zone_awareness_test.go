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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

var _ = Describe("Zone Awareness", func() {

	Describe("injectTopologySpreadConstraints", func() {
		It("should return nil when zone awareness is nil", func() {
			cluster := &valkeyiov1alpha1.ValkeyCluster{
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					Shards:   3,
					Replicas: 1,
				},
			}
			constraints := injectTopologySpreadConstraints(cluster, 0)
			Expect(constraints).To(BeNil())
		})

		It("should return nil when zone awareness is disabled", func() {
			cluster := &valkeyiov1alpha1.ValkeyCluster{
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					Shards:   3,
					Replicas: 1,
					ZoneAwareness: &valkeyiov1alpha1.ZoneConfig{
						Enabled: false,
					},
				},
			}
			constraints := injectTopologySpreadConstraints(cluster, 0)
			Expect(constraints).To(BeNil())
		})

		It("should inject constraints with configured topologyKey and maxSkew", func() {
			cluster := &valkeyiov1alpha1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "mycluster"},
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					Shards:   3,
					Replicas: 1,
					ZoneAwareness: &valkeyiov1alpha1.ZoneConfig{
						Enabled:     true,
						TopologyKey: "topology.kubernetes.io/zone",
						MaxSkew:     1,
					},
				},
			}
			constraints := injectTopologySpreadConstraints(cluster, 2)
			Expect(constraints).To(HaveLen(1))

			c := constraints[0]
			Expect(c.MaxSkew).To(Equal(int32(1)))
			Expect(c.TopologyKey).To(Equal("topology.kubernetes.io/zone"))
			Expect(c.WhenUnsatisfiable).To(Equal(corev1.DoNotSchedule))
			Expect(c.LabelSelector).NotTo(BeNil())
			Expect(c.LabelSelector.MatchLabels).To(HaveKeyWithValue("app.kubernetes.io/instance", "mycluster"))
			Expect(c.LabelSelector.MatchLabels).To(HaveKeyWithValue(LabelShardIndex, "2"))
		})

		It("should use default topologyKey when empty", func() {
			cluster := &valkeyiov1alpha1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					Shards:   3,
					Replicas: 1,
					ZoneAwareness: &valkeyiov1alpha1.ZoneConfig{
						Enabled: true,
					},
				},
			}
			constraints := injectTopologySpreadConstraints(cluster, 0)
			Expect(constraints).To(HaveLen(1))
			Expect(constraints[0].TopologyKey).To(Equal(DefaultTopologyKey))
			Expect(constraints[0].MaxSkew).To(Equal(DefaultMaxSkew))
		})

		It("should use custom maxSkew", func() {
			cluster := &valkeyiov1alpha1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					Shards:   3,
					Replicas: 1,
					ZoneAwareness: &valkeyiov1alpha1.ZoneConfig{
						Enabled: true,
						MaxSkew: 2,
					},
				},
			}
			constraints := injectTopologySpreadConstraints(cluster, 1)
			Expect(constraints).To(HaveLen(1))
			Expect(constraints[0].MaxSkew).To(Equal(int32(2)))
		})
	})

	Describe("countDistinctZones", func() {
		It("should return 0 when no pods have zone labels", func() {
			pods := &corev1.PodList{
				Items: []corev1.Pod{
					{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "valkey"}}},
					{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "valkey"}}},
				},
			}
			Expect(countDistinctZones(pods, "topology.kubernetes.io/zone")).To(Equal(0))
		})

		It("should count distinct zones from pod labels", func() {
			pods := &corev1.PodList{
				Items: []corev1.Pod{
					{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"topology.kubernetes.io/zone": "us-east-1a"}}},
					{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"topology.kubernetes.io/zone": "us-east-1b"}}},
					{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"topology.kubernetes.io/zone": "us-east-1a"}}},
					{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"topology.kubernetes.io/zone": "us-east-1c"}}},
				},
			}
			Expect(countDistinctZones(pods, "topology.kubernetes.io/zone")).To(Equal(3))
		})

		It("should count zones from nodeSelector as fallback", func() {
			pods := &corev1.PodList{
				Items: []corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}},
						Spec:       corev1.PodSpec{NodeSelector: map[string]string{"topology.kubernetes.io/zone": "zone-a"}},
					},
					{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}},
						Spec:       corev1.PodSpec{NodeSelector: map[string]string{"topology.kubernetes.io/zone": "zone-b"}},
					},
				},
			}
			Expect(countDistinctZones(pods, "topology.kubernetes.io/zone")).To(Equal(2))
		})

		It("should return 0 for empty pod list", func() {
			pods := &corev1.PodList{}
			Expect(countDistinctZones(pods, "topology.kubernetes.io/zone")).To(Equal(0))
		})
	})

	Describe("checkZoneSpread", func() {
		var recorder *events.FakeRecorder

		BeforeEach(func() {
			recorder = events.NewFakeRecorder(100)
		})

		It("should do nothing when zone awareness is nil", func() {
			cluster := &valkeyiov1alpha1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					Shards:   3,
					Replicas: 1,
				},
			}
			pods := &corev1.PodList{}
			checkZoneSpread(recorder, cluster, pods)

			evts := collectEvents(recorder)
			Expect(evts).To(BeEmpty())
			Expect(cluster.Status.Conditions).To(BeEmpty())
		})

		It("should set ZoneSpreadDegraded=True when zones < replicas+1", func() {
			cluster := &valkeyiov1alpha1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					Shards:   3,
					Replicas: 2,
					ZoneAwareness: &valkeyiov1alpha1.ZoneConfig{
						Enabled:     true,
						TopologyKey: "topology.kubernetes.io/zone",
						MaxSkew:     1,
					},
				},
			}
			pods := &corev1.PodList{
				Items: []corev1.Pod{
					{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"topology.kubernetes.io/zone": "us-east-1a"}}},
					{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"topology.kubernetes.io/zone": "us-east-1b"}}},
				},
			}
			checkZoneSpread(recorder, cluster, pods)

			// Should emit warning event
			evts := collectEvents(recorder)
			warnings := filterEvents(evts, "InsufficientZones")
			Expect(warnings).To(HaveLen(1))
			Expect(warnings[0]).To(ContainSubstring("Warning"))

			// Should set condition
			Expect(cluster.Status.Conditions).To(HaveLen(1))
			cond := cluster.Status.Conditions[0]
			Expect(cond.Type).To(Equal(valkeyiov1alpha1.ConditionZoneSpreadDegraded))
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(valkeyiov1alpha1.ReasonInsufficientZones))
		})

		It("should set ZoneSpreadDegraded=False when zones >= replicas+1", func() {
			cluster := &valkeyiov1alpha1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					Shards:   3,
					Replicas: 1,
					ZoneAwareness: &valkeyiov1alpha1.ZoneConfig{
						Enabled:     true,
						TopologyKey: "topology.kubernetes.io/zone",
						MaxSkew:     1,
					},
				},
			}
			pods := &corev1.PodList{
				Items: []corev1.Pod{
					{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"topology.kubernetes.io/zone": "us-east-1a"}}},
					{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"topology.kubernetes.io/zone": "us-east-1b"}}},
					{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"topology.kubernetes.io/zone": "us-east-1c"}}},
				},
			}
			checkZoneSpread(recorder, cluster, pods)

			// Should NOT emit warning event
			evts := collectEvents(recorder)
			warnings := filterEvents(evts, "InsufficientZones")
			Expect(warnings).To(BeEmpty())

			// Should set condition to False
			Expect(cluster.Status.Conditions).To(HaveLen(1))
			cond := cluster.Status.Conditions[0]
			Expect(cond.Type).To(Equal(valkeyiov1alpha1.ConditionZoneSpreadDegraded))
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(valkeyiov1alpha1.ReasonZoneSpreadOK))
		})

		It("should handle replicas=0 (no replicas, only 1 zone needed)", func() {
			cluster := &valkeyiov1alpha1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					Shards:   3,
					Replicas: 0,
					ZoneAwareness: &valkeyiov1alpha1.ZoneConfig{
						Enabled:     true,
						TopologyKey: "topology.kubernetes.io/zone",
						MaxSkew:     1,
					},
				},
			}
			pods := &corev1.PodList{
				Items: []corev1.Pod{
					{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"topology.kubernetes.io/zone": "us-east-1a"}}},
				},
			}
			checkZoneSpread(recorder, cluster, pods)

			evts := collectEvents(recorder)
			warnings := filterEvents(evts, "InsufficientZones")
			Expect(warnings).To(BeEmpty())

			Expect(cluster.Status.Conditions).To(HaveLen(1))
			Expect(cluster.Status.Conditions[0].Status).To(Equal(metav1.ConditionFalse))
		})
	})

	Describe("ApplyZoneDefaults", func() {
		It("should not create ZoneConfig when nil", func() {
			spec := &valkeyiov1alpha1.ValkeyClusterSpec{}
			ApplyZoneDefaults(spec)
			Expect(spec.ZoneAwareness).To(BeNil())
		})

		It("should fill in default topologyKey and maxSkew", func() {
			spec := &valkeyiov1alpha1.ValkeyClusterSpec{
				ZoneAwareness: &valkeyiov1alpha1.ZoneConfig{
					Enabled: true,
				},
			}
			ApplyZoneDefaults(spec)
			Expect(spec.ZoneAwareness.TopologyKey).To(Equal(DefaultTopologyKey))
			Expect(spec.ZoneAwareness.MaxSkew).To(Equal(DefaultMaxSkew))
		})

		It("should not overwrite explicitly set values", func() {
			spec := &valkeyiov1alpha1.ValkeyClusterSpec{
				ZoneAwareness: &valkeyiov1alpha1.ZoneConfig{
					Enabled:     true,
					TopologyKey: "custom.io/zone",
					MaxSkew:     3,
				},
			}
			ApplyZoneDefaults(spec)
			Expect(spec.ZoneAwareness.TopologyKey).To(Equal("custom.io/zone"))
			Expect(spec.ZoneAwareness.MaxSkew).To(Equal(int32(3)))
		})
	})
})

var _ = Describe("PodDisruptionBudget", func() {

	Describe("pdbName", func() {
		It("should return the expected name pattern", func() {
			Expect(pdbName("mycluster", 0)).To(Equal("mycluster-shard-0-pdb"))
			Expect(pdbName("mycluster", 5)).To(Equal("mycluster-shard-5-pdb"))
			Expect(pdbName("prod", 999)).To(Equal("prod-shard-999-pdb"))
		})
	})

	Describe("createShardPDB", func() {
		It("should create a PDB with minAvailable=1 for the given shard", func() {
			cluster := &valkeyiov1alpha1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "mycluster", Namespace: "default"},
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					Shards:   3,
					Replicas: 1,
					ZoneAwareness: &valkeyiov1alpha1.ZoneConfig{
						Enabled: true,
					},
				},
			}

			pdb := createShardPDB(cluster, 1)

			Expect(pdb.Name).To(Equal("mycluster-shard-1-pdb"))
			Expect(pdb.Namespace).To(Equal("default"))
			Expect(pdb.Spec.MinAvailable).NotTo(BeNil())
			Expect(pdb.Spec.MinAvailable.IntValue()).To(Equal(1))
			Expect(pdb.Spec.Selector).NotTo(BeNil())
			Expect(pdb.Spec.Selector.MatchLabels).To(HaveKeyWithValue("app.kubernetes.io/instance", "mycluster"))
			Expect(pdb.Spec.Selector.MatchLabels).To(HaveKeyWithValue(LabelShardIndex, "1"))
		})

		It("should set standard labels on the PDB", func() {
			cluster := &valkeyiov1alpha1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns1"},
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					Shards:   5,
					Replicas: 1,
				},
			}

			pdb := createShardPDB(cluster, 3)

			Expect(pdb.Labels).To(HaveKeyWithValue("app.kubernetes.io/managed-by", "valkey-operator"))
			Expect(pdb.Labels).To(HaveKeyWithValue("app.kubernetes.io/instance", "test"))
			Expect(pdb.Labels).To(HaveKeyWithValue(LabelShardIndex, "3"))
		})

		It("should create PDBs matching shard count", func() {
			cluster := &valkeyiov1alpha1.ValkeyCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "big", Namespace: "default"},
				Spec: valkeyiov1alpha1.ValkeyClusterSpec{
					Shards:   10,
					Replicas: 1,
				},
			}

			pdbs := make(map[string]bool)
			for shard := range int(cluster.Spec.Shards) {
				pdb := createShardPDB(cluster, shard)
				pdbs[pdb.Name] = true
				Expect(pdb.Spec.MinAvailable.IntValue()).To(Equal(1))
			}
			Expect(pdbs).To(HaveLen(10))
		})
	})
})

