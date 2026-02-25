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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterState represents the high-level state of the ValkeyCluster.
// +kubebuilder:validation:Enum=Initializing;Reconciling;Ready;Degraded;Failed
type ClusterState string

const (
	// ClusterStateInitializing indicates the cluster is being created for the first time.
	ClusterStateInitializing ClusterState = "Initializing"
	// ClusterStateReconciling indicates the cluster is being updated.
	ClusterStateReconciling ClusterState = "Reconciling"
	// ClusterStateReady indicates the cluster is healthy and serving traffic.
	ClusterStateReady ClusterState = "Ready"
	// ClusterStateDegraded indicates the cluster is partially functional.
	ClusterStateDegraded ClusterState = "Degraded"
	// ClusterStateFailed indicates the cluster has failed and cannot recover.
	ClusterStateFailed ClusterState = "Failed"
)

// ValkeyClusterSpec defines the desired state of ValkeyCluster.
type ValkeyClusterSpec struct {

	// Override the default Valkey image
	Image string `json:"image,omitempty"`

	// The number of shards groups. Each shard group contains one primary and N replicas.
	// +kubebuilder:validation:Minimum=1
	Shards int32 `json:"shards,omitempty"`

	// The number of replicas for each shard group.
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas,omitempty"`

	// Override resource requirements for the Valkey container in each pod
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Tolerations to apply to the pods
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// NodeSelector to apply to the pods
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Affinity to apply to the pods, overrides NodeSelector if set
	// Some basic anti-affinity rules will be applied by default to spread pods across nodes and zones
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// Metrics exporter options
	// +kubebuilder:default:={enabled:true}
	// +optional
	Exporter ExporterSpec `json:"exporter,omitempty"`

	// ClusterConfig holds tunable Valkey cluster parameters.
	// If omitted, safe large-scale defaults are applied.
	// +optional
	ClusterConfig *ClusterConfig `json:"clusterConfig,omitempty"`

	// Admission controls how new nodes are introduced to the cluster.
	// +optional
	Admission *AdmissionConfig `json:"admission,omitempty"`

	// ZoneAwareness controls zone-aware placement behavior.
	// +optional
	ZoneAwareness *ZoneConfig `json:"zoneAwareness,omitempty"`

	// Metrics controls the observability pipeline for the ValkeyCluster.
	// +optional
	Metrics *MetricsConfig `json:"metrics,omitempty"`
}

type ExporterSpec struct {

	// Override the default exporter image
	Image string `json:"image,omitempty"`

	// Override resource requirements for the exporter container in each pod
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Enable or disable the exporter sidecar container
	Enabled bool `json:"enabled,omitempty"`
}

// ClusterConfig holds tunable Valkey cluster parameters.
// These are written into the generated valkey.conf ConfigMap.
type ClusterConfig struct {
	// ClusterNodeTimeoutMs is the cluster-node-timeout in milliseconds.
	// At 2000 nodes, the default 2000ms is too aggressive; 15000-30000ms is recommended.
	// +kubebuilder:validation:Minimum=1000
	// +kubebuilder:validation:Maximum=60000
	// +kubebuilder:default=15000
	// +optional
	ClusterNodeTimeoutMs int32 `json:"clusterNodeTimeoutMs,omitempty"`

	// AdditionalConfig appends raw valkey.conf directives after operator-managed
	// base directives (port, cluster-enabled, protected-mode, cluster-node-timeout).
	// Later directives win when duplicated, so this can be used to override base
	// values for non-production benchmarking.
	// +optional
	AdditionalConfig []string `json:"additionalConfig,omitempty"`
}

// AdmissionConfig controls how new nodes are introduced to the cluster.
type AdmissionConfig struct {
	// Parallelism is the max number of nodes admitted per reconcile batch.
	// Higher values speed up bootstrap but increase gossip load.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=200
	// +kubebuilder:default=50
	// +optional
	Parallelism int32 `json:"parallelism,omitempty"`

	// MeetStrategy controls how new nodes discover the cluster.
	// "seed" (default): MEET a small seed set, rely on gossip.
	// "all": MEET every primary (legacy behavior, expensive at scale).
	// +kubebuilder:validation:Enum=seed;all
	// +kubebuilder:default=seed
	// +optional
	MeetStrategy string `json:"meetStrategy,omitempty"`

	// SeedCount is the number of existing primaries used as seeds for CLUSTER MEET
	// when MeetStrategy is "seed". Nodes are selected round-robin across zones.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +kubebuilder:default=3
	// +optional
	SeedCount int32 `json:"seedCount,omitempty"`
}

// ZoneConfig controls zone-aware placement behavior.
type ZoneConfig struct {
	// Enabled activates zone-aware topology spread and PDB creation.
	// +kubebuilder:default=true
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// TopologyKey is the node label used for zone detection.
	// +kubebuilder:default="topology.kubernetes.io/zone"
	// +optional
	TopologyKey string `json:"topologyKey,omitempty"`

	// MaxSkew is the maximum difference in pod count between zones.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	// +optional
	MaxSkew int32 `json:"maxSkew,omitempty"`
}

// MetricsConfig controls the observability pipeline for the ValkeyCluster.
type MetricsConfig struct {
	// Enabled activates the metrics sidecar and aggregator deployment.
	// +kubebuilder:default=true
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ScrapeIntervalSec is how often the sidecar collects metrics from the Valkey process.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=60
	// +kubebuilder:default=1
	// +optional
	ScrapeIntervalSec int32 `json:"scrapeIntervalSec,omitempty"`

	// AggregatorReplicas is the number of metric aggregator pods.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3
	// +kubebuilder:default=1
	// +optional
	AggregatorReplicas int32 `json:"aggregatorReplicas,omitempty"`

	// HighResolution enables 1-second metric resolution.
	// When false, uses standard 15s Prometheus scrape interval.
	// +kubebuilder:default=false
	// +optional
	HighResolution bool `json:"highResolution,omitempty"`
}

// ValkeyClusterStatus defines the observed state of ValkeyCluster.
type ValkeyClusterStatus struct {
	// State provides a high-level summary of the cluster's current state.
	// +kubebuilder:default=Initializing
	// +optional
	State ClusterState `json:"state,omitempty"`

	// Reason provides a brief machine-readable explanation for the current state.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Message provides human-readable details about the current state.
	// +optional
	Message string `json:"message,omitempty"`

	// Shards represents the number of shards currently formed in the cluster.
	// +kubebuilder:default=0
	// +optional
	Shards int32 `json:"shards,omitempty"`

	// ReadyShards represents the number of shards that are fully healthy.
	// +kubebuilder:default=0
	// +optional
	ReadyShards int32 `json:"readyShards,omitempty"`

	// Conditions represent the current state of the ValkeyCluster resource.
	// Standard condition types:
	// - "Ready": the cluster is fully functional and serving traffic
	// - "Progressing": the cluster is being created, updated, or scaled
	// - "Degraded": the cluster is impaired but may be partially functional
	// Valkey-specific condition types:
	// - "ClusterFormed": all nodes have joined and meet the shard/replica layout
	// - "SlotsAssigned": all 16384 hash slots are assigned to primaries
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

const (
	ConditionReady              = "Ready"
	ConditionProgressing        = "Progressing"
	ConditionDegraded           = "Degraded"
	ConditionClusterFormed      = "ClusterFormed"
	ConditionSlotsAssigned      = "SlotsAssigned"
	ConditionZoneSpreadDegraded = "ZoneSpreadDegraded"
)

const (
	// Common reasons for conditions
	ReasonInitializing      = "Initializing"
	ReasonReconciling       = "Reconciling"
	ReasonClusterHealthy    = "ClusterHealthy"
	ReasonServiceError      = "ServiceError"
	ReasonConfigMapError    = "ConfigMapError"
	ReasonDeploymentError   = "DeploymentError"
	ReasonPodListError      = "PodListError"
	ReasonAddingNodes       = "AddingNodes"
	ReasonNodeAddFailed     = "NodeAddFailed"
	ReasonMissingShards     = "MissingShards"
	ReasonMissingReplicas   = "MissingReplicas"
	ReasonReconcileComplete = "ReconcileComplete"
	ReasonTopologyComplete  = "TopologyComplete"
	ReasonAllSlotsAssigned  = "AllSlotsAssigned"
	ReasonSlotsUnassigned   = "SlotsUnassigned"
	ReasonPrimaryLost       = "PrimaryLost"
	ReasonNoSlots           = "NoSlotsAvailable"
	ReasonInsufficientZones = "InsufficientZones"
	ReasonZoneSpreadOK      = "ZoneSpreadOK"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=vkc

// ValkeyCluster is the Schema for the valkeyclusters API
// +kubebuilder:printcolumn:name="State",type="string",JSONPath=".status.state",description="Current state of the cluster"
// +kubebuilder:printcolumn:name="Reason",type="string",JSONPath=".status.reason",description="Reason for current state"
// +kubebuilder:printcolumn:name="ReadyShards",type="integer",JSONPath=".status.readyShards",description="Ready shards",priority=1
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Time since creation"
type ValkeyCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of ValkeyCluster
	// +required
	Spec ValkeyClusterSpec `json:"spec"`

	// status defines the observed state of ValkeyCluster
	// +kubebuilder:default:={state: "Initializing", readyShards:0}
	// +optional
	Status ValkeyClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ValkeyClusterList contains a list of ValkeyCluster
type ValkeyClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ValkeyCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ValkeyCluster{}, &ValkeyClusterList{})
}
