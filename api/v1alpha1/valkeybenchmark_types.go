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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BenchmarkClientType defines the benchmark client to use.
// +kubebuilder:validation:Enum=valkey-benchmark;valkeypy;valkeyglide;iovalkey;valkeygo;redisson
type BenchmarkClientType string

const (
	BenchmarkClientValkeyBenchmark BenchmarkClientType = "valkey-benchmark"
	BenchmarkClientValkeyPy        BenchmarkClientType = "valkeypy"
	BenchmarkClientValkeyGlide     BenchmarkClientType = "valkeyglide"
	BenchmarkClientIOValkey        BenchmarkClientType = "iovalkey"
	BenchmarkClientValkeyGo        BenchmarkClientType = "valkeygo"
	BenchmarkClientRedisson        BenchmarkClientType = "redisson"
)

// BenchmarkPhase represents the current execution phase of a benchmark.
// +kubebuilder:validation:Enum=Running;Completed;Failed
type BenchmarkPhase string

const (
	BenchmarkPhaseRunning   BenchmarkPhase = "Running"
	BenchmarkPhaseCompleted BenchmarkPhase = "Completed"
	BenchmarkPhaseFailed    BenchmarkPhase = "Failed"
)

// ValkeyBenchmarkSpec defines a benchmark workload against a ValkeyCluster.
type ValkeyBenchmarkSpec struct {
	// ClusterRef is the name of the ValkeyCluster to benchmark.
	// +required
	ClusterRef string `json:"clusterRef"`

	// ClientType is the benchmark client to use.
	// +kubebuilder:validation:Enum=valkey-benchmark;valkeypy;valkeyglide;iovalkey;valkeygo;redisson
	// +kubebuilder:default=valkey-benchmark
	ClientType BenchmarkClientType `json:"clientType"`

	// ClientCount is the number of client pod instances to launch.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000
	// +kubebuilder:default=10
	ClientCount int32 `json:"clientCount"`

	// DurationSec is how long the benchmark runs in seconds.
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:default=300
	DurationSec int32 `json:"durationSec"`

	// KeyDistribution controls how keys are distributed across slots.
	// "uniform" distributes keys evenly across all 16384 slots using CRC16.
	// "zipfian" uses a Zipfian distribution for hot-key workloads.
	// +kubebuilder:validation:Enum=uniform;zipfian
	// +kubebuilder:default=uniform
	KeyDistribution string `json:"keyDistribution"`
}

// ValkeyBenchmarkStatus tracks benchmark execution and results.
type ValkeyBenchmarkStatus struct {
	// Phase is the current execution phase.
	// +kubebuilder:validation:Enum=Running;Completed;Failed
	// +optional
	Phase BenchmarkPhase `json:"phase,omitempty"`

	// RPS is the aggregate requests per second (sum across clients), rounded to
	// the nearest whole request per second.
	// +optional
	RPS int64 `json:"rps,omitempty"`

	// AvgLatencyUs is the average latency in microseconds, rounded to the
	// nearest microsecond.
	// +optional
	AvgLatencyUs int64 `json:"avgLatencyUs,omitempty"`

	// P90LatencyUs is the 90th percentile latency in microseconds, rounded to
	// the nearest microsecond.
	// +optional
	P90LatencyUs int64 `json:"p90LatencyUs,omitempty"`

	// P99LatencyUs is the 99th percentile latency in microseconds, rounded to
	// the nearest microsecond.
	// +optional
	P99LatencyUs int64 `json:"p99LatencyUs,omitempty"`

	// MaxDowntimeMs is the maximum continuous downtime in milliseconds.
	// +optional
	MaxDowntimeMs int64 `json:"maxDowntimeMs,omitempty"`

	// StartTime is when the benchmark began.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when the benchmark finished.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=vkb

// ValkeyBenchmark is the Schema for the valkeybenchmarks API.
// It provides a declarative API for running benchmark workloads against Valkey clusters.
// +kubebuilder:printcolumn:name="ClientType",type="string",JSONPath=".spec.clientType",description="Benchmark client type"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Current execution phase"
// +kubebuilder:printcolumn:name="RPS",type="number",JSONPath=".status.rps",description="Aggregate requests per second"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Time since creation"
type ValkeyBenchmark struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired benchmark workload
	// +required
	Spec ValkeyBenchmarkSpec `json:"spec"`

	// status defines the observed state of the benchmark
	// +optional
	Status ValkeyBenchmarkStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ValkeyBenchmarkList contains a list of ValkeyBenchmark
type ValkeyBenchmarkList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ValkeyBenchmark `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ValkeyBenchmark{}, &ValkeyBenchmarkList{})
}
