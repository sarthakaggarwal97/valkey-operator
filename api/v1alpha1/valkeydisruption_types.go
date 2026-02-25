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

// DisruptionAction defines the type of disruption to perform.
// +kubebuilder:validation:Enum=PauseProcess;ResumeProcess;KillProcess;Failover;DeletePod;PauseResumeLoop
type DisruptionAction string

const (
	// DisruptionPauseProcess sends SIGSTOP to the valkey-server process (PID 1) inside the container.
	// This freezes the entire process including cluster bus communication,
	// causing the node to become pfail within cluster-node-timeout.
	// Equivalent to: kill -STOP 1 (valkey-server is PID 1 in the container)
	DisruptionPauseProcess DisruptionAction = "PauseProcess"

	// DisruptionResumeProcess sends SIGCONT to a previously SIGSTOP'd valkey-server process (PID 1).
	// The process resumes and rejoins the cluster via gossip.
	// Equivalent to: kill -CONT 1
	DisruptionResumeProcess DisruptionAction = "ResumeProcess"

	// DisruptionKillProcess sends SIGKILL to the valkey-server process (PID 1) to simulate a crash.
	// Since valkey-server is PID 1 (the container's main process), killing it causes the
	// container to exit. Kubernetes then restarts the pod via the Deployment's restart policy.
	// The node rejoins the cluster automatically after restart.
	// Equivalent to: kill -9 1
	DisruptionKillProcess DisruptionAction = "KillProcess"

	// DisruptionFailover triggers CLUSTER FAILOVER on the target primary's replica.
	// The executor looks up the replica for the targeted primary's shard
	// and sends CLUSTER FAILOVER to that replica (not the primary itself).
	DisruptionFailover DisruptionAction = "Failover"

	// DisruptionDeletePod deletes the Kubernetes pod, triggering a full pod restart.
	DisruptionDeletePod DisruptionAction = "DeletePod"

	// DisruptionPauseResumeLoop runs automated cycles of SIGSTOP → wait → SIGCONT → wait,
	// repeated N times. Replicates the failover-tool's pause-resume loop pattern.
	// Implemented as a state machine with requeue-based scheduling to avoid
	// blocking controller workers. Each reconcile checks status.loopProgress.currentPhase
	// and acts accordingly, then requeues with ctrl.Result{RequeueAfter: intervalSec}.
	// The controller temporarily relaxes liveness probes on targeted pods to prevent
	// kubelet from restarting SIGSTOP'd containers before the intended pause duration completes.
	DisruptionPauseResumeLoop DisruptionAction = "PauseResumeLoop"
)

// DisruptionPhase represents the current execution phase of a disruption.
// +kubebuilder:validation:Enum=Pending;Executing;Completed;Failed
type DisruptionPhase string

const (
	DisruptionPhasePending   DisruptionPhase = "Pending"
	DisruptionPhaseExecuting DisruptionPhase = "Executing"
	DisruptionPhaseCompleted DisruptionPhase = "Completed"
	DisruptionPhaseFailed    DisruptionPhase = "Failed"
)

// ValkeyDisruptionSpec defines the desired disruption action.
type ValkeyDisruptionSpec struct {
	// ClusterRef is the name of the ValkeyCluster to disrupt.
	// +required
	ClusterRef string `json:"clusterRef"`

	// Action is the disruption type to perform.
	// +kubebuilder:validation:Enum=PauseProcess;ResumeProcess;KillProcess;Failover;DeletePod;PauseResumeLoop
	// +required
	Action DisruptionAction `json:"action"`

	// Selector determines which nodes are targeted.
	// +required
	Selector DisruptionSelector `json:"selector"`

	// PauseTimeoutSec is the maximum duration to keep a process paused (SIGSTOP).
	// After this timeout, the controller automatically sends SIGCONT as a safety net.
	// Only applies to PauseProcess action.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=600
	// +kubebuilder:default=30
	// +optional
	PauseTimeoutSec int32 `json:"pauseTimeoutSec,omitempty"`

	// Loop configures automated pause→wait→resume→wait cycles.
	// Only applies to PauseResumeLoop action.
	// +optional
	Loop *PauseResumeLoopConfig `json:"loop,omitempty"`
}

// DisruptionSelector identifies target nodes for a disruption action.
type DisruptionSelector struct {
	// Role filters by node role: "primary" or "replica".
	// When omitted (empty), matches all roles.
	// +kubebuilder:validation:Enum=primary;replica
	// +optional
	Role string `json:"role,omitempty"`

	// Count is the number of nodes to target. 0 means all matching nodes
	// (requires valkey.io/confirm-destructive: "true" annotation).
	// +kubebuilder:validation:Minimum=0
	// +optional
	Count int32 `json:"count,omitempty"`

	// Zone filters by topology.kubernetes.io/zone label.
	// +optional
	Zone string `json:"zone,omitempty"`

	// ShardRange targets shards in [start, end] inclusive.
	// +optional
	ShardRange *ShardRange `json:"shardRange,omitempty"`
}

// ShardRange defines an inclusive range of shard indices.
type ShardRange struct {
	// Start is the first shard index in the range (inclusive).
	// +kubebuilder:validation:Minimum=0
	Start int32 `json:"start"`

	// End is the last shard index in the range (inclusive).
	// Must be >= Start.
	// +kubebuilder:validation:Minimum=0
	End int32 `json:"end"`
}

// PauseResumeLoopConfig controls automated pause-resume cycling.
// Replicates the failover-tool's pause-resume loop: impair → sleep → resume → sleep, repeated N times.
type PauseResumeLoopConfig struct {
	// Repeat is the number of pause-resume cycles to execute.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=1
	Repeat int32 `json:"repeat"`

	// IntervalSec is the wait duration (in seconds) between pause and resume,
	// and between resume and the next pause. Mirrors the toolkit's 2-minute default.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=600
	// +kubebuilder:default=120
	IntervalSec int32 `json:"intervalSec"`
}

// ValkeyDisruptionStatus tracks execution progress of a disruption action.
type ValkeyDisruptionStatus struct {
	// Phase is the current execution phase.
	// +kubebuilder:validation:Enum=Pending;Executing;Completed;Failed
	// +optional
	Phase DisruptionPhase `json:"phase,omitempty"`

	// TargetCount is the number of nodes selected for disruption.
	// +optional
	TargetCount int32 `json:"targetCount,omitempty"`

	// CompletedCount is the number of nodes where the action succeeded.
	// +optional
	CompletedCount int32 `json:"completedCount,omitempty"`

	// FailedNodes lists nodes where the action failed with reasons.
	// +optional
	FailedNodes []FailedNode `json:"failedNodes,omitempty"`

	// LoopProgress tracks PauseResumeLoop iteration progress.
	// +optional
	LoopProgress *LoopProgressStatus `json:"loopProgress,omitempty"`

	// StartTime is when execution began.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime is when execution finished.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Conditions for standard status reporting.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// LoopProgressStatus tracks the current state of a PauseResumeLoop.
// The loop is implemented as a state machine with requeue-based scheduling.
// Each reconcile checks currentPhase, performs the action, updates the phase,
// and returns ctrl.Result{RequeueAfter: intervalSec} to yield the worker.
type LoopProgressStatus struct {
	// CurrentIteration is the loop iteration currently executing (1-indexed).
	CurrentIteration int32 `json:"currentIteration"`

	// TotalIterations is the total number of iterations configured.
	TotalIterations int32 `json:"totalIterations"`

	// CurrentPhase is what the loop is currently doing.
	// State transitions: Pausing → WaitingAfterPause → Resuming → WaitingAfterResume → (next iteration or complete)
	// +kubebuilder:validation:Enum=Pausing;WaitingAfterPause;Resuming;WaitingAfterResume
	CurrentPhase string `json:"currentPhase"`

	// OriginalLivenessThresholds stores the original liveness probe failureThreshold
	// per Deployment name, before it was increased. Used to restore original values
	// after loop completion or CR deletion.
	// +optional
	OriginalLivenessThresholds map[string]int32 `json:"originalLivenessThresholds,omitempty"`
}

// FailedNode records a node where a disruption action failed.
type FailedNode struct {
	// Address is the address of the failed node.
	Address string `json:"address"`

	// Reason describes why the action failed (e.g., "no-synced-replica", "unreachable").
	Reason string `json:"reason"`
}

// GetOriginalLivenessThresholds returns the OriginalLivenessThresholds map,
// or nil if the LoopProgressStatus is nil. This is a safe accessor for use
// when copying thresholds during loop initialization.
func (lps *LoopProgressStatus) GetOriginalLivenessThresholds() map[string]int32 {
	if lps == nil {
		return nil
	}
	return lps.OriginalLivenessThresholds
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=vkd

// ValkeyDisruption is the Schema for the valkeydisruptions API.
// It provides a declarative API for fault injection against Valkey clusters.
// +kubebuilder:printcolumn:name="Action",type="string",JSONPath=".spec.action",description="Disruption action type"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Current execution phase"
// +kubebuilder:printcolumn:name="Targets",type="integer",JSONPath=".status.targetCount",description="Number of targeted nodes"
// +kubebuilder:printcolumn:name="Completed",type="integer",JSONPath=".status.completedCount",description="Number of completed nodes"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Time since creation"
type ValkeyDisruption struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired disruption action
	// +required
	Spec ValkeyDisruptionSpec `json:"spec"`

	// status defines the observed state of the disruption
	// +kubebuilder:default:={phase: "Pending"}
	// +optional
	Status ValkeyDisruptionStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ValkeyDisruptionList contains a list of ValkeyDisruption
type ValkeyDisruptionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ValkeyDisruption `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ValkeyDisruption{}, &ValkeyDisruptionList{})
}
