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
	"context"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
	"valkey.io/valkey-operator/internal/valkey"
)

const (
	// confirmDestructiveAnnotation is required when selector.count=0 (all matching nodes).
	confirmDestructiveAnnotation = "valkey.io/confirm-destructive"
)

// ValkeyDisruptionReconciler reconciles a ValkeyDisruption object.
type ValkeyDisruptionReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Clientset kubernetes.Interface
	Config    *rest.Config
}

// +kubebuilder:rbac:groups=valkey.io,resources=valkeydisruptions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=valkey.io,resources=valkeydisruptions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=valkey.io,resources=valkeydisruptions/finalizers,verbs=update
// +kubebuilder:rbac:groups=valkey.io,resources=valkeyclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="apps",resources=deployments,verbs=get;list;watch;update;patch

// Reconcile drives the ValkeyDisruption CR toward its desired state.
// The pipeline:
//  1. Fetch the ValkeyDisruption CR.
//  2. Fetch the referenced ValkeyCluster.
//  3. Get cluster state by connecting to all pods.
//  4. Select targets based on selector criteria.
//  5. Validate safety constraints.
//  6. Execute disruption actions via DisruptionExecutor.
//  7. Update status.
func (r *ValkeyDisruptionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.V(1).Info("reconciling ValkeyDisruption")

	// Step 1: Fetch the ValkeyDisruption CR.
	disruption := &valkeyiov1alpha1.ValkeyDisruption{}
	if err := r.Get(ctx, req.NamespacedName, disruption); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Step 1a: Handle finalizer (add on create, cleanup on delete).
	deleted, err := r.handleFinalizer(ctx, disruption)
	if err != nil {
		return ctrl.Result{}, err
	}
	if deleted {
		return ctrl.Result{}, nil
	}

	// Step 1b: Handle auto-resume for PauseProcess that has completed.
	// If the pause timeout has been reached, send SIGCONT and restore liveness probes.
	if disruption.Status.Phase == valkeyiov1alpha1.DisruptionPhaseCompleted &&
		disruption.Spec.Action == valkeyiov1alpha1.DisruptionPauseProcess {
		autoResumed, err := r.handleAutoResume(ctx, disruption)
		if err != nil {
			return ctrl.Result{}, err
		}
		if autoResumed {
			// Auto-resume triggered — update status to reflect the resume.
			disruption.Status.Phase = valkeyiov1alpha1.DisruptionPhaseCompleted
			if err := r.Status().Update(ctx, disruption); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		// Not yet time to auto-resume — requeue after remaining time.
		remaining := autoResumeRequeueAfter(disruption)
		if remaining > 0 {
			return ctrl.Result{RequeueAfter: remaining}, nil
		}
		return ctrl.Result{}, nil
	}

	// If already completed or failed (non-PauseProcess), nothing to do.
	if disruption.Status.Phase == valkeyiov1alpha1.DisruptionPhaseCompleted ||
		disruption.Status.Phase == valkeyiov1alpha1.DisruptionPhaseFailed {
		return ctrl.Result{}, nil
	}

	// PauseResumeLoop is handled by the loop orchestration state machine.
	// If the action is PauseResumeLoop and we're already executing with loop progress,
	// delegate to the loop state machine.
	if disruption.Spec.Action == valkeyiov1alpha1.DisruptionPauseResumeLoop &&
		disruption.Status.Phase == valkeyiov1alpha1.DisruptionPhaseExecuting &&
		disruption.Status.LoopProgress != nil {
		return r.reconcilePauseResumeLoop(ctx, disruption)
	}

	// Step 2: Fetch the referenced ValkeyCluster.
	cluster := &valkeyiov1alpha1.ValkeyCluster{}
	clusterKey := client.ObjectKey{
		Namespace: disruption.Namespace,
		Name:      disruption.Spec.ClusterRef,
	}
	if err := r.Get(ctx, clusterKey, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			log.Error(err, "referenced ValkeyCluster not found", "clusterRef", disruption.Spec.ClusterRef)
			return r.setFailed(ctx, disruption, fmt.Sprintf("ValkeyCluster %q not found", disruption.Spec.ClusterRef))
		}
		return ctrl.Result{}, err
	}

	// Step 3: Get cluster state.
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(cluster.Namespace), client.MatchingLabels(labels(cluster))); err != nil {
		log.Error(err, "failed to list pods")
		return ctrl.Result{}, err
	}
	state := getClusterStateFromPods(ctx, pods)
	defer state.CloseClients()
	addressZones := r.resolveAddressZones(ctx, pods)

	// Validate confirm-destructive annotation when count=0.
	if disruption.Spec.Selector.Count == 0 {
		if disruption.Annotations[confirmDestructiveAnnotation] != "true" {
			msg := "selector.count=0 targets all matching nodes; requires annotation valkey.io/confirm-destructive: \"true\""
			return r.setFailed(ctx, disruption, msg)
		}
	}

	// Validate PauseResumeLoop has loop config.
	if disruption.Spec.Action == valkeyiov1alpha1.DisruptionPauseResumeLoop {
		if disruption.Spec.Loop == nil || disruption.Spec.Loop.Repeat < 1 || disruption.Spec.Loop.IntervalSec < 1 {
			return r.setFailed(ctx, disruption, "PauseResumeLoop requires loop config with repeat >= 1 and intervalSec >= 1")
		}
	}

	// Step 4: Select targets.
	targets, err := r.selectTargets(disruption.Spec, state, pods, addressZones)
	if err != nil {
		return r.setFailed(ctx, disruption, fmt.Sprintf("target selection failed: %v", err))
	}

	if len(targets) == 0 {
		return r.setFailed(ctx, disruption, "no matching targets found for selector")
	}

	// Step 5: Validate safety constraints and collect failed nodes.
	validTargets, failedNodes := r.validateSafety(disruption.Spec.Action, targets, state)

	// Update status to Executing.
	now := metav1.Now()
	disruption.Status.Phase = valkeyiov1alpha1.DisruptionPhaseExecuting
	disruption.Status.TargetCount = int32(len(targets))
	disruption.Status.StartTime = &now
	disruption.Status.FailedNodes = failedNodes
	if err := r.Status().Update(ctx, disruption); err != nil {
		log.Error(err, "failed to update status to Executing")
		return ctrl.Result{}, err
	}

	// Step 5a: For PauseProcess and PauseResumeLoop, patch liveness probes before
	// executing the pause to prevent kubelet from restarting SIGSTOP'd containers.
	if disruption.Spec.Action == valkeyiov1alpha1.DisruptionPauseProcess ||
		disruption.Spec.Action == valkeyiov1alpha1.DisruptionPauseResumeLoop {
		targetPods := collectTargetPods(validTargets, pods)
		if err := r.patchLivenessThresholds(ctx, disruption, pods, targetPods); err != nil {
			log.Error(err, "failed to patch liveness thresholds")
			return ctrl.Result{}, err
		}
		// Persist the original thresholds in status.
		if err := r.Status().Update(ctx, disruption); err != nil {
			log.Error(err, "failed to update status with original liveness thresholds")
			return ctrl.Result{}, err
		}
	}

	// Step 5b: For ResumeProcess, restore liveness probes that were previously patched.
	if disruption.Spec.Action == valkeyiov1alpha1.DisruptionResumeProcess {
		if err := r.restoreLivenessThresholds(ctx, disruption); err != nil {
			log.Error(err, "failed to restore liveness thresholds on ResumeProcess")
			// Continue with the resume — restoring probes is best-effort.
		}
	}

	// Step 6: For PauseResumeLoop, initialize loop progress and requeue immediately.
	// The loop state machine will handle SIGSTOP/SIGCONT cycling via requeue-based scheduling.
	// We do NOT run the executor here — the first SIGSTOP is sent in the Pausing phase.
	if disruption.Spec.Action == valkeyiov1alpha1.DisruptionPauseResumeLoop {
		disruption.Status.Phase = valkeyiov1alpha1.DisruptionPhaseExecuting
		disruption.Status.LoopProgress = &valkeyiov1alpha1.LoopProgressStatus{
			CurrentIteration:           1,
			TotalIterations:            disruption.Spec.Loop.Repeat,
			CurrentPhase:               LoopPhasePausing,
			OriginalLivenessThresholds: disruption.Status.LoopProgress.GetOriginalLivenessThresholds(),
		}
		if err := r.Status().Update(ctx, disruption); err != nil {
			log.Error(err, "failed to initialize PauseResumeLoop progress")
			return ctrl.Result{}, err
		}
		log.V(1).Info("PauseResumeLoop initialized, starting loop state machine",
			"repeat", disruption.Spec.Loop.Repeat,
			"intervalSec", disruption.Spec.Loop.IntervalSec)
		// Requeue immediately to enter the loop state machine.
		return ctrl.Result{Requeue: true}, nil
	}

	// Step 6 (non-loop): Execute disruption actions via DisruptionExecutor.
	executor := NewDisruptionExecutor(r.Client, r.Clientset, r.Config)
	var completedCount int32

	for _, target := range validTargets {
		pod := findPodForNode(target, pods)
		if pod == nil {
			failedNodes = append(failedNodes, valkeyiov1alpha1.FailedNode{
				Address: target.Address,
				Reason:  "unreachable",
			})
			continue
		}

		// Check pod is running.
		if pod.Status.Phase != corev1.PodRunning {
			failedNodes = append(failedNodes, valkeyiov1alpha1.FailedNode{
				Address: target.Address,
				Reason:  "unreachable",
			})
			continue
		}

		execErr := executor.executeDisruptionAction(ctx, disruption.Spec.Action, target, pod, state)

		// Handle ResumeProcess on already-running process as no-op with warning.
		if execErr == nil && isAlreadyRunning(disruption.Spec.Action) {
			log.V(1).Info("ResumeProcess on already-running process is a no-op",
				"target", target.Address, "warning", "already-running")
			completedCount++
			continue
		}

		if execErr != nil {
			log.Error(execErr, "disruption action failed", "target", target.Address, "action", disruption.Spec.Action)
			failedNodes = append(failedNodes, valkeyiov1alpha1.FailedNode{
				Address: target.Address,
				Reason:  fmt.Sprintf("execution-failed: %v", execErr),
			})
			continue
		}
		completedCount++
	}

	// Step 7: Update final status.
	completionTime := metav1.Now()
	disruption.Status.CompletedCount = completedCount
	disruption.Status.FailedNodes = failedNodes
	disruption.Status.CompletionTime = &completionTime

	if len(failedNodes) > 0 && completedCount == 0 {
		disruption.Status.Phase = valkeyiov1alpha1.DisruptionPhaseFailed
	} else {
		disruption.Status.Phase = valkeyiov1alpha1.DisruptionPhaseCompleted
	}

	if err := r.Status().Update(ctx, disruption); err != nil {
		log.Error(err, "failed to update final status")
		return ctrl.Result{}, err
	}

	log.V(1).Info("disruption reconcile complete",
		"action", disruption.Spec.Action,
		"completed", completedCount,
		"failed", len(failedNodes))

	// For PauseProcess, arm the auto-resume timer by requeuing after pauseTimeoutSec.
	if disruption.Spec.Action == valkeyiov1alpha1.DisruptionPauseProcess &&
		disruption.Status.Phase == valkeyiov1alpha1.DisruptionPhaseCompleted &&
		completedCount > 0 {
		requeueAfter := time.Duration(disruption.Spec.PauseTimeoutSec) * time.Second
		log.V(1).Info("arming auto-resume timer", "requeueAfter", requeueAfter)
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	return ctrl.Result{}, nil
}

// collectTargetPods collects the pods corresponding to the valid target nodes.
func collectTargetPods(targets []*valkey.NodeState, pods *corev1.PodList) []*corev1.Pod {
	var result []*corev1.Pod
	for _, target := range targets {
		pod := findPodForNode(target, pods)
		if pod != nil {
			result = append(result, pod)
		}
	}
	return result
}

// selectTargets selects nodes from the cluster state that match ALL selector
// criteria: role AND zone AND shardRange AND count.
// If count is 0, all matching nodes are returned.
// If count > 0, up to count matching nodes are returned.
func (r *ValkeyDisruptionReconciler) selectTargets(
	spec valkeyiov1alpha1.ValkeyDisruptionSpec,
	state *valkey.ClusterState,
	pods *corev1.PodList,
	addressZones map[string]string,
) ([]*valkey.NodeState, error) {
	var candidates []*valkey.NodeState

	for _, shard := range state.Shards {
		for _, node := range shard.Nodes {
			if matchesSelector(node, spec.Selector, shard, state, pods, addressZones) {
				candidates = append(candidates, node)
			}
		}
	}

	// Apply count limit.
	if spec.Selector.Count > 0 && int(spec.Selector.Count) < len(candidates) {
		candidates = candidates[:spec.Selector.Count]
	}

	return candidates, nil
}

// matchesSelector checks if a node matches ALL selector criteria.
func matchesSelector(
	node *valkey.NodeState,
	selector valkeyiov1alpha1.DisruptionSelector,
	shard *valkey.ShardState,
	state *valkey.ClusterState,
	pods *corev1.PodList,
	addressZones map[string]string,
) bool {
	// Match role.
	if selector.Role != "" {
		if selector.Role == "primary" && !node.IsPrimary() {
			return false
		}
		if selector.Role == "replica" && node.IsPrimary() {
			return false
		}
	}

	// Match zone.
	if selector.Zone != "" {
		nodeZone := addressZones[node.Address]
		if nodeZone != selector.Zone {
			return false
		}
	}

	// Match shard range.
	if selector.ShardRange != nil {
		shardIndex := findShardIndex(shard, state, pods)
		if shardIndex < 0 {
			return false
		}
		if int32(shardIndex) < selector.ShardRange.Start || int32(shardIndex) > selector.ShardRange.End {
			return false
		}
	}

	return true
}

// findShardIndex determines the shard index for a given ShardState by looking
// up any node in the shard against the pod labels.
func findShardIndex(shard *valkey.ShardState, state *valkey.ClusterState, pods *corev1.PodList) int {
	for _, node := range shard.Nodes {
		_, shardIdx := podRoleAndShard(node.Address, pods)
		if shardIdx >= 0 {
			return shardIdx
		}
	}
	return -1
}

// resolveAddressZones builds a map of podIP -> zone. It prefers the pod label
// topology.kubernetes.io/zone and falls back to the hosting Node label when the
// pod label is not present.
func (r *ValkeyDisruptionReconciler) resolveAddressZones(ctx context.Context, pods *corev1.PodList) map[string]string {
	const zoneLabel = "topology.kubernetes.io/zone"

	addressZones := make(map[string]string, len(pods.Items))
	nodeNames := make(map[string]struct{})

	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.PodIP == "" {
			continue
		}
		if zone := pod.Labels[zoneLabel]; zone != "" {
			addressZones[pod.Status.PodIP] = zone
			continue
		}
		if pod.Spec.NodeName != "" {
			nodeNames[pod.Spec.NodeName] = struct{}{}
		}
	}

	nodeZones := make(map[string]string, len(nodeNames))
	for nodeName := range nodeNames {
		node := &corev1.Node{}
		if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
			continue
		}
		if zone := node.Labels[zoneLabel]; zone != "" {
			nodeZones[nodeName] = zone
		}
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.PodIP == "" || addressZones[pod.Status.PodIP] != "" || pod.Spec.NodeName == "" {
			continue
		}
		if zone := nodeZones[pod.Spec.NodeName]; zone != "" {
			addressZones[pod.Status.PodIP] = zone
		}
	}

	return addressZones
}

// validateSafety checks safety constraints for the disruption action and
// returns the list of valid targets and any failed nodes.
//
// Safety rules:
// - Failover/KillProcess on primaries: reject if no synced replica (reason "no-synced-replica")
// - Failover: all targets must be primaries AND each must have at least one replica
func (r *ValkeyDisruptionReconciler) validateSafety(
	action valkeyiov1alpha1.DisruptionAction,
	targets []*valkey.NodeState,
	state *valkey.ClusterState,
) ([]*valkey.NodeState, []valkeyiov1alpha1.FailedNode) {
	var validTargets []*valkey.NodeState
	var failedNodes []valkeyiov1alpha1.FailedNode

	for _, target := range targets {
		switch action {
		case valkeyiov1alpha1.DisruptionFailover:
			// Failover targets must be primaries.
			if !target.IsPrimary() {
				failedNodes = append(failedNodes, valkeyiov1alpha1.FailedNode{
					Address: target.Address,
					Reason:  "failover-requires-primary",
				})
				continue
			}
			// Each primary must have at least one replica.
			if !primaryHasReplica(target, state) {
				failedNodes = append(failedNodes, valkeyiov1alpha1.FailedNode{
					Address: target.Address,
					Reason:  "no-synced-replica",
				})
				continue
			}
			// Check that at least one replica is synced.
			if !primaryHasSyncedReplica(target, state) {
				failedNodes = append(failedNodes, valkeyiov1alpha1.FailedNode{
					Address: target.Address,
					Reason:  "no-synced-replica",
				})
				continue
			}
			validTargets = append(validTargets, target)

		case valkeyiov1alpha1.DisruptionKillProcess:
			// KillProcess on primaries: reject if no synced replica.
			if target.IsPrimary() && !primaryHasSyncedReplica(target, state) {
				failedNodes = append(failedNodes, valkeyiov1alpha1.FailedNode{
					Address: target.Address,
					Reason:  "no-synced-replica",
				})
				continue
			}
			validTargets = append(validTargets, target)

		default:
			// PauseProcess, ResumeProcess, DeletePod, PauseResumeLoop: no extra safety checks.
			validTargets = append(validTargets, target)
		}
	}

	return validTargets, failedNodes
}

// primaryHasReplica checks if a primary node has at least one replica in its shard.
func primaryHasReplica(primary *valkey.NodeState, state *valkey.ClusterState) bool {
	for _, shard := range state.Shards {
		if shard.PrimaryId != primary.Id {
			continue
		}
		for _, node := range shard.Nodes {
			if node.Id != shard.PrimaryId {
				return true
			}
		}
	}
	return false
}

// primaryHasSyncedReplica checks if a primary has at least one replica with
// master_link_status == "up" (synced replication link).
func primaryHasSyncedReplica(primary *valkey.NodeState, state *valkey.ClusterState) bool {
	for _, shard := range state.Shards {
		if shard.PrimaryId != primary.Id {
			continue
		}
		for _, node := range shard.Nodes {
			if node.Id != shard.PrimaryId && node.IsReplicationInSync() {
				return true
			}
		}
	}
	return false
}

// findPodForNode finds the pod matching a node's IP address.
func findPodForNode(node *valkey.NodeState, pods *corev1.PodList) *corev1.Pod {
	idx := slices.IndexFunc(pods.Items, func(p corev1.Pod) bool {
		return p.Status.PodIP == node.Address
	})
	if idx == -1 {
		return nil
	}
	return &pods.Items[idx]
}

// getClusterStateFromPods builds the Valkey cluster state from the pod list.
func getClusterStateFromPods(ctx context.Context, pods *corev1.PodList) *valkey.ClusterState {
	ips := make([]string, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.Status.PodIP == "" {
			continue
		}
		ips = append(ips, pod.Status.PodIP)
	}
	return valkey.GetClusterState(ctx, ips, DefaultPort)
}

// setFailed updates the disruption status to Failed with the given message.
func (r *ValkeyDisruptionReconciler) setFailed(ctx context.Context, disruption *valkeyiov1alpha1.ValkeyDisruption, message string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	disruption.Status.Phase = valkeyiov1alpha1.DisruptionPhaseFailed
	now := metav1.Now()
	disruption.Status.CompletionTime = &now
	disruption.Status.FailedNodes = append(disruption.Status.FailedNodes, valkeyiov1alpha1.FailedNode{
		Address: "",
		Reason:  message,
	})
	if err := r.Status().Update(ctx, disruption); err != nil {
		log.Error(err, "failed to update status to Failed")
		return ctrl.Result{}, err
	}
	log.Info("disruption failed", "reason", message)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ValkeyDisruptionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&valkeyiov1alpha1.ValkeyDisruption{}).
		Named("valkeydisruption").
		Complete(r)
}
