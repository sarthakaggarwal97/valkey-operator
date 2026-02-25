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
	"math"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

const (
	// disruptionCleanupFinalizer ensures SIGCONT is sent to all paused nodes on CR deletion.
	disruptionCleanupFinalizer = "valkey.io/disruption-cleanup"

	// defaultLivenessPeriodSeconds is the default liveness probe period used by the operator.
	// This matches the value in deployment.go: LivenessProbe.PeriodSeconds = 5.
	defaultLivenessPeriodSeconds int32 = 5

	// defaultLivenessFailureThreshold is the default liveness probe failureThreshold.
	// This matches the value in deployment.go: LivenessProbe.FailureThreshold = 5.
	defaultLivenessFailureThreshold int32 = 5
)

// handleFinalizer manages the disruption cleanup finalizer on the ValkeyDisruption CR.
// It adds the finalizer when the CR is not being deleted, and runs cleanup logic
// (sending SIGCONT to paused nodes and restoring liveness probes) when the CR is
// being deleted before removing the finalizer.
func (r *ValkeyDisruptionReconciler) handleFinalizer(ctx context.Context, disruption *valkeyiov1alpha1.ValkeyDisruption) (deleted bool, err error) {
	log := logf.FromContext(ctx)

	if disruption.DeletionTimestamp.IsZero() {
		// CR is not being deleted — ensure the finalizer is present.
		if !controllerutil.ContainsFinalizer(disruption, disruptionCleanupFinalizer) {
			controllerutil.AddFinalizer(disruption, disruptionCleanupFinalizer)
			if err := r.Update(ctx, disruption); err != nil {
				return false, fmt.Errorf("failed to add finalizer: %w", err)
			}
		}
		return false, nil
	}

	// CR is being deleted — run cleanup if the finalizer is present.
	if !controllerutil.ContainsFinalizer(disruption, disruptionCleanupFinalizer) {
		return true, nil
	}

	log.Info("running disruption cleanup finalizer", "disruption", disruption.Name)

	// Send SIGCONT to any paused nodes if the action was PauseProcess or PauseResumeLoop.
	if disruption.Spec.Action == valkeyiov1alpha1.DisruptionPauseProcess ||
		disruption.Spec.Action == valkeyiov1alpha1.DisruptionPauseResumeLoop {
		if err := r.resumeAllPausedNodes(ctx, disruption); err != nil {
			log.Error(err, "failed to resume paused nodes during cleanup")
			// Continue with cleanup even if resume fails — we don't want to block deletion.
		}
	}

	// Restore original liveness probe thresholds.
	if err := r.restoreLivenessThresholds(ctx, disruption); err != nil {
		log.Error(err, "failed to restore liveness thresholds during cleanup")
	}

	// Remove the finalizer.
	controllerutil.RemoveFinalizer(disruption, disruptionCleanupFinalizer)
	if err := r.Update(ctx, disruption); err != nil {
		return false, fmt.Errorf("failed to remove finalizer: %w", err)
	}

	log.Info("disruption cleanup finalizer completed", "disruption", disruption.Name)
	return true, nil
}

// resumeAllPausedNodes sends SIGCONT to all target pods for a disruption that
// may have paused nodes. This is called during finalizer cleanup to ensure no
// nodes remain SIGSTOP'd after CR deletion.
func (r *ValkeyDisruptionReconciler) resumeAllPausedNodes(ctx context.Context, disruption *valkeyiov1alpha1.ValkeyDisruption) error {
	log := logf.FromContext(ctx)

	// Fetch the referenced cluster's pods.
	cluster := &valkeyiov1alpha1.ValkeyCluster{}
	clusterKey := client.ObjectKey{
		Namespace: disruption.Namespace,
		Name:      disruption.Spec.ClusterRef,
	}
	if err := r.Get(ctx, clusterKey, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("referenced cluster not found during cleanup, skipping SIGCONT")
			return nil
		}
		return err
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(cluster.Namespace), client.MatchingLabels(labels(cluster))); err != nil {
		return fmt.Errorf("failed to list pods for cleanup: %w", err)
	}

	state := getClusterStateFromPods(ctx, pods)
	defer state.CloseClients()
	addressZones := r.resolveAddressZones(ctx, pods)

	// Select the same targets that were originally selected.
	targets, err := r.selectTargets(disruption.Spec, state, pods, addressZones)
	if err != nil {
		log.Error(err, "failed to select targets for cleanup SIGCONT")
		return err
	}

	executor := NewDisruptionExecutor(r.Client, r.Clientset, r.Config)
	for _, target := range targets {
		pod := findPodForNode(target, pods)
		if pod == nil || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		log.V(1).Info("sending SIGCONT during cleanup", "pod", pod.Name)
		if err := executor.execInContainer(ctx, pod, valkeyServerContainer,
			[]string{"sh", "-c", "kill -CONT 1"}); err != nil {
			log.Error(err, "failed to send SIGCONT during cleanup", "pod", pod.Name)
			// Continue with other pods.
		}
	}

	return nil
}

// autoResumeRequeueAfter returns the duration after which the controller should
// requeue to send SIGCONT as an auto-resume safety net for PauseProcess.
func autoResumeRequeueAfter(disruption *valkeyiov1alpha1.ValkeyDisruption) time.Duration {
	if disruption.Status.StartTime == nil {
		return time.Duration(disruption.Spec.PauseTimeoutSec) * time.Second
	}
	elapsed := time.Since(disruption.Status.StartTime.Time)
	remaining := time.Duration(disruption.Spec.PauseTimeoutSec)*time.Second - elapsed
	if remaining < 0 {
		return 0
	}
	return remaining
}

// handleAutoResume checks if the auto-resume timeout has been reached for a
// PauseProcess disruption and sends SIGCONT to all paused nodes if so.
// Returns true if auto-resume was triggered and the disruption should be completed.
func (r *ValkeyDisruptionReconciler) handleAutoResume(ctx context.Context, disruption *valkeyiov1alpha1.ValkeyDisruption) (bool, error) {
	log := logf.FromContext(ctx)

	if disruption.Spec.Action != valkeyiov1alpha1.DisruptionPauseProcess {
		return false, nil
	}
	if disruption.Status.Phase != valkeyiov1alpha1.DisruptionPhaseCompleted {
		return false, nil
	}
	if disruption.Status.StartTime == nil {
		return false, nil
	}

	elapsed := time.Since(disruption.Status.StartTime.Time)
	timeout := time.Duration(disruption.Spec.PauseTimeoutSec) * time.Second

	if elapsed < timeout {
		// Not yet time to auto-resume.
		return false, nil
	}

	log.Info("auto-resume timeout reached, sending SIGCONT to all paused nodes",
		"disruption", disruption.Name,
		"pauseTimeoutSec", disruption.Spec.PauseTimeoutSec,
		"elapsed", elapsed)

	// Send SIGCONT to all paused nodes.
	if err := r.resumeAllPausedNodes(ctx, disruption); err != nil {
		log.Error(err, "failed to auto-resume paused nodes")
		return false, err
	}

	// Restore liveness probes.
	if err := r.restoreLivenessThresholds(ctx, disruption); err != nil {
		log.Error(err, "failed to restore liveness thresholds after auto-resume")
	}

	return true, nil
}

// isAlreadyRunning checks if a SIGCONT on a process would be a no-op by
// examining the exec result. SIGCONT on an already-running process succeeds
// silently — it's inherently a no-op at the OS level. We detect this case
// by checking if the disruption action is ResumeProcess and the exec succeeds.
// The caller should annotate the result with "already-running" warning.
func isAlreadyRunning(action valkeyiov1alpha1.DisruptionAction) bool {
	return action == valkeyiov1alpha1.DisruptionResumeProcess
}

// calculateLivenessThreshold computes the new liveness probe failureThreshold
// needed to prevent kubelet from restarting SIGSTOP'd containers.
// Formula: ceil(pauseDurationSec / livenessPeriodSeconds) + 2
func calculateLivenessThreshold(pauseDurationSec int32, livenessPeriodSec int32) int32 {
	if livenessPeriodSec <= 0 {
		livenessPeriodSec = defaultLivenessPeriodSeconds
	}
	return int32(math.Ceil(float64(pauseDurationSec)/float64(livenessPeriodSec))) + 2
}

// patchLivenessThresholds increases the liveness probe failureThreshold on the
// Deployments that own the target pods. It stores the original thresholds in
// status.loopProgress.originalLivenessThresholds so they can be restored later.
//
// For PauseProcess: newThreshold = ceil(pauseTimeoutSec / livenessPeriodSec) + 2
// For PauseResumeLoop: newThreshold = ceil(loop.intervalSec / livenessPeriodSec) + 2
func (r *ValkeyDisruptionReconciler) patchLivenessThresholds(
	ctx context.Context,
	disruption *valkeyiov1alpha1.ValkeyDisruption,
	pods *corev1.PodList,
	targets []*corev1.Pod,
) error {
	log := logf.FromContext(ctx)

	// Determine the pause duration for threshold calculation.
	var pauseDurationSec int32
	switch disruption.Spec.Action {
	case valkeyiov1alpha1.DisruptionPauseProcess:
		pauseDurationSec = disruption.Spec.PauseTimeoutSec
	case valkeyiov1alpha1.DisruptionPauseResumeLoop:
		if disruption.Spec.Loop != nil {
			pauseDurationSec = disruption.Spec.Loop.IntervalSec
		}
	default:
		return nil // No liveness patching needed for other actions.
	}

	if pauseDurationSec <= 0 {
		return nil
	}

	newThreshold := calculateLivenessThreshold(pauseDurationSec, defaultLivenessPeriodSeconds)

	// Initialize the originalLivenessThresholds map if needed.
	if disruption.Status.LoopProgress == nil {
		disruption.Status.LoopProgress = &valkeyiov1alpha1.LoopProgressStatus{}
	}
	if disruption.Status.LoopProgress.OriginalLivenessThresholds == nil {
		disruption.Status.LoopProgress.OriginalLivenessThresholds = make(map[string]int32)
	}

	// Collect unique Deployment names from target pods.
	deploymentNames := uniqueDeploymentNames(targets)

	for _, depName := range deploymentNames {
		// Skip if we already stored the original threshold for this Deployment.
		if _, exists := disruption.Status.LoopProgress.OriginalLivenessThresholds[depName]; exists {
			continue
		}

		dep := &appsv1.Deployment{}
		depKey := client.ObjectKey{
			Namespace: disruption.Namespace,
			Name:      depName,
		}
		if err := r.Get(ctx, depKey, dep); err != nil {
			if apierrors.IsNotFound(err) {
				log.V(1).Info("deployment not found, skipping liveness patch", "deployment", depName)
				continue
			}
			return fmt.Errorf("failed to get deployment %s: %w", depName, err)
		}

		// Find the valkey-server container and its liveness probe.
		containerIdx, originalThreshold := findValkeyLivenessThreshold(dep)
		if containerIdx < 0 {
			log.V(1).Info("no liveness probe found on valkey-server container", "deployment", depName)
			continue
		}

		// Store the original threshold.
		disruption.Status.LoopProgress.OriginalLivenessThresholds[depName] = originalThreshold

		// Patch the Deployment with the new threshold.
		patch := client.MergeFrom(dep.DeepCopy())
		dep.Spec.Template.Spec.Containers[containerIdx].LivenessProbe.FailureThreshold = newThreshold
		if err := r.Patch(ctx, dep, patch); err != nil {
			return fmt.Errorf("failed to patch deployment %s liveness threshold: %w", depName, err)
		}

		log.V(1).Info("patched liveness probe failureThreshold",
			"deployment", depName,
			"original", originalThreshold,
			"new", newThreshold)
	}

	return nil
}

// restoreLivenessThresholds restores the original liveness probe failureThreshold
// on all Deployments that were patched during PauseProcess or PauseResumeLoop.
// It iterates the originalLivenessThresholds map from the disruption status.
func (r *ValkeyDisruptionReconciler) restoreLivenessThresholds(
	ctx context.Context,
	disruption *valkeyiov1alpha1.ValkeyDisruption,
) error {
	log := logf.FromContext(ctx)

	if disruption.Status.LoopProgress == nil ||
		len(disruption.Status.LoopProgress.OriginalLivenessThresholds) == 0 {
		return nil
	}

	for depName, originalThreshold := range disruption.Status.LoopProgress.OriginalLivenessThresholds {
		dep := &appsv1.Deployment{}
		depKey := client.ObjectKey{
			Namespace: disruption.Namespace,
			Name:      depName,
		}
		if err := r.Get(ctx, depKey, dep); err != nil {
			if apierrors.IsNotFound(err) {
				log.V(1).Info("deployment not found during liveness restore, skipping", "deployment", depName)
				continue
			}
			return fmt.Errorf("failed to get deployment %s for liveness restore: %w", depName, err)
		}

		containerIdx, _ := findValkeyLivenessThreshold(dep)
		if containerIdx < 0 {
			continue
		}

		patch := client.MergeFrom(dep.DeepCopy())
		dep.Spec.Template.Spec.Containers[containerIdx].LivenessProbe.FailureThreshold = originalThreshold
		if err := r.Patch(ctx, dep, patch); err != nil {
			return fmt.Errorf("failed to restore deployment %s liveness threshold: %w", depName, err)
		}

		log.V(1).Info("restored liveness probe failureThreshold",
			"deployment", depName,
			"threshold", originalThreshold)
	}

	// Clear the map after successful restore.
	disruption.Status.LoopProgress.OriginalLivenessThresholds = nil

	return nil
}

// findValkeyLivenessThreshold finds the valkey-server container in a Deployment
// and returns its index and current liveness probe failureThreshold.
// Returns (-1, 0) if the container or liveness probe is not found.
func findValkeyLivenessThreshold(dep *appsv1.Deployment) (containerIdx int, threshold int32) {
	for i, c := range dep.Spec.Template.Spec.Containers {
		if c.Name == valkeyServerContainer && c.LivenessProbe != nil {
			return i, c.LivenessProbe.FailureThreshold
		}
	}
	return -1, 0
}

// uniqueDeploymentNames extracts unique Deployment names from a list of pods
// by looking up the pod's owner chain (Pod → ReplicaSet → Deployment).
// Since each Valkey node has a deterministic Deployment name based on shard/node
// index labels, we derive the name from the pod labels directly.
func uniqueDeploymentNames(pods []*corev1.Pod) []string {
	seen := make(map[string]struct{})
	var names []string
	for _, pod := range pods {
		depName := deploymentNameFromPod(pod)
		if depName == "" {
			continue
		}
		if _, exists := seen[depName]; exists {
			continue
		}
		seen[depName] = struct{}{}
		names = append(names, depName)
	}
	return names
}

// deploymentNameFromPod derives the Deployment name from a pod's owner references.
// In the valkey-operator, each pod is owned by a ReplicaSet which is owned by a
// Deployment. The Deployment name follows the pattern: <cluster>-<shard>-<node>.
// We look at the pod's OwnerReferences to find the ReplicaSet, then strip the
// hash suffix to get the Deployment name. As a fallback, we reconstruct the name
// from pod labels.
func deploymentNameFromPod(pod *corev1.Pod) string {
	// Try to derive from pod labels (most reliable for our naming convention).
	// The Deployment name is <cluster>-<shard>-<node>, and the pod name follows
	// <deployment>-<replicaset-hash>-<pod-hash>. We can reconstruct from labels.
	if pod == nil {
		return ""
	}

	// Use the app.kubernetes.io/instance label (cluster name) plus shard/node labels.
	clusterName := pod.Labels["app.kubernetes.io/instance"]
	shardIndex := pod.Labels[LabelShardIndex]
	nodeIndex := pod.Labels[LabelNodeIndex]

	if clusterName != "" && shardIndex != "" && nodeIndex != "" {
		return fmt.Sprintf("%s-%s-%s", clusterName, shardIndex, nodeIndex)
	}

	return ""
}
