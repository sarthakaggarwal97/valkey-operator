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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
	"valkey.io/valkey-operator/internal/valkey"
)

// Loop phase constants matching the LoopProgressStatus.CurrentPhase enum.
const (
	LoopPhasePausing            = "Pausing"
	LoopPhaseWaitingAfterPause  = "WaitingAfterPause"
	LoopPhaseResuming           = "Resuming"
	LoopPhaseWaitingAfterResume = "WaitingAfterResume"
)

// reconcilePauseResumeLoop drives the PauseResumeLoop state machine.
// It is called from the main Reconcile method when the disruption action is
// PauseResumeLoop and the status phase is Executing with loop progress set.
//
// State machine transitions:
//
//	Pausing → send SIGSTOP to all targets → set phase to WaitingAfterPause → requeue after intervalSec
//	WaitingAfterPause → send SIGCONT to all targets → set phase to WaitingAfterResume, iteration++ → requeue after intervalSec
//	WaitingAfterResume → if iteration < repeat: set phase to Pausing → requeue immediately
//	                   → if iteration == repeat: restore liveness probes → set phase to Completed
//
// On controller restart: if currentPhase is WaitingAfterPause (nodes are paused),
// send SIGCONT first as a safety measure before resuming the loop.
func (r *ValkeyDisruptionReconciler) reconcilePauseResumeLoop(
	ctx context.Context,
	disruption *valkeyiov1alpha1.ValkeyDisruption,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	progress := disruption.Status.LoopProgress

	if progress == nil {
		return ctrl.Result{}, fmt.Errorf("loop progress is nil for PauseResumeLoop")
	}

	log.V(1).Info("reconciling PauseResumeLoop",
		"iteration", progress.CurrentIteration,
		"total", progress.TotalIterations,
		"phase", progress.CurrentPhase)

	intervalSec := disruption.Spec.Loop.IntervalSec

	// Fetch the referenced cluster and pods for target resolution.
	cluster := &valkeyiov1alpha1.ValkeyCluster{}
	clusterKey := client.ObjectKey{
		Namespace: disruption.Namespace,
		Name:      disruption.Spec.ClusterRef,
	}
	if err := r.Get(ctx, clusterKey, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			log.Error(err, "referenced ValkeyCluster not found during loop")
			return r.setFailed(ctx, disruption, fmt.Sprintf("ValkeyCluster %q not found", disruption.Spec.ClusterRef))
		}
		return ctrl.Result{}, err
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(cluster.Namespace), client.MatchingLabels(labels(cluster))); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list pods: %w", err)
	}

	state := getClusterStateFromPods(ctx, pods)
	defer state.CloseClients()
	addressZones := r.resolveAddressZones(ctx, pods)

	// Select the same targets that were originally selected.
	targets, err := r.selectTargets(disruption.Spec, state, pods, addressZones)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to select targets for loop: %w", err)
	}

	executor := NewDisruptionExecutor(r.Client, r.Clientset, r.Config)

	switch progress.CurrentPhase {
	case LoopPhasePausing:
		return r.loopPhasePausing(ctx, disruption, targets, pods, executor, intervalSec)

	case LoopPhaseWaitingAfterPause:
		// On controller restart, nodes may still be paused. Send SIGCONT first
		// as a safety measure before transitioning to the resume phase.
		return r.loopPhaseWaitingAfterPause(ctx, disruption, targets, pods, executor, intervalSec)

	case LoopPhaseResuming:
		return r.loopPhaseResuming(ctx, disruption, targets, pods, executor, intervalSec)

	case LoopPhaseWaitingAfterResume:
		return r.loopPhaseWaitingAfterResume(ctx, disruption)

	default:
		return ctrl.Result{}, fmt.Errorf("unknown loop phase: %s", progress.CurrentPhase)
	}
}

// loopPhasePausing sends SIGSTOP to all targets and transitions to WaitingAfterPause.
func (r *ValkeyDisruptionReconciler) loopPhasePausing(
	ctx context.Context,
	disruption *valkeyiov1alpha1.ValkeyDisruption,
	targets []*valkey.NodeState,
	pods *corev1.PodList,
	executor *DisruptionExecutor,
	intervalSec int32,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	progress := disruption.Status.LoopProgress

	log.V(1).Info("loop: sending SIGSTOP to targets",
		"iteration", progress.CurrentIteration,
		"targetCount", len(targets))

	// Send SIGSTOP to all targets.
	for _, target := range targets {
		pod := findPodForNode(target, pods)
		if pod == nil || pod.Status.Phase != corev1.PodRunning {
			log.V(1).Info("skipping unreachable target during pause", "target", target.Address)
			continue
		}
		if err := executor.execInContainer(ctx, pod, valkeyServerContainer,
			[]string{"sh", "-c", "kill -STOP 1"}); err != nil {
			log.Error(err, "failed to send SIGSTOP during loop", "target", target.Address)
			// Continue with other targets — partial pause is acceptable.
		}
	}

	// Transition to WaitingAfterPause and requeue after intervalSec.
	progress.CurrentPhase = LoopPhaseWaitingAfterPause
	if err := r.Status().Update(ctx, disruption); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update loop status to WaitingAfterPause: %w", err)
	}

	return ctrl.Result{RequeueAfter: time.Duration(intervalSec) * time.Second}, nil
}

// loopPhaseWaitingAfterPause sends SIGCONT to all targets and transitions to WaitingAfterResume.
// This phase is reached after the pause interval has elapsed.
// On controller restart, if we find ourselves in WaitingAfterPause, we send SIGCONT
// immediately as a safety measure (nodes may still be paused from before the restart).
func (r *ValkeyDisruptionReconciler) loopPhaseWaitingAfterPause(
	ctx context.Context,
	disruption *valkeyiov1alpha1.ValkeyDisruption,
	targets []*valkey.NodeState,
	pods *corev1.PodList,
	executor *DisruptionExecutor,
	intervalSec int32,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	progress := disruption.Status.LoopProgress

	log.V(1).Info("loop: sending SIGCONT to targets (resume after pause)",
		"iteration", progress.CurrentIteration,
		"targetCount", len(targets))

	// Send SIGCONT to all targets.
	for _, target := range targets {
		pod := findPodForNode(target, pods)
		if pod == nil || pod.Status.Phase != corev1.PodRunning {
			log.V(1).Info("skipping unreachable target during resume", "target", target.Address)
			continue
		}
		if err := executor.execInContainer(ctx, pod, valkeyServerContainer,
			[]string{"sh", "-c", "kill -CONT 1"}); err != nil {
			log.Error(err, "failed to send SIGCONT during loop", "target", target.Address)
			// Continue — SIGCONT on already-running process is a no-op.
		}
	}

	// Increment iteration and transition to WaitingAfterResume.
	progress.CurrentIteration++
	progress.CurrentPhase = LoopPhaseWaitingAfterResume
	if err := r.Status().Update(ctx, disruption); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update loop status to WaitingAfterResume: %w", err)
	}

	return ctrl.Result{RequeueAfter: time.Duration(intervalSec) * time.Second}, nil
}

// loopPhaseResuming is an alias for the resume action within the loop.
// In the current state machine design, resuming is handled in WaitingAfterPause.
// This phase exists for completeness if the state machine is extended.
func (r *ValkeyDisruptionReconciler) loopPhaseResuming(
	ctx context.Context,
	disruption *valkeyiov1alpha1.ValkeyDisruption,
	targets []*valkey.NodeState,
	pods *corev1.PodList,
	executor *DisruptionExecutor,
	intervalSec int32,
) (ctrl.Result, error) {
	// Resuming phase: send SIGCONT and transition to WaitingAfterResume.
	// This mirrors WaitingAfterPause behavior for safety.
	return r.loopPhaseWaitingAfterPause(ctx, disruption, targets, pods, executor, intervalSec)
}

// loopPhaseWaitingAfterResume checks if more iterations remain.
// If so, transitions back to Pausing for the next cycle.
// If all iterations are complete, restores liveness probes and sets phase to Completed.
func (r *ValkeyDisruptionReconciler) loopPhaseWaitingAfterResume(
	ctx context.Context,
	disruption *valkeyiov1alpha1.ValkeyDisruption,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	progress := disruption.Status.LoopProgress

	if progress.CurrentIteration <= progress.TotalIterations {
		// More iterations remain — transition to Pausing for the next cycle.
		log.V(1).Info("loop: starting next iteration",
			"nextIteration", progress.CurrentIteration,
			"total", progress.TotalIterations)

		progress.CurrentPhase = LoopPhasePausing
		if err := r.Status().Update(ctx, disruption); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update loop status to Pausing: %w", err)
		}

		// Requeue immediately to start the next pause cycle.
		return ctrl.Result{Requeue: true}, nil
	}

	// All iterations complete — restore liveness probes and mark as Completed.
	log.V(1).Info("loop: all iterations complete, restoring liveness probes",
		"totalIterations", progress.TotalIterations)

	if err := r.restoreLivenessThresholds(ctx, disruption); err != nil {
		log.Error(err, "failed to restore liveness thresholds after loop completion")
		// Continue to mark as completed — restoring probes is best-effort.
	}

	now := metav1.Now()
	disruption.Status.Phase = valkeyiov1alpha1.DisruptionPhaseCompleted
	disruption.Status.CompletionTime = &now
	if err := r.Status().Update(ctx, disruption); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update loop status to Completed: %w", err)
	}

	log.Info("PauseResumeLoop completed",
		"iterations", progress.TotalIterations,
		"disruption", disruption.Name)

	return ctrl.Result{}, nil
}
