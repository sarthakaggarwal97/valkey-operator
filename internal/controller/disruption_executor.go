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
	"bytes"
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
	"valkey.io/valkey-operator/internal/valkey"
)

const (
	// valkeyServerContainer is the container name for the valkey-server process.
	valkeyServerContainer = "valkey-server"
)

// DisruptionExecutor performs actual disruption actions on target containers.
// It uses client-go remotecommand for exec into containers (kubectl exec equivalent)
// and the controller-runtime client for Kubernetes API operations.
type DisruptionExecutor struct {
	client    client.Client
	clientset kubernetes.Interface
	config    *rest.Config
}

// NewDisruptionExecutor creates a new DisruptionExecutor with the given clients.
func NewDisruptionExecutor(c client.Client, clientset kubernetes.Interface, config *rest.Config) *DisruptionExecutor {
	return &DisruptionExecutor{
		client:    c,
		clientset: clientset,
		config:    config,
	}
}

// executeDisruptionAction performs the actual disruption on a target node's container.
// It routes to the correct execution path based on the action type.
// valkey-server runs as PID 1 (the container's main process); all signal commands
// target PID 1 directly — no PID file lookup or pgrep needed.
func (de *DisruptionExecutor) executeDisruptionAction(
	ctx context.Context,
	action valkeyiov1alpha1.DisruptionAction,
	target *valkey.NodeState,
	pod *corev1.Pod,
	clusterState *valkey.ClusterState,
) error {
	log := logf.FromContext(ctx)

	switch action {
	case valkeyiov1alpha1.DisruptionPauseProcess:
		// SIGSTOP freezes the entire process including cluster bus.
		// The node will become pfail within cluster-node-timeout.
		log.V(1).Info("sending SIGSTOP to valkey-server", "pod", pod.Name, "target", target.Address)
		return de.execInContainer(ctx, pod, valkeyServerContainer,
			[]string{"sh", "-c", "kill -STOP 1"})

	case valkeyiov1alpha1.DisruptionResumeProcess:
		// SIGCONT resumes a SIGSTOP'd process. The node rejoins via gossip.
		log.V(1).Info("sending SIGCONT to valkey-server", "pod", pod.Name, "target", target.Address)
		return de.execInContainer(ctx, pod, valkeyServerContainer,
			[]string{"sh", "-c", "kill -CONT 1"})

	case valkeyiov1alpha1.DisruptionKillProcess:
		// SIGKILL to PID 1 causes the container to exit.
		// Kubernetes restarts the pod via the Deployment's restart policy.
		// The node rejoins the cluster automatically after restart.
		log.V(1).Info("sending SIGKILL to valkey-server", "pod", pod.Name, "target", target.Address)
		return de.execInContainer(ctx, pod, valkeyServerContainer,
			[]string{"sh", "-c", "kill -9 1"})

	case valkeyiov1alpha1.DisruptionFailover:
		// CLUSTER FAILOVER must be sent to a REPLICA, not the primary.
		// Look up the replica for the target primary's shard.
		log.V(1).Info("initiating failover for primary", "pod", pod.Name, "target", target.Address)
		replica, err := de.findReplicaForPrimary(target, clusterState)
		if err != nil {
			return fmt.Errorf("failed to find replica for primary %s: %w", target.Address, err)
		}
		log.V(1).Info("sending CLUSTER FAILOVER to replica", "replica", replica.Address, "primary", target.Address)
		return replica.Client.Do(ctx, replica.Client.B().ClusterFailover().Build()).Error()

	case valkeyiov1alpha1.DisruptionDeletePod:
		// Delete the pod via the Kubernetes API. The Deployment controller will recreate it.
		log.V(1).Info("deleting pod", "pod", pod.Name, "target", target.Address)
		return de.client.Delete(ctx, pod)

	case valkeyiov1alpha1.DisruptionPauseResumeLoop:
		// PauseResumeLoop orchestration is handled by the controller (task 7.4).
		// The executor delegates individual pause/resume actions within the loop.
		// This case should not be called directly — the controller manages the
		// state machine and calls PauseProcess/ResumeProcess for each phase.
		return fmt.Errorf("PauseResumeLoop should be orchestrated by the controller, not called directly on the executor")

	default:
		return fmt.Errorf("unknown disruption action: %s", action)
	}
}

// execInContainer executes a command inside a container using client-go remotecommand,
// equivalent to kubectl exec. It targets the specified container within the pod.
func (de *DisruptionExecutor) execInContainer(
	ctx context.Context,
	pod *corev1.Pod,
	containerName string,
	command []string,
) error {
	req := de.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(de.config, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to create SPDY executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return fmt.Errorf("exec failed (stderr: %s): %w", stderr.String(), err)
	}

	return nil
}

// findReplicaForPrimary looks up the replica for a targeted primary's shard
// from the cluster state. The returned replica is required to be in sync
// (master_link_status=up) so CLUSTER FAILOVER can succeed predictably.
func (de *DisruptionExecutor) findReplicaForPrimary(
	target *valkey.NodeState,
	clusterState *valkey.ClusterState,
) (*valkey.NodeState, error) {
	// Find the shard that contains this primary.
	for _, shard := range clusterState.Shards {
		if shard.PrimaryId != target.Id {
			continue
		}
		// Find an in-sync replica in this shard.
		var outOfSyncReplica *valkey.NodeState
		for _, node := range shard.Nodes {
			if node.Id != shard.PrimaryId && !node.IsPrimary() {
				if node.IsReplicationInSync() {
					return node, nil
				}
				if outOfSyncReplica == nil {
					outOfSyncReplica = node
				}
			}
		}
		if outOfSyncReplica != nil {
			return nil, fmt.Errorf("primary %s (shard %s) has replicas but none are in sync", target.Address, shard.Id)
		}
		return nil, fmt.Errorf("primary %s (shard %s) has no replica", target.Address, shard.Id)
	}
	return nil, fmt.Errorf("primary %s not found in any shard", target.Address)
}
