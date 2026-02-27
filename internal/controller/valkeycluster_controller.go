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
	"embed"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
	"valkey.io/valkey-operator/internal/valkey"
)

const (
	DefaultPort           = 6379
	DefaultClusterBusPort = 16379
	DefaultImage          = "valkey/valkey:9.0.0"
	DefaultExporterImage  = "oliver006/redis_exporter:v1.80.0"
	DefaultExporterPort   = 9121

	// Error messages
	statusUpdateFailedMsg = "failed to update status"
)

// ValkeyClusterReconciler reconciles a ValkeyCluster object
type ValkeyClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
}

//go:embed scripts/*
var scripts embed.FS

// +kubebuilder:rbac:groups=valkey.io,resources=valkeyclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=valkey.io,resources=valkeyclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=valkey.io,resources=valkeyclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="apps",resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="policy",resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=prometheusrules,verbs=get;list;watch;create;update;patch;delete

// Reconcile is the main reconciliation loop. On each invocation it drives the
// cluster one step closer to the desired state described by the ValkeyCluster
// spec. The pipeline runs in the following order:
//
//  1. Ensure the headless Service exists (upsertService).
//  2. Ensure the ConfigMap with valkey.conf and health-check scripts exists
//     (upsertConfigMap).
//  3. Ensure one Deployment per (shard, node) pair exists, each named
//     deterministically (e.g. mycluster-0-0) (upsertDeployments).
//  4. List all pods and build the Valkey cluster state by connecting to each
//     node and scraping CLUSTER INFO / CLUSTER NODES.
//  5. Forget stale nodes that no longer have a backing pod.
//  6. For every pending node (primary with no slots and cluster_known_nodes
//     <= 1), admit nodes in parallel batches using the AdmissionManager.
//     Primaries are admitted before replicas. The SeedMeetStrategy controls
//     how new nodes discover the cluster (seed-based or all-primaries).
//  7. Verify that the expected number of shards and replicas exist.
//  8. Verify that all 16384 hash slots are assigned.
//  9. If everything is healthy, mark the cluster Ready and requeue after 30s
//     for periodic health checks.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/reconcile
func (r *ValkeyClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.V(1).Info("reconcile...")

	cluster := &valkeyiov1alpha1.ValkeyCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Apply safe large-scale defaults for any omitted config fields and
	// emit warning events for potentially dangerous configurations.
	ApplyClusterDefaults(&cluster.Spec)
	EmitConfigWarnings(r.Recorder, cluster)

	if err := r.upsertService(ctx, cluster); err != nil {
		setCondition(cluster, valkeyiov1alpha1.ConditionReady, valkeyiov1alpha1.ReasonServiceError, err.Error(), metav1.ConditionFalse)
		_ = r.updateStatus(ctx, cluster, nil)
		return ctrl.Result{}, err
	}

	if err := r.upsertConfigMap(ctx, cluster); err != nil {
		setCondition(cluster, valkeyiov1alpha1.ConditionReady, valkeyiov1alpha1.ReasonConfigMapError, err.Error(), metav1.ConditionFalse)
		_ = r.updateStatus(ctx, cluster, nil)
		return ctrl.Result{}, err
	}

	if err := r.upsertDeployments(ctx, cluster); err != nil {
		setCondition(cluster, valkeyiov1alpha1.ConditionReady, valkeyiov1alpha1.ReasonDeploymentError, err.Error(), metav1.ConditionFalse)
		_ = r.updateStatus(ctx, cluster, nil)
		return ctrl.Result{}, err
	}

	// Ensure per-shard PodDisruptionBudgets exist when zone awareness is
	// enabled. PDBs are created after deployments so that the pods they
	// protect already exist (or are being created).
	if err := r.upsertPDBs(ctx, cluster); err != nil {
		log.Error(err, "failed to upsert PDBs")
		return ctrl.Result{}, err
	}

	// Ensure Prometheus ServiceMonitor and PrometheusRule resources exist
	// when metrics.enabled is true. These are created as unstructured objects
	// and degrade gracefully if the Prometheus Operator CRDs are not installed.
	if err := r.reconcileObservability(ctx, cluster); err != nil {
		log.Error(err, "failed to reconcile observability resources")
		// Non-fatal: observability is optional, continue reconciliation.
	}

	// Get all pods and their current Valkey Cluster state
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(cluster.Namespace), client.MatchingLabels(labels(cluster))); err != nil {
		log.Error(err, "failed to list Pods")
		setCondition(cluster, valkeyiov1alpha1.ConditionReady, valkeyiov1alpha1.ReasonPodListError, err.Error(), metav1.ConditionFalse)
		_ = r.updateStatus(ctx, cluster, nil)
		return ctrl.Result{}, err
	}
	state := r.getValkeyClusterState(ctx, pods)
	defer state.CloseClients()

	// Check zone spread when zone awareness is enabled. This emits a warning
	// event and sets ZoneSpreadDegraded=True if zones < replicas + 1.
	ApplyZoneDefaults(&cluster.Spec)
	checkZoneSpread(r.Recorder, cluster, pods)

	// Check if we need to forget stale non-existing nodes
	r.forgetStaleNodes(ctx, cluster, state, pods)

	// Process pending nodes in batches using the AdmissionManager. Nodes are
	// admitted in parallel up to the configured parallelism, with primaries
	// processed before replicas. The SeedMeetStrategy controls how new nodes
	// discover the cluster: "seed" MEETs a small seed set, "all" MEETs every
	// primary (legacy behavior).
	if len(state.PendingNodes) > 0 {
		log.V(1).Info("processing pending nodes", "count", len(state.PendingNodes))
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "BatchAdmission", "AddNodes",
			"Admitting up to %d of %d pending nodes", cluster.Spec.Admission.Parallelism, len(state.PendingNodes))
		setCondition(cluster, valkeyiov1alpha1.ConditionProgressing, valkeyiov1alpha1.ReasonAddingNodes, "Adding nodes to cluster", metav1.ConditionTrue)
		setCondition(cluster, valkeyiov1alpha1.ConditionReady, valkeyiov1alpha1.ReasonReconciling, "Cluster is Reconciling", metav1.ConditionFalse)
		setCondition(cluster, valkeyiov1alpha1.ConditionSlotsAssigned, valkeyiov1alpha1.ReasonSlotsUnassigned, "Assigning slots to nodes", metav1.ConditionFalse)
		_ = r.updateStatus(ctx, cluster, state)

		// Initialize SeedMeetStrategy based on admission config.
		seedStrategy := r.newSeedMeetStrategy(cluster, state)

		am := valkey.NewAdmissionManager(*cluster.Spec.Admission, seedStrategy)
		slotTracker := valkey.NewSlotTracker(state.GetUnassignedSlots())
		existingShards := len(state.Shards)
		assignedPrimaries := 0

		// Wire callbacks to existing controller methods.
		assignSlots := func(ctx context.Context, node *valkey.NodeState) error {
			slotRange, err := nextSlotRangeForPrimary(cluster, slotTracker, existingShards, assignedPrimaries)
			if err != nil {
				return err
			}
			if err := r.assignSlotsRangeToPrimary(ctx, cluster, node, slotRange); err != nil {
				return err
			}
			if err := slotTracker.Assign(slotRange); err != nil {
				return err
			}
			assignedPrimaries++
			return nil
		}
		attachReplica := func(ctx context.Context, node *valkey.NodeState, primaryID string) error {
			return node.Client.Do(ctx, node.Client.B().ClusterReplicate().NodeId(primaryID).Build()).Error()
		}

		batchResult, err := am.ProcessPendingNodes(
			ctx, state, pods, cluster,
			assignSlots,
			attachReplica,
			findShardPrimary,
			podRoleAndShard,
			shardExistsInTopology,
		)
		if err != nil {
			log.Error(err, "batch admission failed")
			r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, "BatchAdmissionFailed", "AddNodes",
				"Batch admission failed: %v", err)
			setCondition(cluster, valkeyiov1alpha1.ConditionDegraded, valkeyiov1alpha1.ReasonNodeAddFailed, err.Error(), metav1.ConditionTrue)
			_ = r.updateStatus(ctx, cluster, state)
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}

		if len(batchResult.Failed) > 0 {
			for _, f := range batchResult.Failed {
				log.Error(f.Err, "node admission failed", "address", f.NodeAddress)
			}
			r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, "BatchAdmissionPartial", "AddNodes",
				"Admitted %d nodes, %d failed", batchResult.Admitted, len(batchResult.Failed))
		} else {
			r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "BatchAdmissionComplete", "AddNodes",
				"Admitted %d nodes in %v (gossip convergence: %v)",
				batchResult.Admitted, batchResult.Duration, batchResult.GossipConvergence)
		}

		// Requeue to process remaining pending nodes or verify cluster state.
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// Check cluster status
	if len(state.Shards) < int(cluster.Spec.Shards) {
		log.V(1).Info("missing shards, requeue..")
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "WaitingForShards", "CheckShards", "%d of %d shards exist", len(state.Shards), cluster.Spec.Shards)
		setCondition(cluster, valkeyiov1alpha1.ConditionReady, valkeyiov1alpha1.ReasonMissingShards, "Waiting for all shards to be created", metav1.ConditionFalse)
		setCondition(cluster, valkeyiov1alpha1.ConditionProgressing, valkeyiov1alpha1.ReasonReconciling, "Creating shards", metav1.ConditionTrue)
		setCondition(cluster, valkeyiov1alpha1.ConditionClusterFormed, valkeyiov1alpha1.ReasonMissingShards, "Waiting for shards", metav1.ConditionFalse)
		_ = r.updateStatus(ctx, cluster, state)
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	for _, shard := range state.Shards {
		if len(shard.Nodes) < (1 + int(cluster.Spec.Replicas)) {
			log.V(1).Info("missing replicas, requeue..")
			r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "WaitingForReplicas", "CheckReplicas", "Shard has %d of %d nodes", len(shard.Nodes), 1+int(cluster.Spec.Replicas))
			setCondition(cluster, valkeyiov1alpha1.ConditionReady, valkeyiov1alpha1.ReasonMissingReplicas, "Waiting for all replicas to be created", metav1.ConditionFalse)
			setCondition(cluster, valkeyiov1alpha1.ConditionProgressing, valkeyiov1alpha1.ReasonReconciling, "Creating replicas", metav1.ConditionTrue)
			setCondition(cluster, valkeyiov1alpha1.ConditionClusterFormed, valkeyiov1alpha1.ReasonMissingReplicas, "Waiting for replicas", metav1.ConditionFalse)
			_ = r.updateStatus(ctx, cluster, state)
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
	}

	// Check if all slots are assigned
	unassignedSlots := state.GetUnassignedSlots()
	allSlotsAssigned := len(unassignedSlots) == 0
	if !allSlotsAssigned {
		log.V(1).Info("slots are not assigned, requeue..", "unassignedSlots", unassignedSlots)
		setCondition(cluster, valkeyiov1alpha1.ConditionSlotsAssigned, valkeyiov1alpha1.ReasonSlotsUnassigned, "Waiting for slots to be assigned", metav1.ConditionFalse)
		setCondition(cluster, valkeyiov1alpha1.ConditionReady, valkeyiov1alpha1.ReasonReconciling, "Waiting for all slots to be assigned", metav1.ConditionFalse)
		setCondition(cluster, valkeyiov1alpha1.ConditionProgressing, valkeyiov1alpha1.ReasonReconciling, "Waiting for slots to be assigned", metav1.ConditionTrue)
		setCondition(cluster, valkeyiov1alpha1.ConditionClusterFormed, valkeyiov1alpha1.ReasonSlotsUnassigned, "Waiting for slots to be assigned", metav1.ConditionFalse)
		_ = r.updateStatus(ctx, cluster, state)
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// Detect shard count changes and trigger resharding. This only runs when
	// the cluster is already formed (all slots assigned) — not during initial
	// bootstrap when shards are still being created. Scale-in moves slots from
	// removed shards to remaining ones; scale-out redistributes slots after
	// new primaries have been admitted.
	if int(cluster.Spec.Shards) != len(state.Shards) {
		log.Info("shard count changed, triggering resharding",
			"currentShards", len(state.Shards),
			"desiredShards", cluster.Spec.Shards)
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "ReshardingStarted", "Reshard",
			"Resharding from %d to %d shards", len(state.Shards), cluster.Spec.Shards)

		setCondition(cluster, valkeyiov1alpha1.ConditionReady, valkeyiov1alpha1.ReasonReconciling,
			"Resharding in progress", metav1.ConditionFalse)
		setCondition(cluster, valkeyiov1alpha1.ConditionProgressing, valkeyiov1alpha1.ReasonReconciling,
			fmt.Sprintf("Resharding from %d to %d shards", len(state.Shards), cluster.Spec.Shards), metav1.ConditionTrue)
		_ = r.updateStatus(ctx, cluster, state)

		rc := valkey.NewReshardController(r.Client, r.Recorder)
		if err := rc.ReshardCluster(ctx, state, cluster); err != nil {
			log.Error(err, "resharding failed")
			r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, "ReshardingFailed", "Reshard",
				"Resharding failed: %v", err)
			setCondition(cluster, valkeyiov1alpha1.ConditionDegraded, valkey.ReasonReshardFailed,
				fmt.Sprintf("Resharding failed: %v", err), metav1.ConditionTrue)
			_ = r.updateStatus(ctx, cluster, state)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "ReshardingComplete", "Reshard",
			"Successfully resharded from %d to %d shards", len(state.Shards), cluster.Spec.Shards)
		// Requeue to re-evaluate cluster state after resharding.
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// Check that all replicas have their replication link up (master_link_status:up).
	// before marking the cluster Ready, we need to make sure all replicas are in sync with their primary.
	for _, shard := range state.Shards {
		for _, node := range shard.Nodes {
			if !node.IsReplicationInSync() {
				log.V(1).Info("replica not yet in sync, requeue..", "address", node.Address)
				setCondition(cluster, valkeyiov1alpha1.ConditionReady, valkeyiov1alpha1.ReasonReconciling, "Waiting for replicas to sync with primary", metav1.ConditionFalse)
				setCondition(cluster, valkeyiov1alpha1.ConditionProgressing, valkeyiov1alpha1.ReasonReconciling, "Waiting for replica sync", metav1.ConditionTrue)
				_ = r.updateStatus(ctx, cluster, state)
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
		}
	}

	// Cluster is healthy - set all positive conditions
	r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "ClusterReady", "ReconcileCluster", "Cluster ready with %d shards and %d replicas", cluster.Spec.Shards, cluster.Spec.Replicas)
	setCondition(cluster, valkeyiov1alpha1.ConditionReady, valkeyiov1alpha1.ReasonClusterHealthy, "Cluster is healthy", metav1.ConditionTrue)
	setCondition(cluster, valkeyiov1alpha1.ConditionProgressing, valkeyiov1alpha1.ReasonReconcileComplete, "No changes needed", metav1.ConditionFalse)
	meta.RemoveStatusCondition(&cluster.Status.Conditions, valkeyiov1alpha1.ConditionDegraded)
	setCondition(cluster, valkeyiov1alpha1.ConditionClusterFormed, valkeyiov1alpha1.ReasonTopologyComplete, "All nodes joined cluster", metav1.ConditionTrue)
	setCondition(cluster, valkeyiov1alpha1.ConditionSlotsAssigned, valkeyiov1alpha1.ReasonAllSlotsAssigned, "All slots assigned", metav1.ConditionTrue)

	if err := r.updateStatus(ctx, cluster, state); err != nil {
		log.Error(err, statusUpdateFailedMsg)
		return ctrl.Result{}, err
	}

	log.V(1).Info("reconcile done")
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// newSeedMeetStrategy creates a SeedMeetStrategy based on the cluster's
// admission config. For "seed" strategy, uses the configured seedCount.
// For "all" strategy, sets seedCount to the total number of primaries so
// that every primary is used as a seed (legacy behavior).
func (r *ValkeyClusterReconciler) newSeedMeetStrategy(cluster *valkeyiov1alpha1.ValkeyCluster, state *valkey.ClusterState) *valkey.SeedMeetStrategy {
	seedCount := int(cluster.Spec.Admission.SeedCount)
	if cluster.Spec.Admission.MeetStrategy == "all" {
		// "all" strategy: MEET every primary by setting seedCount to total primaries.
		totalPrimaries := len(state.Shards)
		if totalPrimaries > 0 {
			seedCount = totalPrimaries
		}
	}
	return valkey.NewSeedMeetStrategy(seedCount)
}

// Create or update a headless service (client connects to pods directly)
func (r *ValkeyClusterReconciler) upsertService(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster) error {
	servicePorts := []corev1.ServicePort{
		{
			Name:       "valkey",
			Port:       DefaultPort,
			TargetPort: intstr.FromString("client"),
		},
	}
	if cluster.Spec.Metrics != nil && cluster.Spec.Metrics.Enabled {
		servicePorts = append(servicePorts, corev1.ServicePort{
			Name:       "metrics-sidecar",
			Port:       DefaultMetricsSidecarPort,
			TargetPort: intstr.FromString("metrics-sidecar"),
		})
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			Labels:    labels(cluster),
		},
		Spec: corev1.ServiceSpec{
			Type:      corev1.ServiceTypeClusterIP,
			ClusterIP: "None",
			Selector:  labels(cluster),
			Ports:     servicePorts,
		},
	}
	if err := controllerutil.SetControllerReference(cluster, svc, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, svc); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if err := r.Update(ctx, svc); err != nil {
				r.Recorder.Eventf(cluster, svc, corev1.EventTypeWarning, "ServiceUpdateFailed", "UpdateService", "Failed to update Service: %v", err)
				return err
			}
		} else {
			return err
		}
	} else {
		r.Recorder.Eventf(cluster, svc, corev1.EventTypeNormal, "ServiceCreated", "CreateService", "Created headless Service")
	}
	return nil
}

// buildValkeyConfig renders valkey.conf content from the cluster spec.
// Base directives are always set by the operator; AdditionalConfig is appended
// after base directives so users can override values for benchmarking.
func buildValkeyConfig(cluster *valkeyiov1alpha1.ValkeyCluster) string {
	lines := []string{
		"port 6379",
		"cluster-enabled yes",
		"cluster-config-file nodes.conf",
		"protected-mode no",
		"cluster-node-timeout " + strconv.FormatInt(int64(cluster.Spec.ClusterConfig.ClusterNodeTimeoutMs), 10),
	}

	for _, raw := range cluster.Spec.ClusterConfig.AdditionalConfig {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}

	return strings.Join(lines, "\n")
}

// Create or update a basic valkey.conf
func (r *ValkeyClusterReconciler) upsertConfigMap(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster) error {
	readiness, err := scripts.ReadFile("scripts/readiness-check.sh")
	if err != nil {
		return err
	}
	liveness, err := scripts.ReadFile("scripts/liveness-check.sh")
	if err != nil {
		return err
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			Labels:    labels(cluster),
		},
		Data: map[string]string{
			"readiness-check.sh": string(readiness),
			"liveness-check.sh":  string(liveness),
			"valkey.conf":        buildValkeyConfig(cluster),
		},
	}
	if err := controllerutil.SetControllerReference(cluster, cm, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, cm); err != nil {
		if apierrors.IsAlreadyExists(err) {
			if err := r.Update(ctx, cm); err != nil {
				r.Recorder.Eventf(cluster, cm, corev1.EventTypeWarning, "ConfigMapUpdateFailed", "UpdateConfigMap", "Failed to update ConfigMap: %v", err)
				return err
			}
		} else {
			r.Recorder.Eventf(cluster, cm, corev1.EventTypeWarning, "ConfigMapCreationFailed", "CreateConfigMap", "Failed to create ConfigMap: %v", err)
			return err
		}
	} else {
		r.Recorder.Eventf(cluster, cm, corev1.EventTypeNormal, "ConfigMapCreated", "CreateConfigMap", "Created ConfigMap with configuration")
	}
	return nil
}

// upsertDeployments ensures every (shard, nodeIndex) pair has a Deployment.
// Deployments are discovered via a single list call to avoid issuing one API
// create request per node on every reconcile.
//
// Each Deployment manages exactly one Pod (Replicas=1) and is named
// deterministically:
//
//	<cluster>-<N>-<M>
//
// where N is the shard index and M is the node index (0 = initial primary,
// 1+ = replicas). Existing Deployments are updated when mutable fields drift
// from the desired template (for example probe/resource changes).
//
// For a 3-shard cluster with 2 replicas per shard, this produces 9 Deployments:
//
//	mycluster-0-0, mycluster-0-1, mycluster-0-2,
//	mycluster-1-0, mycluster-1-1, mycluster-1-2,
//	mycluster-2-0, mycluster-2-1, mycluster-2-2.
func (r *ValkeyClusterReconciler) upsertDeployments(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster) error {
	log := logf.FromContext(ctx)

	nodesPerShard := 1 + int(cluster.Spec.Replicas)
	created := 0
	expected := int(cluster.Spec.Shards) * nodesPerShard

	existing := &appsv1.DeploymentList{}
	if err := r.List(
		ctx,
		existing,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels(labels(cluster)),
	); err != nil {
		return fmt.Errorf("failed to list deployments: %w", err)
	}

	existingByName := make(map[string]*appsv1.Deployment, len(existing.Items))
	for i := range existing.Items {
		d := &existing.Items[i]
		existingByName[d.Name] = d
	}

	for shard := range int(cluster.Spec.Shards) {
		for ni := range nodesPerShard {
			name := deploymentName(cluster.Name, shard, ni)
			current, found := existingByName[name]
			if !found {
				if err := r.ensureDeployment(ctx, cluster, shard, ni, expected, &created); err != nil {
					return err
				}
				continue
			}
			delete(existingByName, name)
			// Intentionally skip updates for existing Deployments to avoid
			// large-scale rollout churn during very large cluster bootstraps.
			_ = current
		}
	}

	if created > 0 {
		log.V(1).Info("created deployments", "count", created)
	}

	// Remove stale Deployments when shard/replica counts are reduced.
	deleted := 0
	for _, stale := range existingByName {
		if err := r.Delete(ctx, stale); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete stale deployment %s: %w", stale.Name, err)
		}
		deleted++
	}
	if deleted > 0 {
		log.V(1).Info("deleted stale deployments", "count", deleted)
	}

	return nil
}

// upsertPDBs ensures one PodDisruptionBudget per shard exists when zone
// awareness is enabled. Each PDB has minAvailable=1 so that at least one
// pod per shard survives voluntary disruptions. Stale PDBs (from a previous
// higher shard count) are cleaned up.
func (r *ValkeyClusterReconciler) upsertPDBs(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster) error {
	zone := cluster.Spec.ZoneAwareness
	if zone == nil || !zone.Enabled {
		return nil
	}

	log := logf.FromContext(ctx)

	// Create or update a PDB for each shard.
	for shard := range int(cluster.Spec.Shards) {
		pdb := createShardPDB(cluster, shard)
		if err := controllerutil.SetControllerReference(cluster, pdb, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, pdb); err != nil {
			if apierrors.IsAlreadyExists(err) {
				continue
			}
			return fmt.Errorf("failed to create PDB for shard %d: %w", shard, err)
		}
		log.V(1).Info("created PDB", "name", pdb.Name, "shard", shard)
	}

	// Clean up stale PDBs when shard count decreases. List all PDBs owned
	// by this cluster and delete any whose shard index >= current shard count.
	pdbList := &policyv1.PodDisruptionBudgetList{}
	if err := r.List(ctx, pdbList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{
			"app.kubernetes.io/instance":   cluster.Name,
			"app.kubernetes.io/managed-by": "valkey-operator",
		},
	); err != nil {
		return fmt.Errorf("failed to list PDBs: %w", err)
	}

	for i := range pdbList.Items {
		pdb := &pdbList.Items[i]
		shardStr, ok := pdb.Labels[LabelShardIndex]
		if !ok {
			continue
		}
		shardIdx, err := strconv.Atoi(shardStr)
		if err != nil {
			continue
		}
		if shardIdx >= int(cluster.Spec.Shards) {
			if err := r.Delete(ctx, pdb); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete stale PDB %s: %w", pdb.Name, err)
			}
			log.V(1).Info("deleted stale PDB", "name", pdb.Name, "shard", shardIdx)
		}
	}

	return nil
}

// ensureDeployment creates a single Deployment if it doesn't already exist.
func (r *ValkeyClusterReconciler) ensureDeployment(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, shard int, nodeIndex int, expected int, created *int) error {
	deployment := createClusterDeployment(cluster, shard, nodeIndex)
	if err := controllerutil.SetControllerReference(cluster, deployment, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, deployment); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		r.Recorder.Eventf(cluster, deployment, corev1.EventTypeWarning, "DeploymentCreationFailed", "CreateDeployment", "Failed to create deployment: %v", err)
		return err
	}
	*created++
	r.Recorder.Eventf(cluster, deployment, corev1.EventTypeNormal, "DeploymentCreated", "CreateDeployment", "Created deployment for shard %d node %d (%d of %d)", shard, nodeIndex, *created, expected)
	return nil
}

func (r *ValkeyClusterReconciler) getValkeyClusterState(ctx context.Context, pods *corev1.PodList) *valkey.ClusterState {
	// Create a list of addresses to possible Valkey nodes
	ips := []string{}
	for _, pod := range pods.Items {
		if pod.Status.PodIP == "" {
			continue
		}
		ips = append(ips, pod.Status.PodIP)
	}

	// Get current state of the Valkey cluster
	return valkey.GetClusterState(ctx, ips, DefaultPort)
}

// addValkeyNode introduces a pending Valkey node into the cluster. The node's
// intended role is derived from its pod labels (e.g. shard-index=0, node-index=0
// for the initial primary, shard-index=1, node-index=1 for a replica), which
// was set at Deployment-creation time by upsertDeployments. This removes all
// guesswork from the reconciler:
//
//  1. MEET: if the node is isolated (cluster_known_nodes <= 1), introduce it
//     to existing cluster members via CLUSTER MEET and return. The next
//     reconcile will proceed once gossip propagates.
//
//  2. PRIMARY: if node index is 0, assign the next available slot range via
//     CLUSTER ADDSLOTSRANGE — unless the shard already has members in the
//     cluster topology (post-failover or mid-failover), in which case fall
//     through to step 3.
//
//  3. REPLICA: if node index is >= 1 (or a post-failover replacement), find
//     the actual primary for the same shard and issue CLUSTER REPLICATE.
func (r *ValkeyClusterReconciler) addValkeyNode(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, state *valkey.ClusterState, node *valkey.NodeState, pods *corev1.PodList) error {
	log := logf.FromContext(ctx)

	// --- Step 1: MEET if this node is isolated ---
	// A freshly-started Valkey node only knows itself (cluster_known_nodes=1).
	// Before we can assign slots or replicate, the node must be introduced to
	// at least one existing cluster member via CLUSTER MEET. We MEET every
	// known primary so that the new node learns the full topology through
	// gossip rather than depending on a single point of contact. After the
	// MEET we return immediately; the next reconcile will see
	// cluster_known_nodes > 1 and proceed to Step 2 or 3.
	if sval, ok := node.ClusterInfo["cluster_known_nodes"]; ok {
		if val, err := strconv.Atoi(sval); err == nil {
			if val <= 1 && len(state.Shards) > 0 {
				for _, shard := range state.Shards {
					primary := shard.GetPrimaryNode()
					if primary == nil {
						continue
					}
					log.V(1).Info("meet other node", "this node", node.Address, "other node", primary.Address)
					if err = node.Client.Do(ctx, node.Client.B().ClusterMeet().Ip(primary.Address).Port(int64(primary.Port)).Build()).Error(); err != nil {
						log.Error(err, "command failed: CLUSTER MEET", "from", node.Address, "to", primary.Address)
						r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, "ClusterMeetFailed", "ClusterMeet", "CLUSTER MEET failed: %v", err)
						return err
					}
					r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "ClusterMeet", "ClusterMeet", "Node %v met node %v", node.Address, primary.Address)
				}
				return nil
			}
		}
	}

	// --- Resolve pod labels for this node ---
	role, shardIndex := podRoleAndShard(node.Address, pods)

	// --- Step 2: PRIMARY – assign slots ---
	if role == RolePrimary {
		// Post-failover detection: if the shard already has members in the
		// cluster topology (either a promoted primary or a replica mid-
		// failover), the replacement node-index=0 pod must NOT try to claim
		// new slots. Instead it joins as a replica. If the failover hasn't
		// completed yet, replicateToShardPrimary will return an error and
		// the reconciler retries on the next cycle.
		if shardExistsInTopology(state, shardIndex, pods) {
			log.V(1).Info("shard already exists in topology (post-failover); attaching as replica",
				"shardIndex", shardIndex, "node", node.Address)
			return r.replicateToShardPrimary(ctx, cluster, state, node, shardIndex, pods)
		}
		return r.assignSlotsToNewPrimary(ctx, cluster, state, node)
	}

	// --- Step 3: REPLICA – replicate to the matching primary ---
	if role == RoleReplica {
		return r.replicateToShardPrimary(ctx, cluster, state, node, shardIndex, pods)
	}

	return errors.New("cannot determine node role from pod name")
}

// assignSlotsToNewPrimary computes the next slot range for a single new primary
// from the current cluster snapshot and assigns it.
func (r *ValkeyClusterReconciler) assignSlotsToNewPrimary(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, state *valkey.ClusterState, node *valkey.NodeState) error {
	slotTracker := valkey.NewSlotTracker(state.GetUnassignedSlots())
	slotRange, err := nextSlotRangeForPrimary(cluster, slotTracker, len(state.Shards), 0)
	if err != nil {
		setCondition(cluster, valkeyiov1alpha1.ConditionDegraded, valkeyiov1alpha1.ReasonNoSlots, err.Error(), metav1.ConditionTrue)
		return err
	}
	return r.assignSlotsRangeToPrimary(ctx, cluster, node, slotRange)
}

// nextSlotRangeForPrimary computes the next slot range for a new primary during
// admission, given the current slot tracker and how many primaries have already
// been assigned in the current batch.
func nextSlotRangeForPrimary(
	cluster *valkeyiov1alpha1.ValkeyCluster,
	slotTracker *valkey.SlotTracker,
	existingShards int,
	assignedPrimaries int,
) (valkey.SlotsRange, error) {
	remaining := slotTracker.Remaining()
	if len(remaining) == 0 {
		return valkey.SlotsRange{}, errors.New("no unassigned slots available for new shard")
	}

	desiredShards := int(cluster.Spec.Shards)
	if desiredShards < 1 {
		return valkey.SlotsRange{}, fmt.Errorf("invalid desired shard count: %d", desiredShards)
	}

	remainingNewShards := desiredShards - (existingShards + assignedPrimaries)
	if remainingNewShards <= 0 {
		return valkey.SlotsRange{}, fmt.Errorf(
			"no additional shard slots required (desired=%d existing=%d assigned=%d)",
			desiredShards, existingShards, assignedPrimaries,
		)
	}

	// Last shard absorbs the remaining slots so all 16384 slots are covered.
	if remainingNewShards == 1 {
		if len(remaining) != 1 {
			return valkey.SlotsRange{}, errors.New("assigning multiple slot ranges to one shard is not supported")
		}
		return remaining[0], nil
	}

	slotsPerShard := valkey.TotalSlots / desiredShards
	first := remaining[0]
	if first.End-first.Start+1 < slotsPerShard {
		return valkey.SlotsRange{}, fmt.Errorf(
			"first unassigned slot range %d-%d too small for shard size %d",
			first.Start, first.End, slotsPerShard,
		)
	}

	return valkey.SlotsRange{
		Start: first.Start,
		End:   first.Start + slotsPerShard - 1,
	}, nil
}

// assignSlotsRangeToPrimary executes CLUSTER ADDSLOTSRANGE for a specific slot
// range on a target primary node.
func (r *ValkeyClusterReconciler) assignSlotsRangeToPrimary(
	ctx context.Context,
	cluster *valkeyiov1alpha1.ValkeyCluster,
	node *valkey.NodeState,
	slotRange valkey.SlotsRange,
) error {
	log := logf.FromContext(ctx)
	log.V(1).Info("assigning slot range to new primary",
		"node", node.Address,
		"slotStart", slotRange.Start,
		"slotEnd", slotRange.End,
	)

	if err := node.Client.Do(
		ctx,
		node.Client.B().ClusterAddslotsrange().
			StartSlotEndSlot().
			StartSlotEndSlot(int64(slotRange.Start), int64(slotRange.End)).
			Build(),
	).Error(); err != nil {
		log.Error(err, "command failed: CLUSTER ADDSLOTSRANGE",
			"slotStart", slotRange.Start,
			"slotEnd", slotRange.End,
		)
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, "SlotAssignmentFailed", "AssignSlots", "Failed to assign slots: %v", err)
		return err
	}

	r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "PrimaryCreated", "CreatePrimary", "Created primary with slots %d-%d", slotRange.Start, slotRange.End)
	return nil
}

// replicateToShardPrimary issues CLUSTER REPLICATE to attach this node as a
// replica of the primary in the same shard.
//
// The primary is found by scanning all pods in the shard against the live
// Valkey topology (via findShardPrimary). This handles both the normal case
// (node-index=0 is the primary) and the post-failover case (a promoted
// replica is the primary). If no primary is found (e.g. the primary pod
// hasn't started yet or hasn't joined the cluster), we return an error and
// the reconciler retries on the next cycle.
func (r *ValkeyClusterReconciler) replicateToShardPrimary(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, state *valkey.ClusterState, node *valkey.NodeState, shardIndex int, pods *corev1.PodList) error {
	log := logf.FromContext(ctx)

	// Find the actual primary for this shard by scanning all shard pods
	// against the live Valkey topology. This handles both the normal case
	// (node-index=0 is the primary) and the post-failover case (a promoted
	// replica is the primary).
	primaryNodeId, primaryIP := findShardPrimary(state, shardIndex, pods)
	if primaryNodeId == "" {
		return errors.New("primary Valkey node not found in cluster state for shard " + strconv.Itoa(shardIndex))
	}

	log.V(1).Info("add a new replica", "primary IP", primaryIP, "primary Id", primaryNodeId, "replica address", node.Address, "shardIndex", shardIndex)
	if err := node.Client.Do(ctx, node.Client.B().ClusterReplicate().NodeId(primaryNodeId).Build()).Error(); err != nil {
		log.Error(err, "command failed: CLUSTER REPLICATE", "nodeId", primaryNodeId)
		r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, "ReplicaCreationFailed", "CreateReplica", "Failed to create replica: %v", err)
		return err
	}
	r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "ReplicaCreated", "CreateReplica", "Created replica for primary %v (shard %d)", primaryNodeId, shardIndex)
	return nil
}

// Check each cluster node and forget stale nodes (noaddr or status fail)
func (r *ValkeyClusterReconciler) forgetStaleNodes(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, state *valkey.ClusterState, pods *corev1.PodList) {
	log := logf.FromContext(ctx)
	for _, shard := range state.Shards {
		for _, node := range shard.Nodes {
			// Get known nodes that are failing.
			for _, failing := range node.GetFailingNodes() {
				idx := slices.IndexFunc(pods.Items, func(p corev1.Pod) bool { return p.Status.PodIP == failing.Address })
				if idx == -1 {
					// Could not find a pod with the address of a failing node. Lets forget this node.
					log.V(1).Info("forget a failing node", "address", failing.Address, "Id", failing.Id)
					if err := node.Client.Do(ctx, node.Client.B().ClusterForget().NodeId(failing.Id).Build()).Error(); err != nil {
						log.Error(err, "command failed: CLUSTER FORGET")
						r.Recorder.Eventf(cluster, nil, corev1.EventTypeWarning, "NodeForgetFailed", "ForgetNode", "Failed to forget node: %v", err)
					} else {
						r.Recorder.Eventf(cluster, nil, corev1.EventTypeNormal, "StaleNodeForgotten", "ForgetNode", "Forgot stale node %v", failing.Address)
					}
				}

			}
		}
	}
}

// updateStatus updates the status with the current conditions and computes the Valkey Cluster state
func (r *ValkeyClusterReconciler) updateStatus(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster, state *valkey.ClusterState) error {
	log := logf.FromContext(ctx)
	// Fetch current status to compare
	current := &valkeyiov1alpha1.ValkeyCluster{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), current); err != nil {
		return err
	}
	// Update shard counts
	if state != nil {
		cluster.Status.ReadyShards = r.countReadyShards(state, cluster)
		cluster.Status.Shards = int32(len(state.Shards))
	}
	// compute Valkey Cluster state from conditions (priority order: Degraded > Ready > Progressing > Failed)
	readyCondition := meta.FindStatusCondition(cluster.Status.Conditions, valkeyiov1alpha1.ConditionReady)
	progressingCondition := meta.FindStatusCondition(cluster.Status.Conditions, valkeyiov1alpha1.ConditionProgressing)
	degradedCondition := meta.FindStatusCondition(cluster.Status.Conditions, valkeyiov1alpha1.ConditionDegraded)

	switch {
	case degradedCondition != nil && degradedCondition.Status == metav1.ConditionTrue:
		cluster.Status.State = valkeyiov1alpha1.ClusterStateDegraded
		cluster.Status.Reason = degradedCondition.Reason
		cluster.Status.Message = degradedCondition.Message
	case readyCondition != nil && readyCondition.Status == metav1.ConditionTrue:
		cluster.Status.State = valkeyiov1alpha1.ClusterStateReady
		cluster.Status.Reason = readyCondition.Reason
		cluster.Status.Message = readyCondition.Message
	case progressingCondition != nil && progressingCondition.Status == metav1.ConditionTrue:
		cluster.Status.State = valkeyiov1alpha1.ClusterStateReconciling
		cluster.Status.Reason = progressingCondition.Reason
		cluster.Status.Message = progressingCondition.Message
	case readyCondition != nil && readyCondition.Status == metav1.ConditionFalse:
		cluster.Status.State = valkeyiov1alpha1.ClusterStateFailed
		cluster.Status.Reason = readyCondition.Reason
		cluster.Status.Message = readyCondition.Message
	}

	// Only update if status has changed
	if statusChanged(current.Status, cluster.Status) {
		if err := r.Status().Update(ctx, cluster); err != nil {
			log.Error(err, statusUpdateFailedMsg)
			return err
		}
		log.V(1).Info("status updated", "state", cluster.Status.State, "reason", cluster.Status.Reason)
	} else {
		log.V(2).Info("status unchanged, skipping update")
	}
	return nil
}

// countReadyShards counts shards that have all required nodes, are healthy,
// and have all replicas in sync with their primary.
func (r *ValkeyClusterReconciler) countReadyShards(state *valkey.ClusterState, cluster *valkeyiov1alpha1.ValkeyCluster) int32 {
	var readyCount int32 = 0
	requiredNodes := 1 + int(cluster.Spec.Replicas)
	for _, shard := range state.Shards {
		if len(shard.Nodes) < requiredNodes || shard.GetPrimaryNode() == nil {
			continue
		}
		// Check if all nodes in this shard are healthy and in sync
		allHealthy := true
		for _, node := range shard.Nodes {
			if slices.Contains(node.Flags, "fail") || slices.Contains(node.Flags, "pfail") {
				allHealthy = false
				break
			}
			if !node.IsReplicationInSync() {
				allHealthy = false
				break
			}
		}
		if allHealthy {
			readyCount++
		}
	}
	return readyCount
}

// SetupWithManager sets up the controller with the Manager.
func (r *ValkeyClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&valkeyiov1alpha1.ValkeyCluster{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&appsv1.Deployment{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Named("valkeycluster").
		Complete(r)
}
