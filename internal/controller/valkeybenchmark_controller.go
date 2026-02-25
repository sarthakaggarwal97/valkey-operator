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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

const (
	// benchmarkFinalizerName is the finalizer used to clean up benchmark resources.
	benchmarkFinalizerName = "valkey.io/benchmark-cleanup"

	// benchmarkJobSuffix is appended to the benchmark name to form the Job name.
	benchmarkJobSuffix = "-job"

	// benchmarkMetricsPort is the port on which benchmark client pods expose
	// Prometheus metrics.
	benchmarkMetricsPort = 9121

	// totalSlots is the total number of hash slots in a Valkey cluster.
	totalSlots = 16384
)

// clientImageMap maps BenchmarkClientType to the container image used for that client.
var clientImageMap = map[valkeyiov1alpha1.BenchmarkClientType]string{
	valkeyiov1alpha1.BenchmarkClientValkeyBenchmark: "valkey/valkey:9.0.0",
	valkeyiov1alpha1.BenchmarkClientValkeyPy:        "valkey/benchmark-valkeypy:latest",
	valkeyiov1alpha1.BenchmarkClientValkeyGlide:     "valkey/benchmark-valkeyglide:latest",
	valkeyiov1alpha1.BenchmarkClientIOValkey:        "valkey/benchmark-iovalkey:latest",
	valkeyiov1alpha1.BenchmarkClientValkeyGo:        "valkey/benchmark-valkeygo:latest",
	valkeyiov1alpha1.BenchmarkClientRedisson:        "valkey/benchmark-redisson:latest",
}

// ValkeyBenchmarkReconciler reconciles a ValkeyBenchmark object.
type ValkeyBenchmarkReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=valkey.io,resources=valkeybenchmarks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=valkey.io,resources=valkeybenchmarks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=valkey.io,resources=valkeybenchmarks/finalizers,verbs=update
// +kubebuilder:rbac:groups=valkey.io,resources=valkeyclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get

// Reconcile drives the ValkeyBenchmark CR toward its desired state.
// The pipeline:
//  1. Fetch the ValkeyBenchmark CR.
//  2. Handle finalizer for cleanup.
//  3. Fetch the referenced ValkeyCluster.
//  4. Create a benchmark Job if not yet running.
//  5. Monitor Job completion.
//  6. Aggregate metrics from completed pods and update status.
//  7. Clean up benchmark pods after completion.
func (r *ValkeyBenchmarkReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.V(1).Info("reconciling ValkeyBenchmark")

	// Step 1: Fetch the ValkeyBenchmark CR.
	benchmark := &valkeyiov1alpha1.ValkeyBenchmark{}
	if err := r.Get(ctx, req.NamespacedName, benchmark); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Step 2: Handle finalizer.
	if benchmark.DeletionTimestamp != nil {
		if controllerutil.ContainsFinalizer(benchmark, benchmarkFinalizerName) {
			if err := r.cleanupBenchmarkResources(ctx, benchmark); err != nil {
				log.Error(err, "failed to clean up benchmark resources")
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(benchmark, benchmarkFinalizerName)
			if err := r.Update(ctx, benchmark); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer if not present.
	if !controllerutil.ContainsFinalizer(benchmark, benchmarkFinalizerName) {
		controllerutil.AddFinalizer(benchmark, benchmarkFinalizerName)
		if err := r.Update(ctx, benchmark); err != nil {
			return ctrl.Result{}, err
		}
	}

	// If already completed or failed, nothing to do.
	if benchmark.Status.Phase == valkeyiov1alpha1.BenchmarkPhaseCompleted ||
		benchmark.Status.Phase == valkeyiov1alpha1.BenchmarkPhaseFailed {
		return ctrl.Result{}, nil
	}

	// Step 3: Fetch the referenced ValkeyCluster.
	cluster := &valkeyiov1alpha1.ValkeyCluster{}
	clusterKey := client.ObjectKey{
		Namespace: benchmark.Namespace,
		Name:      benchmark.Spec.ClusterRef,
	}
	if err := r.Get(ctx, clusterKey, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			log.Error(err, "referenced ValkeyCluster not found", "clusterRef", benchmark.Spec.ClusterRef)
			return r.setBenchmarkFailed(ctx, benchmark, fmt.Sprintf("ValkeyCluster %q not found", benchmark.Spec.ClusterRef))
		}
		return ctrl.Result{}, err
	}

	// Step 4: Check if the benchmark Job already exists.
	jobName := benchmark.Name + benchmarkJobSuffix
	existingJob := &batchv1.Job{}
	jobKey := client.ObjectKey{Namespace: benchmark.Namespace, Name: jobName}
	jobExists := true
	if err := r.Get(ctx, jobKey, existingJob); err != nil {
		if apierrors.IsNotFound(err) {
			jobExists = false
		} else {
			return ctrl.Result{}, err
		}
	}

	// Create the Job if it doesn't exist.
	if !jobExists {
		job := r.buildBenchmarkJob(benchmark, cluster)
		if err := controllerutil.SetControllerReference(benchmark, job, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, job); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// Race condition — job was created between our check and create.
				log.V(1).Info("benchmark Job already exists, continuing")
			} else {
				log.Error(err, "failed to create benchmark Job")
				return r.setBenchmarkFailed(ctx, benchmark, fmt.Sprintf("failed to create Job: %v", err))
			}
		} else {
			log.V(1).Info("created benchmark Job", "job", jobName)
		}

		// Update status to Running.
		now := metav1.Now()
		benchmark.Status.Phase = valkeyiov1alpha1.BenchmarkPhaseRunning
		benchmark.Status.StartTime = &now
		if err := r.Status().Update(ctx, benchmark); err != nil {
			log.Error(err, "failed to update status to Running")
			return ctrl.Result{}, err
		}

		// Requeue to monitor Job progress.
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Step 5: Monitor Job completion.
	if existingJob.Status.Succeeded >= benchmark.Spec.ClientCount {
		// All client pods completed successfully.
		log.V(1).Info("benchmark Job completed", "succeeded", existingJob.Status.Succeeded)

		// Step 6: Aggregate metrics from completed pods.
		if err := r.aggregateMetrics(ctx, benchmark, existingJob); err != nil {
			log.Error(err, "failed to aggregate benchmark metrics")
			// Still mark as completed — metrics aggregation is best-effort.
		}

		// Mark as completed.
		now := metav1.Now()
		benchmark.Status.Phase = valkeyiov1alpha1.BenchmarkPhaseCompleted
		benchmark.Status.CompletionTime = &now
		if err := r.Status().Update(ctx, benchmark); err != nil {
			log.Error(err, "failed to update status to Completed")
			return ctrl.Result{}, err
		}

		// Step 7: Clean up benchmark pods.
		if err := r.cleanupBenchmarkResources(ctx, benchmark); err != nil {
			log.Error(err, "failed to clean up benchmark resources after completion")
			// Non-fatal — finalizer will handle cleanup on deletion.
		}

		return ctrl.Result{}, nil
	}

	// Check for Job failure.
	if isJobFailed(existingJob) {
		log.V(1).Info("benchmark Job failed")
		return r.setBenchmarkFailed(ctx, benchmark, "benchmark Job failed")
	}

	// Job still running — requeue to check again.
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// buildBenchmarkJob constructs a batch/v1 Job that runs benchmark client pods
// targeting the ValkeyCluster's headless service. Each client pod is assigned a
// slot range via CRC16 slot table routing to distribute load across primaries
// and replicas.
func (r *ValkeyBenchmarkReconciler) buildBenchmarkJob(
	benchmark *valkeyiov1alpha1.ValkeyBenchmark,
	cluster *valkeyiov1alpha1.ValkeyCluster,
) *batchv1.Job {
	jobName := benchmark.Name + benchmarkJobSuffix
	clientCount := benchmark.Spec.ClientCount
	// The headless service name matches the cluster name.
	headlessSvc := fmt.Sprintf("%s.%s.svc.cluster.local", cluster.Name, cluster.Namespace)

	// Compute slot ranges for CRC16-based distribution across clients.
	slotRanges := computeSlotRanges(int(clientCount))

	// Build the container command and image based on client type.
	image := clientImageForType(benchmark.Spec.ClientType)
	backoffLimit := int32(3)
	completions := clientCount
	parallelism := clientCount

	// Pod template with Prometheus annotations for metric scraping.
	podTemplate := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"app.kubernetes.io/name":       "valkey-benchmark",
				"app.kubernetes.io/instance":   benchmark.Name,
				"app.kubernetes.io/managed-by": "valkey-operator",
				"valkey.io/benchmark":          benchmark.Name,
			},
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   strconv.Itoa(benchmarkMetricsPort),
				"prometheus.io/path":   "/metrics",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:  "benchmark-client",
					Image: image,
					Env: []corev1.EnvVar{
						{Name: "VALKEY_HOST", Value: headlessSvc},
						{Name: "VALKEY_PORT", Value: strconv.Itoa(DefaultPort)},
						{Name: "DURATION_SEC", Value: strconv.Itoa(int(benchmark.Spec.DurationSec))},
						{Name: "KEY_DISTRIBUTION", Value: benchmark.Spec.KeyDistribution},
						{Name: "CLIENT_TYPE", Value: string(benchmark.Spec.ClientType)},
						{Name: "SLOT_RANGES", Value: marshalSlotRanges(slotRanges)},
						{Name: "METRICS_PORT", Value: strconv.Itoa(benchmarkMetricsPort)},
					},
					Command: buildClientCommand(benchmark.Spec.ClientType, headlessSvc, benchmark.Spec.DurationSec),
					Ports: []corev1.ContainerPort{
						{
							Name:          "metrics",
							ContainerPort: int32(benchmarkMetricsPort),
							Protocol:      corev1.ProtocolTCP,
						},
					},
				},
			},
		},
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: benchmark.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "valkey-benchmark",
				"app.kubernetes.io/instance":   benchmark.Name,
				"app.kubernetes.io/managed-by": "valkey-operator",
				"valkey.io/benchmark":          benchmark.Name,
			},
		},
		Spec: batchv1.JobSpec{
			Completions:  &completions,
			Parallelism:  &parallelism,
			BackoffLimit: &backoffLimit,
			Template:     podTemplate,
		},
	}

	return job
}

// slotRange represents a contiguous range of hash slots [Start, End].
type slotRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// computeSlotRanges distributes the 16384 hash slots evenly across clientCount
// clients using CRC16 slot table routing. Each client gets a contiguous range
// of slots to target, ensuring load is distributed across primaries and replicas.
func computeSlotRanges(clientCount int) []slotRange {
	if clientCount <= 0 {
		clientCount = 1
	}
	ranges := make([]slotRange, clientCount)
	slotsPerClient := totalSlots / clientCount
	remainder := totalSlots % clientCount

	start := 0
	for i := 0; i < clientCount; i++ {
		size := slotsPerClient
		if i < remainder {
			size++
		}
		ranges[i] = slotRange{
			Start: start,
			End:   start + size - 1,
		}
		start += size
	}
	return ranges
}

// marshalSlotRanges serializes slot ranges to a JSON string for passing
// to benchmark client pods via environment variable.
func marshalSlotRanges(ranges []slotRange) string {
	data, err := json.Marshal(ranges)
	if err != nil {
		return "[]"
	}
	return string(data)
}

// clientImageForType returns the container image for the given benchmark client type.
func clientImageForType(clientType valkeyiov1alpha1.BenchmarkClientType) string {
	if img, ok := clientImageMap[clientType]; ok {
		return img
	}
	return clientImageMap[valkeyiov1alpha1.BenchmarkClientValkeyBenchmark]
}

// buildClientCommand returns the container command for the given client type.
// For valkey-benchmark, it uses the native CLI tool. For other client types,
// it delegates to a wrapper script that handles the specific client library.
func buildClientCommand(
	clientType valkeyiov1alpha1.BenchmarkClientType,
	host string,
	durationSec int32,
) []string {
	switch clientType {
	case valkeyiov1alpha1.BenchmarkClientValkeyBenchmark:
		// valkey-benchmark native CLI with cluster mode and a finite request
		// count so Job completions can be observed deterministically.
		return []string{
			"valkey-benchmark",
			"-h", host,
			"-p", strconv.Itoa(DefaultPort),
			"--cluster",
			"-t", "set,get",
			"-c", "50",
			"-d", "128",
			"--threads", "4",
			"-n", strconv.FormatInt(benchmarkRequestCount(durationSec), 10),
		}
	default:
		// All other client types use a wrapper entrypoint that reads
		// configuration from environment variables (VALKEY_HOST, VALKEY_PORT,
		// DURATION_SEC, KEY_DISTRIBUTION, SLOT_RANGES, METRICS_PORT).
		return []string{"/entrypoint.sh"}
	}
}

// benchmarkRequestCount returns a finite request budget for valkey-benchmark.
// Keep the count proportional to requested duration while guaranteeing at least
// one request.
func benchmarkRequestCount(durationSec int32) int64 {
	if durationSec <= 0 {
		return 1
	}
	return int64(durationSec) * 100000
}

// benchmarkClientMetrics holds the per-client metrics parsed from pod logs.
type benchmarkClientMetrics struct {
	RPS           float64 `json:"rps"`
	AvgLatencyUs  float64 `json:"avgLatencyUs"`
	P90LatencyUs  float64 `json:"p90LatencyUs"`
	P99LatencyUs  float64 `json:"p99LatencyUs"`
	MaxDowntimeMs int64   `json:"maxDowntimeMs"`
}

// aggregateMetrics reads completed benchmark pod logs, parses per-client metrics,
// and aggregates them into the ValkeyBenchmarkStatus.
//
// Aggregation rules:
//   - RPS: sum across all clients
//   - AvgLatencyUs: weighted average across all clients
//   - P90LatencyUs: max of per-client P90 (conservative upper bound)
//   - P99LatencyUs: max of per-client P99 (conservative upper bound)
//   - MaxDowntimeMs: max across all clients
func (r *ValkeyBenchmarkReconciler) aggregateMetrics(
	ctx context.Context,
	benchmark *valkeyiov1alpha1.ValkeyBenchmark,
	job *batchv1.Job,
) error {
	log := logf.FromContext(ctx)

	// List pods belonging to this benchmark Job.
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(benchmark.Namespace),
		client.MatchingLabels{"valkey.io/benchmark": benchmark.Name},
	); err != nil {
		return fmt.Errorf("failed to list benchmark pods: %w", err)
	}

	var (
		totalRPS        float64
		totalLatencySum float64
		clientCount     int
		maxP90          float64
		maxP99          float64
		maxDowntime     int64
	)

	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase != corev1.PodSucceeded {
			continue
		}

		metrics, err := r.parseClientMetrics(ctx, pod)
		if err != nil {
			log.V(1).Info("failed to parse metrics from pod", "pod", pod.Name, "error", err)
			continue
		}

		totalRPS += metrics.RPS
		totalLatencySum += metrics.AvgLatencyUs
		clientCount++

		if metrics.P90LatencyUs > maxP90 {
			maxP90 = metrics.P90LatencyUs
		}
		if metrics.P99LatencyUs > maxP99 {
			maxP99 = metrics.P99LatencyUs
		}
		if metrics.MaxDowntimeMs > maxDowntime {
			maxDowntime = metrics.MaxDowntimeMs
		}
	}

	// Update benchmark status with aggregated metrics.
	benchmark.Status.RPS = int64(math.Round(totalRPS))
	if clientCount > 0 {
		benchmark.Status.AvgLatencyUs = int64(math.Round(totalLatencySum / float64(clientCount)))
	}
	benchmark.Status.P90LatencyUs = int64(math.Round(maxP90))
	benchmark.Status.P99LatencyUs = int64(math.Round(maxP99))
	benchmark.Status.MaxDowntimeMs = maxDowntime

	log.V(1).Info("aggregated benchmark metrics",
		"rps", benchmark.Status.RPS,
		"avgLatencyUs", benchmark.Status.AvgLatencyUs,
		"p90LatencyUs", benchmark.Status.P90LatencyUs,
		"p99LatencyUs", benchmark.Status.P99LatencyUs,
		"maxDowntimeMs", benchmark.Status.MaxDowntimeMs,
		"clientCount", clientCount)

	return nil
}

// parseClientMetrics extracts benchmark metrics from a completed pod's
// termination message. Benchmark client pods write their metrics as JSON
// to the container's termination message.
func (r *ValkeyBenchmarkReconciler) parseClientMetrics(
	ctx context.Context,
	pod *corev1.Pod,
) (*benchmarkClientMetrics, error) {
	// Read metrics from the pod's termination message.
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != "benchmark-client" {
			continue
		}
		if cs.State.Terminated == nil {
			continue
		}
		msg := cs.State.Terminated.Message
		if msg == "" {
			continue
		}
		var metrics benchmarkClientMetrics
		if err := json.Unmarshal([]byte(msg), &metrics); err != nil {
			return nil, fmt.Errorf("failed to parse termination message as metrics JSON: %w", err)
		}
		return &metrics, nil
	}
	return nil, fmt.Errorf("no termination message found for benchmark-client container in pod %s", pod.Name)
}

// cleanupBenchmarkResources deletes the benchmark Job and its pods.
func (r *ValkeyBenchmarkReconciler) cleanupBenchmarkResources(
	ctx context.Context,
	benchmark *valkeyiov1alpha1.ValkeyBenchmark,
) error {
	log := logf.FromContext(ctx)

	// Delete the Job with propagation to delete pods.
	jobName := benchmark.Name + benchmarkJobSuffix
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: benchmark.Namespace,
		},
	}
	propagation := metav1.DeletePropagationBackground
	if err := r.Delete(ctx, job, &client.DeleteOptions{
		PropagationPolicy: &propagation,
	}); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete benchmark Job %s: %w", jobName, err)
		}
		log.V(1).Info("benchmark Job already deleted", "job", jobName)
	} else {
		log.V(1).Info("deleted benchmark Job", "job", jobName)
	}

	// Also clean up any orphaned pods with the benchmark label.
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(benchmark.Namespace),
		client.MatchingLabels{"valkey.io/benchmark": benchmark.Name},
	); err != nil {
		return fmt.Errorf("failed to list benchmark pods for cleanup: %w", err)
	}
	for i := range pods.Items {
		if err := r.Delete(ctx, &pods.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			log.V(1).Info("failed to delete orphaned benchmark pod", "pod", pods.Items[i].Name, "error", err)
		}
	}

	return nil
}

// setBenchmarkFailed updates the benchmark status to Failed.
func (r *ValkeyBenchmarkReconciler) setBenchmarkFailed(
	ctx context.Context,
	benchmark *valkeyiov1alpha1.ValkeyBenchmark,
	message string,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	now := metav1.Now()
	benchmark.Status.Phase = valkeyiov1alpha1.BenchmarkPhaseFailed
	benchmark.Status.CompletionTime = &now
	if err := r.Status().Update(ctx, benchmark); err != nil {
		log.Error(err, "failed to update status to Failed")
		return ctrl.Result{}, err
	}
	log.Info("benchmark failed", "reason", message)
	return ctrl.Result{}, nil
}

// isJobFailed checks if a Job has a Failed condition.
func isJobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *ValkeyBenchmarkReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&valkeyiov1alpha1.ValkeyBenchmark{}).
		Owns(&batchv1.Job{}).
		Named("valkeybenchmark").
		Complete(r)
}
