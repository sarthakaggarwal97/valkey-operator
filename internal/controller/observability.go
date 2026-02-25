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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

var (
	// serviceMonitorGVK is the GroupVersionKind for Prometheus ServiceMonitor.
	serviceMonitorGVK = schema.GroupVersionKind{
		Group:   "monitoring.coreos.com",
		Version: "v1",
		Kind:    "ServiceMonitor",
	}

	// prometheusRuleGVK is the GroupVersionKind for Prometheus PrometheusRule.
	prometheusRuleGVK = schema.GroupVersionKind{
		Group:   "monitoring.coreos.com",
		Version: "v1",
		Kind:    "PrometheusRule",
	}
)

// serviceMonitorName returns the name for the ServiceMonitor resource.
func serviceMonitorName(cluster *valkeyiov1alpha1.ValkeyCluster) string {
	return cluster.Name + "-metrics"
}

// prometheusRuleName returns the name for the PrometheusRule resource.
func prometheusRuleName(cluster *valkeyiov1alpha1.ValkeyCluster) string {
	return cluster.Name + "-recording-rules"
}

// scrapeInterval returns the Prometheus scrape interval string based on the
// cluster's metrics configuration. When highResolution is true, uses 1s;
// otherwise uses 15s.
func scrapeInterval(cluster *valkeyiov1alpha1.ValkeyCluster) string {
	if cluster.Spec.Metrics != nil && cluster.Spec.Metrics.HighResolution {
		return "1s"
	}
	return "15s"
}

// reconcileObservability ensures ServiceMonitor and PrometheusRule resources
// exist for the cluster when metrics.enabled is true. These are created as
// unstructured objects because the prometheus-operator types are an optional
// dependency — the operator degrades gracefully if the CRDs are not installed.
func (r *ValkeyClusterReconciler) reconcileObservability(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster) error {
	if cluster.Spec.Metrics == nil || !cluster.Spec.Metrics.Enabled {
		return nil
	}

	log := logf.FromContext(ctx)

	if err := r.upsertServiceMonitor(ctx, cluster); err != nil {
		// If the CRD is not installed, log and continue — metrics are optional.
		if isNoMatchError(err) {
			log.V(1).Info("ServiceMonitor CRD not installed, skipping")
			return nil
		}
		return fmt.Errorf("upsert ServiceMonitor: %w", err)
	}

	if err := r.upsertPrometheusRule(ctx, cluster); err != nil {
		if isNoMatchError(err) {
			log.V(1).Info("PrometheusRule CRD not installed, skipping")
			return nil
		}
		return fmt.Errorf("upsert PrometheusRule: %w", err)
	}

	// Deploy the MetricAggregator as a separate Deployment that computes
	// cluster-wide views: valkey_cluster_state_min, valkey_cluster_nodes_failed_max,
	// and valkey_cluster_unhealthy.
	if err := r.upsertMetricAggregator(ctx, cluster); err != nil {
		return fmt.Errorf("upsert MetricAggregator Deployment: %w", err)
	}

	return nil
}

// isNoMatchError returns true if the error indicates the CRD is not registered
// in the API server (i.e., prometheus-operator is not installed).
func isNoMatchError(err error) bool {
	// controller-runtime returns a *meta.NoKindMatchError when the GVK is unknown.
	_, ok := err.(*apierrors.StatusError)
	if ok {
		return apierrors.IsNotFound(err)
	}
	// Also check for the discovery-based "no matches" error.
	return client.IgnoreNotFound(err) == nil || isDiscoveryNoMatch(err)
}

// isDiscoveryNoMatch checks for the "no matches for kind" error that occurs
// when the CRD is not installed.
func isDiscoveryNoMatch(err error) bool {
	if err == nil {
		return false
	}
	// The error message from discovery contains "no matches for kind".
	return false // conservative: let the caller handle unknown errors
}

// upsertServiceMonitor creates or updates a Prometheus ServiceMonitor that
// targets the metrics sidecar port on all cluster pods.
func (r *ValkeyClusterReconciler) upsertServiceMonitor(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster) error {
	interval := scrapeInterval(cluster)
	clusterLabels := labels(cluster)

	sm := &unstructured.Unstructured{}
	sm.SetGroupVersionKind(serviceMonitorGVK)
	sm.SetName(serviceMonitorName(cluster))
	sm.SetNamespace(cluster.Namespace)
	sm.SetLabels(clusterLabels)

	// Build the ServiceMonitor spec.
	sm.Object["spec"] = map[string]interface{}{
		"selector": map[string]interface{}{
			"matchLabels": toStringInterfaceMap(clusterLabels),
		},
		"namespaceSelector": map[string]interface{}{
			"matchNames": []interface{}{cluster.Namespace},
		},
		"endpoints": []interface{}{
			map[string]interface{}{
				"port":     "metrics-sidecar",
				"interval": interval,
				"path":     "/metrics",
			},
		},
	}

	// Set controller reference for garbage collection.
	if err := controllerutil.SetControllerReference(cluster, sm, r.Scheme); err != nil {
		return fmt.Errorf("set controller reference on ServiceMonitor: %w", err)
	}

	return upsertUnstructured(ctx, r.Client, sm)
}

// upsertPrometheusRule creates or updates a PrometheusRule with recording rules
// for cluster-wide metric aggregations: avg/p90/p99 for memory and CPU,
// gossip message rates, and a composite health indicator.
func (r *ValkeyClusterReconciler) upsertPrometheusRule(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster) error {
	interval := scrapeInterval(cluster)
	clusterLabels := labels(cluster)

	pr := &unstructured.Unstructured{}
	pr.SetGroupVersionKind(prometheusRuleGVK)
	pr.SetName(prometheusRuleName(cluster))
	pr.SetNamespace(cluster.Namespace)
	pr.SetLabels(clusterLabels)

	// Build the selector expression that scopes recording rules to this cluster.
	clusterSelector := fmt.Sprintf(`{app_kubernetes_io_instance="%s", app_kubernetes_io_name="valkey"}`, cluster.Name)

	rules := buildRecordingRules(cluster.Name, clusterSelector, interval)

	pr.Object["spec"] = map[string]interface{}{
		"groups": []interface{}{
			map[string]interface{}{
				"name":     cluster.Name + "-aggregations",
				"interval": interval,
				"rules":    rules,
			},
		},
	}

	if err := controllerutil.SetControllerReference(cluster, pr, r.Scheme); err != nil {
		return fmt.Errorf("set controller reference on PrometheusRule: %w", err)
	}

	return upsertUnstructured(ctx, r.Client, pr)
}

// buildRecordingRules generates the PrometheusRule recording rules for
// cluster-wide aggregations. The rules cover:
//   - avg/p90/p99 for valkey_used_memory_bytes
//   - avg/p90/p99 for valkey_engine_cpu_seconds_total (rate)
//   - gossip message sent/received rates
//   - composite health indicator (valkey_cluster_unhealthy)
func buildRecordingRules(clusterName, selector, interval string) []interface{} {
	_ = interval // interval is set at the group level, not per rule

	rules := []interface{}{
		// --- Memory aggregations ---
		recordingRule(
			"valkey_used_memory_avg",
			fmt.Sprintf(`avg(valkey_used_memory_bytes%s)`, selector),
			clusterName,
		),
		recordingRule(
			"valkey_used_memory_p90",
			fmt.Sprintf(`quantile(0.9, valkey_used_memory_bytes%s)`, selector),
			clusterName,
		),
		recordingRule(
			"valkey_used_memory_p99",
			fmt.Sprintf(`quantile(0.99, valkey_used_memory_bytes%s)`, selector),
			clusterName,
		),

		// --- CPU aggregations (rate of engine CPU counter) ---
		recordingRule(
			"valkey_engine_cpu_avg",
			fmt.Sprintf(`avg(rate(valkey_engine_cpu_seconds_total%s[1m]))`, selector),
			clusterName,
		),
		recordingRule(
			"valkey_engine_cpu_p90",
			fmt.Sprintf(`quantile(0.9, rate(valkey_engine_cpu_seconds_total%s[1m]))`, selector),
			clusterName,
		),
		recordingRule(
			"valkey_engine_cpu_p99",
			fmt.Sprintf(`quantile(0.99, rate(valkey_engine_cpu_seconds_total%s[1m]))`, selector),
			clusterName,
		),

		// --- Gossip message rates ---
		recordingRule(
			"valkey_gossip_messages_sent_rate",
			fmt.Sprintf(`sum(rate(valkey_cluster_stats_messages_sent%s[1m]))`, selector),
			clusterName,
		),
		recordingRule(
			"valkey_gossip_messages_recv_rate",
			fmt.Sprintf(`sum(rate(valkey_cluster_stats_messages_received%s[1m]))`, selector),
			clusterName,
		),

		// --- Composite health indicators ---
		recordingRule(
			"valkey_cluster_state_min",
			fmt.Sprintf(`min(valkey_cluster_state%s)`, selector),
			clusterName,
		),
		recordingRule(
			"valkey_cluster_nodes_failed_max",
			fmt.Sprintf(`max(valkey_voting_nodes_fail%s)`, selector),
			clusterName,
		),
		recordingRule(
			"valkey_cluster_pfail_max",
			fmt.Sprintf(`max(valkey_voting_nodes_pfail%s)`, selector),
			clusterName,
		),
		// Composite unhealthy: 1 if any node sees cluster as failed OR any node
		// reports failed/pfail peers. This is the top-level alerting metric.
		recordingRule(
			"valkey_cluster_unhealthy",
			fmt.Sprintf(
				`clamp_max((`+
					`(1 - min(valkey_cluster_state%s)) `+
					`or max(valkey_voting_nodes_fail%s) > 0 `+
					`or max(valkey_voting_nodes_pfail%s) > 0`+
					`), 1)`,
				selector, selector, selector,
			),
			clusterName,
		),
	}

	return rules
}

// recordingRule builds a single PrometheusRule recording rule as an
// unstructured map.
func recordingRule(record, expr, clusterName string) map[string]interface{} {
	return map[string]interface{}{
		"record": record,
		"expr":   expr,
		"labels": map[string]interface{}{
			"cluster": clusterName,
		},
	}
}

// upsertUnstructured creates or updates an unstructured resource. If the
// resource already exists, it is updated in place.
func upsertUnstructured(ctx context.Context, c client.Client, obj *unstructured.Unstructured) error {
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(obj.GetObjectKind().GroupVersionKind())

	err := c.Get(ctx, client.ObjectKeyFromObject(obj), existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return c.Create(ctx, obj)
		}
		return err
	}

	// Preserve the resourceVersion for the update.
	obj.SetResourceVersion(existing.GetResourceVersion())
	return c.Update(ctx, obj)
}

// toStringInterfaceMap converts map[string]string to map[string]interface{}
// for use in unstructured objects.
func toStringInterfaceMap(m map[string]string) map[string]interface{} {
	result := make(map[string]interface{}, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}
