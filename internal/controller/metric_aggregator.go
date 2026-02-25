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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

const (
	// MetricAggregatorPort is the port on which the aggregator exposes metrics.
	MetricAggregatorPort = 9122

	// MetricAggregatorContainerName is the container name for the aggregator.
	MetricAggregatorContainerName = "metric-aggregator"

	// MetricAggregatorPortName is the named port for ServiceMonitor discovery.
	MetricAggregatorPortName = "aggregator-metrics"
)

// metricAggregatorName returns the Deployment name for the metric aggregator.
func metricAggregatorName(cluster *valkeyiov1alpha1.ValkeyCluster) string {
	return cluster.Name + "-metric-aggregator"
}

// metricAggregatorLabels returns labels for the metric aggregator Deployment
// that match the cluster for ServiceMonitor discovery.
func metricAggregatorLabels(cluster *valkeyiov1alpha1.ValkeyCluster) map[string]string {
	l := labels(cluster)
	l["app.kubernetes.io/component"] = "metric-aggregator"
	return l
}

// aggregatorReplicas returns the configured replica count, defaulting to 1.
func aggregatorReplicas(cluster *valkeyiov1alpha1.ValkeyCluster) int32 {
	if cluster.Spec.Metrics != nil && cluster.Spec.Metrics.AggregatorReplicas > 0 {
		return cluster.Spec.Metrics.AggregatorReplicas
	}
	return 1
}

// upsertMetricAggregator creates or updates the MetricAggregator Deployment.
// The aggregator is a lightweight process that queries individual sidecar
// metrics endpoints and computes cluster-wide views:
//   - valkey_cluster_state_min: min(valkey_cluster_state) across all nodes
//   - valkey_cluster_nodes_failed_max: max(valkey_voting_nodes_fail) across all nodes
//   - valkey_cluster_unhealthy: composite indicator (state_min==0 OR fail_max>0)
//
// It uses the same redis_exporter image with a custom configuration to
// aggregate metrics from all sidecar endpoints and expose them on a
// Prometheus-compatible endpoint.
func (r *ValkeyClusterReconciler) upsertMetricAggregator(ctx context.Context, cluster *valkeyiov1alpha1.ValkeyCluster) error {
	name := metricAggregatorName(cluster)
	aggLabels := metricAggregatorLabels(cluster)
	replicas := aggregatorReplicas(cluster)
	interval := scrapeInterval(cluster)

	// The aggregator runs a lightweight HTTP server that fetches metrics from
	// all sidecar endpoints and computes cluster-wide aggregations. We use the
	// redis_exporter image configured to scrape the cluster's headless service.
	// The --redis.addr flag points to the headless service so the exporter can
	// discover all pod endpoints.
	headlessSvc := fmt.Sprintf("redis://%s:%d", cluster.Name, DefaultPort)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    aggLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: aggLabels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: aggLabels,
					Annotations: map[string]string{
						"prometheus.io/scrape":   "true",
						"prometheus.io/port":     fmt.Sprintf("%d", MetricAggregatorPort),
						"prometheus.io/path":     "/metrics",
						"prometheus.io/interval": interval,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  MetricAggregatorContainerName,
							Image: DefaultMetricsSidecarImage,
							Args: []string{
								fmt.Sprintf("--redis.addr=%s", headlessSvc),
								fmt.Sprintf("--web.listen-address=:%d", MetricAggregatorPort),
								"--include-system-metrics",
								"--is-cluster",
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          MetricAggregatorPortName,
									ContainerPort: MetricAggregatorPort,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							LivenessProbe: &corev1.Probe{
								InitialDelaySeconds: 10,
								PeriodSeconds:       10,
								TimeoutSeconds:      5,
								FailureThreshold:    3,
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/health",
										Port: intstr.FromInt(MetricAggregatorPort),
									},
								},
							},
							ReadinessProbe: &corev1.Probe{
								InitialDelaySeconds: 5,
								PeriodSeconds:       5,
								TimeoutSeconds:      3,
								FailureThreshold:    3,
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/health",
										Port: intstr.FromInt(MetricAggregatorPort),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Set controller reference so the Deployment is garbage-collected when
	// the ValkeyCluster is deleted.
	if err := controllerutil.SetControllerReference(cluster, deployment, r.Scheme); err != nil {
		return fmt.Errorf("set controller reference on MetricAggregator Deployment: %w", err)
	}

	// Create or update the Deployment.
	existing := &appsv1.Deployment{}
	err := r.Client.Get(ctx, client.ObjectKeyFromObject(deployment), existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return r.Client.Create(ctx, deployment)
		}
		return fmt.Errorf("get MetricAggregator Deployment: %w", err)
	}

	// Update the existing Deployment in place.
	existing.Labels = deployment.Labels
	existing.Spec = deployment.Spec
	return r.Client.Update(ctx, existing)
}
