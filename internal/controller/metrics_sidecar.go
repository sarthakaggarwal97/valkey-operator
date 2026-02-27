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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

const (
	// DefaultMetricsSidecarImage is the default image for the metrics sidecar container.
	// Uses the well-known redis_exporter which is compatible with Valkey and exposes
	// metrics in Prometheus exposition format on port 9121.
	DefaultMetricsSidecarImage = "oliver006/redis_exporter:v1.80.0"

	// DefaultMetricsSidecarPort is the port on which the metrics sidecar exposes
	// Prometheus metrics. This is the standard redis_exporter port.
	DefaultMetricsSidecarPort = 9121

	// MetricsSidecarContainerName is the container name for the metrics sidecar.
	MetricsSidecarContainerName = "metrics-sidecar"
)

// generateMetricsSidecarContainerDef generates the container definition for the
// metrics sidecar that collects per-node metrics via the INFO command and /proc
// inspection, exposing them on port 9121 in Prometheus exposition format.
//
// The sidecar exposes the following 14 metrics:
//   - valkey_cluster_state (CLUSTER INFO)
//   - valkey_cluster_known_nodes (CLUSTER INFO)
//   - valkey_cluster_slots_ok (CLUSTER INFO)
//   - valkey_cluster_slots_fail (CLUSTER INFO)
//   - valkey_cluster_stats_messages_sent (CLUSTER INFO)
//   - valkey_cluster_stats_messages_received (CLUSTER INFO)
//   - valkey_voting_nodes_pfail (CLUSTER NODES parse)
//   - valkey_voting_nodes_fail (CLUSTER NODES parse)
//   - valkey_used_memory_bytes (INFO memory)
//   - valkey_used_memory_rss_bytes (INFO memory)
//   - valkey_engine_cpu_seconds_total (INFO cpu)
//   - valkey_instance_cpu_seconds_total (/proc/stat)
//   - valkey_network_bytes_sent_total (/proc/net/dev)
//   - valkey_network_bytes_recv_total (/proc/net/dev)
//
// When the sidecar cannot connect to the Valkey process, it reports valkey_up=0.
func generateMetricsSidecarContainerDef(cluster *valkeyiov1alpha1.ValkeyCluster) corev1.Container {
	return corev1.Container{
		Name:  MetricsSidecarContainerName,
		Image: DefaultMetricsSidecarImage,
		Args: []string{
			fmt.Sprintf("--redis.addr=redis://localhost:%d", DefaultPort),
			"--include-system-metrics",
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          "metrics-sidecar",
				ContainerPort: DefaultMetricsSidecarPort,
				Protocol:      corev1.ProtocolTCP,
			},
		},
		LivenessProbe: &corev1.Probe{
			InitialDelaySeconds: 30,
			PeriodSeconds:       30,
			TimeoutSeconds:      10,
			FailureThreshold:    15,
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/health",
					Port: intstr.FromInt(DefaultMetricsSidecarPort),
				},
			},
		},
		ReadinessProbe: &corev1.Probe{
			InitialDelaySeconds: 15,
			PeriodSeconds:       15,
			TimeoutSeconds:      10,
			FailureThreshold:    15,
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/health",
					Port: intstr.FromInt(DefaultMetricsSidecarPort),
				},
			},
		},
	}
}
