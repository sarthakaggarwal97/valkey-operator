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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	valkeyv1 "valkey.io/valkey-operator/api/v1alpha1"
)

func TestGenerateMetricsSidecarContainerDef(t *testing.T) {
	t.Run("should use default sidecar image", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Metrics: &valkeyv1.MetricsConfig{Enabled: true},
			},
		}
		container := generateMetricsSidecarContainerDef(cluster)
		assert.Equal(t, DefaultMetricsSidecarImage, container.Image)
	})

	t.Run("should set container name to metrics-sidecar", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Metrics: &valkeyv1.MetricsConfig{Enabled: true},
			},
		}
		container := generateMetricsSidecarContainerDef(cluster)
		assert.Equal(t, MetricsSidecarContainerName, container.Name)
	})

	t.Run("should expose port 9121", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Metrics: &valkeyv1.MetricsConfig{Enabled: true},
			},
		}
		container := generateMetricsSidecarContainerDef(cluster)
		require.Len(t, container.Ports, 1)
		assert.Equal(t, int32(DefaultMetricsSidecarPort), container.Ports[0].ContainerPort)
		assert.Equal(t, "metrics-sidecar", container.Ports[0].Name)
	})

	t.Run("should include system metrics flag", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Metrics: &valkeyv1.MetricsConfig{Enabled: true},
			},
		}
		container := generateMetricsSidecarContainerDef(cluster)
		assert.Contains(t, container.Args, "--include-system-metrics")
	})

	t.Run("should connect to localhost valkey on default port", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Metrics: &valkeyv1.MetricsConfig{Enabled: true},
			},
		}
		container := generateMetricsSidecarContainerDef(cluster)
		assert.Contains(t, container.Args, "--redis.addr=redis://localhost:6379")
	})

	t.Run("should have liveness probe on /health", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Metrics: &valkeyv1.MetricsConfig{Enabled: true},
			},
		}
		container := generateMetricsSidecarContainerDef(cluster)
		require.NotNil(t, container.LivenessProbe)
		require.NotNil(t, container.LivenessProbe.HTTPGet)
		assert.Equal(t, "/health", container.LivenessProbe.HTTPGet.Path)
	})

	t.Run("should have readiness probe on /health", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Metrics: &valkeyv1.MetricsConfig{Enabled: true},
			},
		}
		container := generateMetricsSidecarContainerDef(cluster)
		require.NotNil(t, container.ReadinessProbe)
		require.NotNil(t, container.ReadinessProbe.HTTPGet)
		assert.Equal(t, "/health", container.ReadinessProbe.HTTPGet.Path)
	})
}

func TestGenerateContainersDef_MetricsSidecar(t *testing.T) {
	t.Run("should not include metrics sidecar when metrics is nil", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{},
		}
		containers := generateContainersDef(cluster)
		assert.False(t, containerExists(containers, MetricsSidecarContainerName),
			"metrics sidecar should not be present when metrics config is nil")
	})

	t.Run("should not include metrics sidecar when metrics.enabled is false", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Metrics: &valkeyv1.MetricsConfig{Enabled: false},
			},
		}
		containers := generateContainersDef(cluster)
		assert.False(t, containerExists(containers, MetricsSidecarContainerName),
			"metrics sidecar should not be present when metrics.enabled is false")
	})

	t.Run("should include metrics sidecar when metrics.enabled is true", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Metrics: &valkeyv1.MetricsConfig{Enabled: true},
			},
		}
		containers := generateContainersDef(cluster)
		assert.True(t, containerExists(containers, MetricsSidecarContainerName),
			"metrics sidecar should be present when metrics.enabled is true")
	})

	t.Run("should include both exporter and metrics sidecar when both enabled", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Exporter: valkeyv1.ExporterSpec{Enabled: true},
				Metrics:  &valkeyv1.MetricsConfig{Enabled: true},
			},
		}
		containers := generateContainersDef(cluster)
		assert.True(t, containerExists(containers, "valkey-server"), "valkey-server should exist")
		assert.True(t, containerExists(containers, "metrics-exporter"), "legacy exporter should exist")
		assert.True(t, containerExists(containers, MetricsSidecarContainerName), "metrics sidecar should exist")
		assert.Len(t, containers, 3, "should have 3 containers total")
	})

	t.Run("metrics sidecar should expose port 9121", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Metrics: &valkeyv1.MetricsConfig{Enabled: true},
			},
		}
		containers := generateContainersDef(cluster)
		sidecar := findContainer(containers, MetricsSidecarContainerName)
		require.NotNil(t, sidecar)
		require.Len(t, sidecar.Ports, 1)
		assert.Equal(t, int32(9121), sidecar.Ports[0].ContainerPort)
	})
}
