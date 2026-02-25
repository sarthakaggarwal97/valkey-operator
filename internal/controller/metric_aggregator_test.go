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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	valkeyv1 "valkey.io/valkey-operator/api/v1alpha1"
)

func TestMetricAggregatorName(t *testing.T) {
	cluster := &valkeyv1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "my-cluster"},
	}
	assert.Equal(t, "my-cluster-metric-aggregator", metricAggregatorName(cluster))
}

func TestMetricAggregatorLabels(t *testing.T) {
	cluster := &valkeyv1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster"},
	}
	l := metricAggregatorLabels(cluster)

	assert.Equal(t, "metric-aggregator", l["app.kubernetes.io/component"])
	assert.Equal(t, "test-cluster", l["app.kubernetes.io/instance"])
	assert.Equal(t, "valkey", l["app.kubernetes.io/name"])
	assert.Equal(t, "valkey-operator", l["app.kubernetes.io/managed-by"])
}

func TestAggregatorReplicas(t *testing.T) {
	t.Run("returns configured replicas", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Metrics: &valkeyv1.MetricsConfig{
					Enabled:            true,
					AggregatorReplicas: 3,
				},
			},
		}
		assert.Equal(t, int32(3), aggregatorReplicas(cluster))
	})

	t.Run("defaults to 1 when not set", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Metrics: &valkeyv1.MetricsConfig{
					Enabled: true,
				},
			},
		}
		assert.Equal(t, int32(1), aggregatorReplicas(cluster))
	})

	t.Run("defaults to 1 when metrics is nil", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{},
		}
		assert.Equal(t, int32(1), aggregatorReplicas(cluster))
	})
}
