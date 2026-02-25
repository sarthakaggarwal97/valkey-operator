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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	valkeyv1 "valkey.io/valkey-operator/api/v1alpha1"
)

func TestScrapeInterval(t *testing.T) {
	t.Run("returns 1s when highResolution is true", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Metrics: &valkeyv1.MetricsConfig{
					Enabled:        true,
					HighResolution: true,
				},
			},
		}
		assert.Equal(t, "1s", scrapeInterval(cluster))
	})

	t.Run("returns 15s when highResolution is false", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{
				Metrics: &valkeyv1.MetricsConfig{
					Enabled:        true,
					HighResolution: false,
				},
			},
		}
		assert.Equal(t, "15s", scrapeInterval(cluster))
	})

	t.Run("returns 15s when metrics is nil", func(t *testing.T) {
		cluster := &valkeyv1.ValkeyCluster{
			Spec: valkeyv1.ValkeyClusterSpec{},
		}
		assert.Equal(t, "15s", scrapeInterval(cluster))
	})
}

func TestServiceMonitorName(t *testing.T) {
	cluster := &valkeyv1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "my-cluster"},
	}
	assert.Equal(t, "my-cluster-metrics", serviceMonitorName(cluster))
}

func TestPrometheusRuleName(t *testing.T) {
	cluster := &valkeyv1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "my-cluster"},
	}
	assert.Equal(t, "my-cluster-recording-rules", prometheusRuleName(cluster))
}

func TestBuildRecordingRules(t *testing.T) {
	clusterName := "test-cluster"
	selector := `{app_kubernetes_io_instance="test-cluster", app_kubernetes_io_name="valkey"}`
	interval := "15s"

	rules := buildRecordingRules(clusterName, selector, interval)

	t.Run("generates all expected recording rules", func(t *testing.T) {
		// We expect: 3 memory + 3 CPU + 2 gossip + 3 health indicators + 1 composite = 12 rules
		require.Len(t, rules, 12)
	})

	t.Run("includes memory avg/p90/p99 rules", func(t *testing.T) {
		ruleNames := extractRuleNames(rules)
		assert.Contains(t, ruleNames, "valkey_used_memory_avg")
		assert.Contains(t, ruleNames, "valkey_used_memory_p90")
		assert.Contains(t, ruleNames, "valkey_used_memory_p99")
	})

	t.Run("includes CPU avg/p90/p99 rules", func(t *testing.T) {
		ruleNames := extractRuleNames(rules)
		assert.Contains(t, ruleNames, "valkey_engine_cpu_avg")
		assert.Contains(t, ruleNames, "valkey_engine_cpu_p90")
		assert.Contains(t, ruleNames, "valkey_engine_cpu_p99")
	})

	t.Run("includes gossip rate rules", func(t *testing.T) {
		ruleNames := extractRuleNames(rules)
		assert.Contains(t, ruleNames, "valkey_gossip_messages_sent_rate")
		assert.Contains(t, ruleNames, "valkey_gossip_messages_recv_rate")
	})

	t.Run("includes composite health rules", func(t *testing.T) {
		ruleNames := extractRuleNames(rules)
		assert.Contains(t, ruleNames, "valkey_cluster_state_min")
		assert.Contains(t, ruleNames, "valkey_cluster_nodes_failed_max")
		assert.Contains(t, ruleNames, "valkey_cluster_pfail_max")
		assert.Contains(t, ruleNames, "valkey_cluster_unhealthy")
	})

	t.Run("all rules have cluster label", func(t *testing.T) {
		for _, r := range rules {
			rule, ok := r.(map[string]interface{})
			require.True(t, ok)
			labels, ok := rule["labels"].(map[string]interface{})
			require.True(t, ok)
			assert.Equal(t, clusterName, labels["cluster"])
		}
	})

	t.Run("all rules have non-empty expr", func(t *testing.T) {
		for _, r := range rules {
			rule, ok := r.(map[string]interface{})
			require.True(t, ok)
			expr, ok := rule["expr"].(string)
			require.True(t, ok)
			assert.NotEmpty(t, expr)
		}
	})

	t.Run("memory rules use correct PromQL functions", func(t *testing.T) {
		ruleMap := buildRuleMap(rules)
		assert.Contains(t, ruleMap["valkey_used_memory_avg"], "avg(valkey_used_memory_bytes")
		assert.Contains(t, ruleMap["valkey_used_memory_p90"], "quantile(0.9, valkey_used_memory_bytes")
		assert.Contains(t, ruleMap["valkey_used_memory_p99"], "quantile(0.99, valkey_used_memory_bytes")
	})

	t.Run("CPU rules use rate function", func(t *testing.T) {
		ruleMap := buildRuleMap(rules)
		assert.Contains(t, ruleMap["valkey_engine_cpu_avg"], "rate(valkey_engine_cpu_seconds_total")
		assert.Contains(t, ruleMap["valkey_engine_cpu_p90"], "rate(valkey_engine_cpu_seconds_total")
		assert.Contains(t, ruleMap["valkey_engine_cpu_p99"], "rate(valkey_engine_cpu_seconds_total")
	})

	t.Run("gossip rules use sum of rate", func(t *testing.T) {
		ruleMap := buildRuleMap(rules)
		assert.Contains(t, ruleMap["valkey_gossip_messages_sent_rate"], "sum(rate(valkey_cluster_stats_messages_sent")
		assert.Contains(t, ruleMap["valkey_gossip_messages_recv_rate"], "sum(rate(valkey_cluster_stats_messages_received")
	})

	t.Run("unhealthy rule combines state, fail, and pfail", func(t *testing.T) {
		ruleMap := buildRuleMap(rules)
		expr := ruleMap["valkey_cluster_unhealthy"]
		assert.Contains(t, expr, "valkey_cluster_state")
		assert.Contains(t, expr, "valkey_voting_nodes_fail")
		assert.Contains(t, expr, "valkey_voting_nodes_pfail")
	})
}

func TestToStringInterfaceMap(t *testing.T) {
	input := map[string]string{"a": "1", "b": "2"}
	result := toStringInterfaceMap(input)
	assert.Equal(t, "1", result["a"])
	assert.Equal(t, "2", result["b"])
	assert.Len(t, result, 2)
}

// extractRuleNames returns the "record" field from each rule.
func extractRuleNames(rules []interface{}) []string {
	names := make([]string, 0, len(rules))
	for _, r := range rules {
		rule, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		if name, ok := rule["record"].(string); ok {
			names = append(names, name)
		}
	}
	return names
}

// buildRuleMap returns a map of record name → expr for easy lookup.
func buildRuleMap(rules []interface{}) map[string]string {
	m := make(map[string]string)
	for _, r := range rules {
		rule, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := rule["record"].(string)
		expr, _ := rule["expr"].(string)
		m[name] = expr
	}
	return m
}
