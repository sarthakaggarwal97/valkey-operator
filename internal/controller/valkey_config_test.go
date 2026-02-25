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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

func TestBuildValkeyConfig_BaseDirectives(t *testing.T) {
	cluster := &valkeyiov1alpha1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg-test"},
		Spec: valkeyiov1alpha1.ValkeyClusterSpec{
			ClusterConfig: &valkeyiov1alpha1.ClusterConfig{
				ClusterNodeTimeoutMs: 15000,
			},
		},
	}

	cfg := buildValkeyConfig(cluster)
	require.Contains(t, cfg, "port 6379")
	require.Contains(t, cfg, "cluster-enabled yes")
	require.Contains(t, cfg, "cluster-config-file nodes.conf")
	require.Contains(t, cfg, "protected-mode no")
	require.Contains(t, cfg, "cluster-node-timeout 15000")
}

func TestBuildValkeyConfig_AdditionalConfigAppended(t *testing.T) {
	cluster := &valkeyiov1alpha1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg-test-extra"},
		Spec: valkeyiov1alpha1.ValkeyClusterSpec{
			ClusterConfig: &valkeyiov1alpha1.ClusterConfig{
				ClusterNodeTimeoutMs: 15000,
				AdditionalConfig: []string{
					"",
					"save \"\"",
					"maxmemory 50gb",
					"  cluster-node-timeout 30000  ",
				},
			},
		},
	}

	cfg := buildValkeyConfig(cluster)
	lines := strings.Split(cfg, "\n")

	require.Contains(t, lines, "save \"\"")
	require.Contains(t, lines, "maxmemory 50gb")
	require.Contains(t, lines, "cluster-node-timeout 30000")
	require.NotContains(t, lines, "")
}
