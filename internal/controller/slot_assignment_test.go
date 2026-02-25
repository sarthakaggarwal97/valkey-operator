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

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
	"valkey.io/valkey-operator/internal/valkey"
)

func TestNextSlotRangeForPrimarySequential(t *testing.T) {
	cluster := &valkeyiov1alpha1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
		Spec: valkeyiov1alpha1.ValkeyClusterSpec{
			Shards: 3,
		},
	}
	tracker := valkey.NewSlotTracker([]valkey.SlotsRange{{Start: 0, End: 16383}})

	first, err := nextSlotRangeForPrimary(cluster, tracker, 0, 0)
	require.NoError(t, err)
	require.Equal(t, valkey.SlotsRange{Start: 0, End: 5460}, first)
	require.NoError(t, tracker.Assign(first))

	second, err := nextSlotRangeForPrimary(cluster, tracker, 0, 1)
	require.NoError(t, err)
	require.Equal(t, valkey.SlotsRange{Start: 5461, End: 10921}, second)
	require.NoError(t, tracker.Assign(second))

	third, err := nextSlotRangeForPrimary(cluster, tracker, 0, 2)
	require.NoError(t, err)
	require.Equal(t, valkey.SlotsRange{Start: 10922, End: 16383}, third)
}

func TestNextSlotRangeForPrimaryNoRemainingSlots(t *testing.T) {
	cluster := &valkeyiov1alpha1.ValkeyCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
		Spec: valkeyiov1alpha1.ValkeyClusterSpec{
			Shards: 3,
		},
	}
	tracker := valkey.NewSlotTracker(nil)

	_, err := nextSlotRangeForPrimary(cluster, tracker, 0, 0)
	require.Error(t, err)
}
