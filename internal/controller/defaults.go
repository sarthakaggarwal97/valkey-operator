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
	"k8s.io/client-go/tools/events"
	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

// Default values for ClusterConfig and AdmissionConfig.
// These are safe large-scale defaults suitable for clusters up to 2000 nodes.
const (
	DefaultClusterNodeTimeoutMs int32 = 15000
	DefaultParallelism          int32 = 50
	DefaultMeetStrategy               = "seed"
	DefaultSeedCount            int32 = 3
)

// ApplyClusterDefaults fills in safe large-scale defaults for any omitted
// ClusterConfig or AdmissionConfig fields. It mutates the spec in-place.
// This is called at the start of each reconcile so that downstream logic
// can always assume non-nil config structs with valid values.
func ApplyClusterDefaults(spec *valkeyiov1alpha1.ValkeyClusterSpec) {
	if spec.ClusterConfig == nil {
		spec.ClusterConfig = &valkeyiov1alpha1.ClusterConfig{}
	}
	if spec.ClusterConfig.ClusterNodeTimeoutMs == 0 {
		spec.ClusterConfig.ClusterNodeTimeoutMs = DefaultClusterNodeTimeoutMs
	}

	if spec.Admission == nil {
		spec.Admission = &valkeyiov1alpha1.AdmissionConfig{}
	}
	if spec.Admission.Parallelism == 0 {
		spec.Admission.Parallelism = DefaultParallelism
	}
	if spec.Admission.MeetStrategy == "" {
		spec.Admission.MeetStrategy = DefaultMeetStrategy
	}
	if spec.Admission.SeedCount == 0 {
		spec.Admission.SeedCount = DefaultSeedCount
	}

	// Apply zone awareness defaults if configured.
	ApplyZoneDefaults(spec)
}

// EmitConfigWarnings checks the cluster spec for potentially dangerous
// configuration combinations at large scale and emits warning events on
// the ValkeyCluster resource.
//
// Warnings emitted:
//   - shards > 100 and clusterNodeTimeoutMs < 5000: timeout may be too
//     aggressive for large clusters, causing excessive pfail/fail churn.
//   - shards > 500 and parallelism < 10: low parallelism will make
//     bootstrap/scaling extremely slow at this cluster size.
func EmitConfigWarnings(recorder events.EventRecorder, cluster *valkeyiov1alpha1.ValkeyCluster) {
	spec := &cluster.Spec
	ApplyClusterDefaults(spec)

	shards := spec.Shards
	timeoutMs := spec.ClusterConfig.ClusterNodeTimeoutMs
	parallelism := spec.Admission.Parallelism

	if shards > 100 && timeoutMs < 5000 {
		recorder.Eventf(
			cluster, nil, corev1.EventTypeWarning,
			"LowClusterNodeTimeout", "ConfigWarning",
			fmt.Sprintf("clusterNodeTimeoutMs=%d is below 5000 with %d shards; consider increasing to avoid excessive pfail churn", timeoutMs, shards),
		)
	}

	if shards > 500 && parallelism < 10 {
		recorder.Eventf(
			cluster, nil, corev1.EventTypeWarning,
			"LowAdmissionParallelism", "ConfigWarning",
			fmt.Sprintf("admission.parallelism=%d is below 10 with %d shards; bootstrap will be very slow", parallelism, shards),
		)
	}
}
