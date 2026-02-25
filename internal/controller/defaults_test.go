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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	valkeyiov1alpha1 "valkey.io/valkey-operator/api/v1alpha1"
)

var _ = Describe("ApplyClusterDefaults", func() {
	It("should fill in all defaults when ClusterConfig and Admission are nil", func() {
		spec := &valkeyiov1alpha1.ValkeyClusterSpec{}
		ApplyClusterDefaults(spec)

		Expect(spec.ClusterConfig).NotTo(BeNil())
		Expect(spec.ClusterConfig.ClusterNodeTimeoutMs).To(Equal(DefaultClusterNodeTimeoutMs))
		Expect(spec.Admission).NotTo(BeNil())
		Expect(spec.Admission.Parallelism).To(Equal(DefaultParallelism))
		Expect(spec.Admission.MeetStrategy).To(Equal(DefaultMeetStrategy))
		Expect(spec.Admission.SeedCount).To(Equal(DefaultSeedCount))
	})

	It("should not overwrite explicitly set values", func() {
		spec := &valkeyiov1alpha1.ValkeyClusterSpec{
			ClusterConfig: &valkeyiov1alpha1.ClusterConfig{
				ClusterNodeTimeoutMs: 30000,
			},
			Admission: &valkeyiov1alpha1.AdmissionConfig{
				Parallelism:  100,
				MeetStrategy: "all",
				SeedCount:    5,
			},
		}
		ApplyClusterDefaults(spec)

		Expect(spec.ClusterConfig.ClusterNodeTimeoutMs).To(Equal(int32(30000)))
		Expect(spec.Admission.Parallelism).To(Equal(int32(100)))
		Expect(spec.Admission.MeetStrategy).To(Equal("all"))
		Expect(spec.Admission.SeedCount).To(Equal(int32(5)))
	})

	It("should fill in defaults for partially set config", func() {
		spec := &valkeyiov1alpha1.ValkeyClusterSpec{
			ClusterConfig: &valkeyiov1alpha1.ClusterConfig{
				ClusterNodeTimeoutMs: 5000,
			},
			// Admission is nil
		}
		ApplyClusterDefaults(spec)

		Expect(spec.ClusterConfig.ClusterNodeTimeoutMs).To(Equal(int32(5000)))
		Expect(spec.Admission).NotTo(BeNil())
		Expect(spec.Admission.Parallelism).To(Equal(DefaultParallelism))
		Expect(spec.Admission.MeetStrategy).To(Equal(DefaultMeetStrategy))
		Expect(spec.Admission.SeedCount).To(Equal(DefaultSeedCount))
	})
})

var _ = Describe("EmitConfigWarnings", func() {
	var (
		recorder *events.FakeRecorder
	)

	BeforeEach(func() {
		recorder = events.NewFakeRecorder(100)
	})

	It("should emit LowClusterNodeTimeout warning when shards > 100 and timeout < 5000", func() {
		cluster := &valkeyiov1alpha1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: valkeyiov1alpha1.ValkeyClusterSpec{
				Shards: 150,
				ClusterConfig: &valkeyiov1alpha1.ClusterConfig{
					ClusterNodeTimeoutMs: 2000,
				},
			},
		}
		EmitConfigWarnings(recorder, cluster)

		evts := collectEvents(recorder)
		matched := filterEvents(evts, "LowClusterNodeTimeout")
		Expect(matched).To(HaveLen(1))
		Expect(matched[0]).To(ContainSubstring("Warning"))
		Expect(matched[0]).To(ContainSubstring("2000"))
	})

	It("should NOT emit LowClusterNodeTimeout when timeout >= 5000", func() {
		cluster := &valkeyiov1alpha1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: valkeyiov1alpha1.ValkeyClusterSpec{
				Shards: 150,
				ClusterConfig: &valkeyiov1alpha1.ClusterConfig{
					ClusterNodeTimeoutMs: 5000,
				},
			},
		}
		EmitConfigWarnings(recorder, cluster)

		evts := collectEvents(recorder)
		matched := filterEvents(evts, "LowClusterNodeTimeout")
		Expect(matched).To(BeEmpty())
	})

	It("should NOT emit LowClusterNodeTimeout when shards <= 100", func() {
		cluster := &valkeyiov1alpha1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: valkeyiov1alpha1.ValkeyClusterSpec{
				Shards: 100,
				ClusterConfig: &valkeyiov1alpha1.ClusterConfig{
					ClusterNodeTimeoutMs: 2000,
				},
			},
		}
		EmitConfigWarnings(recorder, cluster)

		evts := collectEvents(recorder)
		matched := filterEvents(evts, "LowClusterNodeTimeout")
		Expect(matched).To(BeEmpty())
	})

	It("should emit LowAdmissionParallelism warning when shards > 500 and parallelism < 10", func() {
		cluster := &valkeyiov1alpha1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: valkeyiov1alpha1.ValkeyClusterSpec{
				Shards: 501,
				Admission: &valkeyiov1alpha1.AdmissionConfig{
					Parallelism: 5,
				},
			},
		}
		EmitConfigWarnings(recorder, cluster)

		evts := collectEvents(recorder)
		matched := filterEvents(evts, "LowAdmissionParallelism")
		Expect(matched).To(HaveLen(1))
		Expect(matched[0]).To(ContainSubstring("Warning"))
		Expect(matched[0]).To(ContainSubstring("5"))
	})

	It("should NOT emit LowAdmissionParallelism when parallelism >= 10", func() {
		cluster := &valkeyiov1alpha1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: valkeyiov1alpha1.ValkeyClusterSpec{
				Shards: 501,
				Admission: &valkeyiov1alpha1.AdmissionConfig{
					Parallelism: 10,
				},
			},
		}
		EmitConfigWarnings(recorder, cluster)

		evts := collectEvents(recorder)
		matched := filterEvents(evts, "LowAdmissionParallelism")
		Expect(matched).To(BeEmpty())
	})

	It("should NOT emit LowAdmissionParallelism when shards <= 500", func() {
		cluster := &valkeyiov1alpha1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: valkeyiov1alpha1.ValkeyClusterSpec{
				Shards: 500,
				Admission: &valkeyiov1alpha1.AdmissionConfig{
					Parallelism: 5,
				},
			},
		}
		EmitConfigWarnings(recorder, cluster)

		evts := collectEvents(recorder)
		matched := filterEvents(evts, "LowAdmissionParallelism")
		Expect(matched).To(BeEmpty())
	})

	It("should emit both warnings when both conditions are met", func() {
		cluster := &valkeyiov1alpha1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: valkeyiov1alpha1.ValkeyClusterSpec{
				Shards: 600,
				ClusterConfig: &valkeyiov1alpha1.ClusterConfig{
					ClusterNodeTimeoutMs: 2000,
				},
				Admission: &valkeyiov1alpha1.AdmissionConfig{
					Parallelism: 5,
				},
			},
		}
		EmitConfigWarnings(recorder, cluster)

		evts := collectEvents(recorder)
		timeoutWarnings := filterEvents(evts, "LowClusterNodeTimeout")
		parallelismWarnings := filterEvents(evts, "LowAdmissionParallelism")
		Expect(timeoutWarnings).To(HaveLen(1))
		Expect(parallelismWarnings).To(HaveLen(1))
	})

	It("should emit no warnings when defaults are used with small cluster", func() {
		cluster := &valkeyiov1alpha1.ValkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: valkeyiov1alpha1.ValkeyClusterSpec{
				Shards: 3,
			},
		}
		EmitConfigWarnings(recorder, cluster)

		evts := collectEvents(recorder)
		warnings := make([]string, 0)
		for _, e := range evts {
			if strings.Contains(e, "Warning") {
				warnings = append(warnings, e)
			}
		}
		Expect(warnings).To(BeEmpty())
	})
})
