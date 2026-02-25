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

package valkey

import (
	"reflect"
	"testing"
)

func TestComputeSlotRanges_SingleShard(t *testing.T) {
	ranges, err := ComputeSlotRanges(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range, got %d", len(ranges))
	}
	expected := SlotsRange{Start: 0, End: 16383}
	if ranges[0] != expected {
		t.Errorf("expected %v, got %v", expected, ranges[0])
	}
}

func TestComputeSlotRanges_ThreeShards(t *testing.T) {
	ranges, err := ComputeSlotRanges(3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ranges) != 3 {
		t.Fatalf("expected 3 ranges, got %d", len(ranges))
	}
	// 16384 / 3 = 5461 per shard, last shard absorbs remainder.
	expected := []SlotsRange{
		{Start: 0, End: 5460},
		{Start: 5461, End: 10921},
		{Start: 10922, End: 16383},
	}
	if !reflect.DeepEqual(ranges, expected) {
		t.Errorf("expected %v, got %v", expected, ranges)
	}
}

func TestComputeSlotRanges_CompleteCoverage(t *testing.T) {
	for _, n := range []int{1, 2, 3, 4, 5, 10, 100, 1000, 2000} {
		ranges, err := ComputeSlotRanges(n)
		if err != nil {
			t.Fatalf("n=%d: unexpected error: %v", n, err)
		}
		if len(ranges) != n {
			t.Fatalf("n=%d: expected %d ranges, got %d", n, n, len(ranges))
		}
		// Verify complete coverage: first starts at 0, last ends at 16383.
		if ranges[0].Start != 0 {
			t.Errorf("n=%d: first range starts at %d, expected 0", n, ranges[0].Start)
		}
		if ranges[n-1].End != TotalSlots-1 {
			t.Errorf("n=%d: last range ends at %d, expected %d", n, ranges[n-1].End, TotalSlots-1)
		}
		// Verify non-overlapping and contiguous.
		for i := 1; i < n; i++ {
			if ranges[i].Start != ranges[i-1].End+1 {
				t.Errorf("n=%d: gap or overlap between range %d (%v) and %d (%v)",
					n, i-1, ranges[i-1], i, ranges[i])
			}
		}
		// Verify total slot count.
		total := 0
		for _, r := range ranges {
			total += r.End - r.Start + 1
		}
		if total != TotalSlots {
			t.Errorf("n=%d: total slots = %d, expected %d", n, total, TotalSlots)
		}
	}
}

func TestComputeSlotRanges_InvalidInput(t *testing.T) {
	_, err := ComputeSlotRanges(0)
	if err == nil {
		t.Error("expected error for totalShards=0")
	}
	_, err = ComputeSlotRanges(-1)
	if err == nil {
		t.Error("expected error for totalShards=-1")
	}
}

func TestSlotTracker_NextRange(t *testing.T) {
	st := NewSlotTracker([]SlotsRange{{0, 16383}})

	// Allocate first range.
	r, err := st.NextRange(5461)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r != (SlotsRange{0, 5460}) {
		t.Errorf("expected {0, 5460}, got %v", r)
	}

	// Allocate second range.
	r, err = st.NextRange(5461)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r != (SlotsRange{5461, 10921}) {
		t.Errorf("expected {5461, 10921}, got %v", r)
	}

	// Allocate remaining.
	r, err = st.NextRange(5462)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r != (SlotsRange{10922, 16383}) {
		t.Errorf("expected {10922, 16383}, got %v", r)
	}

	// No more slots.
	if st.TotalRemaining() != 0 {
		t.Errorf("expected 0 remaining, got %d", st.TotalRemaining())
	}
}

func TestSlotTracker_Assign(t *testing.T) {
	st := NewSlotTracker([]SlotsRange{{0, 16383}})

	// Assign a range from the middle.
	err := st.Assign(SlotsRange{100, 199})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []SlotsRange{{0, 99}, {200, 16383}}
	if !reflect.DeepEqual(st.Remaining(), expected) {
		t.Errorf("expected %v, got %v", expected, st.Remaining())
	}

	if st.TotalRemaining() != 16284 {
		t.Errorf("expected 16284 remaining, got %d", st.TotalRemaining())
	}
}

func TestSlotTracker_SequentialAssignment(t *testing.T) {
	// Simulate batch admission: 3 primaries assigned sequentially.
	st := NewSlotTracker([]SlotsRange{{0, 16383}})

	ranges, err := ComputeSlotRanges(3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i, r := range ranges {
		allocated, err := st.NextRange(r.End - r.Start + 1)
		if err != nil {
			t.Fatalf("shard %d: unexpected error: %v", i, err)
		}
		if allocated != r {
			t.Errorf("shard %d: expected %v, got %v", i, r, allocated)
		}
	}

	if st.TotalRemaining() != 0 {
		t.Errorf("expected 0 remaining after all assignments, got %d", st.TotalRemaining())
	}
}

func TestSlotTracker_DoesNotMutateInput(t *testing.T) {
	input := []SlotsRange{{0, 16383}}
	st := NewSlotTracker(input)
	_, _ = st.NextRange(100)

	// Original input should be unchanged.
	if input[0] != (SlotsRange{0, 16383}) {
		t.Errorf("input was mutated: %v", input)
	}
}
