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

import "fmt"

// SlotTracker tracks unassigned slots during batch admission. It is
// initialised from the cluster's current unassigned slot ranges and
// decremented after each primary is assigned slots, ensuring that
// multiple primaries in the same batch never receive overlapping ranges.
type SlotTracker struct {
	// remaining holds the unassigned slot ranges, kept sorted by Start.
	remaining []SlotsRange
}

// NewSlotTracker creates a SlotTracker seeded with the given unassigned
// slot ranges (typically from ClusterState.GetUnassignedSlots).
func NewSlotTracker(unassigned []SlotsRange) *SlotTracker {
	// Copy to avoid mutating the caller's slice.
	r := make([]SlotsRange, len(unassigned))
	copy(r, unassigned)
	return &SlotTracker{remaining: r}
}

// Remaining returns the current unassigned slot ranges.
func (st *SlotTracker) Remaining() []SlotsRange {
	return st.remaining
}

// TotalRemaining returns the total number of unassigned slots.
func (st *SlotTracker) TotalRemaining() int {
	total := 0
	for _, r := range st.remaining {
		total += r.End - r.Start + 1
	}
	return total
}

// Assign removes the given slot range from the tracker. Returns an error
// if the range is not fully contained in the remaining unassigned slots.
func (st *SlotTracker) Assign(r SlotsRange) error {
	var next []SlotsRange
	removed := 0
	need := r.End - r.Start + 1

	for _, base := range st.remaining {
		parts := subtractSlotsRange(base, r)
		// Count how many slots were removed from this base range.
		baseSize := base.End - base.Start + 1
		partsSize := 0
		for _, p := range parts {
			partsSize += p.End - p.Start + 1
		}
		removed += baseSize - partsSize
		next = append(next, parts...)
	}

	if removed != need {
		return fmt.Errorf("slot range %d-%d not fully available (removed %d of %d)",
			r.Start, r.End, removed, need)
	}
	st.remaining = next
	return nil
}

// NextRange allocates the next contiguous slot range of the given size
// from the front of the remaining slots. Returns the allocated range.
// Returns an error if not enough contiguous slots are available.
func (st *SlotTracker) NextRange(size int) (SlotsRange, error) {
	if len(st.remaining) == 0 || size <= 0 {
		return SlotsRange{}, fmt.Errorf("no slots available (requested %d)", size)
	}

	first := st.remaining[0]
	available := first.End - first.Start + 1
	if available < size {
		return SlotsRange{}, fmt.Errorf("first unassigned range %d-%d has %d slots, need %d",
			first.Start, first.End, available, size)
	}

	allocated := SlotsRange{Start: first.Start, End: first.Start + size - 1}

	// Shrink or remove the first range.
	if available == size {
		st.remaining = st.remaining[1:]
	} else {
		st.remaining[0] = SlotsRange{Start: first.Start + size, End: first.End}
	}

	return allocated, nil
}

// ComputeSlotRanges computes a complete, non-overlapping partition of
// [0, 16383] across totalShards shards. Each shard gets
// TotalSlots/totalShards slots, with the last shard absorbing the
// remainder so that exactly 16384 slots are covered.
//
// Returns an error if totalShards < 1.
func ComputeSlotRanges(totalShards int) ([]SlotsRange, error) {
	if totalShards < 1 {
		return nil, fmt.Errorf("totalShards must be >= 1, got %d", totalShards)
	}

	ranges := make([]SlotsRange, totalShards)
	slotsPerShard := TotalSlots / totalShards
	start := 0

	for i := 0; i < totalShards; i++ {
		end := start + slotsPerShard - 1
		if i == totalShards-1 {
			// Last shard absorbs the remainder.
			end = TotalSlots - 1
		}
		ranges[i] = SlotsRange{Start: start, End: end}
		start = end + 1
	}

	return ranges, nil
}
