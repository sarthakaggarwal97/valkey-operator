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

	"valkey.io/valkey-operator/internal/valkey"
)

func TestFindReplicaForPrimaryPrefersSyncedReplica(t *testing.T) {
	primary := &valkey.NodeState{
		Id:      "primary-1",
		Address: "10.0.0.1",
		Flags:   []string{"master"},
	}
	outOfSync := &valkey.NodeState{
		Id:      "replica-1",
		Address: "10.0.0.2",
		Flags:   []string{"slave"},
		Info: map[string]string{
			"master_link_status": "down",
		},
	}
	synced := &valkey.NodeState{
		Id:      "replica-2",
		Address: "10.0.0.3",
		Flags:   []string{"slave"},
		Info: map[string]string{
			"master_link_status": "up",
		},
	}
	state := &valkey.ClusterState{
		Shards: []*valkey.ShardState{
			{
				Id:        "shard-1",
				PrimaryId: primary.Id,
				Nodes:     []*valkey.NodeState{primary, outOfSync, synced},
			},
		},
	}

	de := &DisruptionExecutor{}
	replica, err := de.findReplicaForPrimary(primary, state)
	require.NoError(t, err)
	require.Equal(t, synced.Id, replica.Id)
}

func TestFindReplicaForPrimaryFailsWhenNoSyncedReplica(t *testing.T) {
	primary := &valkey.NodeState{
		Id:      "primary-1",
		Address: "10.0.0.1",
		Flags:   []string{"master"},
	}
	outOfSync := &valkey.NodeState{
		Id:      "replica-1",
		Address: "10.0.0.2",
		Flags:   []string{"slave"},
		Info: map[string]string{
			"master_link_status": "down",
		},
	}
	state := &valkey.ClusterState{
		Shards: []*valkey.ShardState{
			{
				Id:        "shard-1",
				PrimaryId: primary.Id,
				Nodes:     []*valkey.NodeState{primary, outOfSync},
			},
		},
	}

	de := &DisruptionExecutor{}
	_, err := de.findReplicaForPrimary(primary, state)
	require.Error(t, err)
	require.Contains(t, err.Error(), "none are in sync")
}
