# KubeCon 2000-Node Benchmark and Failure Injection Runbook

This runbook uses the manifests in `/config/samples/kubecon-2000` to run a 2000-node Valkey cluster benchmark and live disruption scenarios.

## 1. Scope

- Cluster size: `1000 shards * (1 primary + 1 replica) = 2000 nodes`
- Namespace: `valkey-kubecon`
- Cluster resource name: `kubecon-2000`
- Benchmark resources:
  - `/config/samples/kubecon-2000/benchmarks/uniform_15m.yaml`
  - `/config/samples/kubecon-2000/benchmarks/zipfian_15m.yaml`
- Disruption resources:
  - `/config/samples/kubecon-2000/disruptions/pause_50_primaries.yaml`
  - `/config/samples/kubecon-2000/disruptions/resume_50_primaries.yaml`
  - `/config/samples/kubecon-2000/disruptions/failover_25_primaries.yaml`
  - `/config/samples/kubecon-2000/disruptions/pause_resume_loop_10_primaries.yaml`
  - `/config/samples/kubecon-2000/disruptions/pause_all_primaries_destructive.yaml`

## 2. Prerequisites

- Operator and CRDs installed (`make install` + `make deploy` or equivalent).
- Kubernetes capacity for 2000 pods plus operator overhead.
- At least 2 worker zones when running `replicas=1` with strict zone spread.
- Prometheus stack with ServiceMonitor/PrometheusRule support if you want the recording rules.

## 2.1 Config mapping (your requested valkey.conf style)

The operator now supports raw directives through `spec.clusterConfig.additionalConfig`.

- Base directives always managed by operator:
  - `port 6379`
  - `cluster-enabled yes`
  - `cluster-config-file nodes.conf`
  - `protected-mode no`
  - `cluster-node-timeout <value>`
- Demo tuning directives are already included in:
  - `/config/samples/kubecon-2000/valkeycluster_2000_nodes.yaml`

If you also want file-based logging/PID semantics from VM-style configs, add lines like:

```yaml
clusterConfig:
  additionalConfig:
  - logfile "/tmp/valkey-cluster.log"
  - pidfile "/tmp/valkey.pid"
```

For Kubernetes demos, stdout logging is usually preferable (omit `logfile`).

## 3. Deploy the 2000-node profile

```sh
kubectl apply -k config/samples/kubecon-2000
```

Track progress:

```sh
kubectl -n valkey-kubecon get valkeycluster kubecon-2000 -w
kubectl -n valkey-kubecon get deploy | wc -l
kubectl -n valkey-kubecon get pods | wc -l
```

Expected:

- `valkeycluster.status.shards` approaches `1000`.
- `valkeycluster.status.readyShards` approaches `1000`.
- Conditions eventually include `Ready=True`, `ClusterFormed=True`, `SlotsAssigned=True`.

## 4. Benchmark sequence

Run one benchmark at a time.

### 4.1 Uniform baseline

```sh
kubectl apply -f config/samples/kubecon-2000/benchmarks/uniform_15m.yaml
kubectl -n valkey-kubecon get valkeybenchmark kubecon-2000-uniform-15m -w
```

Get results:

```sh
kubectl -n valkey-kubecon get valkeybenchmark kubecon-2000-uniform-15m -o yaml
```

### 4.2 Zipfian hot-key run

```sh
kubectl apply -f config/samples/kubecon-2000/benchmarks/zipfian_15m.yaml
kubectl -n valkey-kubecon get valkeybenchmark kubecon-2000-zipfian-15m -w
```

## 5. Failure-injection scenarios

### 5.1 Pause 50 primaries (manual resume)

```sh
kubectl apply -f config/samples/kubecon-2000/disruptions/pause_50_primaries.yaml
kubectl -n valkey-kubecon get valkeydisruption kubecon-pause-50-primaries -w
```

Resume early (on-demand):

```sh
kubectl apply -f config/samples/kubecon-2000/disruptions/resume_50_primaries.yaml
kubectl -n valkey-kubecon get valkeydisruption kubecon-resume-50-primaries -w
```

Notes:

- If you do not run `ResumeProcess`, the operator auto-resumes after `pauseTimeoutSec`.

Target any `N` primaries (on-demand) by creating an ad-hoc disruption:

```sh
N=120
cat <<EOF | kubectl apply -f -
apiVersion: valkey.io/v1alpha1
kind: ValkeyDisruption
metadata:
  name: kubecon-pause-${N}-primaries
  namespace: valkey-kubecon
spec:
  clusterRef: kubecon-2000
  action: PauseProcess
  pauseTimeoutSec: 120
  selector:
    role: primary
    count: ${N}
EOF
```

### 5.2 Trigger failover on 25 primaries

```sh
kubectl apply -f config/samples/kubecon-2000/disruptions/failover_25_primaries.yaml
kubectl -n valkey-kubecon get valkeydisruption kubecon-failover-25-primaries -w
```

### 5.3 Pause/resume loop on 10 primaries

```sh
kubectl apply -f config/samples/kubecon-2000/disruptions/pause_resume_loop_10_primaries.yaml
kubectl -n valkey-kubecon get valkeydisruption kubecon-loop-10-primaries -w
```

### 5.4 Destructive scenario: pause all primaries

```sh
kubectl apply -f config/samples/kubecon-2000/disruptions/pause_all_primaries_destructive.yaml
kubectl -n valkey-kubecon get valkeydisruption kubecon-pause-all-primaries -w
```

This uses `selector.count=0` with required annotation `valkey.io/confirm-destructive: "true"`.

For zone-targeted disruption, add `selector.zone` to any disruption:

```yaml
selector:
  role: primary
  zone: us-east-1a
  count: 20
```

## 6. Expected observability signals

During disruptions, these recording rules should move:

- `valkey_cluster_unhealthy` may flip to `1`.
- `valkey_cluster_pfail_max` should increase during pause events.
- `valkey_cluster_nodes_failed_max` may increase for harder failures.
- `valkey_gossip_messages_sent_rate` and `valkey_gossip_messages_recv_rate` should spike around admission/failover events.
- `valkey_engine_cpu_avg`, `valkey_engine_cpu_p90`, `valkey_engine_cpu_p99` should rise under benchmark load.

Cluster-health query set:

```promql
valkey_cluster_unhealthy{cluster="kubecon-2000"}
valkey_cluster_state_min{cluster="kubecon-2000"}
valkey_cluster_nodes_failed_max{cluster="kubecon-2000"}
valkey_cluster_pfail_max{cluster="kubecon-2000"}
```

Traffic/load query set:

```promql
valkey_gossip_messages_sent_rate{cluster="kubecon-2000"}
valkey_gossip_messages_recv_rate{cluster="kubecon-2000"}
valkey_engine_cpu_p99{cluster="kubecon-2000"}
valkey_used_memory_p99{cluster="kubecon-2000"}
```

## 7. Cleanup

```sh
kubectl -n valkey-kubecon delete valkeybenchmark --all
kubectl -n valkey-kubecon delete valkeydisruption --all
kubectl delete -k config/samples/kubecon-2000
```
