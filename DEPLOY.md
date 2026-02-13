# Self-Hosted SP1 Cluster — Deployment Guide

Step-by-step guide to deploy a self-hosted SP1 proving cluster on AWS EKS using Kubernetes and Helm.

## What You'll Deploy

A proving cluster capable of generating SP1 proofs independently, without relying on the Succinct Prover Network.

```
  Your Application (SP1 SDK)
       │
       ▼
    [API] ──────► [PostgreSQL]
       │
       ▼
  [Coordinator]
       │
   ┌───┴────┐
   ▼        ▼
 [CPU]    [GPU]
 Worker   Workers
   │        │
   └───┬────┘
       ▼
    [Redis]
```

| Component | Role |
|-----------|------|
| **API** | Receives proof requests via gRPC (port 50051) |
| **Coordinator** | Decomposes proofs into tasks, assigns them to workers |
| **CPU Worker** | Handles non-GPU proving stages (setup, recursion, Plonk wrapping) |
| **GPU Workers** | Handles GPU-accelerated proving (core proving, shrink/wrap) |
| **PostgreSQL** | Stores proof request state |
| **Redis** | Shared artifact store for intermediate proof data between workers |

---

## Prerequisites

### Tools (install on your local machine)

| Tool | Purpose | Install |
|------|---------|---------|
| **AWS CLI** | Interact with AWS | [Install guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) |
| **eksctl** | Create EKS clusters | `brew install eksctl` or [Install guide](https://eksctl.io/installation/) |
| **kubectl** | Manage Kubernetes resources | [Install guide](https://kubernetes.io/docs/tasks/tools/) |
| **Helm** | Deploy Kubernetes applications | [Install guide](https://helm.sh/docs/intro/install/) |

### Accounts & Access

- **AWS account** with permissions to create EKS clusters, EC2 instances, and IAM roles
- **GitHub account** with access to `ghcr.io/succinctlabs/sp1-cluster` container images
  - You'll need a GitHub Personal Access Token (PAT) with `read:packages` scope

### Hardware Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| **CPU nodes** | ≥40 GB RAM, >30 GB disk | ≥64 GB DDR5 RAM, high core count |
| **GPU nodes** | ≥16 GB RAM, 1x GPU (≥24 GB VRAM, Compute Capability ≥8.6) | ≥32 GB DDR5 RAM, RTX 5090/4090 or L4/A10G |

> **Note:** RTX 5090 users must set `MOONGATE_DISABLE_GRIND_DEVICE=true` environment variable due to driver issues.

---

## Step 1: Clone the Repository

```bash
git clone https://github.com/succinctlabs/sp1-cluster.git
cd sp1-cluster
```

---

## Step 2: Create the EKS Cluster

### Choose a region

Pick a region with GPU instance availability (`g6.2xlarge`). Some regions hit AWS quota limits.

> **Heads up:** AWS accounts have default limits of 5 VPCs and 5 Elastic IPs per region. If you get a quota error, either request an increase or try a different region. Regions like `us-west-2` and `us-east-1` tend to have good GPU availability.

Create a file called `cluster.yaml`:

```yaml
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig

metadata:
  name: sp1-cluster
  region: us-west-2          # change to your preferred region

managedNodeGroups:
  - name: cpu
    instanceType: m7a.8xlarge    # 32 vCPU, 128 GB RAM
    desiredCapacity: 1
    minSize: 0
    maxSize: 3
    labels:
      role: compute

  - name: gpu
    instanceType: g6.2xlarge     # 1x NVIDIA L4 (24 GB VRAM), 8 vCPU, 32 GB RAM
    desiredCapacity: 1           # start with 1, scale up as needed
    minSize: 0
    maxSize: 60
    labels:
      role: gpu
      nvidia.com/gpu: "true"
    taints:
      - key: nvidia.com/gpu
        value: "true"
        effect: NoSchedule       # prevents non-GPU workloads from running on GPU nodes
```

Create the cluster (~15-20 minutes):

```bash
eksctl create cluster -f cluster.yaml
```

> eksctl automatically installs the **NVIDIA device plugin** on GPU node groups, so GPU workers can schedule without manual setup.

When complete, verify the nodes are ready:

```bash
kubectl get nodes
```

Expected output (2 nodes — 1 CPU, 1 GPU):

```
NAME                                          STATUS   ROLES    AGE   VERSION
ip-192-168-x-x.us-west-2.compute.internal    Ready    <none>   2m    v1.31.x
ip-192-168-y-y.us-west-2.compute.internal    Ready    <none>   2m    v1.31.x
```

### Grant access to teammates (optional)

The IAM user/role who creates the cluster is the implicit admin. To grant access to others:

```bash
eksctl create iamidentitymapping \
  --cluster sp1-cluster \
  --region us-west-2 \
  --arn arn:aws:iam::<ACCOUNT_ID>:user/<USERNAME> \
  --group system:masters \
  --username <USERNAME>
```

Or use the AWS Console: EKS → your cluster → **Access** tab → add IAM principal with `AmazonEKSClusterAdminPolicy`.

---

## Step 3: Create Namespace and Secrets

### Create the namespace

```bash
kubectl create namespace sp1-cluster
```

Expected: `namespace/sp1-cluster created`

### Create application secrets

Choose passwords for PostgreSQL and Redis, then create the secret:

```bash
kubectl create secret generic cluster-secrets \
  --from-literal=DATABASE_URL=postgresql://postgres:<YOUR_DB_PASSWORD>@postgresql:5432/postgres \
  --from-literal=REDIS_NODES=redis://:<YOUR_REDIS_PASSWORD>@redis-master:6379/0 \
  -n sp1-cluster
```

> **Note:** The hostnames `postgresql` and `redis-master` are Kubernetes service names — they resolve automatically inside the cluster. Do not replace them with external URLs.

### Create image pull secret

Required to pull container images from GitHub Container Registry:

```bash
kubectl create secret docker-registry ghcr-secret \
  --docker-server=ghcr.io \
  --docker-username=<YOUR_GITHUB_USERNAME> \
  --docker-password=<YOUR_GITHUB_PAT> \
  -n sp1-cluster
```

Expected: `secret/ghcr-secret created`

---

## Step 4: Configure the Helm Values

```bash
cp infra/charts/sp1-cluster/values-example.yaml infra/charts/sp1-cluster/values.yaml
```

Edit `values.yaml` with the following changes. The example file includes all required environment variables for each component — the sections below cover only the values you need to customize. Do not remove existing `extraEnv` entries.

### Critical: Allow insecure images

> **You must add this or the deploy will fail.** The Bitnami Redis sub-chart uses legacy image signatures that Helm's security policy rejects by default. Without this flag, you'll get: _"Original containers have been substituted for unrecognized ones"_.

Add this under the existing `global` section in `values.yaml`:

```yaml
global:
  security:
    allowInsecureImages: true
```

### Required: Passwords

Must match the passwords you used in Step 3:

```yaml
global:
  redis:
    password: <YOUR_REDIS_PASSWORD>

postgresql:
  auth:
    postgresPassword: <YOUR_DB_PASSWORD>
```

### Required: Node placement (EKS)

Pin non-GPU components to CPU nodes so they don't land on expensive GPU instances. Add `nodeSelector` to each component:

```yaml
cpu-node:
  nodeSelector:
    eks.amazonaws.com/nodegroup: cpu

api:
  nodeSelector:
    eks.amazonaws.com/nodegroup: cpu

coordinator:
  nodeSelector:
    eks.amazonaws.com/nodegroup: cpu

postgresql:
  primary:
    nodeSelector:
      eks.amazonaws.com/nodegroup: cpu

redis-store:
  redis:
    nodeSelector:
      eks.amazonaws.com/nodegroup: cpu

gpu-node:
  tolerations:
    - key: nvidia.com/gpu
      operator: Equal
      value: "true"
      effect: NoSchedule
  nodeSelector:
    eks.amazonaws.com/nodegroup: gpu
```

> GPU nodes have a `NoSchedule` taint (from Step 2), so GPU pods need the toleration above or they'll be stuck in `Pending`.

### Recommended: Worker scaling

```yaml
cpu-node:
  replicaCount: 1                        # 1 is usually sufficient

gpu-node:
  replicaCount: 1                        # 1 to start — scale up to match your GPU node count
```

### Recommended: Worker capacity

Set `WORKER_MAX_WEIGHT_OVERRIDE` to match available RAM in GB on each node type:

```yaml
cpu-node:
  extraEnv:
    WORKER_MAX_WEIGHT_OVERRIDE: "32"     # for m7a.8xlarge with 128 GB, 32 is conservative

gpu-node:
  extraEnv:
    WORKER_MAX_WEIGHT_OVERRIDE: "24"     # for g6.2xlarge with 32 GB total
```

### Optional: PostgreSQL persistence

For production, enable persistence so proof history survives restarts:

```yaml
postgresql:
  primary:
    persistence:
      enabled: true                      # default is false (data lost on restart)
```

---

## Step 5: Deploy

### Pull Helm dependencies

```bash
helm dependency update infra/charts/redis-store
helm dependency update infra/charts/sp1-cluster
```

### Install the cluster

```bash
helm upgrade --install sp1-cluster infra/charts/sp1-cluster \
  -f infra/charts/sp1-cluster/values.yaml \
  -n sp1-cluster \
  --debug
```

This command is idempotent — re-run it after any `values.yaml` change.

---

## Step 6: Verify

```bash
kubectl get pods -n sp1-cluster
```

Wait for all pods to show `Running` (may take 1-2 minutes):

```
NAME                            READY   STATUS    RESTARTS   AGE
api-7b8f4c9d5f-xxxxx            1/1     Running   0          90s
coordinator-6c5d8e7f4b-xxxxx    1/1     Running   0          90s
cpu-node-5a4b3c2d1e-xxxxx       1/1     Running   0          90s
gpu-node-9f8e7d6c5b-xxxxx       1/1     Running   0          90s
postgresql-0                     1/1     Running   0          90s
redis-master-0                   1/1     Running   0          90s
```

> **Normal behavior:** The API pod may show `CrashLoopBackOff` briefly while PostgreSQL is still starting up. It resolves automatically once PostgreSQL is ready — just wait and re-check.

To check logs for any component:

```bash
kubectl logs <pod-name> -n sp1-cluster
```

---

## Step 7: Test with a Fibonacci Proof

The `fibonacci` benchmark generates a proving workload inside the SP1 zkVM. The argument controls workload size in millions of cycles — `5` is a quick smoke test, `20` is a moderate test.

Run a temporary CLI pod that executes the benchmark directly:

```bash
kubectl run cli -n sp1-cluster --rm -it \
  --image=ghcr.io/succinctlabs/sp1-cluster:base-v2.0.0-rc.1 \
  --env="RUST_LOG=info" \
  --env="CLI_CLUSTER_RPC=http://api-grpc:50051" \
  --env="CLI_REDIS_NODES=redis://:<YOUR_REDIS_PASSWORD>@redis-master:6379/0" \
  --restart=Never \
  -- /cli bench fibonacci 5
```

Expected output:

```
Running 1x Fibonacci Compressed for 5 million cycles...
... (proving takes a few minutes) ...
Proof completed in <duration>
```

This proves the full pipeline works: CLI → API → Coordinator → Workers → proof generated and verified.

---

## Next Steps

Once your cluster passes the fibonacci test, you can use it with any SP1 application by setting `SP1_PROVER=cluster` and providing the cluster connection details.

For OP Stack validity proofs, see the [op-succinct self-hosted cluster guide](https://succinctlabs.github.io/op-succinct/advanced/self-hosted-cluster.html).

---

## Troubleshooting

### Pod stuck in `Pending`

```bash
kubectl describe pod <pod-name> -n sp1-cluster
```

Common causes:
- **Insufficient resources** — node doesn't have enough CPU/memory/GPU for the pod's requests
- **GPU pods without NVIDIA plugin** — verify with `kubectl get pods -n kube-system | grep nvidia`; eksctl should auto-install this, but if missing, apply manually:
  ```bash
  kubectl apply -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.17.0/deployments/static/nvidia-device-plugin.yml
  ```
- **Taint/toleration mismatch** — GPU pods need tolerations for the GPU node taint
- **No nodes available** — node group may be scaled to 0 (`eksctl scale nodegroup` to fix)

### Pod in `CrashLoopBackOff`

The container starts but crashes repeatedly. Check logs:

```bash
kubectl logs <pod-name> -n sp1-cluster --previous
```

Common causes:
- **API can't reach PostgreSQL** — the API pod often crashes until PostgreSQL is fully ready. Wait 1-2 minutes and it should stabilize.
- **Wrong database URL** — verify `DATABASE_URL` secret matches postgresql password
- **Redis unreachable** — verify `REDIS_NODES` secret and that redis-master pod is running

### Pod in `ImagePullBackOff`

```bash
kubectl describe pod <pod-name> -n sp1-cluster
```

Common causes:
- **`ghcr-secret` missing or invalid** — verify your GitHub PAT has `read:packages` scope
- **Wrong image name** — if you see `bitnami/postgresql` failing, change to `bitnamilegacy/postgresql` in your values file

> **Tip:** If a StatefulSet pod (like postgresql-0) is stuck pulling a bad image even after fixing the values, delete the pod to force re-creation: `kubectl delete pod postgresql-0 -n sp1-cluster`

### Proof request hangs or CLI produces no output

1. **Missing `RUST_LOG`** — the CLI requires `RUST_LOG=info` (or `debug`) to produce any output. Without it, tracing is completely silent.
2. Check coordinator logs for task assignment:
   ```bash
   kubectl logs -l app=coordinator -n sp1-cluster
   ```
3. Check worker logs for task execution:
   ```bash
   kubectl logs -l app=cpu-node -n sp1-cluster
   kubectl logs -l app=gpu-node -n sp1-cluster
   ```
4. Verify Redis connectivity — workers need Redis to exchange artifacts

### AWS quota errors during cluster creation

- **"The maximum number of VPCs has been reached"** — default limit is 5 VPCs per region. Request a quota increase or use a region with fewer VPCs.
- **"The maximum number of addresses has been reached"** — default limit is 5 Elastic IPs per region. Request a quota increase or try another region.
- **GPU instances unavailable** — not all regions have `g6.2xlarge` capacity. `us-west-2`, `us-east-1` are reliable choices.

### Helm deploy errors

- **"Original containers have been substituted for unrecognized ones"** — add `global.security.allowInsecureImages: true` to your values file.
- **Template rendering errors after editing charts** — re-run `helm dependency update infra/charts/sp1-cluster` before deploying.

---

## Scaling

### Add more GPU workers

1. Scale the GPU node group:
   ```bash
   eksctl scale nodegroup --cluster sp1-cluster --name gpu --nodes 10 --region us-west-2
   ```

2. Update `gpu-node.replicaCount` in your values file to match, then redeploy:
   ```bash
   helm upgrade --install sp1-cluster infra/charts/sp1-cluster \
     -f infra/charts/sp1-cluster/values.yaml \
     -n sp1-cluster
   ```

### Scale down to save costs

Set node groups to 0 when not in use:

```bash
eksctl scale nodegroup --cluster sp1-cluster --name gpu --nodes 0 --region us-west-2
eksctl scale nodegroup --cluster sp1-cluster --name cpu --nodes 0 --region us-west-2
```

Pods will go to `Pending` and resume automatically when nodes scale back up.

---

## Teardown

Remove all deployed components:

```bash
helm uninstall sp1-cluster -n sp1-cluster
```

Delete the EKS cluster and all associated resources:

```bash
eksctl delete cluster -f cluster.yaml
```

> This deletes all EC2 instances, VPC, subnets, and IAM roles created by eksctl. Ensure you no longer need the cluster before running this.

