# Kubernetes Deployment for LNR Gossip Pipeline

This directory contains Kubernetes manifests for deploying the Lightning Network Research Gossip Pipeline to a k3s cluster.

## Architecture

- **Namespace**: `lnr`
- **Registry**: `registry.media.snyke.net/lnr/gossip-pipeline`
- **Persistent Data**: Host paths for database and annex files
- **Web Interface**: Available on port 8000

## Storage

The deployment uses hostPath volumes for persistent data:

- **Database**: `/var/lib/lnr/db/gossip.db` on the host
- **Annex**: `/var/lib/lnr/annex/` on the host

These directories will be created automatically if they don't exist.

## Environment Variables

The following environment variables can be customized in `deployment.yaml`:

- `RABBITMQ_URL`: Local RabbitMQ connection
- `UPSTREAM_RABBITMQ_URL`: Upstream RabbitMQ for glbridge
- `UPSTREAM_QUEUE_NAME`: Upstream exchange name
- `DATABASE_PATH`: SQLite database path (maps to hostPath)
- `ARCHIVE_DIRECTORY`: Archive directory path (maps to hostPath)
- `ARCHIVE_ROTATION`: `daily` or `hourly`
- `LOG_LEVEL`: Logging level

## Deployment with Skaffold

### Prerequisites

1. Install Skaffold
2. Configure kubectl for your k3s cluster
3. Ensure registry access to `registry.media.snyke.net`

### Deploy

```bash
# Deploy to k3s cluster
skaffold run

# Deploy with dev profile (no ingress)
skaffold run -p dev

# Deploy with prod profile (includes ingress)
skaffold run -p prod

# Continuous development
skaffold dev
```

### Manual Deployment

```bash
# Apply all manifests
kubectl apply -f k8s/

# Check deployment status
kubectl get all -n lnr

# View logs
kubectl logs -f deployment/gossip-pipeline -n lnr
```

## Accessing the Application

### Via Service (within cluster)
```bash
kubectl port-forward service/gossip-pipeline 8000:8000 -n lnr
```

### Via Ingress (if enabled)
The application is available at:
- **HTTPS**: https://lnr.media.snyke.net
- **HTTP**: http://lnr.media.snyke.net (redirects to HTTPS)

The ingress is configured with:
- Let's Encrypt TLS certificates
- Automatic HTTP to HTTPS redirect

## Monitoring

### Health Checks
- **Liveness**: `GET /health` every 10s after 30s
- **Readiness**: `GET /health` every 5s after 5s

### Resource Limits
- **Memory**: 256Mi request, 512Mi limit
- **CPU**: 100m request, 500m limit

## Troubleshooting

### Check Pod Status
```bash
kubectl get pods -n lnr
kubectl describe pod <pod-name> -n lnr
```

### View Logs
```bash
kubectl logs -f deployment/gossip-pipeline -n lnr
```

### Check Volumes
```bash
kubectl exec -it deployment/gossip-pipeline -n lnr -- ls -la /data/
```

### Debug Database
```bash
kubectl exec -it deployment/gossip-pipeline -n lnr -- sqlite3 /data/db/gossip.db ".tables"
```