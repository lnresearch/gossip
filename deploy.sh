#!/bin/bash

# Deploy LNR Gossip Pipeline to k3s
set -e

echo "🚀 Deploying LNR Gossip Pipeline to k3s..."

# Set kubeconfig
export KUBECONFIG=/home/cdecker/.kube/k3s

# Check if kubectl is configured
echo "📋 Checking kubectl configuration..."
kubectl cluster-info

# Create host directories if they don't exist
echo "📁 Ensuring host directories exist..."
sudo mkdir -p /var/lib/lnr/db /var/lib/lnr/annex
sudo chmod 755 /var/lib/lnr/db /var/lib/lnr/annex

# Deploy with Skaffold
echo "🏗️  Building and deploying with Skaffold..."
case "${1:-prod}" in
    "dev")
        echo "📦 Deploying dev profile (no ingress)..."
        skaffold run -p dev
        ;;
    "prod")
        echo "📦 Deploying prod profile (with HTTPS ingress)..."
        skaffold run -p prod
        ;;
    "delete")
        echo "🗑️  Deleting deployment..."
        skaffold delete
        exit 0
        ;;
    *)
        echo "Usage: $0 [dev|prod|delete]"
        exit 1
        ;;
esac

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📊 Check status:"
echo "  kubectl get all -n lnr"
echo ""
echo "📝 View logs:"
echo "  kubectl logs -f deployment/gossip-pipeline -n lnr"
echo ""
echo "🌐 Access application:"
echo "  kubectl port-forward service/gossip-pipeline 8000:8000 -n lnr"
echo "  or visit: https://lnr.media.snyke.net"
echo ""