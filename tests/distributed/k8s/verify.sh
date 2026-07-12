#!/bin/sh
set -eu

cluster="lattice-${LATTICE_RUN_ID:-local}"
node_image="kindest/node:v1.35.0@sha256:4613778f3cfcd10e615029370f5786704559103cf27bef934597ba562b269661"
probe_image="lattice-k8s-probe:${LATTICE_RUN_ID:-local}"
cleanup() {
  kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  docker image rm -f "$probe_image" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker build -f tests/distributed/Dockerfile.k8s-probe -t "$probe_image" .
kind create cluster --name "$cluster" --image "$node_image" --wait 120s
kind load docker-image --name "$cluster" "$probe_image"
kind get kubeconfig --internal --name "$cluster" > /tmp/lattice-kind-kubeconfig
export KUBECONFIG=/tmp/lattice-kind-kubeconfig
kubectl apply -f tests/distributed/k8s/workload.yaml
kubectl set image deployment/lattice-probe "probe=$probe_image"
kubectl rollout status deployment/lattice-probe --timeout=120s
kubectl wait --for=jsonpath='{.status.readyReplicas}'=2 deployment/lattice-probe --timeout=120s
kubectl wait --for=jsonpath='{.status.availableReplicas}'=2 deployment/lattice-probe --timeout=120s

kubectl run dns-check \
  --image=busybox:1.37.0@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028 \
  --restart=Never --rm -i -- wget -qO- http://lattice-probe

kubectl patch deployment lattice-probe -p \
  '{"spec":{"template":{"metadata":{"annotations":{"io.lattice.rollout":"verified"}}}}}'
kubectl rollout status deployment/lattice-probe --timeout=120s

pod=$(kubectl get pod -l app=lattice-probe -o jsonpath='{.items[0].metadata.name}')
kubectl create --raw "/api/v1/namespaces/default/pods/$pod/eviction" -f - <<EOF
{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"$pod","namespace":"default"}}
EOF
kubectl rollout status deployment/lattice-probe --timeout=120s
kubectl wait --for=jsonpath='{.status.readyReplicas}'=2 deployment/lattice-probe --timeout=120s
kubectl wait --for=jsonpath='{.status.availableReplicas}'=2 deployment/lattice-probe --timeout=120s
