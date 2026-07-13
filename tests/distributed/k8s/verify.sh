#!/bin/sh
set -eu

cluster="lattice-${LATTICE_RUN_ID:-local}"
node_image="kindest/node:v1.35.0@sha256:4613778f3cfcd10e615029370f5786704559103cf27bef934597ba562b269661"
probe_image="lattice-k8s-probe:${LATTICE_RUN_ID:-local}"
cleanup() {
  status=$?
  cleanup_failed=0
  if [ "$status" -ne 0 ] && [ -s /tmp/lattice-kind-kubeconfig ]; then
    kubectl get all,endpointslices,roles,rolebindings,poddisruptionbudgets -A -o yaml \
      > /artifacts/k8s-state.yaml 2>&1 || true
    kubectl describe pods -l app=lattice-probe \
      > /artifacts/k8s-pods.txt 2>&1 || true
    kubectl logs -l app=lattice-probe --all-containers --prefix \
      > /artifacts/k8s-pods.log 2>&1 || true
  fi
  kind delete cluster --name "$cluster" >/dev/null 2>&1 || cleanup_failed=1
  LATTICE_CURRENT_IMAGE_TAGS="$probe_image" scripts/docker-image-lifecycle.sh cleanup || cleanup_failed=1
  trap - EXIT INT TERM
  [ "$cleanup_failed" -eq 0 ] || exit 1
  exit "$status"
}
trap cleanup EXIT INT TERM

LATTICE_CURRENT_IMAGE_TAGS="$probe_image" scripts/docker-image-lifecycle.sh preflight
docker build --label org.realm-labs.lattice.test=true -f tests/distributed/Dockerfile.k8s-probe -t "$probe_image" .
kind create cluster --name "$cluster" --image "$node_image" --wait 120s
kind load docker-image --name "$cluster" "$probe_image"
kind get kubeconfig --internal --name "$cluster" > /tmp/lattice-kind-kubeconfig
export KUBECONFIG=/tmp/lattice-kind-kubeconfig
kubectl apply -f tests/distributed/k8s/workload.yaml
kubectl set image deployment/lattice-probe "probe=$probe_image"
kubectl rollout status deployment/lattice-probe --timeout=120s
kubectl wait --for=jsonpath='{.status.readyReplicas}'=2 deployment/lattice-probe --timeout=120s
kubectl wait --for=jsonpath='{.status.availableReplicas}'=2 deployment/lattice-probe --timeout=120s
kubectl auth can-i list endpointslices.discovery.k8s.io \
  --as=system:serviceaccount:default:lattice-probe | grep -qx yes
kubectl auth can-i watch endpointslices.discovery.k8s.io \
  --as=system:serviceaccount:default:lattice-probe | grep -qx yes
kubectl auth can-i get endpointslices.discovery.k8s.io \
  --as=system:serviceaccount:default:lattice-probe | grep -qx no

kubectl run dns-check \
  --image=busybox:1.37.0@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028 \
  --restart=Never --rm -i -- wget -qO- http://lattice-probe
kubectl run endpoint-slice-check \
  --image=busybox:1.37.0@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028 \
  --restart=Never --rm -i -- sh -c \
  'body=$(wget -qO- http://lattice-probe/discovery); echo "$body"; echo "$body" | grep -q "\"targets\":\[\"[^\"]*:8080\",\"[^\"]*:8080"'

kubectl patch deployment lattice-probe -p \
  '{"spec":{"template":{"metadata":{"annotations":{"io.lattice.rollout":"verified"}}}}}'
kubectl rollout status deployment/lattice-probe --timeout=120s
kubectl wait --for=jsonpath='{.status.readyReplicas}'=2 deployment/lattice-probe --timeout=120s
kubectl run endpoint-slice-rollout-check \
  --image=busybox:1.37.0@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028 \
  --restart=Never --rm -i -- wget -qO- http://lattice-probe/discovery

pod=$(kubectl get pod -l app=lattice-probe -o jsonpath='{.items[0].metadata.name}')
kubectl create --raw "/api/v1/namespaces/default/pods/$pod/eviction" -f - <<EOF
{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"$pod","namespace":"default"}}
EOF
kubectl rollout status deployment/lattice-probe --timeout=120s
kubectl wait --for=jsonpath='{.status.readyReplicas}'=2 deployment/lattice-probe --timeout=120s
kubectl wait --for=jsonpath='{.status.availableReplicas}'=2 deployment/lattice-probe --timeout=120s
