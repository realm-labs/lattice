#!/bin/sh
set -eu

cluster="lattice-${LATTICE_RUN_ID:-local}"
node_image="kindest/node:v1.35.0@sha256:4613778f3cfcd10e615029370f5786704559103cf27bef934597ba562b269661"
probe_image="lattice-k8s-probe:${LATTICE_RUN_ID:-local}"
busybox_source="busybox:1.37.0@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028"
busybox_image="lattice-k8s-busybox:${LATTICE_RUN_ID:-local}"
artifacts_dir="${LATTICE_ARTIFACTS_DIR:-/artifacts}"
run_bounded() {
  "$@" &
  command_pid=$!
  (
    sleep 10
    kill "$command_pid" >/dev/null 2>&1 || true
  ) &
  watchdog_pid=$!
  result=0
  wait "$command_pid" || result=$?
  kill "$watchdog_pid" >/dev/null 2>&1 || true
  wait "$watchdog_pid" 2>/dev/null || true
  return "$result"
}
run_pod_check() {
  check_name=$1
  shift
  kubectl delete pod "$check_name" --ignore-not-found --wait=false >/dev/null
  kubectl run "$check_name" \
    --image="$busybox_image" \
    --image-pull-policy=IfNotPresent \
    --restart=Never --command -- "$@"
  if ! kubectl wait \
    --for=jsonpath='{.status.phase}'=Succeeded \
    "pod/$check_name" \
    --timeout=60s; then
    kubectl get "pod/$check_name" -o wide || true
    kubectl logs "$check_name" || true
    return 1
  fi
  kubectl logs "$check_name"
  kubectl delete pod "$check_name" --wait=false >/dev/null
}
cleanup() {
  status=$?
  cleanup_failed=0
  if [ "$status" -ne 0 ] && [ -s /tmp/lattice-kind-kubeconfig ]; then
    mkdir -p "$artifacts_dir"
    run_bounded kubectl get all,endpointslices,roles,rolebindings,poddisruptionbudgets -A -o yaml \
      > "$artifacts_dir/k8s-state.yaml" 2>&1 || true
    run_bounded kubectl describe pods -l app=lattice-probe \
      > "$artifacts_dir/k8s-pods.txt" 2>&1 || true
    run_bounded kubectl logs -l app=lattice-probe --all-containers --prefix \
      > "$artifacts_dir/k8s-pods.log" 2>&1 || true
  fi
  kind delete cluster --name "$cluster" >/dev/null 2>&1 || cleanup_failed=1
  LATTICE_CURRENT_IMAGE_TAGS="$probe_image" scripts/docker-image-lifecycle.sh cleanup || cleanup_failed=1
  docker image rm "$busybox_image" >/dev/null 2>&1 || true
  trap - EXIT INT TERM
  [ "$cleanup_failed" -eq 0 ] || exit 1
  exit "$status"
}
trap cleanup EXIT INT TERM

LATTICE_CURRENT_IMAGE_TAGS="$probe_image" scripts/docker-image-lifecycle.sh preflight
docker build --label org.realm-labs.lattice.test=true -f tests/distributed/Dockerfile.k8s-probe -t "$probe_image" .
kind create cluster --name "$cluster" --image "$node_image" --wait 120s
if ! docker image inspect "$busybox_source" >/dev/null 2>&1; then
  attempt=0
  until docker pull "$busybox_source"; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 3 ] || exit 1
    sleep 2
  done
fi
docker tag "$busybox_source" "$busybox_image"
kind load docker-image --name "$cluster" "$probe_image"
kind load docker-image --name "$cluster" "$busybox_image"
if [ "${LATTICE_KIND_INTERNAL_KUBECONFIG:-true}" = "true" ]; then
  kind get kubeconfig --internal --name "$cluster" > /tmp/lattice-kind-kubeconfig
else
  kind get kubeconfig --name "$cluster" > /tmp/lattice-kind-kubeconfig
fi
export KUBECONFIG=/tmp/lattice-kind-kubeconfig
attempt=0
until kubectl --request-timeout=5s get --raw /openapi/v2 >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  [ "$attempt" -lt 30 ] || exit 1
  sleep 1
done
kubectl apply -f tests/distributed/k8s/workload.yaml
kubectl set image deployment/lattice-probe "probe=$probe_image"
kubectl get deployment/lattice-probe \
  -o jsonpath='{.spec.strategy.rollingUpdate.maxUnavailable}' | grep -qx 0
kubectl rollout status deployment/lattice-probe --timeout=120s
kubectl wait --for=jsonpath='{.status.readyReplicas}'=2 deployment/lattice-probe --timeout=120s
kubectl wait --for=jsonpath='{.status.availableReplicas}'=2 deployment/lattice-probe --timeout=120s
kubectl auth can-i list endpointslices.discovery.k8s.io \
  --as=system:serviceaccount:default:lattice-probe | grep -qx yes
kubectl auth can-i watch endpointslices.discovery.k8s.io \
  --as=system:serviceaccount:default:lattice-probe | grep -qx yes
kubectl auth can-i get endpointslices.discovery.k8s.io \
  --as=system:serviceaccount:default:lattice-probe | grep -qx no

run_pod_check dns-check wget -qO- http://lattice-probe
run_pod_check endpoint-slice-check sh -c \
  'body=$(wget -qO- http://lattice-probe/discovery); echo "$body"; echo "$body" | grep -q "\"targets\":\[\"[^\"]*:8080\",\"[^\"]*:8080"'

kubectl patch deployment lattice-probe -p \
  '{"spec":{"template":{"metadata":{"annotations":{"io.lattice.rollout":"verified"}}}}}'
kubectl rollout status deployment/lattice-probe --timeout=120s
kubectl wait --for=jsonpath='{.status.readyReplicas}'=2 deployment/lattice-probe --timeout=120s
run_pod_check endpoint-slice-rollout-check wget -qO- http://lattice-probe/discovery

pod=$(kubectl get pod -l app=lattice-probe -o jsonpath='{.items[0].metadata.name}')
kubectl create --raw "/api/v1/namespaces/default/pods/$pod/eviction" -f - <<EOF
{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"$pod","namespace":"default"}}
EOF
kubectl rollout status deployment/lattice-probe --timeout=120s
kubectl wait --for=jsonpath='{.status.readyReplicas}'=2 deployment/lattice-probe --timeout=120s
kubectl wait --for=jsonpath='{.status.availableReplicas}'=2 deployment/lattice-probe --timeout=120s
