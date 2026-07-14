#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="$root/tests/distributed/compose.yaml"
profile=${1:-}
[ -n "$profile" ] || { echo "usage: $0 <quality|sim|model|e2e|e2e-ha-etcd|chaos|k8s|soak|replay>" >&2; exit 2; }
shift

run_id=${LATTICE_RUN_ID:-"$(date -u +%Y%m%dt%H%M%Sz)-$$"}
project="lattice-${run_id}"
export LATTICE_RUN_ID=$run_id
export LATTICE_TEST_SEED=${LATTICE_TEST_SEED:-1}
export LATTICE_DOCKER_NETWORK="${project}_testnet"
artifacts="$root/target/test-artifacts/$run_id"
mkdir -p "$artifacts"
runner_image="lattice-test-runner:$run_id"
probe_image="lattice-k8s-probe:$run_id"
export LATTICE_RUNNER_IMAGE=$runner_image
export LATTICE_CURRENT_IMAGE_TAGS="$runner_image $probe_image"

"$root/scripts/docker-image-lifecycle.sh" preflight

cleanup() {
  status=$?
  docker compose -f "$compose" -p "$project" --profile "$profile" logs --no-color >"$artifacts/containers.log" 2>&1 || true
  docker compose -f "$compose" -p "$project" --profile "$profile" down --volumes --remove-orphans >/dev/null 2>&1 || cleanup_failed=1
  leaked_containers=$(docker ps -aq --filter "label=io.lattice.test-run=$run_id")
  leaked_networks=$(docker network ls -q --filter "label=io.lattice.test-run=$run_id")
  leaked_volumes=$(docker volume ls -q --filter "label=io.lattice.test-run=$run_id")
  [ -z "$leaked_containers" ] || docker rm -f $leaked_containers >/dev/null 2>&1 || cleanup_failed=1
  [ -z "$leaked_networks" ] || docker network rm $leaked_networks >/dev/null 2>&1 || cleanup_failed=1
  [ -z "$leaked_volumes" ] || docker volume rm $leaked_volumes >/dev/null 2>&1 || cleanup_failed=1
  leaked_containers=$(docker ps -aq --filter "label=io.lattice.test-run=$run_id")
  leaked_networks=$(docker network ls -q --filter "label=io.lattice.test-run=$run_id")
  leaked_volumes=$(docker volume ls -q --filter "label=io.lattice.test-run=$run_id")
  if [ -n "$leaked_containers$leaked_networks$leaked_volumes" ]; then
    echo "Docker cleanup leaked labeled resources for project $project" >&2
    cleanup_failed=1
  fi
  "$root/scripts/docker-image-lifecycle.sh" cleanup || cleanup_failed=1
  if [ "${cleanup_failed:-0}" -ne 0 ]; then
    echo "Docker cleanup failed for project $project" >&2
    exit 1
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

case "$profile" in
  quality|sim|model|e2e|e2e-ha-etcd|chaos|k8s)
    service="runner-$profile"
    ;;
  soak)
    service=runner-soak
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --duration)
          duration=$2
          case "$duration" in
            *h) value=${duration%h}; multiplier=3600 ;;
            *m) value=${duration%m}; multiplier=60 ;;
            *s) value=${duration%s}; multiplier=1 ;;
            *) value=$duration; multiplier=1 ;;
          esac
          case "$value" in ''|*[!0-9]*) echo "invalid soak duration: $duration" >&2; exit 2 ;; esac
          LATTICE_SOAK_SECONDS=$((value * multiplier)); export LATTICE_SOAK_SECONDS; shift 2
          ;;
        --seed) LATTICE_TEST_SEED=$2; export LATTICE_TEST_SEED; shift 2 ;;
        *) echo "unknown soak option: $1" >&2; exit 2 ;;
      esac
    done
    ;;
  replay)
    service=runner-replay
    [ "${1:-}" = "--artifact" ] && [ -n "${2:-}" ] || { echo "replay requires --artifact <trace.json>" >&2; exit 2; }
    case "$2" in
      "$artifacts"/*) LATTICE_REPLAY_ARTIFACT="../artifacts/${2#"$artifacts"/}" ;;
      *) cp "$2" "$artifacts/replay.json"; LATTICE_REPLAY_ARTIFACT=../artifacts/replay.json ;;
    esac
    export LATTICE_REPLAY_ARTIFACT
    ;;
  *) echo "unknown profile: $profile" >&2; exit 2 ;;
esac

docker compose -f "$compose" -p "$project" --profile "$profile" up \
  --build --abort-on-container-exit --exit-code-from "$service" "$service"
