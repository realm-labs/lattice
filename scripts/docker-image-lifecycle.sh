#!/bin/sh
set -eu

label="org.realm-labs.lattice.test=true"
mode=${1:-cleanup}
current_tags=${LATTICE_CURRENT_IMAGE_TAGS:-}
retention_hours=${LATTICE_TEST_IMAGE_RETENTION_HOURS:-}

if [ -z "$retention_hours" ]; then
  if [ "${CI:-}" = "true" ]; then
    retention_hours=72
  else
    retention_hours=168
  fi
fi
case "$retention_hours" in
  ''|*[!0-9]*) echo "invalid lattice image retention hours: $retention_hours" >&2; exit 2 ;;
esac
case "$mode" in
  preflight|cleanup) ;;
  *) echo "usage: $0 <preflight|cleanup>" >&2; exit 2 ;;
esac

docker_root=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || true)
disk_path=$docker_root
[ -n "$disk_path" ] && [ -e "$disk_path" ] || disk_path=/

disk_percent() {
  df -P "$disk_path" | awk 'NR == 2 { value = $(NF - 1); gsub(/%/, "", value); print value }'
}

is_current_tag() {
  candidate=$1
  for tag in $current_tags; do
    [ "$candidate" = "$tag" ] && return 0
  done
  return 1
}

is_current_image() {
  candidate_id=$1
  for tag in $current_tags; do
    current_id=$(docker image ls -q "$tag" 2>/dev/null | head -n 1)
    [ -n "$current_id" ] && [ "$candidate_id" = "$current_id" ] && return 0
  done
  return 1
}

remove_candidate() {
  image_id=$1
  tag=$2
  is_current_tag "$tag" && return 1
  is_current_image "$image_id" && return 1
  if docker ps -q --filter "ancestor=$image_id" | grep -q .; then
    return 1
  fi
  docker image rm "$image_id" >/dev/null 2>&1 || return 1
}

cleanup_expired() {
  now=$(date -u +%s)
  cutoff=$((now - retention_hours * 3600))
  docker image ls --filter "label=$label" --format '{{.ID}} {{.Repository}}:{{.Tag}}' |
    sort -u | while read -r image_id tag; do
      [ -n "$image_id" ] || continue
      created=$(docker image inspect --format '{{.Created}}' "$image_id" 2>/dev/null || true)
      [ -n "$created" ] || continue
      created_epoch=$(date -u -d "$created" +%s 2>/dev/null || echo "$now")
      [ "$created_epoch" -lt "$cutoff" ] || continue
      remove_candidate "$image_id" "$tag" || true
    done
}

cleanup_to_watermark() {
  usage=$(disk_percent)
  [ -n "$usage" ] || return 0
  [ "$usage" -ge 80 ] || return 0
  candidates=$(mktemp)
  trap 'rm -f "$candidates"' EXIT INT TERM
  docker image ls --filter "label=$label" --format '{{.ID}} {{.Repository}}:{{.Tag}}' |
    sort -u | while read -r image_id tag; do
      created=$(docker image inspect --format '{{.Created}}' "$image_id" 2>/dev/null || true)
      [ -n "$created" ] && printf '%s\t%s\t%s\n' "$created" "$image_id" "$tag"
    done | sort >"$candidates"
  while IFS="$(printf '\t')" read -r _created image_id tag; do
    usage=$(disk_percent)
    [ "$usage" -ge 80 ] || break
    remove_candidate "$image_id" "$tag" || true
  done <"$candidates"
  rm -f "$candidates"
  trap - EXIT INT TERM
}

cleanup_expired
cleanup_to_watermark

usage=$(disk_percent)
if [ -n "$usage" ] && [ "$usage" -ge 90 ]; then
  echo "Docker storage is ${usage}% full after scoped lattice test-image cleanup" >&2
  echo "inspect lattice build caches with scripts/docker-test-cache.sh status and, if needed, remove them with scripts/docker-test-cache.sh clean; otherwise free space or move Docker's data root before retrying" >&2
  exit 1
fi
