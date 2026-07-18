#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cache_abi=${LATTICE_DOCKER_CACHE_ABI:-runner-v1-rust-1.97.0-bookworm}
cache_root=${LATTICE_DOCKER_CACHE_ROOT:-}
label="org.realm-labs.lattice.test-cache=true"
maintenance_hours=${LATTICE_DOCKER_CACHE_MAINTENANCE_HOURS:-168}
target_max_mib=${LATTICE_DOCKER_TARGET_MAX_MIB:-12288}
cargo_home_max_mib=${LATTICE_DOCKER_CARGO_HOME_MAX_MIB:-4096}

case "$cache_abi" in
  ''|*[!A-Za-z0-9_.-]*) echo "invalid Docker test cache ABI: $cache_abi" >&2; exit 2 ;;
esac
for setting in "$maintenance_hours" "$target_max_mib" "$cargo_home_max_mib"; do
  case "$setting" in
    ''|*[!0-9]*) echo "Docker cache maintenance settings must be non-negative integers" >&2; exit 2 ;;
  esac
done

docker_arch=$(docker info --format '{{.Architecture}}' 2>/dev/null || true)
[ -n "$docker_arch" ] || { echo "Docker Engine is unavailable" >&2; exit 1; }
case "$docker_arch" in
  *[!A-Za-z0-9_.-]*) echo "invalid Docker architecture: $docker_arch" >&2; exit 2 ;;
esac

home_path=
target_path=
backend=volume
suffix=
if [ -n "$cache_root" ]; then
  case "$cache_root" in
    /*) ;;
    *) cache_root=$root/$cache_root ;;
  esac
  case "$cache_root" in
    "$root/target"|"$root/target"/*) ;;
    *)
      echo "LATTICE_DOCKER_CACHE_ROOT must be under $root/target: $cache_root" >&2
      exit 2
      ;;
  esac
  mkdir -p "$cache_root/cargo-home" "$cache_root/cargo-target"
  cache_root=$(CDPATH= cd -- "$cache_root" && pwd)
  case "$cache_root" in
    "$root/target"|"$root/target"/*) ;;
    *)
      echo "LATTICE_DOCKER_CACHE_ROOT must resolve under $root/target: $cache_root" >&2
      exit 2
      ;;
  esac
  home_path=$cache_root/cargo-home
  target_path=$cache_root/cargo-target
  cache_root_id=$(printf '%s' "$cache_root" | cksum | awk '{ print $1 }')
  backend=bind
  suffix=-bind-$cache_root_id
fi

home_volume="lattice-cargo-home-${cache_abi}-${docker_arch}${suffix}"
target_volume="lattice-cargo-target-${cache_abi}-${docker_arch}${suffix}"

volume_device() {
  docker volume inspect --format '{{ index .Options "device" }}' "$1" 2>/dev/null || true
}

ensure_volume() {
  volume=$1
  path=$2
  if docker volume inspect "$volume" >/dev/null 2>&1; then
    actual_label=$(docker volume inspect --format '{{ index .Labels "org.realm-labs.lattice.test-cache" }}' "$volume" 2>/dev/null || true)
    [ "$actual_label" = true ] || {
      echo "refusing to reuse unlabeled Docker volume $volume" >&2
      exit 1
    }
    actual_abi=$(docker volume inspect --format '{{ index .Labels "org.realm-labs.lattice.cache-abi" }}' "$volume" 2>/dev/null || true)
    [ "$actual_abi" = "$cache_abi" ] || {
      echo "Docker volume $volume has cache ABI $actual_abi, expected $cache_abi" >&2
      exit 1
    }
    if [ -n "$path" ]; then
      actual_device=$(volume_device "$volume")
      [ "$actual_device" = "$path" ] || {
        echo "Docker volume $volume is bound to $actual_device, expected $path" >&2
        exit 1
      }
    fi
    return
  fi

  if [ -n "$path" ]; then
    docker volume create \
      --driver local \
      --opt type=none \
      --opt o=bind \
      --opt "device=$path" \
      --label "$label" \
      --label "org.realm-labs.lattice.cache-abi=$cache_abi" \
      "$volume" >/dev/null
  else
    docker volume create \
      --label "$label" \
      --label "org.realm-labs.lattice.cache-abi=$cache_abi" \
      "$volume" >/dev/null
  fi
}

volume_in_use() {
  docker ps -aq --filter "volume=$1"
}

volume_size() {
  volume=$1
  path=$2
  if [ -n "$path" ] && [ -d "$path" ]; then
    measured=$(du -sh "$path" 2>/dev/null | awk '{ print $1 }')
    if [ -n "$measured" ]; then
      printf '%s\n' "$measured"
      return
    fi
  fi
  mountpoint=$(docker volume inspect --format '{{ .Mountpoint }}' "$volume" 2>/dev/null || true)
  if [ -n "$mountpoint" ] && [ -d "$mountpoint" ]; then
    measured=$(du -sh "$mountpoint" 2>/dev/null | awk '{ print $1 }')
    if [ -n "$measured" ]; then
      printf '%s\n' "$measured"
      return
    fi
  fi
  printf '%s\n' unavailable
}

show_volume() {
  kind=$1
  volume=$2
  path=$3
  if ! docker volume inspect "$volume" >/dev/null 2>&1; then
    printf '%s\tname=%s\tstate=missing\n' "$kind" "$volume"
    return
  fi
  created=$(docker volume inspect --format '{{ .CreatedAt }}' "$volume")
  users=$(volume_in_use "$volume")
  if [ -n "$users" ]; then
    state=in-use
  else
    state=idle
  fi
  size=$(volume_size "$volume" "$path")
  printf '%s\tname=%s\tstate=%s\tsize=%s\tcreated=%s\n' "$kind" "$volume" "$state" "$size" "$created"
}

clear_bind_path() {
  path=$1
  [ -n "$path" ] || return
  if ! find "$path" -mindepth 1 -delete 2>/dev/null; then
    echo "removed the Docker volume, but could not completely clear bind cache directory $path" >&2
    echo "remove that exact directory manually before reusing it" >&2
    return 1
  fi
}

maintain_cache() {
  helper_image=${LATTICE_DOCKER_CACHE_HELPER_IMAGE:-}
  if [ -z "$helper_image" ] || ! docker image inspect "$helper_image" >/dev/null 2>&1; then
    echo "skipping Docker Cargo cache maintenance: helper image is unavailable" >&2
    return 0
  fi
  for volume in "$home_volume" "$target_volume"; do
    users=$(volume_in_use "$volume")
    if [ -n "$users" ]; then
      echo "refusing to maintain in-use Docker cache volume $volume: $users" >&2
      return 1
    fi
  done
  docker run --rm \
    -e "MAINTENANCE_HOURS=$maintenance_hours" \
    -e "TARGET_MAX_MIB=$target_max_mib" \
    -e "CARGO_HOME_MAX_MIB=$cargo_home_max_mib" \
    -v "$home_volume:/cache/cargo-home" \
    -v "$target_volume:/cache/cargo-target" \
    "$helper_image" \
    sh -eu -c '
      marker=/cache/cargo-home/.lattice-maintained-at
      now=$(date -u +%s)
      if [ ! -f "$marker" ]; then
        printf "%s\n" "$now" >"$marker"
        echo "initialized Docker Cargo cache maintenance window"
        exit 0
      fi
      last=$(sed -n "1p" "$marker" 2>/dev/null || true)
      case "$last" in ""|*[!0-9]*) last=0 ;; esac
      interval=$((MAINTENANCE_HOURS * 3600))
      if [ "$interval" -gt 0 ] && [ $((now - last)) -lt "$interval" ]; then
        exit 0
      fi

      set -- $(du -sk /cache/cargo-target)
      target_kib=$1
      set -- $(du -sk /cache/cargo-home)
      home_kib=$1
      target_limit_kib=$((TARGET_MAX_MIB * 1024))
      home_limit_kib=$((CARGO_HOME_MAX_MIB * 1024))

      if [ "$TARGET_MAX_MIB" -gt 0 ] && [ "$target_kib" -gt "$target_limit_kib" ]; then
        find /cache/cargo-target -mindepth 1 -delete
        echo "reset Docker cargo-target cache: ${target_kib} KiB exceeded ${target_limit_kib} KiB"
      else
        find /cache/cargo-target -type d -name incremental -prune -exec find {} -mindepth 1 -delete \;
        find /cache/cargo-target -type d -name incremental -empty -delete
        echo "completed periodic Docker cargo-target incremental cleanup"
      fi

      if [ "$CARGO_HOME_MAX_MIB" -gt 0 ] && [ "$home_kib" -gt "$home_limit_kib" ]; then
        find /cache/cargo-home -mindepth 1 -delete
        echo "reset Docker CARGO_HOME cache: ${home_kib} KiB exceeded ${home_limit_kib} KiB"
      fi
      printf "%s\n" "$now" >"$marker"
    '
}

case "${1:-}" in
  ensure)
    ensure_volume "$home_volume" "$home_path"
    ensure_volume "$target_volume" "$target_path"
    printf '%s:%s\n' "$home_volume" "$target_volume"
    ;;
  status)
    printf 'cache-abi=%s\tarchitecture=%s\tbackend=%s\tmaintenance-hours=%s\ttarget-max-mib=%s\tcargo-home-max-mib=%s\n' \
      "$cache_abi" "$docker_arch" "$backend" "$maintenance_hours" "$target_max_mib" "$cargo_home_max_mib"
    show_volume cargo-home "$home_volume" "$home_path"
    show_volume cargo-target "$target_volume" "$target_path"
    ;;
  clean)
    blocked=0
    for volume in "$home_volume" "$target_volume"; do
      users=$(volume_in_use "$volume")
      if [ -n "$users" ]; then
        echo "refusing to remove in-use Docker cache volume $volume: $users" >&2
        blocked=1
      fi
    done
    [ "$blocked" -eq 0 ] || exit 1
    for volume in "$home_volume" "$target_volume"; do
      if docker volume inspect "$volume" >/dev/null 2>&1; then
        docker volume rm "$volume" >/dev/null
        echo "removed $volume"
      fi
    done
    clear_bind_path "$home_path"
    clear_bind_path "$target_path"
    ;;
  maintain)
    ensure_volume "$home_volume" "$home_path"
    ensure_volume "$target_volume" "$target_path"
    maintain_cache
    ;;
  *)
    echo "usage: $0 <ensure|status|maintain|clean>" >&2
    exit 2
    ;;
esac
