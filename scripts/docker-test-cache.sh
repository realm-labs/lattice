#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cache_abi=${LATTICE_DOCKER_CACHE_ABI:-runner-v1-rust-1.97.0-bookworm}
cache_root=${LATTICE_DOCKER_CACHE_ROOT:-}
label="org.realm-labs.lattice.test-cache=true"

case "$cache_abi" in
  ''|*[!A-Za-z0-9_.-]*) echo "invalid Docker test cache ABI: $cache_abi" >&2; exit 2 ;;
esac

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

case "${1:-}" in
  ensure)
    ensure_volume "$home_volume" "$home_path"
    ensure_volume "$target_volume" "$target_path"
    printf '%s:%s\n' "$home_volume" "$target_volume"
    ;;
  status)
    printf 'cache-abi=%s\tarchitecture=%s\tbackend=%s\n' "$cache_abi" "$docker_arch" "$backend"
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
  *)
    echo "usage: $0 <ensure|status|clean>" >&2
    exit 2
    ;;
esac
