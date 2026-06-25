#!/usr/bin/env bash
#
# _compose.sh — shared helper: pick a working container runtime.
#
# Some setups alias `docker` to `podman` only in interactive shells, so a plain
# `docker compose` inside a script can hit a non-running Docker daemon. This
# detects a working runtime and exposes a `compose` function.
#
# Source this file; do not execute it directly.

# Resolve the compose command once.
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  _COMPOSE=(docker compose)
elif command -v podman >/dev/null 2>&1; then
  _COMPOSE=(podman compose)
elif command -v docker >/dev/null 2>&1; then
  # Fall back to docker even if `docker info` failed; surfaces a clear error.
  _COMPOSE=(docker compose)
else
  echo "ERROR: neither docker nor podman found in PATH." >&2
  exit 3
fi

compose() { "${_COMPOSE[@]}" "$@"; }
