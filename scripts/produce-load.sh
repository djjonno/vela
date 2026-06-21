#!/usr/bin/env bash
#
# produce-load.sh — produce a target volume of random data to a Vela topic.
#
# Drives the existing `vela-ctl produce` command in a loop, sending randomly
# generated records until a total payload size is reached (default 100 MiB).
#
# Why a per-record key by default:
#   `vela-ctl` is a fresh process per produce, and keyless routing uses a
#   *per-process* round-robin counter — so every keyless call would resolve to
#   partition 0 and pile all load onto one partition. Passing a key that varies
#   per record routes via the deterministic FNV-1a hash, spreading load across
#   all partitions. Use --key to pin every record to one partition, or --no-key
#   to force keyless (everything lands on partition 0).
#
# Record sizing:
#   A record's key+value must not exceed 1 MiB (the cluster rejects larger ones),
#   and a value is passed as a CLI argument, so it must also stay under the OS
#   argument-length limit. The 100 KiB default is comfortably under both.
#
# Usage:
#   scripts/produce-load.sh <topic> [options]
#
# Options:
#   -t, --total BYTES       Total payload to produce (default: 104857600 = 100 MiB).
#   -r, --record BYTES      Per-record value size (default: 102400 = 100 KiB; max 1048576).
#   -e, --endpoints LIST    Comma-separated id=url endpoints
#                           (default: the local docker-compose 4-node cluster).
#   -k, --key KEY           Pin every record to one partition using this fixed key.
#       --no-key            Produce keyless (all records route to partition 0).
#   -c, --create N          Create <topic> with N partitions before producing.
#       --ctl PATH          Path to the vela-ctl binary (default: build & use release).
#   -h, --help              Show this help.
#
# Examples:
#   # 100 MiB to an existing 8-partition topic on the local cluster:
#   scripts/produce-load.sh orders
#
#   # 1 GiB in 512 KiB records, creating the topic first:
#   scripts/produce-load.sh bench --total $((1024*1024*1024)) --record $((512*1024)) --create 8
#
#   # Target a single-node dev server:
#   scripts/produce-load.sh orders --endpoints node-a=http://127.0.0.1:7001

set -euo pipefail

# --- Defaults ---------------------------------------------------------------

MIB=$((1024 * 1024))
TOTAL_BYTES=$((100 * MIB))
RECORD_BYTES=$((100 * 1024))
MAX_RECORD_BYTES=$((1024 * 1024)) # 1 MiB cluster limit (key + value).
DEFAULT_ENDPOINTS="node1=http://127.0.0.1:7001,node2=http://127.0.0.1:7002,node3=http://127.0.0.1:7003,node4=http://127.0.0.1:7004"
ENDPOINTS="$DEFAULT_ENDPOINTS"
TOPIC=""
FIXED_KEY=""       # set by --key
KEY_MODE="rotate"  # rotate (default) | fixed | none
CREATE_PARTITIONS=""
CTL=""

usage() {
    sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//; /^set -euo/d'
}

die() {
    echo "produce-load: $*" >&2
    exit 1
}

# --- Argument parsing -------------------------------------------------------

while [ $# -gt 0 ]; do
    case "$1" in
        -t | --total)
            TOTAL_BYTES="${2:?--total needs a value}"
            shift 2
            ;;
        -r | --record)
            RECORD_BYTES="${2:?--record needs a value}"
            shift 2
            ;;
        -e | --endpoints)
            ENDPOINTS="${2:?--endpoints needs a value}"
            shift 2
            ;;
        -k | --key)
            FIXED_KEY="${2:?--key needs a value}"
            KEY_MODE="fixed"
            shift 2
            ;;
        --no-key)
            KEY_MODE="none"
            shift
            ;;
        -c | --create)
            CREATE_PARTITIONS="${2:?--create needs a partition count}"
            shift 2
            ;;
        --ctl)
            CTL="${2:?--ctl needs a path}"
            shift 2
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        -*)
            die "unknown option: $1 (try --help)"
            ;;
        *)
            [ -z "$TOPIC" ] || die "unexpected extra argument: $1"
            TOPIC="$1"
            shift
            ;;
    esac
done

[ -n "$TOPIC" ] || die "a topic name is required (try --help)"

case "$TOTAL_BYTES" in *[!0-9]*) die "--total must be a positive integer (bytes)";; esac
case "$RECORD_BYTES" in *[!0-9]*) die "--record must be a positive integer (bytes)";; esac
[ "$TOTAL_BYTES" -gt 0 ] || die "--total must be greater than 0"
[ "$RECORD_BYTES" -gt 0 ] || die "--record must be greater than 0"
[ "$RECORD_BYTES" -le "$MAX_RECORD_BYTES" ] ||
    die "--record ($RECORD_BYTES) exceeds the 1 MiB per-record limit ($MAX_RECORD_BYTES)"
if [ -n "$CREATE_PARTITIONS" ]; then
    case "$CREATE_PARTITIONS" in *[!0-9]*) die "--create must be a positive integer";; esac
    [ "$CREATE_PARTITIONS" -gt 0 ] || die "--create must be greater than 0"
fi

# --- Locate / build vela-ctl ------------------------------------------------

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -z "$CTL" ]; then
    echo "produce-load: building vela-ctl (release)..." >&2
    cargo build --release -p vela-ctl --manifest-path "$REPO_ROOT/Cargo.toml" >&2
    CTL="$REPO_ROOT/target/release/vela-ctl"
fi
[ -x "$CTL" ] || die "vela-ctl not found or not executable at: $CTL"

# --- Optionally create the topic --------------------------------------------

if [ -n "$CREATE_PARTITIONS" ]; then
    echo "produce-load: creating topic '$TOPIC' with $CREATE_PARTITIONS partition(s)..." >&2
    "$CTL" --endpoints "$ENDPOINTS" create "$TOPIC" --partitions "$CREATE_PARTITIONS" >&2
fi

# --- Produce loop -----------------------------------------------------------

# Generate a printable random value of exactly $1 bytes. base64 of N raw bytes
# yields ceil(N/3)*4 base64 chars; we over-read slightly then trim to length.
random_value() {
    local len="$1"
    local raw=$(((len * 3) / 4 + 3))
    head -c "$raw" /dev/urandom | base64 | tr -d '\n' | head -c "$len"
}

echo "produce-load: producing $TOTAL_BYTES bytes to '$TOPIC' in ${RECORD_BYTES}-byte records" >&2
echo "produce-load: endpoints=$ENDPOINTS key-mode=$KEY_MODE" >&2

produced=0
count=0
start=$(date +%s)

while [ "$produced" -lt "$TOTAL_BYTES" ]; do
    remaining=$((TOTAL_BYTES - produced))
    this=$RECORD_BYTES
    [ "$this" -le "$remaining" ] || this=$remaining

    value="$(random_value "$this")"

    case "$KEY_MODE" in
        rotate) set -- --endpoints "$ENDPOINTS" produce "$TOPIC" --key "rec-$count" --value "$value" ;;
        fixed)  set -- --endpoints "$ENDPOINTS" produce "$TOPIC" --key "$FIXED_KEY" --value "$value" ;;
        none)   set -- --endpoints "$ENDPOINTS" produce "$TOPIC" --value "$value" ;;
    esac

    if ! "$CTL" "$@" >/dev/null; then
        die "produce failed at record $count ($produced/$TOTAL_BYTES bytes sent)"
    fi

    produced=$((produced + this))
    count=$((count + 1))

    # Progress roughly every ~10 MiB.
    if [ $((count % 40)) -eq 0 ] || [ "$produced" -ge "$TOTAL_BYTES" ]; then
        pct=$((produced * 100 / TOTAL_BYTES))
        printf 'produce-load: %3d%%  %d/%d bytes  %d records\n' \
            "$pct" "$produced" "$TOTAL_BYTES" "$count" >&2
    fi
done

elapsed=$(($(date +%s) - start))
[ "$elapsed" -gt 0 ] || elapsed=1
rate=$((produced / elapsed))
echo "produce-load: done — $produced bytes in $count records over ${elapsed}s (~${rate} B/s)" >&2
