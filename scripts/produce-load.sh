#!/usr/bin/env bash
#
# produce-load.sh — produce random 32-character messages to a Vela topic.
#
# Drives the existing `vela-ctl produce` command in a loop, sending a fixed
# number of small random records (32 printable characters each).
#
# Why a per-record key by default:
#   `vela-ctl` is a fresh process per produce, and keyless routing uses a
#   *per-process* round-robin counter — so every keyless call would resolve to
#   partition 0 and pile all load onto one partition. Passing a key that varies
#   per record routes via the deterministic FNV-1a hash, spreading load across
#   all partitions. Use --key to pin every record to one partition, or --no-key
#   to force keyless (everything lands on partition 0).
#
# Usage:
#   scripts/produce-load.sh <topic> [options]
#
# Options:
#   -n, --count N           Number of messages to produce (default: 1000).
#   -e, --endpoints LIST    Comma-separated id=url endpoints
#                           (default: the local docker-compose 4-node cluster).
#   -k, --key KEY           Pin every record to one partition using this fixed key.
#       --no-key            Produce keyless (all records route to partition 0).
#   -c, --create N          Create <topic> with N partitions before producing.
#       --ctl PATH          Path to the vela-ctl binary (default: build & use release).
#   -h, --help              Show this help.
#
# Examples:
#   # 1000 random 32-char messages to an existing topic on the local cluster:
#   scripts/produce-load.sh orders
#
#   # 50k messages, creating the topic first:
#   scripts/produce-load.sh bench --count 50000 --create 8
#
#   # Target a single-node dev server:
#   scripts/produce-load.sh orders --endpoints node-a=http://127.0.0.1:7001

set -euo pipefail

# --- Defaults ---------------------------------------------------------------

RECORD_CHARS=32
COUNT=1000
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
        -n | --count)
            COUNT="${2:?--count needs a value}"
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

case "$COUNT" in *[!0-9]*) die "--count must be a positive integer";; esac
[ "$COUNT" -gt 0 ] || die "--count must be greater than 0"
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

# Generate a printable random value of exactly $RECORD_CHARS characters. base64
# of N raw bytes yields ceil(N/3)*4 base64 chars; we over-read slightly then
# trim to length.
random_value() {
    local raw=$(((RECORD_CHARS * 3) / 4 + 3))
    head -c "$raw" /dev/urandom | base64 | tr -d '\n' | head -c "$RECORD_CHARS"
}

echo "produce-load: producing $COUNT random ${RECORD_CHARS}-char messages to '$TOPIC'" >&2
echo "produce-load: endpoints=$ENDPOINTS key-mode=$KEY_MODE" >&2

count=0
start=$(date +%s)

while [ "$count" -lt "$COUNT" ]; do
    value="$(random_value)"

    case "$KEY_MODE" in
        rotate) set -- --endpoints "$ENDPOINTS" produce "$TOPIC" --key "rec-$count" --value "$value" ;;
        fixed)  set -- --endpoints "$ENDPOINTS" produce "$TOPIC" --key "$FIXED_KEY" --value "$value" ;;
        none)   set -- --endpoints "$ENDPOINTS" produce "$TOPIC" --value "$value" ;;
    esac

    if ! "$CTL" "$@" >/dev/null; then
        die "produce failed at record $count ($count/$COUNT messages sent)"
    fi

    count=$((count + 1))

    # Progress roughly every 100 messages.
    if [ $((count % 100)) -eq 0 ] || [ "$count" -ge "$COUNT" ]; then
        pct=$((count * 100 / COUNT))
        printf 'produce-load: %3d%%  %d/%d messages\n' "$pct" "$count" "$COUNT" >&2
    fi
done

elapsed=$(($(date +%s) - start))
[ "$elapsed" -gt 0 ] || elapsed=1
rate=$((count / elapsed))
echo "produce-load: done — $count messages over ${elapsed}s (~${rate} msg/s)" >&2
