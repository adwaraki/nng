#!/usr/bin/env bash
# ofi_benchmark.sh — OFI transport benchmark suite for NNG
#
# Tests all socket types × message sizes × topology (loopback / cross-node).
# Outputs CSV results for automated comparison.
#
# Usage:
#   ofi_benchmark.sh [OPTIONS]
#
# Options:
#   --build-dir DIR       Path to NNG build directory (default: auto-detect)
#   --mode loopback|crossnode|both
#                         Topology to test (default: loopback)
#   --server-ip IP        HSN IP for cross-node server (auto-detected if omitted)
#   --client-node NODE    SLURM nodename for cross-node client
#   --server-node NODE    SLURM nodename for cross-node server
#   --iterations N        Round-trip count for latency tests (default: 1000)
#   --thr-count N         Message count for throughput tests (default: 10000)
#   --csv FILE            Output CSV file (default: stdout)
#   --no-pair0            Disable pair0 tests
#   --no-pair1            Disable pair1 tests
#   --no-reqrep           Disable req/rep tests
#   --no-bus              Disable bus tests
#   --no-pubsub           Disable pub/sub tests
#   --no-pipeline         Disable push/pull tests
#   --no-survey           Disable survey tests (latency only)
#   --no-throughput       Disable throughput tests
#   --no-latency          Disable latency tests
#   --sizes "s1 s2 ..."   Override message sizes (default: "8 64 1024 65536 262144")
#   --timeout SEC         Per-test timeout in seconds (default: 60)
#   --provider PROV       Force FI_PROVIDER (default: auto-detect cxi, fallback tcp)
#   --help                Show this help
#
# Exit codes:
#   0 = all tests passed
#   1 = some tests failed (results still emitted)
#   2 = fatal error (bad arguments, missing binaries, etc.)

set -o pipefail
# Note: we intentionally do NOT use set -e (errexit) because individual test
# failures are expected and handled via exit code checks.  We do NOT use
# set -u (nounset) because empty bash arrays trigger "unbound variable" errors
# in bash < 4.4.

# --- Defaults ---
BUILD_DIR=""
MODE="loopback"
SERVER_IP=""
CLIENT_NODE=""
SERVER_NODE=""
ITERATIONS=1000
THR_COUNT=10000
CSV_FILE=""
TIMEOUT=60
PROVIDER=""
SIZES="8 64 1024 65536 262144"

# Socket type flags (all enabled by default)
DO_PAIR0=1
DO_PAIR1=1
DO_REQREP=1
DO_BUS=1
DO_PUBSUB=1
DO_PIPELINE=1
DO_SURVEY=1
DO_THROUGHPUT=1
DO_LATENCY=1

FAILURES=0
BASE_PORT=55600

# --- Helpers ---
usage() {
    sed -n '2,/^$/s/^# \?//p' "$0"
    exit 0
}

die() { echo "FATAL: $*" >&2; exit 2; }

next_port() {
    BASE_PORT=$((BASE_PORT + 1))
    echo "$BASE_PORT"
}

# --- Argument parsing ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --build-dir)    BUILD_DIR="$2"; shift 2 ;;
        --mode)         MODE="$2"; shift 2 ;;
        --server-ip)    SERVER_IP="$2"; shift 2 ;;
        --client-node)  CLIENT_NODE="$2"; shift 2 ;;
        --server-node)  SERVER_NODE="$2"; shift 2 ;;
        --iterations)   ITERATIONS="$2"; shift 2 ;;
        --thr-count)    THR_COUNT="$2"; shift 2 ;;
        --csv)          CSV_FILE="$2"; shift 2 ;;
        --timeout)      TIMEOUT="$2"; shift 2 ;;
        --provider)     PROVIDER="$2"; shift 2 ;;
        --sizes)        SIZES="$2"; shift 2 ;;
        --no-pair0)     DO_PAIR0=0; shift ;;
        --no-pair1)     DO_PAIR1=0; shift ;;
        --no-reqrep)    DO_REQREP=0; shift ;;
        --no-bus)       DO_BUS=0; shift ;;
        --no-pubsub)    DO_PUBSUB=0; shift ;;
        --no-pipeline)  DO_PIPELINE=0; shift ;;
        --no-survey)    DO_SURVEY=0; shift ;;
        --no-throughput) DO_THROUGHPUT=0; shift ;;
        --no-latency)   DO_LATENCY=0; shift ;;
        --help|-h)      usage ;;
        *)              die "Unknown option: $1" ;;
    esac
done

# --- Locate build directory ---
if [[ -z "$BUILD_DIR" ]]; then
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    # Try common locations relative to the script
    for candidate in \
        "$SCRIPT_DIR/../../../build" \
        "$SCRIPT_DIR/../../../../build" \
        "$(pwd)/build" \
        "$(pwd)"; do
        if [[ -x "$candidate/src/tools/perf/local_lat" ]]; then
            BUILD_DIR="$(cd "$candidate" && pwd)"
            break
        fi
    done
fi
[[ -x "$BUILD_DIR/src/tools/perf/local_lat" ]] || die "Cannot find perf binaries. Use --build-dir."

LOCAL_LAT="$BUILD_DIR/src/tools/perf/local_lat"
REMOTE_LAT="$BUILD_DIR/src/tools/perf/remote_lat"
LOCAL_THR="$BUILD_DIR/src/tools/perf/local_thr"
REMOTE_THR="$BUILD_DIR/src/tools/perf/remote_thr"

# --- Auto-detect provider ---
if [[ -z "$PROVIDER" ]]; then
    if FI_PROVIDER=cxi fi_info >/dev/null 2>&1; then
        PROVIDER="cxi"
    else
        PROVIDER="tcp"
    fi
fi
export FI_PROVIDER="$PROVIDER"

# Add Cray libfabric to path if present
if [[ -d /opt/cray/libfabric/1.22.0/lib64 ]]; then
    export LD_LIBRARY_PATH="/opt/cray/libfabric/1.22.0/lib64:${LD_LIBRARY_PATH:-}"
fi

# --- Auto-detect server IP ---
if [[ -z "$SERVER_IP" ]]; then
    # Try HSN first, then fall back to hostname IP
    SERVER_IP=$(ip addr show hsn0 2>/dev/null | grep 'inet ' | awk '{print $2}' | cut -d/ -f1 || true)
    if [[ -z "$SERVER_IP" ]]; then
        SERVER_IP="127.0.0.1"
    fi
fi

# --- CSV output setup ---
csv_out() {
    if [[ -n "$CSV_FILE" ]]; then
        echo "$*" >> "$CSV_FILE"
    fi
    echo "$*"
}

# Write CSV header
CSV_HEADER="test_type,socket_pattern,msg_size_bytes,topology,iterations,total_time_s,avg_latency_us,throughput_msg_s,throughput_mbps,status"
if [[ -n "$CSV_FILE" ]]; then
    echo "$CSV_HEADER" > "$CSV_FILE"
else
    echo "$CSV_HEADER"
fi

# --- Latency test runner ---
# Runs local_lat (server) + remote_lat (client) and parses output.
# Args: $1=pattern_flag $2=pattern_name $3=msg_size $4=iterations $5=topology $6=server_url
run_latency_test() {
    local pattern_flag="$1"
    local pattern_name="$2"
    local msg_size="$3"
    local iters="$4"
    local topology="$5"
    local port
    port=$(next_port)
    local url="ofi://${SERVER_IP}:${port}"

    local tmpdir
    tmpdir=$(mktemp -d)
    local server_out="$tmpdir/server.out"
    local client_out="$tmpdir/client.out"

    # Start server
    if [[ -n "$pattern_flag" ]]; then
        timeout "$TIMEOUT" "$LOCAL_LAT" "--${pattern_flag}" "$url" "$msg_size" "$iters" \
            > "$server_out" 2>&1 &
    else
        timeout "$TIMEOUT" "$LOCAL_LAT" "$url" "$msg_size" "$iters" \
            > "$server_out" 2>&1 &
    fi
    local server_pid=$!
    sleep 2

    # Start client
    if [[ -n "$pattern_flag" ]]; then
        timeout "$TIMEOUT" "$REMOTE_LAT" "--${pattern_flag}" "$url" "$msg_size" "$iters" \
            > "$client_out" 2>&1
    else
        timeout "$TIMEOUT" "$REMOTE_LAT" "$url" "$msg_size" "$iters" \
            > "$client_out" 2>&1
    fi
    local client_rc=$?

    wait "$server_pid" 2>/dev/null || true

    # Parse client output
    local total_time="N/A"
    local avg_latency="N/A"
    local status="PASS"

    if [[ $client_rc -ne 0 ]]; then
        status="FAIL"
        FAILURES=$((FAILURES + 1))
        echo "#   FAIL (rc=$client_rc): $(cat "$client_out" 2>/dev/null | head -3)" >&2
        echo "#   server: $(cat "$server_out" 2>/dev/null | head -3)" >&2
    else
        total_time=$(grep -oP 'total time: \K[0-9.]+' "$client_out" 2>/dev/null || echo "N/A")
        avg_latency=$(grep -oP 'average latency: \K[0-9.]+' "$client_out" 2>/dev/null || echo "N/A")
    fi

    csv_out "latency,$pattern_name,$msg_size,$topology,$iters,$total_time,$avg_latency,N/A,N/A,$status"

    rm -rf "$tmpdir"
}

# --- Throughput test runner ---
# Runs local_thr (server) + remote_thr (client) and parses output.
# Only supports pair1 (hard-coded in perf.c throughput_server/client).
# Args: $1=msg_size $2=count $3=topology
run_throughput_test() {
    local msg_size="$1"
    local count="$2"
    local topology="$3"
    local port
    port=$(next_port)
    local url="ofi://${SERVER_IP}:${port}"

    local tmpdir
    tmpdir=$(mktemp -d)
    local server_out="$tmpdir/server.out"
    local client_out="$tmpdir/client.out"

    # Start server (throughput receiver)
    timeout "$TIMEOUT" "$LOCAL_THR" "$url" "$msg_size" "$count" \
        > "$server_out" 2>&1 &
    local server_pid=$!
    sleep 2

    # Start client (throughput sender)
    timeout "$TIMEOUT" "$REMOTE_THR" "$url" "$msg_size" "$count" \
        > "$client_out" 2>&1
    local client_rc=$?

    wait "$server_pid" 2>/dev/null || true

    # Parse server output (it has the timing)
    local total_time="N/A"
    local msg_per_sec="N/A"
    local mbps="N/A"
    local status="PASS"

    if [[ $client_rc -ne 0 ]]; then
        status="FAIL"
        FAILURES=$((FAILURES + 1))
        echo "#   FAIL (rc=$client_rc): $(cat "$client_out" 2>/dev/null | head -3)" >&2
        echo "#   server: $(cat "$server_out" 2>/dev/null | head -3)" >&2
    else
        total_time=$(grep -oP 'total time: \K[0-9.]+' "$server_out" 2>/dev/null || echo "N/A")
        msg_per_sec=$(grep -oP 'throughput: \K[0-9.]+(?= \[msg/s\])' "$server_out" 2>/dev/null || echo "N/A")
        mbps=$(grep -oP 'throughput: \K[0-9.]+(?= \[Mb/s\])' "$server_out" 2>/dev/null || echo "N/A")
    fi

    csv_out "throughput,pair1,$msg_size,$topology,$count,$total_time,N/A,$msg_per_sec,$mbps,$status"

    rm -rf "$tmpdir"
}

# --- Pub/sub delivery test ---
# Uses local_lat server (listen) + remote_lat client (dial) with pubsub0 flag.
# The perf tool doesn't support pubsub natively, so we use a simple inline test.
# Args: $1=msg_size $2=count $3=topology
run_pubsub_test() {
    local msg_size="$1"
    local count="$2"
    local topology="$3"
    local port
    port=$(next_port)
    local url="ofi://${SERVER_IP}:${port}"

    # pub/sub is unidirectional; use --pubsub0 if perf supports it.
    # Current perf.c has the option but latency tests require bidirectional echo.
    # Record as "unsupported" in latency mode, but still attempt in case
    # the protocol implements it (pubsub doesn't support echo — skip gracefully).
    csv_out "latency,pubsub,$msg_size,$topology,$count,N/A,N/A,N/A,N/A,SKIP_UNIDIRECTIONAL"
}

# --- Push/pull delivery test ---
# Same constraint as pub/sub: unidirectional, so latency ping-pong won't work.
# Args: $1=msg_size $2=count $3=topology
run_pipeline_test() {
    local msg_size="$1"
    local count="$2"
    local topology="$3"
    local port
    port=$(next_port)
    local url="ofi://${SERVER_IP}:${port}"

    csv_out "latency,pipeline,$msg_size,$topology,$count,N/A,N/A,N/A,N/A,SKIP_UNIDIRECTIONAL"
}

# --- Cross-node wrappers ---
# For cross-node mode, we use srun to launch server on one node and client on another.
run_latency_crossnode() {
    local pattern_flag="$1"
    local pattern_name="$2"
    local msg_size="$3"
    local iters="$4"
    local port
    port=$(next_port)
    local url="ofi://${SERVER_IP}:${port}"

    local server_args=""
    local client_args=""
    if [[ -n "$pattern_flag" ]]; then
        server_args="--${pattern_flag}"
        client_args="--${pattern_flag}"
    fi

    local tmpdir
    tmpdir=$(mktemp -d)
    local server_out="$tmpdir/server.out"
    local client_out="$tmpdir/client.out"

    # Launch server on server node
    srun --nodes=1 --nodelist="$SERVER_NODE" --ntasks=1 \
        env FI_PROVIDER="$PROVIDER" LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}" \
        timeout "$TIMEOUT" "$LOCAL_LAT" $server_args "$url" "$msg_size" "$iters" \
        > "$server_out" 2>&1 &
    local server_pid=$!
    sleep 2

    # Launch client on client node
    srun --nodes=1 --nodelist="$CLIENT_NODE" --ntasks=1 \
        env FI_PROVIDER="$PROVIDER" LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}" \
        timeout "$TIMEOUT" "$REMOTE_LAT" $client_args "$url" "$msg_size" "$iters" \
        > "$client_out" 2>&1
    local client_rc=$?

    wait "$server_pid" 2>/dev/null || true

    local total_time="N/A"
    local avg_latency="N/A"
    local status="PASS"

    if [[ $client_rc -ne 0 ]]; then
        status="FAIL"
        FAILURES=$((FAILURES + 1))
    else
        total_time=$(grep -oP 'total time: \K[0-9.]+' "$client_out" 2>/dev/null || echo "N/A")
        avg_latency=$(grep -oP 'average latency: \K[0-9.]+' "$client_out" 2>/dev/null || echo "N/A")
    fi

    csv_out "latency,$pattern_name,$msg_size,crossnode,$iters,$total_time,$avg_latency,N/A,N/A,$status"

    rm -rf "$tmpdir"
}

run_throughput_crossnode() {
    local msg_size="$1"
    local count="$2"
    local port
    port=$(next_port)
    local url="ofi://${SERVER_IP}:${port}"

    local tmpdir
    tmpdir=$(mktemp -d)
    local server_out="$tmpdir/server.out"
    local client_out="$tmpdir/client.out"

    srun --nodes=1 --nodelist="$SERVER_NODE" --ntasks=1 \
        env FI_PROVIDER="$PROVIDER" LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}" \
        timeout "$TIMEOUT" "$LOCAL_THR" "$url" "$msg_size" "$count" \
        > "$server_out" 2>&1 &
    local server_pid=$!
    sleep 2

    srun --nodes=1 --nodelist="$CLIENT_NODE" --ntasks=1 \
        env FI_PROVIDER="$PROVIDER" LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}" \
        timeout "$TIMEOUT" "$REMOTE_THR" "$url" "$msg_size" "$count" \
        > "$client_out" 2>&1
    local client_rc=$?

    wait "$server_pid" 2>/dev/null || true

    local total_time="N/A"
    local msg_per_sec="N/A"
    local mbps="N/A"
    local status="PASS"

    if [[ $client_rc -ne 0 ]]; then
        status="FAIL"
        FAILURES=$((FAILURES + 1))
    else
        total_time=$(grep -oP 'total time: \K[0-9.]+' "$server_out" 2>/dev/null || echo "N/A")
        msg_per_sec=$(grep -oP 'throughput: \K[0-9.]+(?= \[msg/s\])' "$server_out" 2>/dev/null || echo "N/A")
        mbps=$(grep -oP 'throughput: \K[0-9.]+(?= \[Mb/s\])' "$server_out" 2>/dev/null || echo "N/A")
    fi

    csv_out "throughput,pair1,$msg_size,crossnode,$count,$total_time,N/A,$msg_per_sec,$mbps,$status"

    rm -rf "$tmpdir"
}

# --- Main test matrix ---
echo "# OFI Benchmark Suite" >&2
echo "# Provider: $PROVIDER" >&2
echo "# Server IP: $SERVER_IP" >&2
echo "# Mode: $MODE" >&2
echo "# Sizes: $SIZES" >&2
echo "# Latency iterations: $ITERATIONS" >&2
echo "# Throughput count: $THR_COUNT" >&2
echo "#" >&2

# Build pattern list: (flag, name)
# The flag is passed as --flag to the perf tools; empty string = default (pair1).
declare -a LATENCY_PATTERNS=()
if [[ $DO_PAIR1 -eq 1 ]]; then LATENCY_PATTERNS+=(",pair1"); fi
if [[ $DO_PAIR0 -eq 1 ]]; then LATENCY_PATTERNS+=("pair0,pair0"); fi
if [[ $DO_REQREP -eq 1 ]]; then LATENCY_PATTERNS+=("reqrep0,reqrep"); fi
if [[ $DO_BUS -eq 1 ]]; then LATENCY_PATTERNS+=("bus0,bus"); fi

run_topology() {
    local topology="$1"

    # --- Latency tests (bidirectional patterns) ---
    if [[ $DO_LATENCY -eq 1 ]]; then
        for entry in "${LATENCY_PATTERNS[@]}"; do
            IFS=',' read -r flag name <<< "$entry"
            for sz in $SIZES; do
                echo "# [latency] $name ${sz}B $topology ..." >&2
                if [[ "$topology" == "crossnode" ]]; then
                    run_latency_crossnode "$flag" "$name" "$sz" "$ITERATIONS"
                else
                    run_latency_test "$flag" "$name" "$sz" "$ITERATIONS" "$topology"
                fi
            done
        done

        # Pub/sub — unidirectional, latency ping-pong not applicable
        if [[ $DO_PUBSUB -eq 1 ]]; then
            for sz in $SIZES; do
                echo "# [latency] pubsub ${sz}B $topology (skip: unidirectional) ..." >&2
                run_pubsub_test "$sz" "$ITERATIONS" "$topology"
            done
        fi

        # Push/pull — unidirectional, latency ping-pong not applicable
        if [[ $DO_PIPELINE -eq 1 ]]; then
            for sz in $SIZES; do
                echo "# [latency] pipeline ${sz}B $topology (skip: unidirectional) ..." >&2
                run_pipeline_test "$sz" "$ITERATIONS" "$topology"
            done
        fi

        # Survey — bidirectional via survey/response protocol.
        # perf.c doesn't support --survey0, so we mark it for the unit test only.
        if [[ $DO_SURVEY -eq 1 ]]; then
            for sz in $SIZES; do
                echo "# [latency] survey ${sz}B $topology (skip: no perf support) ..." >&2
                csv_out "latency,survey,$sz,$topology,$ITERATIONS,N/A,N/A,N/A,N/A,SKIP_NO_PERF_SUPPORT"
            done
        fi
    fi

    # --- Throughput tests (pair1 only — perf tool limitation) ---
    if [[ $DO_THROUGHPUT -eq 1 && $DO_PAIR1 -eq 1 ]]; then
        for sz in $SIZES; do
            echo "# [throughput] pair1 ${sz}B $topology ..." >&2
            if [[ "$topology" == "crossnode" ]]; then
                run_throughput_crossnode "$sz" "$THR_COUNT"
            else
                run_throughput_test "$sz" "$THR_COUNT" "$topology"
            fi
        done
    fi
}

# --- Execute ---
case "$MODE" in
    loopback)
        run_topology "loopback"
        ;;
    crossnode)
        [[ -n "$SERVER_NODE" ]] || die "--server-node required for crossnode mode"
        [[ -n "$CLIENT_NODE" ]] || die "--client-node required for crossnode mode"
        run_topology "crossnode"
        ;;
    both)
        run_topology "loopback"
        if [[ -n "$SERVER_NODE" && -n "$CLIENT_NODE" ]]; then
            run_topology "crossnode"
        else
            echo "# Skipping crossnode: --server-node and --client-node not set" >&2
        fi
        ;;
    *)
        die "Unknown mode: $MODE (use loopback, crossnode, or both)"
        ;;
esac

echo "#" >&2
echo "# Benchmark complete. Failures: $FAILURES" >&2

if [[ -n "$CSV_FILE" ]]; then
    echo "# Results written to: $CSV_FILE" >&2
fi

if [[ $FAILURES -gt 0 ]]; then
    exit 1
fi
exit 0
