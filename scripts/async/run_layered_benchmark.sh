#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "${script_dir}/../.." && pwd)
build_dir=${ORCHFS_LAYERED_BENCHMARK_BUILD_DIR:-${repo_root}/build-kfs}
spdk_perf=${ORCHFS_SPDK_PERF_BIN:-/opt/orchfs/spdk/build/bin/spdk_nvme_perf}
runs=${ORCHFS_LAYERED_BENCHMARK_RUNS:-5}
block_size=65536
coroutines=${ORCHFS_ASYNC_BENCHMARK_COROUTINES:-64}
operations=${ORCHFS_LAYERED_BENCHMARK_OPERATIONS:-16384}
server_cpus=${ORCHFS_ASYNC_BENCHMARK_SERVER_CPUS:-1,3,5,7}
client_cpus=${ORCHFS_ASYNC_BENCHMARK_CLIENT_CPUS:-9,11,13,15}
device_offset=${ORCHFS_LAYERED_BENCHMARK_DEVICE_OFFSET:-2199023255552}
endpoint=${ORCHFS_ASYNC_ENDPOINT:-/tmp/orchfs-layered-benchmark-$$.sock}
bdf=${ORCHFS_SPDK_PCI_BDF:-}
nsid=${ORCHFS_SPDK_NSID:-}

die() {
  printf 'layered benchmark: %s\n' "$*" >&2
  exit 2
}

[[ ${ORCHFS_LAYERED_BENCHMARK_DESTRUCTIVE:-} == ERASE_AND_REFORMAT ]] ||
  die 'set ORCHFS_LAYERED_BENCHMARK_DESTRUCTIVE=ERASE_AND_REFORMAT; raw writes and mkfs destroy the selected namespace'
[[ -n ${bdf} && -n ${nsid} ]] ||
  die 'ORCHFS_SPDK_PCI_BDF and ORCHFS_SPDK_NSID are required'
[[ ${runs} == 5 ]] || die 'the acceptance protocol requires exactly five runs'
[[ ${coroutines} =~ ^[0-9]+$ && ${coroutines} -gt 0 ]] ||
  die 'ORCHFS_ASYNC_BENCHMARK_COROUTINES must be a positive integer'
[[ ${operations} =~ ^[0-9]+$ && ${operations} -gt 0 ]] ||
  die 'ORCHFS_LAYERED_BENCHMARK_OPERATIONS must be a positive integer'
[[ -x ${spdk_perf} ]] || die "SPDK perf binary is missing: ${spdk_perf}"
cache_file=${build_dir}/CMakeCache.txt
[[ -f ${cache_file} ]] || die "CMake cache is missing: ${cache_file}"
build_type=$(sed -n 's/^CMAKE_BUILD_TYPE:STRING=//p' "${cache_file}")
[[ ${build_type} == RelWithDebInfo ]] ||
  die "benchmark build must use CMAKE_BUILD_TYPE=RelWithDebInfo, found '${build_type:-unset}'"
for binary in mkfs orchfs_layered_benchmark kfs_main async_client_server_test; do
  [[ -x ${build_dir}/${binary} ]] ||
    die "benchmark binary is missing: ${build_dir}/${binary}"
done
[[ -e /dev/dax0.1 ]] || die '/dev/dax0.1 is required by the current KFS format'

device_path=/sys/bus/pci/devices/${bdf}
[[ -d ${device_path} ]] || die "PCI device does not exist: ${bdf}"
driver=$(basename -- "$(readlink -- "${device_path}/driver" 2>/dev/null || true)")
case ${driver} in
  vfio-pci|uio_pci_generic) ;;
  *) die "${bdf} is bound to '${driver:-no driver}', not an SPDK userspace driver" ;;
esac

hugepages=$(awk '/HugePages_Total:/ {print $2}' /proc/meminfo)
[[ ${hugepages:-0} -gt 0 ]] || die 'no reserved hugepages are available'

IFS=',' read -r -a server_cpu_array <<< "${server_cpus}"
IFS=',' read -r -a client_cpu_array <<< "${client_cpus}"
worker_count=${#server_cpu_array[@]}
[[ ${worker_count} -eq 4 && ${#client_cpu_array[@]} -eq 4 ]] ||
  die 'the comparable benchmark requires four server and four client CPUs'
((coroutines % worker_count == 0)) ||
  die 'coroutine count must be divisible by the worker count'
((operations % coroutines == 0)) ||
  die 'operation count must be divisible by the coroutine count'

core_mask=0
for cpu in "${server_cpu_array[@]}"; do
  [[ ${cpu} =~ ^[0-9]+$ && ${cpu} -lt 63 ]] ||
    die "cannot encode server CPU '${cpu}' in the SPDK core mask"
  core_mask=$((core_mask | (1 << cpu)))
done
printf -v core_mask_hex '0x%x' "${core_mask}"
raw_qd=$((coroutines / worker_count))
raw_ios=$((operations / worker_count))
client_operations=$((operations / worker_count))

timestamp=$(date +%Y%m%d-%H%M%S)
result_dir=${ORCHFS_LAYERED_BENCHMARK_RESULT_DIR:-${repo_root}/benchmark-results/layered-${timestamp}}
[[ ! -e ${result_dir} ]] || die "result directory already exists: ${result_dir}"
mkdir -p -- "${result_dir}"
sample_file=${result_dir}/samples.tsv
output_file=${result_dir}/benchmark.log
server_log=${result_dir}/kfs-server.log
: > "${sample_file}"
: > "${output_file}"

server_pid=
cleanup() {
  if [[ -n ${server_pid} ]] && kill -0 "${server_pid}" 2>/dev/null; then
    kill -TERM "${server_pid}" 2>/dev/null || true
    wait "${server_pid}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

record_sample() {
  local layer=$1 phase=$2 run=$3 mib=$4 iops=$5 p99=$6 line=$7
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${layer}" "${phase}" "${run}" "${mib}" "${iops}" "${p99}" >> "${sample_file}"
  printf '%s\n' "${line}" | tee -a "${output_file}"
}

run_raw() {
  local workload=$1 phase=$2 run=$3 output metrics mib iops p99
  if ! output=$(taskset -c "${server_cpus}" "${spdk_perf}" \
    -q "${raw_qd}" -o "${block_size}" -w "${workload}" \
    -t 3600 -d "${raw_ios}" -c "${core_mask_hex}" \
    --use-every-core -s 1024 -L \
    -r "trtype:PCIe traddr:${bdf} ns:${nsid}" 2>&1); then
    printf '%s\n' "${output}" | tee -a "${output_file}" >&2
    die "raw SPDK ${workload} run ${run} failed"
  fi
  printf '%s\n' "${output}" >> "${output_file}"
  metrics=$(printf '%s\n' "${output}" | awk '
    /^Total/ {iops=$3; mib=$4}
    /^[[:space:]]*99\.00000%/ {p99=$3}
    END {if (iops == "" || mib == "") exit 1; print mib, iops, p99+0}') ||
    die "cannot parse raw SPDK ${workload} run ${run}"
  read -r mib iops p99 <<< "${metrics}"
  record_sample raw_spdk "${phase}" "${run}" "${mib}" "${iops}" "${p99}" \
    "orchfs_layered_benchmark layer=raw_spdk phase=${phase} block_size=${block_size} qd=${coroutines} coroutines=0 bytes=$((operations * block_size)) MiB_per_s=${mib} IOPS=${iops} p99_us=${p99} run=${run}"
}

printf 'layered benchmark results: %s\n' "${result_dir}" | tee -a "${output_file}"
printf 'target=%s nsid=%s server_cpus=%s client_cpus=%s qd=%s coroutines=%s operations=%s\n' \
  "${bdf}" "${nsid}" "${server_cpus}" "${client_cpus}" \
  "${coroutines}" "${coroutines}" "${operations}" | tee -a "${output_file}"

for workload in write read; do
  phase=${workload}
  for run in 1 2 3 4 5; do
    run_raw "${workload}" "${phase}" "${run}"
  done
done

# Raw write intentionally invalidates the namespace. Recreate the OrchFS
# format before either direct KFS or RPC measurements touch it.
first_cpu_mask=$((1 << server_cpu_array[0]))
printf -v first_cpu_mask_hex '0x%x' "${first_cpu_mask}"
env \
  "ORCHFS_SPDK_PCI_BDF=${bdf}" \
  "ORCHFS_SPDK_NSID=${nsid}" \
  "ORCHFS_SPDK_CPU_LIST=${server_cpu_array[0]}" \
  "ORCHFS_SPDK_POLLER_COUNT=1" \
  "ORCHFS_SPDK_REACTOR_MASK=${first_cpu_mask_hex}" \
  "ORCHFS_SPDK_HUGEPAGE_DIR=${ORCHFS_SPDK_HUGEPAGE_DIR:-/dev/hugepages}" \
  "ORCHFS_SPDK_SHM_ID=${ORCHFS_SPDK_SHM_ID:--1}" \
  taskset -c "${server_cpu_array[0]}" "${build_dir}/mkfs" 2>&1 |
  tee -a "${output_file}"

common_server_env=(
  "ORCHFS_KFS_WORKERS=${worker_count}"
  "ORCHFS_IPC_HUGEPAGES=1"
  "ORCHFS_IPC_RING_CAPACITY=64"
  "ORCHFS_IPC_DATA_SLOT_SIZE=$((block_size + 128))"
  "ORCHFS_SPDK_PCI_BDF=${bdf}"
  "ORCHFS_SPDK_NSID=${nsid}"
  "ORCHFS_SPDK_QUEUE_DEPTH=${ORCHFS_SPDK_QUEUE_DEPTH:-32}"
  "ORCHFS_SPDK_BOUNCE_BUFFERS=${ORCHFS_SPDK_BOUNCE_BUFFERS:-32}"
  "ORCHFS_SPDK_MAX_TRANSFER_SIZE=${ORCHFS_SPDK_MAX_TRANSFER_SIZE:-1048576}"
  "ORCHFS_SPDK_HUGEPAGE_DIR=${ORCHFS_SPDK_HUGEPAGE_DIR:-/dev/hugepages}"
  "ORCHFS_SPDK_SHM_ID=${ORCHFS_SPDK_SHM_ID:--1}"
  "ORCHFS_ASYNC_BENCHMARK_COROUTINES=${coroutines}"
  "ORCHFS_LAYERED_BENCHMARK_OPERATIONS=${operations}"
  "ORCHFS_LAYERED_BENCHMARK_DEVICE_OFFSET=${device_offset}"
)

for run in 1 2 3 4 5; do
  direct_output=$(env "${common_server_env[@]}" \
    taskset -c "${server_cpus}" "${build_dir}/orchfs_layered_benchmark" 2>&1)
  printf '%s\n' "${direct_output}" >> "${output_file}"
  while IFS= read -r line; do
    [[ ${line} == orchfs_layered_benchmark\ layer=* ]] || continue
    layer=$(sed -n 's/.* layer=\([^ ]*\).*/\1/p' <<< "${line}")
    phase=$(sed -n 's/.* phase=\([^ ]*\).*/\1/p' <<< "${line}")
    mib=$(sed -n 's/.* MiB_per_s=\([^ ]*\).*/\1/p' <<< "${line}")
    iops=$(sed -n 's/.* IOPS=\([^ ]*\).*/\1/p' <<< "${line}")
    p99=$(sed -n 's/.* p99_us=\([^ ]*\).*/\1/p' <<< "${line}")
    record_sample "${layer}" "${phase}" "${run}" "${mib}" "${iops}" "${p99}" "${line} run=${run}"
  done <<< "${direct_output}"
done

[[ ! -e ${endpoint} && ! -S ${endpoint} && ! -L ${endpoint} ]] ||
  die "refusing to replace an existing endpoint: ${endpoint}"
env "${common_server_env[@]}" "ORCHFS_ASYNC_ENDPOINT=${endpoint}" \
  taskset -c "${server_cpus}" "${build_dir}/kfs_main" > "${server_log}" 2>&1 &
server_pid=$!
for _ in $(seq 1 200); do
  [[ -S ${endpoint} ]] && break
  kill -0 "${server_pid}" 2>/dev/null ||
    die "KFS exited before creating ${endpoint}; inspect ${server_log}"
  sleep 0.05
done
[[ -S ${endpoint} ]] || die "timed out waiting for ${endpoint}"

for run in 1 2 3 4 5; do
  e2e_output=$(env \
    "ORCHFS_ASYNC_BENCHMARK_CLIENT_CPUS=${client_cpus}" \
    "ORCHFS_ASYNC_BENCHMARK_COROUTINES=${coroutines}" \
    "ORCHFS_ASYNC_BENCHMARK_OPERATIONS=${client_operations}" \
    "ORCHFS_ASYNC_BENCHMARK_SPIN=${ORCHFS_ASYNC_BENCHMARK_SPIN:-256}" \
    "ORCHFS_ASYNC_BENCHMARK_PHASE=both" \
    taskset -c "${client_cpus}" "${build_dir}/async_client_server_test" \
      --benchmark-client "${endpoint}" "${run}" 2>&1)
  printf '%s\n' "${e2e_output}" >> "${output_file}"
  while IFS= read -r line; do
    [[ ${line} == orchfs_layered_benchmark\ layer=client_shm_rpc_kfs_e2e* ]] || continue
    phase=$(sed -n 's/.* phase=\([^ ]*\).*/\1/p' <<< "${line}")
    mib=$(sed -n 's/.* MiB_per_s=\([^ ]*\).*/\1/p' <<< "${line}")
    iops=$(sed -n 's/.* IOPS=\([^ ]*\).*/\1/p' <<< "${line}")
    p99=$(sed -n 's/.* p99_us=\([^ ]*\).*/\1/p' <<< "${line}")
    record_sample client_shm_rpc_kfs_e2e "${phase}" "${run}" \
      "${mib}" "${iops}" "${p99}" "${line} run=${run}"
  done <<< "${e2e_output}"
done

kill -TERM "${server_pid}"
wait "${server_pid}"
server_pid=

median_value() {
  local layer=$1 phase=$2 column=$3
  awk -F '\t' -v layer="${layer}" -v phase="${phase}" -v column="${column}" \
    '$1 == layer && $2 == phase {print $column}' "${sample_file}" |
    sort -n | awk 'NR == 3 {print}'
}

summary_file=${result_dir}/median.tsv
printf 'layer\tphase\tMiB_per_s\tIOPS\tp99_us\n' > "${summary_file}"
for key in \
  'raw_spdk write' 'raw_spdk read' \
  'async_block_device_direct write+sync' 'async_block_device_direct read' \
  'kfs_coroutine_core_direct write+sync' 'kfs_coroutine_core_direct read' \
  'client_shm_rpc_kfs_e2e write+sync' 'client_shm_rpc_kfs_e2e read'; do
  read -r layer phase <<< "${key}"
  mib=$(median_value "${layer}" "${phase}" 4)
  iops=$(median_value "${layer}" "${phase}" 5)
  p99=$(median_value "${layer}" "${phase}" 6)
  [[ -n ${mib} && -n ${iops} && -n ${p99} ]] ||
    die "missing one of five samples for ${layer} ${phase}"
  printf '%s\t%s\t%s\t%s\t%s\n' \
    "${layer}" "${phase}" "${mib}" "${iops}" "${p99}" |
    tee -a "${summary_file}" "${output_file}"
done

printf 'median summary: %s\n' "${summary_file}" | tee -a "${output_file}"
