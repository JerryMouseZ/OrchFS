#!/usr/bin/env bash
# Copy this file outside the checkout, edit every host-specific value, then:
#   source /path/to/orchfs-async.conf
# Sourcing this template never binds or formats a device.

# LibFS <-> KFS coroutine RPC endpoint and worker topology.
export ORCHFS_ASYNC_ENDPOINT="/tmp/orchfs-kfs.sock"
export ORCHFS_CLIENT_WORKERS="4"
export ORCHFS_KFS_WORKERS="16"
# Fixed executor threads that isolate the synchronous legacy core from KFS
# coroutine workers. Empty/unset defaults to ORCHFS_KFS_WORKERS.
export ORCHFS_KFS_BLOCKING_WORKERS="16"
export ORCHFS_IPC_RING_CAPACITY="64"
export ORCHFS_IPC_DATA_SLOT_SIZE="1048576"

# KFS-only SPDK controller selection. PCI BDF must include the domain. Leave
# the placeholder in place until nvme-cli/sysfs confirms the BDF-to-NSID map.
export ORCHFS_SPDK_PCI_BDF="0000:BB:DD.F"
export ORCHFS_SPDK_NSID="1"
export ORCHFS_SPDK_POLLER_COUNT="4"
export ORCHFS_SPDK_QUEUE_DEPTH="128"
export ORCHFS_SPDK_BOUNCE_BUFFERS="64"
export ORCHFS_SPDK_MAX_TRANSFER_SIZE="1048576"
# Empty selects the first CPUs in the process affinity mask. Set an explicit
# comma-separated NUMA-local list (for example, "16,17,18,19") in production.
export ORCHFS_SPDK_CPU_LIST=""

# SPDK/DPDK environment. HUGEMEM is consumed by SPDK's setup.sh; the backend
# consumes HUGEPAGE_DIR and SHM_ID when KFS starts.
export ORCHFS_SPDK_HUGEMEM_MB="4096"
export ORCHFS_SPDK_HUGEPAGE_DIR="/dev/hugepages"
export ORCHFS_SPDK_SHM_ID="-1"
# Empty derives the SPDK environment mask from ORCHFS_SPDK_CPU_LIST.
export ORCHFS_SPDK_REACTOR_MASK=""
