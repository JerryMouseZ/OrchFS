cmake_minimum_required(VERSION 3.16)

project(OrchFS VERSION 0.3 LANGUAGES C CXX)

include(CTest)
find_package(Threads REQUIRED)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(ORCHFS_ENABLE_SPDK
       "Build the KFS NVMe backend with SPDK support" OFF)
option(ORCHFS_BUILD_KFS
       "Build the production Runtime-owned KFS server" OFF)
option(ORCHFS_REPRO_TRACE
       "Compile lightweight source tracing for paper-workload profiling" OFF)
if(ORCHFS_BUILD_KFS AND NOT ORCHFS_ENABLE_SPDK)
  set(ORCHFS_ENABLE_SPDK ON CACHE BOOL
      "Production KFS requires the SPDK backend" FORCE)
  message(STATUS "ORCHFS_BUILD_KFS=ON: enabling ORCHFS_ENABLE_SPDK")
endif()
set(SPDK_ROOT "/opt/orchfs/spdk" CACHE PATH
    "Fixed SPDK source/build prefix (the supported baseline is SPDK v26.01)")

set(ORCHFS_PUBLIC_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

add_library(orchfs_repro_trace STATIC Async/repro_trace.cpp)
target_compile_features(orchfs_repro_trace PUBLIC cxx_std_20)
target_include_directories(orchfs_repro_trace PUBLIC
  "${ORCHFS_PUBLIC_INCLUDE_DIR}")
if(ORCHFS_REPRO_TRACE)
  target_compile_definitions(orchfs_repro_trace PUBLIC
    ORCHFS_REPRO_TRACE_ENABLED=1)
endif()

function(orchfs_enable_project_options target)
  target_compile_options(
    ${target}
    PRIVATE
      -O3
      -mclwb
      -mclflushopt
      -mavx512f
      -fcommon
      -Wall
      $<$<COMPILE_LANGUAGE:C>:-Wno-error=implicit-function-declaration>
      $<$<COMPILE_LANGUAGE:C>:-Wno-error=incompatible-pointer-types>
      $<$<COMPILE_LANGUAGE:C>:-Wno-error=int-conversion>)
endfunction()

# C++20 coroutine runtime and ownership/range scheduling primitives.
add_library(orchfs_async_runtime STATIC
  Async/runtime.cpp
  Async/range_arbiter.cpp)
add_library(orchfs::async_runtime ALIAS orchfs_async_runtime)
target_compile_features(orchfs_async_runtime PUBLIC cxx_std_20)
target_include_directories(orchfs_async_runtime PUBLIC
  "${ORCHFS_PUBLIC_INCLUDE_DIR}")
target_link_libraries(orchfs_async_runtime PUBLIC Threads::Threads)

# Process-local control plane plus shared-memory SQ/CQ transport.
add_library(orchfs_async_ipc STATIC Async/ipc_transport.cpp)
add_library(orchfs::async_ipc ALIAS orchfs_async_ipc)
target_compile_features(orchfs_async_ipc PUBLIC cxx_std_20)
target_include_directories(orchfs_async_ipc PUBLIC
  "${ORCHFS_PUBLIC_INCLUDE_DIR}")
target_link_libraries(orchfs_async_ipc PUBLIC Threads::Threads)

# LibFS-side asynchronous proxy API.
add_library(orchfs_async_client STATIC Async/client.cpp)
add_library(orchfs::async_client ALIAS orchfs_async_client)
target_compile_features(orchfs_async_client PUBLIC cxx_std_20)
target_include_directories(orchfs_async_client PUBLIC
  "${ORCHFS_PUBLIC_INCLUDE_DIR}")
target_link_libraries(orchfs_async_client
  PUBLIC orchfs_async_runtime orchfs_async_ipc orchfs_repro_trace
         Threads::Threads)

# KFS-side authoritative dispatcher. The injected AsyncFilesystem implementation
# is resolved by the final KFS executable, not by this transport-only library.
add_library(orchfs_async_server STATIC Async/server.cpp)
add_library(orchfs::async_server ALIAS orchfs_async_server)
target_compile_features(orchfs_async_server PUBLIC cxx_std_20)
target_include_directories(orchfs_async_server PUBLIC
  "${ORCHFS_PUBLIC_INCLUDE_DIR}"
  "${CMAKE_CURRENT_SOURCE_DIR}/LibFS"
  "${CMAKE_CURRENT_SOURCE_DIR}/KernelFS")
target_link_libraries(orchfs_async_server
  PUBLIC orchfs_async_runtime orchfs_async_ipc orchfs_repro_trace
         Threads::Threads)

add_library(orchfs_async_device STATIC Async/block_device.cpp)
target_compile_features(orchfs_async_device PUBLIC cxx_std_20)
target_include_directories(orchfs_async_device PUBLIC
  "${ORCHFS_PUBLIC_INCLUDE_DIR}")
target_include_directories(orchfs_async_device PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/KernelFS")
target_link_libraries(orchfs_async_device PUBLIC
  orchfs_async_runtime orchfs_repro_trace)
target_link_libraries(orchfs_async_server PUBLIC orchfs_async_device)

add_library(orchfs_kfs_coroutine_core STATIC
  Async/kfs_coroutine_core.cpp)
target_compile_features(orchfs_kfs_coroutine_core PUBLIC cxx_std_20)
target_include_directories(orchfs_kfs_coroutine_core PUBLIC
  "${ORCHFS_PUBLIC_INCLUDE_DIR}")
target_include_directories(orchfs_kfs_coroutine_core PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/LibFS"
  "${CMAKE_CURRENT_SOURCE_DIR}/KernelFS")
target_link_libraries(orchfs_kfs_coroutine_core PUBLIC
  orchfs_async_runtime orchfs_async_device orchfs_repro_trace)

# SPDK-backed NVMe data path. The same target remains usable with SPDK disabled;
# open then reports ENOTSUP and the hardware-independent planning tests still run.
add_library(orchfs_spdk_backend STATIC
  KernelFS/spdk_nvme_backend.cpp
  KernelFS/spdk_nvme_bridge.cpp)
add_library(orchfs::spdk_backend ALIAS orchfs_spdk_backend)
target_compile_features(orchfs_spdk_backend PUBLIC cxx_std_20)
target_include_directories(orchfs_spdk_backend PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/KernelFS")
target_include_directories(orchfs_spdk_backend PRIVATE
  "${ORCHFS_PUBLIC_INCLUDE_DIR}")
target_link_libraries(orchfs_spdk_backend PUBLIC Threads::Threads)

if(ORCHFS_ENABLE_SPDK)
  if(NOT EXISTS "${SPDK_ROOT}/VERSION")
    message(FATAL_ERROR
      "ORCHFS_ENABLE_SPDK=ON requires an SPDK v26.01 source/build tree at "
      "SPDK_ROOT; ${SPDK_ROOT}/VERSION is missing")
  endif()
  file(STRINGS "${SPDK_ROOT}/VERSION" _orchfs_spdk_version LIMIT_COUNT 1)
  string(STRIP "${_orchfs_spdk_version}" _orchfs_spdk_version)
  if(NOT _orchfs_spdk_version MATCHES "^26\\.01($|\\.)")
    message(FATAL_ERROR
      "Unsupported SPDK version '${_orchfs_spdk_version}' at ${SPDK_ROOT}; "
      "OrchFS requires the v26.01 LTS line")
  endif()

  # Use only pkg-config metadata generated inside SPDK_ROOT. In particular, do
  # not consume a system spdk_*.pc file, because it may describe a relocated or
  # otherwise unrelated build tree.
  set(_orchfs_spdk_pc_candidates
    "${SPDK_ROOT}/build/lib/pkgconfig"
    "${SPDK_ROOT}/build/lib64/pkgconfig"
    "${SPDK_ROOT}/lib/pkgconfig"
    "${SPDK_ROOT}/lib64/pkgconfig"
    "${SPDK_ROOT}/dpdk/build/lib/pkgconfig"
    "${SPDK_ROOT}/dpdk/build/lib64/pkgconfig")
  set(_orchfs_spdk_pc_dirs)
  set(_orchfs_spdk_primary_pc_dir "")
  foreach(_pc_dir IN LISTS _orchfs_spdk_pc_candidates)
    if(IS_DIRECTORY "${_pc_dir}")
      list(APPEND _orchfs_spdk_pc_dirs "${_pc_dir}")
      if(EXISTS "${_pc_dir}/spdk_nvme.pc" AND
         EXISTS "${_pc_dir}/spdk_env_dpdk.pc")
        set(_orchfs_spdk_primary_pc_dir "${_pc_dir}")
      endif()
    endif()
  endforeach()
  if(NOT _orchfs_spdk_primary_pc_dir)
    message(FATAL_ERROR
      "ORCHFS_ENABLE_SPDK=ON, but ${SPDK_ROOT} does not contain "
      "spdk_nvme.pc and spdk_env_dpdk.pc. Build SPDK v26.01 at SPDK_ROOT "
      "or pass -DSPDK_ROOT=/absolute/path/to/its/source-build tree.")
  endif()

  find_program(ORCHFS_PKG_CONFIG_EXECUTABLE NAMES pkg-config)
  if(NOT ORCHFS_PKG_CONFIG_EXECUTABLE)
    message(FATAL_ERROR "pkg-config is required to read SPDK_ROOT metadata")
  endif()

  list(JOIN _orchfs_spdk_pc_dirs ":" _orchfs_spdk_pc_path)
  set(_orchfs_spdk_modules spdk_nvme spdk_env_dpdk)
  if(EXISTS "${_orchfs_spdk_primary_pc_dir}/spdk_syslibs.pc")
    list(APPEND _orchfs_spdk_modules spdk_syslibs)
  endif()

  execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
            "PKG_CONFIG_PATH=${_orchfs_spdk_pc_path}"
            "${ORCHFS_PKG_CONFIG_EXECUTABLE}" --static --cflags
            ${_orchfs_spdk_modules}
    RESULT_VARIABLE _orchfs_spdk_cflags_result
    OUTPUT_VARIABLE _orchfs_spdk_cflags_text
    ERROR_VARIABLE _orchfs_spdk_cflags_error
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
            "PKG_CONFIG_PATH=${_orchfs_spdk_pc_path}"
            "${ORCHFS_PKG_CONFIG_EXECUTABLE}" --static --libs
            ${_orchfs_spdk_modules}
    RESULT_VARIABLE _orchfs_spdk_libs_result
    OUTPUT_VARIABLE _orchfs_spdk_libs_text
    ERROR_VARIABLE _orchfs_spdk_libs_error
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(NOT _orchfs_spdk_cflags_result EQUAL 0 OR
     NOT _orchfs_spdk_libs_result EQUAL 0)
    message(FATAL_ERROR
      "Failed to resolve SPDK from ${SPDK_ROOT}.\n"
      "cflags: ${_orchfs_spdk_cflags_error}\n"
      "libs: ${_orchfs_spdk_libs_error}")
  endif()

  separate_arguments(_orchfs_spdk_cflags NATIVE_COMMAND
                     "${_orchfs_spdk_cflags_text}")
  separate_arguments(_orchfs_spdk_libs NATIVE_COMMAND
                     "${_orchfs_spdk_libs_text}")
  # SPDK registers transports and other modules from ELF constructors. Static
  # archive members containing only those constructors are otherwise discarded
  # by the linker (notably nvme_pcie.o), leaving PCIe unavailable at runtime.
  set(_orchfs_spdk_link_items)
  set(_orchfs_spdk_whole_archive FALSE)
  foreach(_link_item IN LISTS _orchfs_spdk_libs)
    if(_link_item MATCHES "^-lspdk_")
      if(NOT _orchfs_spdk_whole_archive)
        list(APPEND _orchfs_spdk_link_items "-Wl,--whole-archive")
        set(_orchfs_spdk_whole_archive TRUE)
      endif()
      list(APPEND _orchfs_spdk_link_items "${_link_item}")
    else()
      if(_orchfs_spdk_whole_archive)
        list(APPEND _orchfs_spdk_link_items "-Wl,--no-whole-archive")
        set(_orchfs_spdk_whole_archive FALSE)
      endif()
      list(APPEND _orchfs_spdk_link_items "${_link_item}")
    endif()
  endforeach()
  if(_orchfs_spdk_whole_archive)
    list(APPEND _orchfs_spdk_link_items "-Wl,--no-whole-archive")
  endif()
  target_compile_definitions(orchfs_spdk_backend PUBLIC ORCHFS_HAS_SPDK=1)
  target_compile_options(orchfs_spdk_backend PRIVATE ${_orchfs_spdk_cflags})
  target_link_libraries(orchfs_spdk_backend PUBLIC ${_orchfs_spdk_link_items})

  # Preserve runtime lookup paths when the prefix contains shared DPDK libs.
  set(ORCHFS_SPDK_LIBRARY_DIRS "")
  foreach(_link_flag IN LISTS _orchfs_spdk_libs)
    if(_link_flag MATCHES "^-L(.+)$")
      list(APPEND ORCHFS_SPDK_LIBRARY_DIRS "${CMAKE_MATCH_1}")
    endif()
  endforeach()
  list(REMOVE_DUPLICATES ORCHFS_SPDK_LIBRARY_DIRS)
  if(ORCHFS_SPDK_LIBRARY_DIRS)
    # SPDK builds may link shared DPDK libraries which themselves depend on
    # sibling librte_*.so files. DT_RPATH is intentionally used here because
    # DT_RUNPATH is not applied to those transitive dependencies.
    target_link_options(orchfs_spdk_backend INTERFACE
      "LINKER:--disable-new-dtags")
    foreach(_library_dir IN LISTS ORCHFS_SPDK_LIBRARY_DIRS)
      target_link_options(orchfs_spdk_backend INTERFACE
        "LINKER:-rpath,${_library_dir}")
    endforeach()
  endif()
  message(STATUS "OrchFS SPDK prefix: ${SPDK_ROOT} (${_orchfs_spdk_version})")
endif()

# Keep the preload boundary independent of the KFS on-media layout core. This is
# both the ownership boundary (LibFS never opens an NVMe controller) and a
# loader-safety requirement: pulling libpmem/device constructors into an
# LD_PRELOAD DSO can recurse through interposed libc calls before real_ops is
# initialized.
add_library(orchfs_async_posix_adapter STATIC
  LibFS/async_adapter.cpp)
target_compile_features(orchfs_async_posix_adapter PUBLIC cxx_std_20)
target_include_directories(orchfs_async_posix_adapter PUBLIC
  LibFS "${ORCHFS_PUBLIC_INCLUDE_DIR}")
target_link_libraries(orchfs_async_posix_adapter PUBLIC
  orchfs_async_client Threads::Threads)

add_library(OrchFS SHARED LibFS/orchfs_wrapper.c)
orchfs_enable_project_options(OrchFS)
target_include_directories(OrchFS PUBLIC LibFS)
target_link_libraries(OrchFS PRIVATE
  orchfs_async_posix_adapter Threads::Threads dl)

if(ORCHFS_BUILD_KFS)
  # On-media layout implementation encapsulated by KfsCoroutineCore.
  # It contains no transport, fd table, I/O pool, or service thread.
  set(ORCHFS_KFS_LAYOUT_SOURCES
    LibFS/index.c
    LibFS/kfs_core_api.c
    LibFS/kfs_data_plan.c
    LibFS/lib_inode.c
    LibFS/lib_log.c
    LibFS/libspace.c
    LibFS/meta_cache.c
    LibFS/req_kernel.c
    LibFS/migrate.c
    util/radixtree.c
    util/orch_list.c
    util/orch_array.c)
  add_library(orchfs_kfs_layout_core STATIC ${ORCHFS_KFS_LAYOUT_SOURCES})
  orchfs_enable_project_options(orchfs_kfs_layout_core)
  target_compile_definitions(orchfs_kfs_layout_core PRIVATE
    ORCHFS_IN_PROCESS_DEVICE_IO=1)
  target_include_directories(orchfs_kfs_layout_core PUBLIC LibFS KernelFS)
  target_link_libraries(orchfs_kfs_layout_core PUBLIC pmem dl)

  add_library(orchfs_kfs_service STATIC
    KernelFS/balloc.c
    KernelFS/device.c
    KernelFS/kernel_func.c
    KernelFS/log.c
    KernelFS/async_server_bridge.cpp
    KernelFS/spdk_device_service.cpp
    util/hashtable.c
    util/orch_list.c
    util/orch_array.c)
  orchfs_enable_project_options(orchfs_kfs_service)
  target_include_directories(orchfs_kfs_service PUBLIC
    KernelFS LibFS "${ORCHFS_PUBLIC_INCLUDE_DIR}")
  target_compile_definitions(orchfs_kfs_service PRIVATE
    ORCHFS_KFS_SERVER=1 ORCHFS_ENABLE_SPDK=1)
  target_link_libraries(orchfs_kfs_service PUBLIC
    pmem orchfs_spdk_backend orchfs_async_server
    orchfs_kfs_coroutine_core orchfs_kfs_layout_core orchfs_repro_trace)

  add_executable(kfs_main KernelFS/kernel_main.c)
  orchfs_enable_project_options(kfs_main)
  target_link_libraries(kfs_main PRIVATE orchfs_kfs_service)
  add_custom_target(run_kfs_main DEPENDS kfs_main COMMAND kfs_main)

  add_executable(orchfs_layered_benchmark
    KernelFS/layered_benchmark.cpp)
  orchfs_enable_project_options(orchfs_layered_benchmark)
  target_include_directories(orchfs_layered_benchmark PRIVATE
    KernelFS "${ORCHFS_PUBLIC_INCLUDE_DIR}")
  target_link_libraries(orchfs_layered_benchmark PRIVATE
    orchfs_kfs_service)

  # The formatter owns SPDK on its calling thread and polls each qpair itself.
  add_executable(mkfs
    KernelFS/mkfs.c
    KernelFS/balloc.c
    KernelFS/device.c
    KernelFS/kindex.c
    KernelFS/log.c
    KernelFS/spdk_device_service.cpp
    util/hashtable.c
    util/orch_list.c
    util/orch_array.c)
  orchfs_enable_project_options(mkfs)
  target_include_directories(mkfs PRIVATE
    KernelFS "${ORCHFS_PUBLIC_INCLUDE_DIR}")
  target_compile_definitions(mkfs PRIVATE
    ORCHFS_KFS_SERVER=1 ORCHFS_ENABLE_SPDK=1 ORCHFS_FORMATTER=1)
  target_link_libraries(mkfs PRIVATE
    pmem orchfs_spdk_backend orchfs_async_runtime orchfs_repro_trace)
  add_custom_target(run_mkfs DEPENDS mkfs COMMAND mkfs)
endif()

add_executable(paralleltest2-ComFS test/paralleltest2-ComFS.c)
orchfs_enable_project_options(paralleltest2-ComFS)
target_link_libraries(paralleltest2-ComFS PRIVATE Threads::Threads)
add_custom_target(run_paralleltest2-ComFS
  DEPENDS paralleltest2-ComFS COMMAND paralleltest2-ComFS)

add_executable(paralleltest3-ComFS test/paralleltest3-ComFS.c)
orchfs_enable_project_options(paralleltest3-ComFS)
target_link_libraries(paralleltest3-ComFS PRIVATE Threads::Threads)
add_custom_target(run_paralleltest3-ComFS
  DEPENDS paralleltest3-ComFS COMMAND paralleltest3-ComFS)

# One deterministic POSIX workload generator is used against both the detached
# synchronous baseline and the current LD_PRELOAD adapter.  Keeping the
# generator independent of either implementation prevents benchmark code from
# silently selecting a different API or request stream for one side.
add_executable(orchfs_repro_bench
  scripts/reproduction/orchfs_repro_bench.c)
target_compile_options(orchfs_repro_bench PRIVATE
  -O3 -Wall -Wextra -Wpedantic)
target_link_libraries(orchfs_repro_bench PRIVATE Threads::Threads)

add_executable(orchfs_repro_mkdir
  scripts/reproduction/orchfs_repro_mkdir.c)

add_executable(gridgraph_edge_converter
  scripts/reproduction/gridgraph_edge_converter.c)
target_compile_options(gridgraph_edge_converter PRIVATE
  -O3 -Wall -Wextra -Wpedantic)

add_executable(orchfs_repro_copy
  scripts/reproduction/orchfs_repro_copy.c)
target_compile_options(orchfs_repro_copy PRIVATE
  -O3 -Wall -Wextra -Wpedantic)

add_executable(async_persistence_probe test/async_persistence_probe.c)
target_compile_options(async_persistence_probe PRIVATE
  -O2 -Wall -Wextra -Wpedantic -D_FORTIFY_SOURCE=2)

add_executable(async_posix_probe test/async_posix_probe.c)
target_compile_options(async_posix_probe PRIVATE
  -O2 -Wall -Wextra -Wpedantic -D_FORTIFY_SOURCE=2)

find_path(ORCHFS_LEVELDB_INCLUDE_DIR leveldb/db.h)
find_library(ORCHFS_LEVELDB_LIBRARY NAMES leveldb)
if(ORCHFS_LEVELDB_INCLUDE_DIR AND ORCHFS_LEVELDB_LIBRARY)
  add_executable(orchfs_ycsb_leveldb
    scripts/reproduction/orchfs_ycsb_leveldb.cpp)
  target_include_directories(orchfs_ycsb_leveldb PRIVATE
    "${ORCHFS_LEVELDB_INCLUDE_DIR}")
  target_link_libraries(orchfs_ycsb_leveldb PRIVATE
    "${ORCHFS_LEVELDB_LIBRARY}" Threads::Threads)
  target_compile_features(orchfs_ycsb_leveldb PRIVATE cxx_std_20)
endif()
target_compile_options(orchfs_repro_mkdir PRIVATE
  -O3 -Wall -Wextra -Wpedantic)

# Explicit, destructive hardware validation.  It is intentionally never
# registered with CTest and has a second runtime opt-in guard.
if(ORCHFS_ENABLE_SPDK)
  add_executable(spdk_nvme_device_test test/spdk_nvme_device_test.cpp)
  target_link_libraries(spdk_nvme_device_test PRIVATE orchfs_spdk_backend)
  if(ORCHFS_SPDK_LIBRARY_DIRS)
    set_target_properties(spdk_nvme_device_test PROPERTIES
      BUILD_RPATH "${ORCHFS_SPDK_LIBRARY_DIRS}")
  endif()
endif()

if(BUILD_TESTING)
  set(ORCHFS_SANITIZER_FLAGS
      "${CMAKE_C_FLAGS} ${CMAKE_CXX_FLAGS} ${CMAKE_EXE_LINKER_FLAGS} ${CMAKE_SHARED_LINKER_FLAGS}")
  if(ORCHFS_SANITIZER_FLAGS MATCHES "(^| )-fsanitize=thread( |$)")
    set(ORCHFS_THREAD_SANITIZER_BUILD ON)
  else()
    set(ORCHFS_THREAD_SANITIZER_BUILD OFF)
  endif()

  add_executable(legacy_wrapper_host_test test/legacy_wrapper_host_test.c)
  target_compile_options(legacy_wrapper_host_test PRIVATE
    -O2 -D_FORTIFY_SOURCE=2)
  add_dependencies(legacy_wrapper_host_test OrchFS)
  add_test(NAME legacy_wrapper_host
    COMMAND "${CMAKE_COMMAND}" -E env
      "LD_PRELOAD=$<TARGET_FILE:OrchFS>"
      "ORCHFS_ASYNC_ENDPOINT=/tmp/orchfs-wrapper-host-no-such-dir/socket"
      $<TARGET_FILE:legacy_wrapper_host_test>)
  set_tests_properties(legacy_wrapper_host PROPERTIES
    TIMEOUT 30 SKIP_RETURN_CODE 77 LABELS "legacy;preload;integration")
  if(ORCHFS_THREAD_SANITIZER_BUILD)
    # Injecting a TSAN-instrumented DSO with LD_PRELOAD can collide with the
    # sanitizer's fixed shadow mapping before main(). The ordinary build still
    # covers this boundary; the runtime-native tests below carry the TSAN run.
    set_tests_properties(legacy_wrapper_host PROPERTIES DISABLED TRUE)
  endif()

  add_executable(async_runtime_test test/async_runtime_test.cpp)
  target_link_libraries(async_runtime_test PRIVATE orchfs_async_runtime)
  add_test(NAME async_runtime COMMAND async_runtime_test)
  set_tests_properties(async_runtime PROPERTIES TIMEOUT 30 LABELS "async;unit")
  if(ORCHFS_THREAD_SANITIZER_BUILD)
    # This test intentionally recreates a Runtime in a child after fork.
    set_tests_properties(async_runtime PROPERTIES
      ENVIRONMENT "TSAN_OPTIONS=die_after_fork=0:halt_on_error=1")
  endif()

  add_executable(async_block_device_test test/async_block_device_test.cpp)
  target_link_libraries(async_block_device_test PRIVATE
    orchfs_async_device orchfs_async_runtime)
  add_test(NAME async_block_device COMMAND async_block_device_test)
  set_tests_properties(async_block_device PROPERTIES
    TIMEOUT 30 LABELS "async;device;unit")

  add_executable(kfs_coroutine_core_test test/kfs_coroutine_core_test.cpp)
  target_link_libraries(kfs_coroutine_core_test PRIVATE
    orchfs_kfs_coroutine_core orchfs_async_device orchfs_async_runtime)
  add_test(NAME kfs_coroutine_core COMMAND kfs_coroutine_core_test)
  set_tests_properties(kfs_coroutine_core PROPERTIES
    TIMEOUT 30 LABELS "async;kfs;unit")

  add_executable(journal_recovery_test
    test/journal_recovery_test.c LibFS/lib_log.c)
  orchfs_enable_project_options(journal_recovery_test)
  target_include_directories(journal_recovery_test PRIVATE
    LibFS KernelFS "${ORCHFS_PUBLIC_INCLUDE_DIR}")
  add_test(NAME journal_recovery COMMAND journal_recovery_test)
  set_tests_properties(journal_recovery PROPERTIES
    TIMEOUT 30 LABELS "async;kfs;journal;unit")

  add_executable(async_ipc_test test/async_ipc_test.cpp)
  target_link_libraries(async_ipc_test PRIVATE orchfs_async_ipc)
  add_test(NAME async_ipc COMMAND async_ipc_test)
  set_tests_properties(async_ipc PROPERTIES TIMEOUT 30 LABELS "async;ipc")

  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test/async_client_server_test.cpp")
    add_executable(async_client_server_test test/async_client_server_test.cpp)
    add_dependencies(async_client_server_test OrchFS)
    target_compile_definitions(async_client_server_test PRIVATE
      ORCHFS_WRAPPER_PATH="$<TARGET_FILE:OrchFS>")
    target_link_libraries(async_client_server_test PRIVATE
      orchfs_async_client orchfs_async_server ${CMAKE_DL_LIBS})
    add_test(NAME async_client_server COMMAND async_client_server_test)
    set_tests_properties(async_client_server PROPERTIES
      TIMEOUT 30 LABELS "async;ipc;integration")
    if(ORCHFS_THREAD_SANITIZER_BUILD)
      # The LD_PRELOAD boundary has its own ordinary-build test above.
      set_tests_properties(async_client_server PROPERTIES
        ENVIRONMENT "ORCHFS_SKIP_WRAPPER_TEST=1;TSAN_OPTIONS=die_after_fork=0:halt_on_error=1")
    endif()
  endif()

  add_executable(spdk_nvme_logic_test test/spdk_nvme_logic_test.cpp)
  target_link_libraries(spdk_nvme_logic_test PRIVATE orchfs_spdk_backend)
  if(ORCHFS_SPDK_LIBRARY_DIRS)
    set_target_properties(spdk_nvme_logic_test PROPERTIES
      BUILD_RPATH "${ORCHFS_SPDK_LIBRARY_DIRS}")
  endif()
  add_test(NAME spdk_nvme_logic COMMAND spdk_nvme_logic_test)
  set_tests_properties(spdk_nvme_logic PROPERTIES TIMEOUT 30 LABELS "spdk;unit")

  if(TARGET kfs_main)
    add_test(NAME runtime_native_static_gate
      COMMAND "${CMAKE_COMMAND}"
        "-DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
        "-DKFS_BINARY=$<TARGET_FILE:kfs_main>"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/runtime_native_gate.cmake")
  else()
    add_test(NAME runtime_native_static_gate
      COMMAND "${CMAKE_COMMAND}"
        "-DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/runtime_native_gate.cmake")
  endif()
  set_tests_properties(runtime_native_static_gate PROPERTIES
    TIMEOUT 30 LABELS "async;static")
endif()
