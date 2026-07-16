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
option(ORCHFS_ENABLE_ASYNC_SERVER
       "Start the authoritative coroutine RPC server inside kfs_main" ON)
set(SPDK_ROOT "/opt/orchfs/spdk" CACHE PATH
    "Fixed SPDK source/build prefix (the supported baseline is SPDK v26.01)")

set(ORCHFS_PUBLIC_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

function(orchfs_enable_legacy_options target)
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
  PUBLIC orchfs_async_runtime orchfs_async_ipc Threads::Threads)

# KFS-side authoritative dispatcher. Legacy OrchFS core symbols are resolved by
# the final KFS executable, not by this static library.
add_library(orchfs_async_server STATIC Async/server.cpp)
add_library(orchfs::async_server ALIAS orchfs_async_server)
target_compile_features(orchfs_async_server PUBLIC cxx_std_20)
target_include_directories(orchfs_async_server PUBLIC
  "${ORCHFS_PUBLIC_INCLUDE_DIR}"
  "${CMAKE_CURRENT_SOURCE_DIR}/LibFS"
  "${CMAKE_CURRENT_SOURCE_DIR}/KernelFS")
target_link_libraries(orchfs_async_server
  PUBLIC orchfs_async_runtime orchfs_async_ipc Threads::Threads)

# SPDK-backed NVMe data path. The same target remains usable with SPDK disabled;
# open then reports ENOTSUP and the hardware-independent planning tests still run.
add_library(orchfs_spdk_backend STATIC
  KernelFS/spdk_nvme_backend.cpp
  KernelFS/spdk_nvme_bridge.cpp)
add_library(orchfs::spdk_backend ALIAS orchfs_spdk_backend)
target_compile_features(orchfs_spdk_backend PUBLIC cxx_std_20)
target_include_directories(orchfs_spdk_backend PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/KernelFS")
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

# ---------------------------------------------------------------------------
# Original LibFS / KFS targets. Their names and command targets are preserved.

set(OrchFS_LIBFS_COREsrc
  LibFS/index.c
  LibFS/io_thdpool.c
  LibFS/lib_dir.c
  LibFS/lib_func.c
  LibFS/lib_inode.c
  LibFS/lib_log.c
  LibFS/lib_rwfunc.c
  LibFS/lib_socket.c
  LibFS/libspace.c
  LibFS/meta_cache.c
  LibFS/req_kernel.c
  LibFS/runtime.c
  LibFS/ffile.c
  LibFS/dir_cache.c
  LibFS/lib_shm.c
  LibFS/migrate.c
  util/hashtable.c
  util/radixtree.c
  util/orch_list.c
  util/orch_array.c
  util/LRU.c)
set(OrchFS_LIBFSsrc ${OrchFS_LIBFS_COREsrc} KernelFS/device.c)

add_library(OrchFS_LIBFS STATIC ${OrchFS_LIBFSsrc})
orchfs_enable_legacy_options(OrchFS_LIBFS)
target_include_directories(OrchFS_LIBFS PUBLIC LibFS)
target_link_libraries(OrchFS_LIBFS PUBLIC pmem Threads::Threads dl)

# Keep the preload boundary independent of the legacy in-process core. This is
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
orchfs_enable_legacy_options(OrchFS)
target_include_directories(OrchFS PUBLIC LibFS)
target_link_libraries(OrchFS PRIVATE
  orchfs_async_posix_adapter Threads::Threads dl)

# The KFS coroutine server embeds the existing filesystem core but uses the
# KFS-owned device instance. Keeping device.c out of this archive guarantees
# that LibFS never opens a second NVMe/SPDK handle in the server process.
if(ORCHFS_ENABLE_ASYNC_SERVER)
  add_library(OrchFS_SERVER_CORE STATIC ${OrchFS_LIBFS_COREsrc})
  orchfs_enable_legacy_options(OrchFS_SERVER_CORE)
  # LibFS and KFS historically used the same two private helper names in
  # separate processes. They now coexist in kfs_main, so namespace the LibFS
  # copies without changing either public interface.
  target_compile_definitions(OrchFS_SERVER_CORE PRIVATE
    init_send_port_basic_info=libfs_init_send_port_basic_info
    init_recv_port_basic_info=libfs_init_recv_port_basic_info)
  target_include_directories(OrchFS_SERVER_CORE PUBLIC LibFS KernelFS)
  target_link_libraries(OrchFS_SERVER_CORE PUBLIC pmem Threads::Threads dl)
endif()

set(OrchFS_KERFSsrc
  KernelFS/balloc.c
  KernelFS/device.c
  KernelFS/kernel_func.c
  KernelFS/kernel_recv.c
  KernelFS/kernel_send.c
  KernelFS/log.c
  KernelFS/thdpool.c
  KernelFS/kindex.c
  KernelFS/close_kfs.c
  KernelFS/execio_thdpool.c
  KernelFS/ker_shm.c
  util/hashtable.c
  util/orch_list.c
  util/orch_array.c)

add_library(OrchFS_KERFS STATIC ${OrchFS_KERFSsrc})
orchfs_enable_legacy_options(OrchFS_KERFS)
target_include_directories(OrchFS_KERFS PUBLIC KernelFS)
target_compile_definitions(OrchFS_KERFS PRIVATE ORCHFS_KFS_SERVER=1)
target_link_libraries(OrchFS_KERFS
  PUBLIC pmem Threads::Threads orchfs_spdk_backend)

# These files are integration seams added by the coroutine/SPDK conversion.
if(ORCHFS_ENABLE_SPDK)
  target_compile_definitions(OrchFS_KERFS PRIVATE ORCHFS_ENABLE_SPDK=1)
endif()
if(ORCHFS_ENABLE_SPDK AND
   EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/KernelFS/spdk_device_service.cpp")
  target_sources(OrchFS_KERFS PRIVATE KernelFS/spdk_device_service.cpp)
endif()
if(ORCHFS_ENABLE_ASYNC_SERVER AND
   EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/KernelFS/async_server_bridge.cpp")
  target_compile_definitions(OrchFS_KERFS PRIVATE
    ORCHFS_ENABLE_ASYNC_SERVER=1)
  target_sources(OrchFS_KERFS PRIVATE KernelFS/async_server_bridge.cpp)
  target_link_libraries(OrchFS_KERFS
    PUBLIC orchfs_async_server OrchFS_SERVER_CORE)
endif()

add_executable(mkfs KernelFS/mkfs.c)
orchfs_enable_legacy_options(mkfs)
target_link_libraries(mkfs PRIVATE OrchFS_KERFS)
add_custom_target(run_mkfs DEPENDS mkfs COMMAND mkfs)

add_executable(kfs_main KernelFS/kernel_main.c)
orchfs_enable_legacy_options(kfs_main)
target_link_libraries(kfs_main PRIVATE OrchFS_KERFS)
add_custom_target(run_kfs_main DEPENDS kfs_main COMMAND kfs_main)

add_executable(close_kfs KernelFS/close_kfs.c)
orchfs_enable_legacy_options(close_kfs)
target_link_libraries(close_kfs PRIVATE OrchFS_KERFS)
add_custom_target(run_close_kfs DEPENDS close_kfs COMMAND close_kfs)

add_executable(paralleltest2-ComFS test/paralleltest2-ComFS.c)
orchfs_enable_legacy_options(paralleltest2-ComFS)
target_link_libraries(paralleltest2-ComFS PRIVATE Threads::Threads)
add_custom_target(run_paralleltest2-ComFS
  DEPENDS paralleltest2-ComFS COMMAND paralleltest2-ComFS)

add_executable(paralleltest3-ComFS test/paralleltest3-ComFS.c)
orchfs_enable_legacy_options(paralleltest3-ComFS)
target_link_libraries(paralleltest3-ComFS PRIVATE Threads::Threads)
add_custom_target(run_paralleltest3-ComFS
  DEPENDS paralleltest3-ComFS COMMAND paralleltest3-ComFS)

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
  # Exercise the legacy functions that form the blocking boundary of the
  # coroutine server without opening a real DAX/NVMe device. Function sections
  # let the small mock harness link only the lib_func.c entry points it tests.
  add_executable(legacy_lib_func_test
    test/legacy_lib_func_test.c
    LibFS/lib_func.c)
  target_include_directories(legacy_lib_func_test PRIVATE LibFS KernelFS)
  target_compile_options(legacy_lib_func_test PRIVATE
    -fcommon -ffunction-sections)
  target_link_options(legacy_lib_func_test PRIVATE -Wl,--gc-sections)
  target_link_libraries(legacy_lib_func_test PRIVATE Threads::Threads)
  add_test(NAME legacy_lib_func COMMAND legacy_lib_func_test)
  set_tests_properties(legacy_lib_func PROPERTIES
    TIMEOUT 30 LABELS "legacy;unit")

  add_executable(legacy_rename_core_test
    test/legacy_rename_core_test.c
    LibFS/lib_dir.c)
  target_include_directories(legacy_rename_core_test PRIVATE LibFS KernelFS)
  target_compile_options(legacy_rename_core_test PRIVATE
    -fcommon -ffunction-sections)
  target_link_options(legacy_rename_core_test PRIVATE -Wl,--gc-sections)
  target_link_libraries(legacy_rename_core_test PRIVATE Threads::Threads)
  add_test(NAME legacy_rename_core COMMAND legacy_rename_core_test)
  set_tests_properties(legacy_rename_core PROPERTIES
    TIMEOUT 30 LABELS "legacy;unit")

  add_executable(legacy_wrapper_host_test test/legacy_wrapper_host_test.c)
  add_dependencies(legacy_wrapper_host_test OrchFS)
  add_test(NAME legacy_wrapper_host
    COMMAND "${CMAKE_COMMAND}" -E env
      "LD_PRELOAD=$<TARGET_FILE:OrchFS>"
      "ORCHFS_ASYNC_ENDPOINT=/tmp/orchfs-wrapper-host-no-such-dir/socket"
      $<TARGET_FILE:legacy_wrapper_host_test>)
  set_tests_properties(legacy_wrapper_host PROPERTIES
    TIMEOUT 30 SKIP_RETURN_CODE 77 LABELS "legacy;preload;integration")

  add_executable(async_runtime_test test/async_runtime_test.cpp)
  target_link_libraries(async_runtime_test PRIVATE orchfs_async_runtime)
  add_test(NAME async_runtime COMMAND async_runtime_test)
  set_tests_properties(async_runtime PROPERTIES TIMEOUT 30 LABELS "async;unit")

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
  endif()

  add_executable(spdk_nvme_logic_test test/spdk_nvme_logic_test.cpp)
  target_link_libraries(spdk_nvme_logic_test PRIVATE orchfs_spdk_backend)
  if(ORCHFS_SPDK_LIBRARY_DIRS)
    set_target_properties(spdk_nvme_logic_test PROPERTIES
      BUILD_RPATH "${ORCHFS_SPDK_LIBRARY_DIRS}")
  endif()
  add_test(NAME spdk_nvme_logic COMMAND spdk_nvme_logic_test)
  set_tests_properties(spdk_nvme_logic PROPERTIES TIMEOUT 30 LABELS "spdk;unit")
endif()
