if(NOT DEFINED SOURCE_DIR)
  message(FATAL_ERROR "SOURCE_DIR is required")
endif()

set(production_sources
  Async/block_device.cpp
  Async/client.cpp
  Async/ipc_transport.cpp
  Async/kfs_coroutine_core.cpp
  Async/range_arbiter.cpp
  Async/runtime.cpp
  Async/server.cpp
  KernelFS/async_server_bridge.cpp
  KernelFS/balloc.c
  KernelFS/device.c
  KernelFS/kernel_func.c
  KernelFS/kernel_main.c
  KernelFS/log.c
  KernelFS/spdk_device_service.cpp
  KernelFS/spdk_nvme_backend.cpp
  LibFS/async_adapter.cpp
  LibFS/index.c
  LibFS/kfs_core_api.c
  LibFS/kfs_data_plan.c
  LibFS/lib_inode.c
  LibFS/lib_log.c
  LibFS/libspace.c
  LibFS/meta_cache.c
  LibFS/migrate.c
  LibFS/req_kernel.c)

set(removed_sources
  LibFS/io_thdpool.c
  LibFS/runtime.c
  LibFS/lib_socket.c
  LibFS/lib_shm.c
  KernelFS/thdpool.c
  KernelFS/execio_thdpool.c
  KernelFS/kernel_recv.c
  KernelFS/kernel_send.c
  KernelFS/close_kfs.c)

foreach(relative IN LISTS removed_sources)
  if(EXISTS "${SOURCE_DIR}/${relative}")
    message(FATAL_ERROR "removed service source returned: ${relative}")
  endif()
endforeach()

set(old_api_pattern
    "BlockingExecutor|blocking_->run|blocking_worker_count|ORCHFS_KFS_BLOCKING_WORKERS|ORCHFS_ENABLE_ASYNC_SERVER|NOASYNC|OrchFS_LIBFS|OrchFS_SERVER_CORE|ORCH_CONFIG_NVMTHD|ORCH_CONFIG_SSDTHD|ORCH_MAX_SPLIT_BLK")
set(lock_pattern
    "pthread_(create|mutex|rwlock|spinlock|cond)|std::(mutex|recursive_mutex|shared_mutex)|sem_wait")

foreach(relative IN LISTS production_sources)
  set(path "${SOURCE_DIR}/${relative}")
  if(NOT EXISTS "${path}")
    message(FATAL_ERROR "production source is missing: ${relative}")
  endif()
  file(READ "${path}" contents)
  if(contents MATCHES "${old_api_pattern}")
    message(FATAL_ERROR "legacy async service API found in ${relative}")
  endif()
  if(contents MATCHES "${lock_pattern}")
    message(FATAL_ERROR "OS lock or service-thread primitive found in ${relative}")
  endif()
  if(NOT relative STREQUAL "Async/runtime.cpp" AND
     contents MATCHES "std::thread[ \\t\\r\\n]+|std::thread[ \\t\\r\\n]*\\(|pthread_create")
    message(FATAL_ERROR "thread creation outside Runtime found in ${relative}")
  endif()

  # device.c also contains the offline ORCHFS_FORMATTER implementation. Every
  # other production source must use only the callback-based device seam.
  if(NOT relative STREQUAL "KernelFS/device.c" AND
     NOT relative STREQUAL "KernelFS/spdk_device_service.cpp" AND
     contents MATCHES "(^|[^A-Za-z0-9_])(read_data_from_devs|write_data_to_devs|device_sync|orchfs_spdk_formatter_(read|write|flush))[ \t\r\n]*\\(")
    message(FATAL_ERROR "synchronous device call found in ${relative}")
  endif()
endforeach()

file(READ "${SOURCE_DIR}/CMakeLists.txt" cmake_contents)
file(READ "${SOURCE_DIR}/config/config-template.sh" config_contents)
if(cmake_contents MATCHES "${old_api_pattern}" OR
   config_contents MATCHES "${old_api_pattern}")
  message(FATAL_ERROR "legacy build/configuration switch remains")
endif()

if(DEFINED KFS_BINARY AND NOT KFS_BINARY STREQUAL "")
  if(NOT EXISTS "${KFS_BINARY}")
    message(FATAL_ERROR "KFS binary does not exist: ${KFS_BINARY}")
  endif()
  execute_process(
    COMMAND nm -C "${KFS_BINARY}"
    RESULT_VARIABLE nm_result
    OUTPUT_VARIABLE symbols
    ERROR_VARIABLE nm_error)
  if(NOT nm_result EQUAL 0)
    message(FATAL_ERROR "nm failed for ${KFS_BINARY}: ${nm_error}")
  endif()
  if(symbols MATCHES "orchfs_spdk_formatter_(read|write|flush)" OR
     symbols MATCHES " [TW] (read_data_from_devs|write_data_to_devs|device_sync)")
    message(FATAL_ERROR "synchronous device symbol linked into production KFS")
  endif()
  if(NOT symbols MATCHES "submit_read_data_from_devs" OR
     NOT symbols MATCHES "submit_write_data_to_devs" OR
     NOT symbols MATCHES "submit_device_sync")
    message(FATAL_ERROR "production KFS is missing the asynchronous device seam")
  endif()
endif()
