#include "kernel_func.h"
#include "balloc.h"
#include "device.h"
#include "log.h"
#include "type.h"
#include "../config/protocol.h"
#include "../config/config.h"

#include "async_server_bridge.h"
#include "../LibFS/kfs_core_api.h"

#ifdef __cplusplus       
extern "C"{
#endif

static void init_kernelFS_impl(int start_server)
{
	int runtime_error = orchfs_async_runtime_start();
	if(runtime_error != 0)
	{
		fprintf(stderr, "start async KFS runtime error: %s (%d)\n",
				strerror(runtime_error), runtime_error);
		exit(1);
	}
	device_set_async_runtime(orchfs_async_runtime_handle());
	device_init();
	// printf("dev init!\n");
	// fflush(stdout);
	int format_error = orchfs_core_validate_format();
	if(format_error != 0)
	{
		fprintf(stderr, "unsupported or corrupt OrchFS format: %s (%d); "
				"run mkfs to create format v%" PRIu64 "\n",
				strerror(format_error), format_error,
				(uint64_t)ORCHFS_DISK_FORMAT_VERSION);
		device_close();
		device_set_async_runtime(NULL);
		(void)orchfs_async_runtime_stop();
		exit(1);
	}

	init_mem_bmp();
	// printf("bitmap init!\n");
	// fflush(stdout);

	init_kernel_log();
	// printf("log init!\n");
	// fflush(stdout);
	int recovery_error = orchfs_core_recover();
	if(recovery_error != 0)
	{
		fprintf(stderr, "recover OrchFS journal error: %s (%d)\n",
				strerror(recovery_error), recovery_error);
		close_kernel_log();
		delete_mem_bmp();
		device_close();
		device_set_async_runtime(NULL);
		(void)orchfs_async_runtime_stop();
		exit(1);
	}

	/* The authoritative coroutine core uses the KFS-owned device, bitmap
	 * allocator, and journal directly. No socket, SHM, or worker-pool service
	 * is started. */
	int core_error = orchfs_core_initialize();
	if(core_error != 0)
	{
		fprintf(stderr, "initialize KFS coroutine core error: %s (%d)\n",
				strerror(core_error), core_error);
		close_kernelFS();
		exit(1);
	}
	if(start_server)
	{
		int async_error = orchfs_async_server_start();
		if(async_error != 0)
		{
			fprintf(stderr, "start async KFS server error: %s (%d)\n",
					strerror(async_error), async_error);
			close_kernelFS();
			exit(1);
		}
	}
}

void init_kernelFS()
{
	init_kernelFS_impl(1);
}

void init_kernelFS_direct()
{
	init_kernelFS_impl(0);
}


void close_kernelFS()
{

	fprintf(stderr,"close begin!\n");
	/* Stop accepting RPCs and drain every client/core coroutine first. */
	int async_error = orchfs_async_server_stop();
	if(async_error != 0)
		fprintf(stderr, "stop async KFS server error: %s (%d)\n",
				strerror(async_error), async_error);
	orchfs_core_shutdown();
	fprintf(stderr,"async server close!\n");
	

	close_kernel_log();
	fprintf(stderr,"log close!\n");

	sync_all_mem_bmp();
	delete_mem_bmp();
	fprintf(stderr,"bmp close!\n");

	device_close();
	fprintf(stderr,"dev close!\n");
	device_set_async_runtime(NULL);
	int runtime_error = orchfs_async_runtime_stop();
	if(runtime_error != 0)
		fprintf(stderr, "stop async KFS runtime error: %s (%d)\n",
				strerror(runtime_error), runtime_error);
}

int orchfs_kfs_alloc_direct(int64_t func_type, int64_t alloc_blk_num,
		int64_t return_type, void* ret_info_buf)
{
	if(ret_info_buf == NULL || alloc_blk_num < 0)
		return EINVAL;
	if(return_type != RET_BLK_ID && return_type != RET_BLK_ADDR)
		return EINVAL;

	int64_t* response = (int64_t*)ret_info_buf;
	uint64_t* blocks = (uint64_t*)(response + 2);
	int error = 0;
	if(func_type == ALLOC_INODE_FUNC)
		error = alloc_inodes(alloc_blk_num, blocks, return_type);
	else if(func_type == ALLOC_INXND_FUNC)
		error = alloc_idx_nodes(alloc_blk_num, blocks, return_type);
	else if(func_type == ALLOC_VIRND_FUNC)
		error = alloc_viridx_nodes(alloc_blk_num, blocks, return_type);
	else if(func_type == ALLOC_BUFMETA_FUNC)
		error = alloc_bufmeta_nodes(alloc_blk_num, blocks, return_type);
	else if(func_type == ALLOC_PAGE_FUNC)
		error = alloc_nvm_pages(alloc_blk_num, blocks, return_type);
	else if(func_type == ALLOC_BLOCK_FUNC)
		error = alloc_ssd_blocks(alloc_blk_num, blocks, return_type);
	else
		return EINVAL;
	if(error != 0)
		return error;

	response[0] = func_type;
	response[1] = alloc_blk_num;
	return 0;
}

int orchfs_kfs_dealloc_direct(int64_t func_type, int64_t dealloc_blk_num,
		int64_t parameter_type, const int64_t* block_ids)
{
	if(dealloc_blk_num < 0 || (dealloc_blk_num != 0 && block_ids == NULL))
		return EINVAL;
	if(parameter_type != PAR_BLK_ID && parameter_type != PAR_BLK_ADDR)
		return EINVAL;

	for(int64_t i = 0; i < dealloc_blk_num; ++i)
	{
		int error;
		if(func_type == DEALLOC_INODE_FUNC)
			error = dealloc_inode(block_ids[i], parameter_type);
		else if(func_type == DEALLOC_INXND_FUNC)
			error = dealloc_idx_node(block_ids[i], parameter_type);
		else if(func_type == DEALLOC_VIRND_FUNC)
			error = dealloc_viridx_node(block_ids[i], parameter_type);
		else if(func_type == DEALLOC_BUFMETA_FUNC)
			error = dealloc_bufmeta_node(block_ids[i], parameter_type);
		else if(func_type == DEALLOC_PAGE_FUNC)
			error = dealloc_nvm_page(block_ids[i], parameter_type);
		else if(func_type == DEALLOC_BLOCK_FUNC)
			error = dealloc_ssd_block(block_ids[i], parameter_type);
		else
			return EINVAL;
		if(error != 0)
			return error;
	}
	return 0;
}

int64_t orchfs_kfs_alloc_log_direct(void)
{
	return alloc_log_segment_from_dev();
}

#ifdef __cplusplus
}
#endif
