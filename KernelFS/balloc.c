#include "balloc.h"
#include "type.h"
#include "device.h"
#include "addr_util.h"
#ifndef ORCHFS_FORMATTER
#include "async_server_bridge.h"
#endif
#include "../config/config.h"

#include <errno.h>
#include <limits.h>

#ifdef __cplusplus       
extern "C"{
#endif

void init_mem_bmp()
{
	void* sb_addr = valloc(SIZE_4KiB);
	nvm_read(sb_addr, 512, 0);
	// orch_super_blk_pt out = (orch_super_blk_pt)sb_addr;
	orch_super_blk_pt sb_pt = (orch_super_blk_pt)sb_addr;
	for(int i = START_BMP_ID; i <= END_BMP_ID; i++)
	{
		mem_bmp_pt bmp_info = mem_bmp_arr + i; 

		// initialize metadata about bitmap
		bmp_info->dev_start_addr = (sb_pt->bmp_dev_addr_list)[i];
		bmp_info->bmp_alloc_cur = (sb_pt->bmp_alloc_cur_list)[i];
		bmp_info->bmp_alloc_range = (sb_pt->bmp_alloc_range_list)[i];
		bmp_info->bmp_used_num = (sb_pt->bmp_used_num_list)[i];

		if(i == INODE_BMP)
		{
			bmp_info->bmp_capacity = MAX_INODE_NUM / 8;
			bmp_info->blk_area_start = OFFSET_INODE;
			bmp_info->blk_size = ORCH_INODE_SIZE;
		}
		else if(i == PAGE_BMP)
		{
			bmp_info->bmp_capacity = MAX_PAGE_NUM / 8;
			bmp_info->blk_area_start = OFFSET_PAGE;
			bmp_info->blk_size = ORCH_PAGE_SIZE;
		}
		else if(i == BLOCK_BMP)
		{
			bmp_info->bmp_capacity = MAX_BLOCK_NUM / 8;
			bmp_info->blk_area_start = OFFSET_BLOCK;
			bmp_info->blk_size = ORCH_BLOCK_SIZE;
		}
		else if(i == BUFMETA_BMP)
		{
			bmp_info->bmp_capacity = MAX_BUFMETA_NUM / 8;
			bmp_info->blk_area_start = OFFSET_BUFMETA;
			bmp_info->blk_size = ORCH_BUFMETA_SIZE;
		}
		else if(i == IDX_NODE_BMP)
		{
			bmp_info->bmp_capacity = MAX_INDEX_NUM / 8;
			bmp_info->blk_area_start = OFFSET_INDEX;
			bmp_info->blk_size = ORCH_IDX_SIZE;
		}
		else if(i == VIR_NODE_BMP)
		{
			bmp_info->bmp_capacity = MAX_VIRND_NUM / 8;
			bmp_info->blk_area_start = OFFSET_VIRND;
			bmp_info->blk_size = ORCH_VIRND_SIZE;
		}
		uint64_t valloc_size = bmp_info->bmp_capacity / MEM_PAGE_SIZE * MEM_PAGE_SIZE + MEM_PAGE_SIZE;
		bmp_info->bmp_start_pt = valloc(valloc_size); 

		// read bitmap data
		nvm_read(bmp_info->bmp_start_pt, bmp_info->bmp_capacity, bmp_info->dev_start_addr);

		/* Bytes beyond bmp_alloc_range have never contained live allocations.
		 * Materialize them once so every Runtime worker can own a fixed shard. */
		if(bmp_info->bmp_alloc_range < bmp_info->bmp_capacity)
			memset((uint8_t*)bmp_info->bmp_start_pt + bmp_info->bmp_alloc_range,
			       0, bmp_info->bmp_capacity - bmp_info->bmp_alloc_range);
		bmp_info->bmp_alloc_range = bmp_info->bmp_capacity;

#ifdef ORCHFS_FORMATTER
		bmp_info->shard_count = 1;
#else
		bmp_info->shard_count = orchfs_async_runtime_worker_count();
		if(bmp_info->shard_count == 0)
			bmp_info->shard_count = 1;
#endif
		bmp_info->shard_cursor = calloc(bmp_info->shard_count,
					       sizeof(*bmp_info->shard_cursor));
		if(bmp_info->shard_cursor == NULL)
		{
			fprintf(stderr, "allocate bitmap shard cursors: %s\n",
					strerror(errno));
			exit(EXIT_FAILURE);
		}
	}
	fflush(stdout);
	free(sb_addr);
}

uint32_t sync_type_mem_bmp(int32_t bmp_type)
{
	mem_bmp_pt bmp_info = mem_bmp_arr+bmp_type;
	// uint8_t* outpt = bmp_info->bmp_start_pt;
	nvm_write(bmp_info->bmp_start_pt, bmp_info->bmp_capacity, bmp_info->dev_start_addr);
	return 0;
}

uint32_t sync_all_mem_bmp()
{
	void* sb_addr = valloc(SIZE_4KiB);
	nvm_read(sb_addr, ORCH_SUPER_BLK_SIZE, OFFSET_SUPER_BLK);
	orch_super_blk_pt sb_pt = (orch_super_blk_pt)sb_addr;
	for(int i = START_BMP_ID; i <= END_BMP_ID; i++)
	{
		sync_type_mem_bmp(i);
		mem_bmp_pt bmp_info = mem_bmp_arr + i;
		(sb_pt->bmp_dev_addr_list)[i] = bmp_info->dev_start_addr;
		(sb_pt->bmp_alloc_cur_list)[i] = 0;
		(sb_pt->bmp_alloc_range_list)[i] = bmp_info->bmp_alloc_range;
		(sb_pt->bmp_used_num_list)[i] = __atomic_load_n(
			&bmp_info->bmp_used_num, __ATOMIC_ACQUIRE);
	}
	nvm_write(sb_addr, ORCH_SUPER_BLK_SIZE, OFFSET_SUPER_BLK);
	free(sb_addr);
	return 0;
}

uint32_t sync_mem_bmp(int32_t bmp_type, int64_t bit_off, int64_t bit_len)
{
	mem_bmp_pt bmp_info = mem_bmp_arr+bmp_type;
	if(bit_off+bit_len > bmp_info->bmp_capacity*8)
	{
		printf("bitmap copy cross the border!\n");
		return 2;
	}
	uint64_t byte_start = bit_off/8, byte_end = (bit_off+bit_len-1)/8;
	uint64_t cpy_bytes = byte_end-byte_start+1;
	nvm_write(bmp_info->bmp_start_pt+byte_start, cpy_bytes, bmp_info->dev_start_addr+byte_start);
	return 0;
}

void delete_mem_bmp()
{
	for(int i = START_BMP_ID; i <= END_BMP_ID; i++)
	{
		if(mem_bmp_arr[i].bmp_start_pt!=NULL)
		{
			free(mem_bmp_arr[i].bmp_start_pt);
			free(mem_bmp_arr[i].shard_cursor);
			mem_bmp_arr[i].bmp_start_pt = NULL;
			mem_bmp_arr[i].shard_cursor = NULL;
		}
		else
		{
			printf("memory bitmap address error!\n");
			exit(0);
		}
	}
}

static int claim_bit(mem_bmp_pt bmp_info, uint64_t bit)
{
	uint8_t* byte = (uint8_t*)bmp_info->bmp_start_pt + bit / 8;
	const uint8_t mask = (uint8_t)(1U << (7U - bit % 8U));
	const uint8_t previous = __atomic_fetch_or(byte, mask, __ATOMIC_ACQ_REL);
	if((previous & mask) != 0)
		return 0;
	__atomic_fetch_add(&bmp_info->bmp_used_num, 1, __ATOMIC_RELAXED);
	return 1;
}

static int allocate_one(mem_bmp_pt bmp_info, size_t worker,
			uint64_t* output)
{
	const uint64_t total = bmp_info->bmp_capacity * 8;
	const size_t shard_count = bmp_info->shard_count;
	worker %= shard_count;
	const uint64_t begin = total * worker / shard_count;
	const uint64_t end = total * (worker + 1) / shard_count;
	const uint64_t span = end - begin;
	uint64_t start = __atomic_fetch_add(&bmp_info->shard_cursor[worker],
					  1, __ATOMIC_RELAXED);

	/* Normal allocation never contends: each worker scans its own shard. */
	for(uint64_t i = 0; i < span; ++i)
	{
		const uint64_t bit = begin + (start + i) % span;
		if(claim_bit(bmp_info, bit))
		{
			*output = bit;
			return 0;
		}
	}

	/* A skewed inode owner may exhaust its shard.  Atomic claims allow a
	 * bounded fallback across other shards without a global spin lock. */
	for(uint64_t i = 0; i < total; ++i)
	{
		const uint64_t bit = (end + i) % total;
		if(bit >= begin && bit < end)
			continue;
		if(claim_bit(bmp_info, bit))
		{
			*output = bit;
			return 0;
		}
	}
	return ENOSPC;
}

uint32_t do_dealloc(int32_t bmp_type, int64_t blk_id);

/* do allocate block operation 
 * @bmp_type: The bitmap area used to search for available bits
 * @alloc_blk_num: The number of bit need to allocate
 * @addr_list[]:
 * @return 
 */
uint32_t do_alloc(int32_t bmp_type, uint64_t alloc_blk_num, uint64_t addr_list[])
{
	if(bmp_type < START_BMP_ID || bmp_type > END_BMP_ID ||
	   (alloc_blk_num != 0 && addr_list == NULL))
		return EINVAL;
	mem_bmp_pt bmp_info = mem_bmp_arr+bmp_type;
#ifdef ORCHFS_FORMATTER
	const size_t worker = 0;
#else
	const size_t worker = orchfs_async_current_worker();
#endif
	uint64_t allocated = 0;
	for(; allocated < alloc_blk_num; ++allocated)
	{
		const int error = allocate_one(bmp_info, worker, &addr_list[allocated]);
		if(error != 0)
		{
			while(allocated != 0)
				do_dealloc(bmp_type, (int64_t)addr_list[--allocated]);
			return error;
		}
	}
	return 0;
}

uint32_t do_dealloc(int32_t bmp_type, int64_t blk_id)
{
	mem_bmp_pt bmp_info = mem_bmp_arr+bmp_type;
	if(blk_id < 0 || (uint64_t)blk_id >= bmp_info->bmp_capacity * 8)
		goto failed;
	uint8_t* now_dealloc_pt = bmp_info->bmp_start_pt + blk_id/8;
	uint32_t bit_off = 7 - blk_id%8;
	const uint8_t mask = (uint8_t)(1U << bit_off);
	const uint8_t previous = __atomic_fetch_and(
		now_dealloc_pt, (uint8_t)~mask, __ATOMIC_ACQ_REL);
	if((previous & mask) != 0)
	{
		__atomic_fetch_sub(&bmp_info->bmp_used_num, 1, __ATOMIC_RELAXED);
	}
	else
		goto warning;
	return 0;
failed:
	printf("dealloc error, the address is out of range!\n");
	return 4;
// error:
// 	printf("The address is Non-aligned!\n");
// 	return 5;
warning:
	printf("dealloc warning, the block does not exist! -- %d %"PRId64"\n",bmp_type,blk_id);
	return 6;
}

/* inode */
uint64_t alloc_single_inode(int return_type)
{
	uint64_t ret_addr_list[3] = {0};
	int error_info = do_alloc(INODE_BMP, 1, ret_addr_list);
	if(error_info==0)
	{
		if(return_type == RET_BLK_ID)
			return ret_addr_list[0];
		else if(return_type == RET_BLK_ADDR)
			return INODE_BLKID_TO_DEVADDR(ret_addr_list[0]);
		else
			printf("return type error!\n");
	}
	exit(0);
}
int alloc_inodes(int64_t alloc_blk_num, uint64_t addr_list[], int return_type)
{
	int error_info = do_alloc(INODE_BMP, alloc_blk_num, addr_list);
	if(error_info == 0 && return_type == RET_BLK_ADDR)
	{
		// ------ balloc check correct ---------
		for(int64_t i = 0; i < alloc_blk_num; i++)
			addr_list[i] = INODE_BLKID_TO_DEVADDR(addr_list[i]);
	}
	return error_info == 0 ? 0 : ENOSPC;
}
int dealloc_inode(uint64_t dealloc_blk_info, int par_type)
{
	int64_t blk_id = 0;
	if(par_type == PAR_BLK_ID)
		blk_id = dealloc_blk_info;
	else if(par_type == PAR_BLK_ADDR)
		blk_id = INODE_DEVADDR_TO_BLKID(dealloc_blk_info);
	int error_info = do_dealloc(INODE_BMP, blk_id);
	return error_info;
}

/* simple index node */
uint64_t alloc_single_idx_node(int return_type)
{
	uint64_t ret_addr_list[3] = {0};
	int error_info = do_alloc(IDX_NODE_BMP, 1, ret_addr_list);
	if(error_info==0)
	{
		if(return_type == RET_BLK_ID)
			return ret_addr_list[0];
		else if(return_type == RET_BLK_ADDR)
			return IDX_BLKID_TO_DEVADDR(ret_addr_list[0]);
		else
			printf("return type error!\n");
	}
	exit(0);
}
int alloc_idx_nodes(int64_t alloc_blk_num, uint64_t addr_list[], int return_type)
{
	int error_info = do_alloc(IDX_NODE_BMP, alloc_blk_num, addr_list);
	if(error_info == 0 && return_type == RET_BLK_ADDR)
	{
		for(int64_t i = 0; i < alloc_blk_num; i++)
			addr_list[i] = IDX_BLKID_TO_DEVADDR(addr_list[i]);
	}
	return error_info == 0 ? 0 : ENOSPC;
}
int dealloc_idx_node(uint64_t dealloc_blk_info, int par_type)
{
	int64_t blk_id = 0;
	if(par_type == PAR_BLK_ID)
		blk_id = dealloc_blk_info;
	else if(par_type == PAR_BLK_ADDR)
		blk_id = IDX_DEVADDR_TO_BLKID(dealloc_blk_info);
	int error_info = do_dealloc(IDX_NODE_BMP, blk_id);
	return error_info;
}

/* virtural index node */
uint64_t alloc_single_viridx_node(int return_type)
{
	uint64_t ret_addr_list[3] = {0};
	int error_info = do_alloc(VIR_NODE_BMP, 1, ret_addr_list);
	if(error_info==0)
	{
		if(return_type == RET_BLK_ID)
			return ret_addr_list[0];
		else if(return_type == RET_BLK_ADDR)
			return VIRND_BLKID_TO_DEVADDR(ret_addr_list[0]);
		else
			printf("return type error!\n");
	}
	exit(0);
}
int alloc_viridx_nodes(int64_t alloc_blk_num, uint64_t addr_list[], int return_type)
{
	int error_info = do_alloc(VIR_NODE_BMP, alloc_blk_num, addr_list);
	if(error_info == 0 && return_type == RET_BLK_ADDR)
	{
		for(int64_t i = 0; i < alloc_blk_num; i++)
			addr_list[i] = VIRND_BLKID_TO_DEVADDR(addr_list[i]);
	}
	return error_info == 0 ? 0 : ENOSPC;
}
int dealloc_viridx_node(uint64_t dealloc_blk_info, int par_type)
{
	int64_t blk_id = 0;
	if(par_type == PAR_BLK_ID)
		blk_id = dealloc_blk_info;
	else if(par_type == PAR_BLK_ADDR)
		blk_id = VIRND_DEVADDR_TO_BLKID(dealloc_blk_info);
	int error_info = do_dealloc(VIR_NODE_BMP, blk_id);
	return error_info;
}

/* buffer metadata node */
uint64_t alloc_single_bufmeta_node(int return_type)
{
	uint64_t ret_addr_list[3] = {0};
	int error_info = do_alloc(BUFMETA_BMP, 1, ret_addr_list);
	if(error_info==0)
	{
		if(return_type == RET_BLK_ID)
			return ret_addr_list[0];
		else if(return_type == RET_BLK_ADDR)
			return BUFMETA_BLKID_TO_DEVADDR(ret_addr_list[0]);
		else
			printf("return type error!\n");
	}
	exit(0);
}
int alloc_bufmeta_nodes(int64_t alloc_blk_num, uint64_t addr_list[], int return_type)
{
	int error_info = do_alloc(BUFMETA_BMP, alloc_blk_num, addr_list);
	if(error_info == 0 && return_type == RET_BLK_ADDR)
	{
		for(int64_t i = 0; i < alloc_blk_num; i++)
			addr_list[i] = BUFMETA_BLKID_TO_DEVADDR(addr_list[i]);
	}
	return error_info == 0 ? 0 : ENOSPC;
}
int dealloc_bufmeta_node(uint64_t dealloc_blk_info, int par_type)
{
	int64_t blk_id = 0;
	if(par_type == PAR_BLK_ID)
		blk_id = dealloc_blk_info;
	else if(par_type == PAR_BLK_ADDR)
		blk_id = BUFMETA_DEVADDR_TO_BLKID(dealloc_blk_info);
	int error_info = do_dealloc(BUFMETA_BMP, blk_id);
	return error_info;
}

/* nvm page */
uint64_t alloc_single_nvm_page(int return_type)
{
	uint64_t ret_addr_list[3] = {0};
	int error_info = do_alloc(PAGE_BMP, 1, ret_addr_list);
	if(error_info==0)
	{
		if(return_type == RET_BLK_ID)
			return ret_addr_list[0];
		else if(return_type == RET_BLK_ADDR)
			return PAGE_BLKID_TO_DEVADDR(ret_addr_list[0]);
		else
			printf("return type error!\n");
	}
	exit(0);
}
int alloc_nvm_pages(int64_t alloc_blk_num, uint64_t addr_list[], int return_type)
{
	int error_info = do_alloc(PAGE_BMP, alloc_blk_num, addr_list);
	if(error_info == 0 && return_type == RET_BLK_ADDR)
	{
		for(int64_t i = 0; i < alloc_blk_num; i++)
			addr_list[i] = PAGE_BLKID_TO_DEVADDR(addr_list[i]);
	}
	return error_info == 0 ? 0 : ENOSPC;
}
int dealloc_nvm_page(uint64_t dealloc_blk_info, int par_type)
{
	int64_t blk_id = 0;
	if(par_type == PAR_BLK_ID)
		blk_id = dealloc_blk_info;
	else if(par_type == PAR_BLK_ADDR)
		blk_id = PAGE_DEVADDR_TO_BLKID(dealloc_blk_info);
	int error_info = do_dealloc(PAGE_BMP, blk_id);
	return error_info;
}

/* ssd block */
uint64_t alloc_single_ssd_block(int return_type)
{
	uint64_t ret_addr_list[3] = {0};
	int error_info = do_alloc(BLOCK_BMP, 1, ret_addr_list);
	if(error_info==0)
	{
		if(return_type == RET_BLK_ID)
			return ret_addr_list[0];
		else if(return_type == RET_BLK_ADDR)
			return BLOCK_BLKID_TO_DEVADDR(ret_addr_list[0]);
		else
			printf("return type error!\n");
	}
	exit(0);
}
int alloc_ssd_blocks(int64_t alloc_blk_num, uint64_t addr_list[], int return_type)
{
	int error_info = do_alloc(BLOCK_BMP, alloc_blk_num, addr_list);
	if(error_info == 0 && return_type == RET_BLK_ADDR)
	{
		for(int64_t i = 0; i < alloc_blk_num; i++)
			addr_list[i] = BLOCK_BLKID_TO_DEVADDR(addr_list[i]);
	}
	return error_info == 0 ? 0 : ENOSPC;
}
int dealloc_ssd_block(uint64_t dealloc_blk_info, int par_type)
{
	int64_t blk_id = 0;
	if(par_type == PAR_BLK_ID)
		blk_id = dealloc_blk_info;
	else if(par_type == PAR_BLK_ADDR)
		blk_id = BLOCK_DEVADDR_TO_BLKID(dealloc_blk_info);
	int error_info = do_dealloc(BLOCK_BMP, blk_id);
	return error_info;
}

#ifdef __cplusplus
}
#endif
