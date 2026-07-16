#include "lib_socket.h"
#include "req_kernel.h"
#include "libspace.h"
#include "orchfs.h"
#include "meta_cache.h"
#include "lib_dir.h"
#include "io_thdpool.h"
#include "lib_inode.h"
#include "runtime.h"
#include "lib_log.h"
#include "lib_shm.h"
#include "index.h"
#include "../KernelFS/device.h"
#include "../KernelFS/type.h"
#include "../config/config.h"
#include "../config/shm_type.h"

static int libfs_initialized;
static int libfs_owns_device;

static void init_libfs_common(int own_device)
{
	if(libfs_initialized)
		return;
	libfs_owns_device = own_device;
	assert(sizeof(struct orch_dirent) == DIRENT_SIZE);

	if(libfs_owns_device)
		device_init();

	// fprintf(stderr,"init_lib_socket_info!\n");
    init_lib_socket_info();

	// fprintf(stderr,"init_runtime_info!\n");
	init_runtime_info();

    init_all_ext();

    init_metadata_cache();
	
    init_io_thread_pool();

	init_lib_shm(KET_MAGIC_OFFSET);
	
    init_mem_log();

	// fprintf(stderr,"libfs init end1!\n");

    // int64_t root_dir_ino = inode_create(DIR_FILE);
	// // fprintf(stderr,"libfs init end2!\n");
    // dir_file_init(root_dir_ino, root_dir_ino);
	orch_super_blk_pt sb = malloc(ORCH_SUPER_BLK_SIZE);
	read_data_from_devs(sb, ORCH_SUPER_BLK_SIZE, 0);
	// fprintf(stderr,"magic_num: %lld %lld %lld\n",sb->magic_num[0],sb->magic_num[1],sb->magic_num[2]);
	if(sb->magic_num[0] != ORCH_MAGIC_NUM || sb->magic_num[1] != ORCH_MAGIC_NUM || sb->magic_num[2] != ORCH_MAGIC_NUM ) 
		goto fs_type_error;
	change_current_dir_ino(sb->root_inode, sb->root_inode);
	free(sb);
	libfs_initialized = 1;

	// orch_inode_pt check = malloc(ORCH_INODE_SIZE);
	// read_data_from_devs(check, ORCH_INODE_SIZE, OFFSET_INODE);
	// printf("type: %lld\n",check->i_type);
	// free(check);

	return;
fs_type_error:
	printf("fs_type_error!\n");
	fflush(stdout);
	exit(1);
}

void init_libfs()
{
	init_libfs_common(1);
}

void init_libfs_server_core()
{
	init_libfs_common(0);
}

static void close_libfs_common()
{
	if(!libfs_initialized)
		return;

	/* Both the normal I/O workers and the migration workers can still touch the
	 * runtime, metadata cache, extent allocator, shared-memory transport, and
	 * device.  Stop and join them before releasing any of those dependencies. */
	destroy_io_thread_pool();

    free_mem_log();

	// fprintf(stderr,"will close kernelfs1!\n");
    close_metadata_cache();

	// fprintf(stderr,"will close kernelfs2!\n");
    return_all_ext();

	// The migration worker holds pointers into orch_rt, so runtime state is
	// released only after every worker has been joined.
	free_runtime_info();

	free_lib_socket_info();

	// fprintf(stderr,"will close kernelfs5!\n");
	close_lib_shm();

    if(libfs_owns_device)
		device_close();
	libfs_initialized = 0;

	// fprintf(stderr,"will close kernelfs6!\n");

	// fprintf(stderr,"will close kernelfs7!\n");
	// req_kernelfs_close();
}

void close_libfs()
{
	close_libfs_common();
}

void close_libfs_server_core()
{
	close_libfs_common();
}

int orchfs_open (const char *pathname, int oflag, ...)
{
	// printf("open: %s %d %d!\n",pathname,oflag & O_CREAT,lib_register_pid);
	if(pathname == NULL)
	{
		errno = EFAULT;
		return -1;
	}
	if(*pathname == '\0')
	{
		errno = ENOENT;
		return -1;
	}
	int64_t ret_ino_id;
	if(oflag & O_CREAT)
	{
		/* Resolve first so O_EXCL is meaningful and a cold directory cache does
		 * not create a duplicate entry for an already existing file. */
		ret_ino_id = path_to_inode(pathname, NOT_CREATE_PATH);
		if(ret_ino_id >= 0 && (oflag & O_EXCL))
		{
			errno = EEXIST;
			return -1;
		}
		if(ret_ino_id < 0)
			ret_ino_id = path_to_inode(pathname, CREATE_PATH);
	}
	else
	{
		ret_ino_id = path_to_inode(pathname, NOT_CREATE_PATH);
	}
	// fprintf(stderr,"ret_ino_id: %lld\n",ret_ino_id);
	int ret_fd = -1;
	if(ret_ino_id == -1)
		goto can_not_create_file;
	else
	{
		// printf("open info: %" PRId64 " \n",ret_ino_id);
		ret_fd = get_unused_fd(ret_ino_id, oflag);
		// fprintf(stderr,"open info1: %d \n",ret_fd);
		if(ret_fd == -1)
			goto too_many_open_files;
	}
	// printf("open and create: %s %d %d!\n",pathname, file_create_flag, ret_fd);
	// fflush(stdout);
	return ret_fd;
can_not_create_file:
	// printf("can_not_create_file! --- %s\n",pathname);
	// fflush(stdout);
	if(errno == 0)
		errno = ENOENT;
	return -1;
too_many_open_files:
	printf("too_many_open_files!\n");
	fflush(stdout);
	if(errno == 0)
		errno = EMFILE;
	return -1;
}

int orchfs_openat (int dirfd, const char *pathname, int mode, ...)
{
	// printf("openat: %s!\n",pathname);
	if(pathname == NULL)
	{
		errno = EFAULT;
		return -1;
	}
	if(*pathname == '\0')
	{
		errno = ENOENT;
		return -1;
	}
	/* POSIX ignores dirfd for absolute paths. */
	if(*pathname == '/')
		return orchfs_open(pathname, mode, 0);

	int64_t dir_ino_id = -1;
	if(*pathname != '/')
	{
		if(dirfd >= ORCH_MAX_FD || dirfd < 0 ||
		   orch_rt.fd_info[dirfd].used_flag != FD_USED)
			goto dirfd_error;
		orch_inode_pt dir_ino_pt = fd_to_inodept(dirfd);
		dir_ino_id = fd_to_inodeid(dirfd);
		if(dir_ino_pt->i_type != DIR_FILE)
			goto file_mode_error;
	}

	int64_t ret_ino_id;
	if(mode & O_CREAT)
	{
		ret_ino_id = path_to_inode_fromdir(
			dir_ino_id, pathname, NOT_CREATE_PATH);
		if(ret_ino_id >= 0 && (mode & O_EXCL))
		{
			errno = EEXIST;
			return -1;
		}
		if(ret_ino_id < 0)
			ret_ino_id = path_to_inode_fromdir(
				dir_ino_id, pathname, CREATE_PATH);
	}
	else
	{
		ret_ino_id = path_to_inode_fromdir(
			dir_ino_id, pathname, NOT_CREATE_PATH);
	}
	int ret_fd = -1;
	if(ret_ino_id == -1)
		goto can_not_create_file;
	else
	{
		ret_fd = get_unused_fd(ret_ino_id, mode);
		if(ret_fd == -1)
			goto too_many_open_files;
		return ret_fd;
	}
dirfd_error:
	errno = EBADF;
	return -1;
file_mode_error:
	errno = ENOTDIR;
	return -1;
can_not_create_file:
	if(errno == 0)
		errno = ENOENT;
	return -1;
too_many_open_files:
	if(errno == 0)
		errno = EMFILE;
	return -1;
}

int orchfs_close(int fd)
{
// #ifdef COUNT_ON
// 	printf("used_pm_pages: %"PRId64"\n",orch_rt.used_pm_pages-orch_rt.used_pm_units);
// 	printf("used_pm_units: %"PRId64"\n",orch_rt.used_pm_units);
// 	printf("used_ssd_blks: %"PRId64"\n",orch_rt.used_ssd_blks);
// 	printf("used_tree_node: %"PRId64"\n",orch_rt.used_tree_nodes);
// 	printf("used_vir_node: %"PRId64"\n",orch_rt.used_vir_nodes);
// 	printf("write back time: %"PRId64"\n",orch_rt.wback_num);
// 	orch_rt.used_pm_pages = 0;
//     orch_rt.used_pm_units = 0;
//     orch_rt.used_ssd_blks = 0;
//     orch_rt.used_tree_nodes = 0;
// 	orch_rt.used_vir_nodes = 0;
// 	orch_rt.wback_num = 0;
// #endif

// #ifdef COUNT_TIME
// 	int64_t all_pm_size = orch_rt.pm_unit_rwsize + orch_rt.pm_page_rwsize;
// 	printf("index_time: %"PRId64"\n",orch_rt.index_time-orch_rt.wback_time);
// 	printf("io_time: %"PRId64"\n",orch_rt.io_time);
// 	printf("pm_unit_time: %.0f\n",(orch_rt.pm_unit_rwsize*1.0 / all_pm_size) * orch_rt.pm_time);
// 	printf("pm_page_time: %.0f\n",(orch_rt.pm_page_rwsize*1.0 / all_pm_size) * orch_rt.pm_time);
// 	printf("wback_time: %"PRId64"\n",orch_rt.wback_time);
// 	printf("ssd time: %"PRId64"\n",orch_rt.blk_time);
// 	orch_rt.index_time = 0;
//     orch_rt.io_time = 0;
//     orch_rt.pm_unit_rwsize = 0;
//     orch_rt.pm_page_rwsize = 0;
//     orch_rt.wback_time = 0;
//     orch_rt.pm_time = 0;
// 	orch_rt.blk_time = 0;
// #endif

	if(fd < 0 || fd >= ORCH_MAX_FD ||
	   orch_rt.fd_info[fd].used_flag != FD_USED)
	{
		errno = EBADF;
		return -1;
	}
	release_fd(fd);
	return 0;
}

int64_t orchfs_pread(int fd, void *buf, int64_t read_len, int64_t offset)
{
	// fprintf(stderr,"read! %lld %lld\n",read_len,offset);
	int64_t now_file_inoid = fd_to_inodeid(fd);
	if(fd >= ORCH_MAX_FD || now_file_inoid == -1)
		goto fd_error;
	
	// if(orch_rt.fd[fd].flags & O_WRONLY)
	// {
	// 	orch_rt.errorn = EBADF;
	// 	return -1;
	// }

	orch_inode_pt file_ino_pt = fd_to_inodept(fd);
	// printf("ino_id: %lld %lld %lld\n",readino_id, offset, read_len);

	file_lock_rdlock(fd);
	// printf("read info: %lld %lld\n",offset,file_ino_pt->i_size);
	// fflush(stdout);
	if(offset >= file_ino_pt->i_size)
	{
		file_lock_unlock(fd);
		return 0;
	}
	else if(offset + read_len >= file_ino_pt->i_size)
	{
		read_len = file_ino_pt->i_size - offset;
	}
	// int64_t buf_size = malloc_usable_size(buf);
	// printf("buf_size: %lld %lld\n",buf_size,buf);
	// read_from_file(fd, offset, read_len, buf);
	#ifdef NEWBASELINE
	read_from_file_newbaseline(fd, offset, read_len, buf);
	#else
	read_from_file(fd, offset, read_len, buf);
	#endif
	file_lock_unlock(fd);
	return read_len;
fd_error:
	printf("The fd is error! -- pread\n");
	exit(1);
}

int64_t orchfs_pwrite(int fd, const void *buf, int64_t write_len, int64_t offset)
{
	// fprintf(stderr,"write! %lld %lld\n",write_len,offset);
	int64_t now_file_inoid = fd_to_inodeid(fd);
	if(fd >= ORCH_MAX_FD || now_file_inoid == -1)
		goto fd_error;
	
	// if((orch_rt.fd[fd].flags & (O_WRONLY | O_RDWR) == 0))
	// {
	// 	orch_rt.errorn = EBADF;
	// 	return 0;
	// }
	
	orch_inode_pt file_ino_pt = fd_to_inodept(fd);
	// int64_t wino_id = fd_to_inodeid(fd);
	// printf("wino_id: %lld\n",wino_id);
	
	file_lock_rdlock(fd);
	int64_t end = offset + write_len;
	// printf("write info: %lld %lld %lld\n",offset,end,file_ino_pt->i_size);
	if(offset > file_ino_pt->i_size)
	{
		file_lock_unlock(fd);
		return 0;
	}
	else
	{
		// printf("write end!\n");
		int64_t wlen_each_time = ORCH_BLOCK_SIZE*16+ORCH_PAGE_SIZE*7;
		int64_t now_write_len = write_len, now_offset = offset;
		if(end > file_ino_pt->i_size)
		{
			file_lock_unlock(fd);
			file_lock_wrlock(fd);
			// if(now_write_len > 4*1024*1024)
			// {
			// 	int64_t now_buf_addr = (int64_t)buf;
			// 	while(now_write_len > 0)
			// 	{
			// 		if(now_write_len >= wlen_each_time)
			// 		{
			// 			write_into_file(fd, now_offset, wlen_each_time, (void*)now_buf_addr);
			// 			now_offset += wlen_each_time; now_write_len -= wlen_each_time; now_buf_addr += wlen_each_time;
			// 		}
			// 		else
			// 		{
			// 			write_into_file(fd, now_offset, now_write_len, (void*)now_buf_addr);
			// 			now_offset += now_write_len; now_write_len -= now_write_len;  now_buf_addr += now_write_len;
			// 		}
			// 	}
			// }
			// else
			// write_into_file(fd, offset, write_len, buf);

			#ifdef NEWBASELINE
			write_into_file_newbaseline(fd, offset, write_len, buf);
			#else
			write_into_file(fd, offset, write_len, buf);
			#endif
			inode_change_file_size(file_ino_pt, end);
			file_lock_unlock(fd);
		}
		else
		{
			
			// file_lock_rdlock(fd);
			// if(now_write_len > 4*1024*1024)
			// {
			// 	int64_t now_buf_addr = (int64_t)buf;
			// 	while(now_write_len > 0)
			// 	{
			// 		if(now_write_len >= wlen_each_time)
			// 		{
			// 			write_into_file(fd, now_offset, wlen_each_time, (void*)now_buf_addr);
			// 			now_offset += wlen_each_time; now_write_len -= wlen_each_time; now_buf_addr += wlen_each_time;
			// 		}
			// 		else
			// 		{
			// 			write_into_file(fd, now_offset, now_write_len, (void*)now_buf_addr);
			// 			now_offset += now_write_len; now_write_len -= now_write_len;  now_buf_addr += now_write_len;
			// 		}
			// 	}
			// }
			// else
			write_into_file(fd, offset, write_len, buf);
			file_lock_unlock(fd);
		}
	}
	// printf("write info: %lld %lld %lld\n",offset,end,file_ino_pt->i_size);
	return write_len;
fd_error:
	printf("The fd is error! -- pwrite\n");
	exit(1);
}

int64_t orchfs_write(int fd, const void *buf, size_t write_len)
{
	int64_t now_file_inoid = fd_to_inodeid(fd);
	if(fd >= ORCH_MAX_FD || now_file_inoid == -1)
		goto fd_error;
	
	// fprintf(stderr,"fd: %d %lld\n",fd,write_len);

	int64_t offset = get_fd_file_offset(fd);
	if(offset == -1)
		goto fd_error;
	int64_t ret = orchfs_pwrite(fd, buf, write_len, offset);
	if(ret > 0)
	{
		change_fd_file_offset(fd, offset+ret);
	}
	return ret;
fd_error:
	printf("The fd is error! -- write\n");
	exit(1);
}

int64_t orchfs_read(int fd, void *buf, int64_t read_len)
{
	int64_t now_file_inoid = fd_to_inodeid(fd);
	if(fd >= ORCH_MAX_FD || now_file_inoid == -1)
		goto fd_error;
	
	int64_t offset = get_fd_file_offset(fd);
	if(offset == -1)
		goto fd_error;
	// printf("read offset: %lld %lld\n",offset,read_len);
	ssize_t ret = orchfs_pread(fd, buf, read_len, offset);
	if(ret > 0)
	{
		change_fd_file_offset(fd, offset+ret);
	}
	return ret;
fd_error:
	printf("The fd is error! -- read\n");
	exit(1);
}

int orchfs_mkdir(const char *pathname, uint16_t mode)
{
	mode |= S_IFDIR;
	// fprintf(stderr,"path name: %s\n",pathname);
	int ret = create_dirent(pathname);
	return ret;
}

int orchfs_rmdir(const char *pathname)
{
	int64_t rm_ino_id = delete_dirent(pathname, DIR_FILE);
	if(rm_ino_id >= 0)
	{
		return 0;
	}
	else
	{
		return -1;
	}
}

int orchfs_unlink (const char *pathname)
{
    if(pathname == NULL || *pathname == '\0')
        return -1;
	
    int64_t ret_ino_id = path_to_inode(pathname, NOT_CREATE_PATH);
	// printf("ret_ino_id: %lld\n",ret_ino_id);
    if(ret_ino_id < 0)
        return -1;

    int now_fd = get_unused_fd(ret_ino_id, O_RDWR);
    orch_inode_pt ino_pt = fd_to_inodept(now_fd);
	// TODO: Implement the operation of linking
    
	int dir_ret = delete_dirent(pathname, ino_pt->i_type);
	release_fd(now_fd);
	if(dir_ret == -1)
		return dir_ret;
    return 0;
}

int orchfs_fstatfs(int fd, struct statfs *buf)
{
	if(buf == NULL)
	{
		// orch_rt.errorn = EINVAL;
		// printf("fd: %d\n",fd);
		return -1;
	}
	if(fd >= ORCH_MAX_FD || fd < 0)
	{
		// orch_rt.errorn = EBADF;
		// printf("fd: %d\n",fd);
		return -1;
	}
	buf->f_type = EXT2_SUPER_MAGIC;
	buf->f_bsize = ORCH_BLOCK_SIZE;
	buf->f_blocks = MAX_BLOCK_NUM;
	buf->f_bfree = MAX_BLOCK_NUM / 2;
	buf->f_bavail = MAX_BLOCK_NUM / 2;
	buf->f_files = MAX_INODE_NUM;
	buf->f_ffree = MAX_INODE_NUM / 2;
	buf->f_fsid.__val[0] = 12345;
	buf->f_fsid.__val[0] = 54321;
	buf->f_namelen = ORCH_MAX_NAME;
	buf->f_frsize = ORCH_PAGE_SIZE;
	buf->f_flags = 0;
	return 0;
}

int orchfs_lstat (const char *pathname, struct stat *buf)
{
    if(unlikely(buf == NULL))
	{
        // printf("pathname: %s\n",pathname);
		return -1;
    }

    int64_t ino_ret = -1;
    ino_ret = path_to_inode(pathname, NOT_CREATE_PATH);
    if(ino_ret < 0)
	{
        // printf("pathname: %s\n",pathname);
		return -1;
    }

	int now_fd= get_unused_fd(ino_ret, O_RDWR);
    orch_inode_pt c = fd_to_inodept(now_fd);

	pthread_spin_lock(&(c->i_lock));
    buf->st_ino = c->i_number;
    buf->st_mode = c->i_mode;
    buf->st_nlink = c->i_nlink;
    buf->st_uid = c->i_uid;
    buf->st_gid = c->i_gid;
    buf->st_size = c->i_size;
    buf->st_blksize = 4096;

    buf->st_atim = c->i_atim;
    buf->st_mtim = c->i_mtim;
    buf->st_ctim = c->i_ctim;
	pthread_spin_unlock(&(c->i_lock));
	release_fd(now_fd);

    return 0;
}

int orchfs_stat(const char *pathname, struct stat *buf)
{
    return orchfs_lstat (pathname, buf);
}

int orchfs_fstat(int fd, struct stat *buf)
{
    if(unlikely(fd >= ORCH_MAX_FD || fd < 0))
	{
		// orch_rt.errorn = EBADF;
		// printf("fd: %d\n",fd);
		return -1;
	}
    // if(unlikely(orch_rt.fd[fd].flags & O_WRONLY)){
	// 	orch_rt.errorn = EBADF;
	// 	return -1;
	// }
    if(unlikely(buf == NULL)){
        // orch_rt.errorn = EFAULT;
		// printf("fd: %d\n",fd);
        return -1;
    }
     // dax_grant_access(orch_rt.mpk[MPK_DEFAULT]);

    // int64_t ino_id = fd_to_inodeid(fd);
    orch_inode_pt c = fd_to_inodept(fd);

	pthread_spin_lock(&(c->i_lock));
    buf->st_ino = c->i_number;
    buf->st_mode = c->i_mode;
    buf->st_nlink = c->i_nlink;
    buf->st_uid = c->i_uid;
    buf->st_gid = c->i_gid;
    buf->st_size = c->i_size;
    buf->st_blksize = 4096;

    buf->st_atim = c->i_atim;
    buf->st_mtim = c->i_mtim;
    buf->st_ctim = c->i_ctim;
	pthread_spin_unlock(&(c->i_lock));
    return 0;
}

int orchfs_lseek(int fd, int64_t offset, int whence)
{
	if(fd < 0 || fd >= ORCH_MAX_FD ||
	   orch_rt.fd_info[fd].used_flag != FD_USED)
	{
		errno = EBADF;
		return -1;
	}
	int64_t base;
	switch (whence)
	{
	case SEEK_SET:
		base = 0;
		break;

	case SEEK_CUR:
		base = get_fd_file_offset(fd);
		break;

	case SEEK_END:
		base = fd_to_inodept(fd)->i_size;
		break;
	
	default:
		errno = EINVAL;
		return -1;
	}

	int64_t new_offset;
	if(__builtin_add_overflow(base, offset, &new_offset))
	{
		errno = EOVERFLOW;
		return -1;
	}
	if(new_offset < 0)
	{
		errno = EINVAL;
		return -1;
	}
	change_fd_file_offset(fd, new_offset);
	return 0;
}

/*
 * A directory stream is an encoded OrchFS descriptor, not a libc DIR object.
 * Store fd + 1 so descriptor zero remains distinguishable from the NULL error
 * return.  Normal file descriptors keep their existing integer values.
 */
static DIR *orchfs_encode_dirfd(int fd)
{
	if(fd < 0 || fd >= ORCH_MAX_FD)
	{
		errno = EBADF;
		return NULL;
	}
	return (DIR *)(uintptr_t)((uint64_t)fd + 1);
}

int orchfs_dirfd(DIR *dirp)
{
	uintptr_t encoded = (uintptr_t)dirp;
	if(encoded == 0 || encoded > ORCH_MAX_FD)
	{
		errno = EBADF;
		return -1;
	}
	return (int)(encoded - 1);
}

DIR* orchfs_opendir(const char *_pathname)
{
	int ret_fd = orchfs_open(_pathname, O_RDONLY);
	if(ret_fd == -1)
	{
		// printf("aaaaaaaaaa\n");
		return NULL;
	}
	// printf("dir fd: %d\n",ret_fd);
	// fflush(stdout);
	orch_inode_pt ino_pt = fd_to_inodept(ret_fd);
	if(ino_pt->i_type != DIR_FILE)
	{
		orchfs_close(ret_fd);
		errno = ENOTDIR;
		// printf("aaaaaaaaaa\n");
		// fflush(stdout);
		return NULL;
	}
	change_fd_file_offset(ret_fd, DIRENT_SIZE*2);
	return orchfs_encode_dirfd(ret_fd);
}

DIR* orchfs_fdopendir(int fd)
{
	if(fd < 0 || fd >= ORCH_MAX_FD ||
	   orch_rt.fd_info[fd].used_flag != FD_USED)
	{
		errno = EBADF;
		return NULL;
	}

	orch_inode_pt ino_pt = fd_to_inodept(fd);
	if(ino_pt->i_type != DIR_FILE)
	{
		errno = ENOTDIR;
		return NULL;
	}

	/*
	 * Directory iteration owns an independent file offset.  Duplicate the
	 * already-resolved inode instead of looking its pathname up again: the
	 * source may have been renamed or unlinked since it was opened.
	 */
	int directory_fd = get_unused_fd(fd_to_inodeid(fd), O_RDONLY);
	if(directory_fd < 0)
	{
		if(errno == 0)
			errno = EMFILE;
		return NULL;
	}
	change_fd_file_offset(directory_fd, DIRENT_SIZE*2);
	return orchfs_encode_dirfd(directory_fd);
}


/*target*/
struct dirent * orchfs_readdir(DIR *dirp)
{
	int fd = orchfs_dirfd(dirp);
	if(fd < 0)
	{
		return NULL;
	}
	// if(orch_rt.fd[fd].flags & O_WRONLY){
	// 	orch_rt.errorn = EBADF;
	// 	return NULL;
	// }
	orch_inode_pt ino_pt = fd_to_inodept(fd);
	if(ino_pt->i_type != DIR_FILE)
	{
		return NULL;
	}
	// ino_t ino_id = fd_to_inodeid(fd);

	// 读取目录文件
    int64_t now_dir_fsize = ino_pt->i_size;
    orch_dirent_pt dir_file_pt = malloc(now_dir_fsize);
    file_lock_rdlock(fd);
    read_from_file(fd, 0, now_dir_fsize, (void*)dir_file_pt); 
    file_lock_unlock(fd);

	int64_t dir_num = now_dir_fsize / DIRENT_SIZE;
	orch_dirent_pt target_pt = malloc(DIRENT_SIZE);
	int64_t start_pos = get_fd_file_offset(fd) / DIRENT_SIZE;
	// printf("startpos: %lld %lld %lld\n",start_pos,dir_num,sizeof(orch_dirent_t));
	// fflush(stdout);
	int find_flag = 0;
    for(int64_t j = start_pos; j < dir_num; j++)
    {
        if(dir_file_pt[j].d_type != INVALID_T)
        {
            memcpy(target_pt, dir_file_pt + j, DIRENT_SIZE);
            find_flag = 1;
			change_fd_file_offset(fd, DIRENT_SIZE*(j+1));
            break;
        }
    }
	if(find_flag == 1)
	{
		orch_rt.fd_info[fd].temp_dirent.d_ino = target_pt->d_ino;
		orch_rt.fd_info[fd].temp_dirent.d_off = target_pt->d_off;
		orch_rt.fd_info[fd].temp_dirent.d_reclen = target_pt->d_namelen;
		orch_rt.fd_info[fd].temp_dirent.d_type = target_pt->d_type;
		strcpy(orch_rt.fd_info[fd].temp_dirent.d_name, target_pt->d_name);
		free(dir_file_pt);
		free(target_pt);
		// printf("addr %d %lld\n",fd,&(orch_rt.fd_info[fd].temp_dirent));
		// printf("%d\n",sizeof(struct dirent *));
		// fflush(stdout);
		return &(orch_rt.fd_info[fd].temp_dirent);
	}
	else
	{
		free(dir_file_pt);
		free(target_pt);
		return NULL;
	}
}

int orchfs_closedir(DIR *dirp)
{
	int fd = orchfs_dirfd(dirp);
	return fd < 0 ? -1 : orchfs_close(fd);
}

int orchfs_truncate(const char *path, size_t length)
{
	// printf("called truncate: %s, %lu\n", path, length);
	int64_t ret_ino_id = path_to_inode(path, NOT_CREATE_PATH);
	if(ret_ino_id < 0)
	{
		return -1;
    }
	int now_fd = get_unused_fd(ret_ino_id, O_RDWR);
	if(now_fd < 0)
		return -1;
	int ret = orchfs_ftruncate(now_fd, length);
	int saved_errno = errno;
	release_fd(now_fd);
	errno = saved_errno;
	return ret;
}

int orchfs_ftruncate(int fd, size_t length)
{
	if(fd < 0 || fd >= ORCH_MAX_FD || orch_rt.fd_info[fd].used_flag != FD_USED)
	{
		errno = EBADF;
		return -1;
	}
	if((orch_rt.fd_info[fd].flags & O_ACCMODE) == O_RDONLY)
	{
		errno = EBADF;
		return -1;
	}
	orch_inode_pt ino_pt = fd_to_inodept(fd);
	if(ino_pt->i_type != SIMPLE_FILE)
	{
		errno = EISDIR;
		return -1;
	}
	const uint64_t max_file_size =
		UINT64_C(1) << (KEY_LEN + ORCH_BLOCK_BW);
	if((uint64_t)length > max_file_size || length > (size_t)INT64_MAX)
	{
		errno = EFBIG;
		return -1;
	}
	file_lock_wrlock(fd);
	if(length > (size_t)ino_pt->i_size)
	{
		int64_t now_offset = ino_pt->i_size;
		int64_t now_write_len = (int64_t)length - now_offset;
		void* buf = calloc(1, (size_t)now_write_len);
		if(buf == NULL)
		{
			file_lock_unlock(fd);
			errno = ENOMEM;
			return -1;
		}
		write_into_file(fd, now_offset, now_write_len, buf);
		free(buf);
		inode_change_file_size(ino_pt, (int64_t)length);
	}
	else
	{
		inode_change_file_size(ino_pt, (int64_t)length);
	}
	file_lock_unlock(fd);
	return 0;
}

int orchfs_fsync(int fd)
{
	if(fd < 0 || fd >= ORCH_MAX_FD || orch_rt.fd_info[fd].used_flag != FD_USED)
	{
		errno = EBADF;
		return -1;
	}
	return device_sync();
}

int64_t orchfs_tell(int fd)
{
	if(fd < 0 || fd >= ORCH_MAX_FD || orch_rt.fd_info[fd].used_flag != FD_USED)
	{
		errno = EBADF;
		return -1;
	}
	return get_fd_file_offset(fd);
}

int64_t orchfs_fd_inode_id(int fd)
{
	if(fd < 0 || fd >= ORCH_MAX_FD || orch_rt.fd_info[fd].used_flag != FD_USED)
	{
		errno = EBADF;
		return -1;
	}
	return fd_to_inodeid(fd);
}

int orchfs_fd_file_type(int fd)
{
	if(fd < 0 || fd >= ORCH_MAX_FD ||
	   orch_rt.fd_info[fd].used_flag != FD_USED)
	{
		errno = EBADF;
		return ORCHFS_FILE_TYPE_ERROR;
	}

	orch_inode_pt inode = fd_to_inodept(fd);
	if(inode->i_type == DIR_FILE)
		return ORCHFS_FILE_TYPE_DIRECTORY;
	if(inode->i_type == SIMPLE_FILE)
		return ORCHFS_FILE_TYPE_REGULAR;
	return ORCHFS_FILE_TYPE_UNKNOWN;
}


// int orchfs_ftruncate(int fd, size_t len, int op){
// 	printf("called ftruncate: %d, %lu\n", fd, len);
// 	if(fd >= ORCH_MAX_FD || orch_rt.fd[fd].inode == NULL){
// 		orch_rt.errorn = EBADF;
// 		return -1;
// 	}
// 	if((orch_rt.fd[fd].flags & (O_WRONLY | O_RDWR)) == 0){
// 		orch_rt.errorn = EBADF;
// 		return -1;
// 	}
// 	 // dax_grant_access(orch_rt.mpk[MPK_DEFAULT]);
// 	ino_t inode_n = orch_rt.fd[fd].inode->i_number;
// 	inode_rw_lock(inode_n);
// 	if(len > orch_rt.fd[fd].inode->i_size[op]){
// 		inode_resize2(orch_rt.fd[fd].inode, len);
// 	}
// 	else{
// 		orch_rt.fd[fd].inode->i_size[op] = len;
// 	}
// 	inode_wb(orch_rt.fd[fd].inode);
// 	inode_rw_unlock(inode_n);
// 	 // dax_stop_access(orch_rt.mpk[MPK_DEFAULT]);
// 	return 0;
// }

// /*待修改*/
// int orchfs_fstatfs(int fd, struct statfs *buf){
// 	if(buf == NULL){
// 		orch_rt.errorn = EINVAL;
// 		return -1;
// 	}
// 	if(fd >= ORCH_MAX_FD || orch_rt.fd[fd].inode == NULL){
// 		orch_rt.errorn = EBADF;
// 		return -1;
// 	}
// 	buf->f_type = EXT2_SUPER_MAGIC;
// 	buf->f_bsize = ORCH_PAGE_SIZE;
// 	buf->f_blocks = ORCH_DAX_ALLOC_SIZE / ORCH_PAGE_SIZE;
// 	buf->f_bfree = ORCH_DAX_ALLOC_SIZE / ORCH_PAGE_SIZE / 2;
// 	buf->f_bavail = ORCH_DAX_ALLOC_SIZE / ORCH_PAGE_SIZE / 2;
// 	buf->f_files = SIZE_MAX_INODE;
// 	buf->f_ffree = SIZE_MAX_INODE / 2;
// 	buf->f_fsid.__val[0] = 12345;
// 	buf->f_fsid.__val[0] = 54321;
// 	buf->f_namelen = ORCH_MAX_NAME;
// 	buf->f_frsize = ORCH_PAGE_SIZE;
// 	buf->f_flags = 0;
// 	return 0;
// }

static int split_rename_path(const char *path, char parent[4096],
	char basename[ORCH_DIRENT_NAME_MAX + 1])
{
	if(path == NULL)
	{
		errno = EFAULT;
		return -1;
	}
	if(*path == '\0')
	{
		errno = ENOENT;
		return -1;
	}

	const char *last_slash = strrchr(path, '/');
	const char *name = last_slash == NULL ? path : last_slash + 1;
	const size_t name_len = strlen(name);
	if(name_len == 0 || strcmp(name, ".") == 0 || strcmp(name, "..") == 0)
	{
		errno = EINVAL;
		return -1;
	}
	if(name_len > ORCH_DIRENT_NAME_MAX)
	{
		errno = ENAMETOOLONG;
		return -1;
	}

	if(last_slash == NULL)
	{
		strcpy(parent, ".");
	}
	else
	{
		const size_t parent_len = (size_t)(last_slash - path);
		int root_parent = 1;
		for(size_t i = 0; i < parent_len; i++)
		{
			if(path[i] != '/')
			{
				root_parent = 0;
				break;
			}
		}
		if(root_parent)
		{
			/* path_to_inode cannot resolve a slash-only spelling, while the
			 * root directory's "." entry resolves to the same inode. */
			strcpy(parent, "/.");
		}
		else
		{
			if(parent_len >= 4096)
			{
				errno = ENAMETOOLONG;
				return -1;
			}
			memcpy(parent, path, parent_len);
			parent[parent_len] = '\0';
		}
	}

	memcpy(basename, name, name_len + 1);
	return 0;
}

int orchfs_rename(const char *oldpath, const char *newpath)
{
	char old_parent[4096] = {0};
	char new_parent[4096] = {0};
	char old_name[ORCH_DIRENT_NAME_MAX + 1] = {0};
	char new_name[ORCH_DIRENT_NAME_MAX + 1] = {0};
	if(split_rename_path(oldpath, old_parent, old_name) != 0 ||
	   split_rename_path(newpath, new_parent, new_name) != 0)
		return -1;

	errno = 0;
	const inode_id_t old_parent_inode =
		path_to_inode(old_parent, NOT_CREATE_PATH);
	if(old_parent_inode < 0)
	{
		if(errno == 0)
			errno = ENOENT;
		return -1;
	}
	errno = 0;
	const inode_id_t new_parent_inode =
		path_to_inode(new_parent, NOT_CREATE_PATH);
	if(new_parent_inode < 0)
	{
		if(errno == 0)
			errno = ENOENT;
		return -1;
	}
	if(old_parent_inode != new_parent_inode)
	{
		errno = EXDEV;
		return -1;
	}
	return file_rename(old_parent_inode, old_name, new_name);
}

int orchfs_fcntl(int fd, int cmd, ...)
{
	va_list ap;
	if(fd < 0 || fd >= ORCH_MAX_FD ||
	   orch_rt.fd_info[fd].used_flag != FD_USED)
	{
		errno = EBADF;
		return -1;
	}
	switch (cmd)
	{
	case F_GETFL:
		// printf("the flags are: %x\n", orch_rt.fd[fd].flags);
		return orch_rt.fd_info[fd].flags;
		break;
	case F_SETFL:
		va_start(ap, cmd);
		{
			int new_flags = va_arg(ap, int);
			int mutable_flags = O_APPEND | O_NONBLOCK;
			orch_rt.fd_info[fd].flags =
				(orch_rt.fd_info[fd].flags & O_ACCMODE) |
				(new_flags & mutable_flags);
		}
		va_end(ap);
		return 0;
	default:
		errno = EOPNOTSUPP;
		return -1;
	}
}
