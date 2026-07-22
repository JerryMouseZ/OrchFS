// Intentionally no include guard: callers define one of the field callbacks
// and include this table wherever the shared schema must be expanded.

#ifdef ORCHFS_ASYNC_FILE_STAT_FIELD
ORCHFS_ASYNC_FILE_STAT_FIELD(std::uint64_t, device, st_dev)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::uint64_t, inode, st_ino)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::uint64_t, mode, st_mode)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::uint64_t, link_count, st_nlink)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::uint64_t, uid, st_uid)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::uint64_t, gid, st_gid)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::uint64_t, rdev, st_rdev)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::int64_t, size, st_size)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::int64_t, block_size, st_blksize)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::int64_t, blocks, st_blocks)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::int64_t, atime_seconds, st_atim.tv_sec)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::int64_t, atime_nanoseconds, st_atim.tv_nsec)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::int64_t, mtime_seconds, st_mtim.tv_sec)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::int64_t, mtime_nanoseconds, st_mtim.tv_nsec)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::int64_t, ctime_seconds, st_ctim.tv_sec)
ORCHFS_ASYNC_FILE_STAT_FIELD(std::int64_t, ctime_nanoseconds, st_ctim.tv_nsec)
#endif

#ifdef ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD
ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD(std::uint64_t, type, f_type)
ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD(std::uint64_t, block_size, f_bsize)
ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD(std::uint64_t, blocks, f_blocks)
ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD(std::uint64_t, blocks_free, f_bfree)
ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD(std::uint64_t, blocks_available, f_bavail)
ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD(std::uint64_t, files, f_files)
ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD(std::uint64_t, files_free, f_ffree)
ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD(std::uint64_t, name_length, f_namelen)
ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD(std::uint64_t, fragment_size, f_frsize)
ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD(std::uint64_t, flags, f_flags)
#endif
