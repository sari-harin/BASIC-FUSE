/**
 * basic_fs.c
 * gcc -Wall basic_fs.c `pkg-config fuse3 --cflags --libs` -o basic_fs
 *
 * 사용법:
 * 1. 백엔드 데이터 디렉토리 생성: mkdir -p /tmp/fuse_data
 * 2. 마운트 포인트 생성: mkdir -p /tmp/fuse_mnt
 * 3. 실행: ./basic_fs /tmp/fuse_mnt
 * 4. 테스트: echo "hello" > /tmp/fuse_mnt/test.txt
 * 5. 언마운트: fusermount3 -u /tmp/fuse_mnt
 */

#define FUSE_USE_VERSION 31

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <assert.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <limits.h>
#include <sys/stat.h>
#include <time.h>
#include <stdlib.h>
#include <sys/time.h>

static const char *DIR_PATH = "/tmp/fuse_data";

static void get_full_path(const char *path, char *fpath_out, size_t out_size)
{
    snprintf(fpath_out, out_size, "%s%s", DIR_PATH, path);
}

/* 1. getattr */
static int basic_getattr(const char *path, struct stat *stbuf,
                         struct fuse_file_info *fi)
{
    (void) fi;
    char fpath[PATH_MAX];
    get_full_path(path, fpath, sizeof(fpath));

    if (lstat(fpath, stbuf) == -1)
        return -errno;

    return 0;
}

/* 2. readdir */
static int basic_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi,
                         enum fuse_readdir_flags flags)
{
    (void) offset;
    (void) fi;
    (void) flags;

    char fpath[PATH_MAX];
    get_full_path(path, fpath, sizeof(fpath));

    DIR *dp = opendir(fpath);
    if (dp == NULL)
        return -errno;

    struct dirent *de;
    while ((de = readdir(dp)) != NULL) {
        struct stat st;
        memset(&st, 0, sizeof(st));

        char child[PATH_MAX];
        snprintf(child, sizeof(child), "%s/%s", fpath, de->d_name);

        if (lstat(child, &st) == -1) {
            continue;
        }

        if (filler(buf, de->d_name, &st, 0, 0))
            break;
    }

    closedir(dp);
    return 0;
}

/* 3. create: 파일 생성 */
static int basic_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    char fpath[PATH_MAX];
    get_full_path(path, fpath, sizeof(fpath));

    int flags = O_CREAT | O_WRONLY;
    if (fi && (fi->flags & O_APPEND))
        flags |= O_APPEND;

    int fd = open(fpath, flags, mode);
    if (fd == -1)
        return -errno;

    fi->fh = (uint64_t) fd;

    return 0;
}

/* 4. open */
static int basic_open(const char *path, struct fuse_file_info *fi)
{
    char fpath[PATH_MAX];
    get_full_path(path, fpath, sizeof(fpath));

    int fd = open(fpath, fi->flags);
    if (fd == -1)
        return -errno;

    fi->fh = (uint64_t) fd;

    return 0;
}

/* 5. read */
static int basic_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi)
{
    (void) path;
    ssize_t res = pread((int)fi->fh, buf, size, offset);
    if (res == -1)
        return -errno;

    return (int)res;
}

/* 6. write (partial write 처리) */
static int basic_write(const char *path, const char *buf, size_t size,
                       off_t offset, struct fuse_file_info *fi)
{
    (void) path;

    size_t to_write = size;
    off_t off = offset;
    const char *p = buf;

    while (to_write > 0) {
        ssize_t written = pwrite((int)fi->fh, p, to_write, off);
        if (written == -1) {
            if (errno == EINTR)
                continue;
            return -errno;
        }
        to_write -= written;
        p += written;
        off += written;
    }

    return (int)size;
}

/* 7. unlink */
static int basic_unlink(const char *path)
{
    char fpath[PATH_MAX];
    get_full_path(path, fpath, sizeof(fpath));

    if (unlink(fpath) == -1)
        return -errno;

    return 0;
}

/* 8. rename */
static int basic_rename(const char *from, const char *to, unsigned int flags)
{
    if (flags)
        return -EINVAL;

    char ffrom[PATH_MAX];
    char fto[PATH_MAX];
    get_full_path(from, ffrom, sizeof(ffrom));
    get_full_path(to, fto, sizeof(fto));

    if (rename(ffrom, fto) == -1)
        return -errno;

    return 0;
}

/* 9. release */
static int basic_release(const char *path, struct fuse_file_info *fi)
{
    (void) path;
    int fd = (int) fi->fh;
    if (fd >= 0) {
        close(fd);
        fi->fh = 0;
    }
    return 0;
}

/* 10. mkdir */
static int basic_mkdir(const char *path, mode_t mode)
{
    char fpath[PATH_MAX];
    get_full_path(path, fpath, sizeof(fpath));

    if (mkdir(fpath, mode) == -1)
        return -errno;

    return 0;
}

/* 11. rmdir */
static int basic_rmdir(const char *path)
{
    char fpath[PATH_MAX];
    get_full_path(path, fpath, sizeof(fpath));

    if (rmdir(fpath) == -1)
        return -errno;

    return 0;
}

/* 12. chmod */
static int basic_chmod(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    (void) fi;
    char fpath[PATH_MAX];
    get_full_path(path, fpath, sizeof(fpath));

    if (chmod(fpath, mode) == -1)
        return -errno;

    return 0;
}

/* 13. truncate */
static int basic_truncate(const char *path, off_t size, struct fuse_file_info *fi)
{
    (void) fi;
    char fpath[PATH_MAX];
    get_full_path(path, fpath, sizeof(fpath));

    if (truncate(fpath, size) == -1)
        return -errno;

    return 0;
}

/* 14. utimens */
static int basic_utimens(const char *path, const struct timespec ts[2],
                         struct fuse_file_info *fi)
{
    (void) fi;
    char fpath[PATH_MAX];
    get_full_path(path, fpath, sizeof(fpath));

    if (utimensat(AT_FDCWD, fpath, ts, 0) == -1)
        return -errno;

    return 0;
}

/* init */
static void *basic_init(struct fuse_conn_info *conn, struct fuse_config *cfg)
{
    (void) conn;
    (void) cfg;

    printf("[INFO] Basic FS Initialized. Backend: %s\n", DIR_PATH);

    return NULL;
}

/* FUSE operations 매핑 */
static struct fuse_operations basic_oper = {
    .init       = basic_init,
    .getattr    = basic_getattr,
    .readdir    = basic_readdir,
    .create     = basic_create,
    .open       = basic_open,
    .read       = basic_read,
    .write      = basic_write,
    .unlink     = basic_unlink,
    .rename     = basic_rename,
    .release    = basic_release,
    .mkdir      = basic_mkdir,
    .rmdir      = basic_rmdir,
    .chmod      = basic_chmod,
    .truncate   = basic_truncate,
    .utimens    = basic_utimens,
};

int main(int argc, char *argv[])
{
    /* 백엔드 디렉토리 존재 여부 확인 권장(없으면 생성하거나 에러 처리) */

    printf("Mounting Basic FUSE FS...\n");
    printf("Target Storage: %s\n", DIR_PATH);

    return fuse_main(argc, argv, &basic_oper, NULL);
}