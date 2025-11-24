/**
 * basic_fs.c (improved)
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

/* 백엔드 데이터 디렉토리 (추후 인자화 가능) */
static const char *DIR_PATH = "/tmp/fuse_data";

/* 안전한 전체 경로 생성: fpath_out 크기를 인자로 받아 overflow 방지 */
static void get_full_path(const char *path, char *fpath_out, size_t out_size)
{
    /* path은 FUSE가 '/'로 최소한 전달하므로 간단히 결합 */
    /* snprintf는 null-terminated 결과를 보장함(출력 잘림 시에도 안전) */
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
        /* 얻은 이름에 대해 실제 lstat 호출하여 정확한 stat 정보 얻기 */
        struct stat st;
        memset(&st, 0, sizeof(st));

        /* build child path */
        char child[PATH_MAX];
        /* handle root "/" path special case to avoid double slashes, snprintf handles it */
        snprintf(child, sizeof(child), "%s/%s", fpath, de->d_name);

        if (lstat(child, &st) == -1) {
            /* skip entries we can't stat, but continue */
            continue;
        }

        /* filler: (buf, name, statbuf, off, flags) -- older fuse versions use different sig,
           but passing 0 for off and 0 for flags is OK for many cases */
        if (filler(buf, de->d_name, &st, 0, 0))
            break;
    }

    closedir(dp);
    return 0;
}

/* 3. create: 파일 생성 (적절한 플래그 사용) */
static int basic_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    char fpath[PATH_MAX];
    get_full_path(path, fpath, sizeof(fpath));

    /* create은 O_CREAT + 주로 쓰기 전용. 기존 예제들처럼 명시적으로 설정 */
    int flags = O_CREAT | O_WRONLY;
    /* 만약 사용자가 쓰기 이외 플래그를 원하면 fi->flags를 참고하여 추가 가능 */
    if (fi && (fi->flags & O_APPEND))
        flags |= O_APPEND;

    int fd = open(fpath, flags, mode);
    if (fd == -1)
        return -errno;

    fi->fh = (uint64_t) fd;

    /* 향후 초기 HMAC 생성 여기에 추가 */

    return 0;
}

/* 4. open */
static int basic_open(const char *path, struct fuse_file_info *fi)
{
    char fpath[PATH_MAX];
    get_full_path(path, fpath, sizeof(fpath));

    /* fi->flags를 그대로 사용 (FUSE가 전달한 플래그) */
    int fd = open(fpath, fi->flags);
    if (fd == -1)
        return -errno;

    fi->fh = (uint64_t) fd;

    /* 향후 HMAC 검증 준비 로직 */

    return 0;
}

/* 5. read */
static int basic_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi)
{
    (void) path; /* path는 fi->fh가 있으면 필요 없음 */
    ssize_t res = pread((int)fi->fh, buf, size, offset);
    if (res == -1)
        return -errno;

    /* 향후 읽은 데이터에 대한 HMAC 검증 로직 */

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

    /* 향후 쓰기 후 HMAC 재계산 및 원자적 갱신 로직 */

    return (int)size;
}

/* 7. unlink */
static int basic_unlink(const char *path)
{
    char fpath[PATH_MAX];
    get_full_path(path, fpath, sizeof(fpath));

    if (unlink(fpath) == -1)
        return -errno;

    /* 향후 xattr 등 HMAC 메타데이터 삭제 로직 추가 */

    return 0;
}

/* 8. rename */
static int basic_rename(const char *from, const char *to, unsigned int flags)
{
    /* 이 구현은 flags를 지원하지 않음 (간단 구현). flags가 주어지면 에러 반환 */
    if (flags)
        return -EINVAL;

    char ffrom[PATH_MAX];
    char fto[PATH_MAX];
    get_full_path(from, ffrom, sizeof(ffrom));
    get_full_path(to, fto, sizeof(fto));

    if (rename(ffrom, fto) == -1)
        return -errno;

    /* 향후 sidecar DB 동기화 등 처리 */

    return 0;
}

/* 9. release (필수) - open/create에서 할당한 fd를 닫음 */
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

    /* utimensat 시 path가 절대/상대 경로 문제 없도록 AT_FDCWD 사용 */
    if (utimensat(AT_FDCWD, fpath, ts, 0) == -1)
        return -errno;

    return 0;
}

/* init */
static void *basic_init(struct fuse_conn_info *conn, struct fuse_config *cfg)
{
    (void) conn;
    (void) cfg;

    /* 기본 키로드 등 초기화시 필요한 데이터 구조를 여기에 할당 가능 */
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
    /* 예: mkdir(DIR_PATH, 0755); // 주의: race condition 가능 */

    printf("Mounting Basic FUSE FS...\n");
    printf("Target Storage: %s\n", DIR_PATH);

    return fuse_main(argc, argv, &basic_oper, NULL);
}