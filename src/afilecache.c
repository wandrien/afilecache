#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <stdlib.h>


int cp(const char *to, const char *from)
{
    int fd_to, fd_from;
    ssize_t nread;
    int saved_errno;
    const int buf_size = 4096 * 64;
    char * buf = NULL;

    fd_from = open(from, O_RDONLY);
    if (fd_from < 0)
        return -1;

    buf = malloc(buf_size);
    if (!buf)
        goto out_error;


    fd_to = open(to, O_WRONLY | O_CREAT | O_EXCL, 0666);
    if (fd_to < 0)
        goto out_error;

    while (nread = read(fd_from, buf, buf_size), nread > 0)
    {
        char *out_ptr = buf;
        ssize_t nwritten;

        do {
            nwritten = write(fd_to, out_ptr, nread);

            if (nwritten >= 0)
            {
                nread -= nwritten;
                out_ptr += nwritten;
            }
            else if (errno != EINTR)
            {
                goto out_error;
            }
        } while (nread > 0);
    }

    if (nread == 0)
    {
        if (close(fd_to) < 0)
        {
            fd_to = -1;
            goto out_error;
        }
        close(fd_from);

        free(buf);

        /* Success! */
        return 0;
    }

  out_error:
    saved_errno = errno;

    close(fd_from);
    if (fd_to >= 0)
        close(fd_to);

    if (buf)
        free(buf);

    errno = saved_errno;
    return -1;
}


const char * progname = "afilecache";

static void perrorf(const char * format, ...)
{
    char buf[512];

    va_list ap;
    va_start(ap, format); 

    memset(buf, 0, 512);
    vsnprintf(buf, 500, format, ap);
    va_end(ap);

    perror(buf);
}


typedef struct _str_buffer_t {
    char * str;
    size_t size;
    size_t len;
} str_buffer_t;

static void str_buffer_extend(str_buffer_t * buffer, size_t inc_len)
{
    int new_len = buffer->len + inc_len;
    if (buffer->size > 1 && new_len < buffer->size - 1)
        return;

    buffer->size = new_len + 16 + new_len / 2;
    buffer->str = realloc(buffer->str, buffer->size);

    if (!buffer->str)
    {
        fprintf(stderr, "%s: Internal error: failed to allocate %u bytes\n", progname, buffer->size);
        abort();
    }
}

static void str_buffer_join(str_buffer_t * buffer, const char * str)
{
    if (!str)
        return;

    size_t str_len = strlen(str);
    str_buffer_extend(buffer, str_len);
    memcpy(buffer->str + buffer->len, str, str_len);
    buffer->len += str_len;
    buffer->str[buffer->len] = 0;
}

static void str_buffer_join_char(str_buffer_t * buffer, char ch)
{
    str_buffer_extend(buffer, 1);
    buffer->str[buffer->len++] = ch;
    buffer->str[buffer->len] = 0;
}


static char * str_join_path(const char * arg, ...)
{
    if (!arg)
        return NULL;

    str_buffer_t buffer = {0, 0, 0};

    str_buffer_join(&buffer, arg);

    va_list ap;
    va_start(ap, arg);

    while (1)
    {
        const char * s = va_arg(ap, const char *);
        if (!s)
            break;
        if ((buffer.len == 0 || buffer.str[buffer.len - 1] != '/') && s[0] != '/')
            str_buffer_join(&buffer, "/");
        str_buffer_join(&buffer, s);
    }

    va_end(ap);

    return buffer.str;
}


enum RETCODES {
    RET_USAGE = 1,
    RET_MISS = 2,
    RET_INTERNAL = 3,
    RET_NO_CACHE_DIR = 4,
    RET_FILE_OPS = 5,
    RET_LOCK = 6
};


static char * encode_id(const char * id)
{
    if (!id)
        return NULL;

    str_buffer_t buffer = {0, 0, 0};
    str_buffer_extend(&buffer, strlen(id));

    char esc[10];

    while (*id)
    {
        char ch = *id;
        if (ch < ' ' || ch == '*' || ch == '?' || ch == '/' || ch == '\\'  || ch == '"' || ch == '\'' || ch == '%')
        {
            snprintf(esc, 5, "%%%u", (unsigned)ch);
            str_buffer_join(&buffer, esc);
        }
        else
        {
            str_buffer_join_char(&buffer, ch);
        }
        id++;
    }

    return buffer.str;
}

char * get_subdir_for_id(const char * id)
{
    unsigned base = ('z' - 'a');

    unsigned long s = 0;
    while (*id)
    {
        unsigned char ch = (unsigned char) *id;
        unsigned char s1 = (unsigned char) (s >> 24);
        s = (s << 8) + (ch ^ s1);
        id++;
    }

    char buf[10];
    int i;
    for (i = 0; i < 4; i++)
    {
        buf[i] = (s % base) + 'a';
        s /= base;
    }
    buf[i] = 0;

    return strdup(buf);
}

typedef struct _cache_entry_path_t {
    char * dirname;
    char * filename;
    char * relpath;
    char * fullpath;
    char * dirfullpath;
} cache_entry_path_t;

static void cache_id_to_path(const char * cache_path, const char * cache_id, cache_entry_path_t * cache_entry_path)
{
    cache_entry_path->filename = encode_id(cache_id);
    cache_entry_path->dirname  = get_subdir_for_id(cache_id);
    cache_entry_path->relpath  = str_join_path(cache_entry_path->dirname, cache_entry_path->filename, 0);
    cache_entry_path->fullpath = str_join_path(cache_path, cache_entry_path->relpath, 0);
    cache_entry_path->dirfullpath = str_join_path(cache_path, cache_entry_path->dirname, 0);
}

static int command_put(const char * cache_path, const char * cache_id, const char * source_file_path)
{
    cache_entry_path_t cache_entry_path;
    cache_id_to_path(cache_path, cache_id, &cache_entry_path);

    struct stat stat_buf;
    if (stat(cache_entry_path.dirfullpath, &stat_buf) < 0)
    {
        if (mkdir(cache_entry_path.dirfullpath, 0777) < 0)
        {
            perrorf("%s: failed to create directory %s", progname, cache_entry_path.dirfullpath);
            return RET_FILE_OPS;
        }
    }
    else if (!S_ISDIR(stat_buf.st_mode))
    {
        fprintf(stderr, "%s: %s: Not a directory\n", progname, cache_entry_path.dirfullpath);
        return RET_FILE_OPS;
    }

    char * tmpfilename = str_join_path(cache_entry_path.dirfullpath, ".?tmpfile", 0);

    if ((unlink(tmpfilename) < 0) && errno != ENOENT)
    {
        perrorf("%s: failed to unlink %s", progname, tmpfilename);
        return RET_FILE_OPS;
    }

    if (cp(tmpfilename, source_file_path) < 0)
    {
        perrorf("%s: failed to copy %s", progname, source_file_path);
        return RET_FILE_OPS;
    }

    if (rename(tmpfilename, cache_entry_path.fullpath) < 0)
    {
        perrorf("%s: failed to rename %s", progname, tmpfilename);
        return RET_FILE_OPS;
    }

    return 0;
}

static int command_get(const char * cache_path, const char * cache_id, const char * source_file_path)
{
    cache_entry_path_t cache_entry_path;
    cache_id_to_path(cache_path, cache_id, &cache_entry_path);

    struct stat stat_buf;
    if (stat(cache_entry_path.fullpath, &stat_buf) < 0)
    {
        return RET_MISS;
    }

    if ((unlink(source_file_path) < 0) && errno != ENOENT)
    {
        perrorf("%s: failed to unlink %s", progname, source_file_path);
        return RET_FILE_OPS;
    }

    if (cp(source_file_path, cache_entry_path.fullpath) < 0)
    {
        perrorf("%s: failed to copy %s", progname, cache_entry_path.fullpath);
        unlink(source_file_path);
        return RET_FILE_OPS;
    }

    return 0;
}

static int command_delete(const char * cache_path, const char * cache_id)
{
    cache_entry_path_t cache_entry_path;
    cache_id_to_path(cache_path, cache_id, &cache_entry_path);

    if ((unlink(cache_entry_path.fullpath) < 0) && errno != ENOENT)
    {
        perrorf("%s: failed to unlink %s", progname, cache_entry_path.fullpath);
        return RET_FILE_OPS;
    }

    return 0;
}

static int command_clean(const char * cache_path, int max_size_mb)
{
    fprintf(stderr, "%s: Not implemented\n", progname);
    return RET_INTERNAL;
}


#define USAGE \
"Usage:\n" \
"\tafilecache <cache directory> put <id> <file path>\n" \
"\tafilecache <cache directory> get <id> <file path>\n" \
"\tafilecache <cache directory> delete <id>\n" \
"\tafilecache <cache directory> clean <max size in MB>\n"

static void usage(void)
{
    fprintf(stderr, USAGE);
}

#define USAGE_CHECK(check) \
if (!(check)) {  \
    usage();  \
    return RET_USAGE; \
}


int main(int argc, char **argv) {

    const char * cache_path;
    const char * command;

    const char * source_file_path = NULL;
    const char * cache_id = NULL;

    int          max_size_mb = 0;

    progname = argv[0];

    USAGE_CHECK(argc >= 3)

    cache_path = argv[1];
    command    = argv[2];

    USAGE_CHECK(*cache_path && *command)

    if (strcmp(command, "put") == 0 || strcmp(command, "get") == 0)
    {
        USAGE_CHECK(argc == 5)
        cache_id = argv[3];
        source_file_path = argv[4];
        USAGE_CHECK(*source_file_path && *cache_id)
    }
    else if (strcmp(command, "delete") == 0)
    {
        USAGE_CHECK(argc == 4)
        cache_id = argv[3];
        USAGE_CHECK(*cache_id)
    }
    else if (strcmp(command, "clean") == 0)
    {
        USAGE_CHECK(argc == 4)
        max_size_mb = atoi(argv[4]);
    }
    else
    {
        USAGE_CHECK(0)
    }

    struct stat stat_buf;
    if (stat(cache_path, &stat_buf) != 0) {
        perrorf("%s: %s", progname, cache_path);
        return RET_NO_CACHE_DIR;
    }

    if (!S_ISDIR(stat_buf.st_mode)) {
        fprintf(stderr, "%s: %s: Not a directory\n", progname, cache_path);
        return RET_NO_CACHE_DIR;
    }

    char * lock_path = str_join_path(cache_path, ".lock", 0);
    int lock_fd = open(lock_path, O_WRONLY | O_APPEND | O_CREAT, 0666);
    if (lock_fd < 0) {
        perrorf("%s: failed to open %s", progname, lock_path);
        return RET_FILE_OPS;
    }

    if (flock(lock_fd, LOCK_EX) < 0) {
        perrorf("%s: failed to lock %s", progname, lock_path);
        return RET_LOCK;
    }


    if (strcmp(command, "put") == 0)
    {
        return command_put(cache_path, cache_id, source_file_path);
    }
    else if (strcmp(command, "get") == 0)
    {
        return command_get(cache_path, cache_id, source_file_path);
    }
    else if (strcmp(command, "delete") == 0)
    {
        return command_delete(cache_path, cache_id);
    }
    else if (strcmp(command, "clean") == 0)
    {
        return command_clean(cache_path, max_size_mb);
    }

    /* NOT REACHED */

    return RET_INTERNAL;
}

