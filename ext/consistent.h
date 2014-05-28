/* vim: set sts=4 sw=4 expandtab: */
/*
Copyright (c) 2013 Mail.RU
Copyright (c) 2013 Sokolov Yura aka funny-falcon

MIT License

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#ifdef CONSISTENT_IMPLEMENTATION
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#endif

#ifndef CONSISTENT_INTERFACE
#define CONSISTENT_INTERFACE

typedef uint64_t CH_handle_t;
typedef int (*CH_key_eq_t)(void *ctx, CH_handle_t key_a, CH_handle_t key_b);
typedef uint32_t (*CH_key_hash_t)(void *ctx, CH_handle_t key);
/* should provide some kind of 128bit hash function, MD5 for example.
 * Murmur3_32 with crazy seed is used if not set. */
typedef void (*CH_points_hash_t)(void *ctx, const char *server, size_t server_len, uint32_t seed, uint32_t digest[4]);
/* should provide some kind of 32bit hash function,
 * Murmur3_32 is used by default, so that, there no big need to change it */
typedef uint32_t (*CH_item_hash_t)(void *ctx, const char *item, size_t item_len, uint32_t seed);

typedef enum CH_use_handle {
    CH_DEFAULT_IS_USE_HANDLE = 0,
    CH_DONOT_USE_HANDLE = 1,
    CH_USE_HANDLE = 2
} CH_use_handle_e;

typedef struct CH_config {
    void       *ctx; /* fill free to set it as NULL %), but functions should accept it */
    void     *(*realloc)(void *ctx, void *old, size_t new_size); /* will be setup to plain realloc if NULL */
    void      (*abort)(void *ctx);                               /* will be setup to plain abort if NULL */
    CH_key_eq_t      handle_eq;                                  /* will be setup to raw equality if NULL */
    CH_key_hash_t    handle_hash;                                /* will be setup to some integer mix if NULL */
    CH_points_hash_t points_hash;                                /* it is better to setup it with MD5 implementation,
                                                                    but it will be setup to some function by default */
    CH_item_hash_t   item_hash;                                  /* will be setup to Murmur3_32 if NULL */
    uint32_t    points_per_server;
    CH_use_handle_e use_handle;                                  /* use handle or not */
} CH_config_t;

/**
 * CH_DEAD - do not count in a ring. If eigther configured value or updated is CH_DEAD, server is counted as a dead.
 * CH_ALIVE - count server in a ring and choose it when appropriate.
 * CH_DOWN - count server in a ring, but skip when choosing server.
 * CH_DEFAULT - when used in update, it means to use configured value.
 */
typedef enum CH_aliveness {
    CH_DEAD = 0,
    CH_ALIVE = 1,
    CH_DOWN = 2,
    CH_DEFAULT = 1 << 30
} CH_aliveness_e;

typedef struct ConsistentHash ConsistentHash_t;

ConsistentHash_t *ConsistentHash_new(CH_config_t config);
void ConsistentHash_free(ConsistentHash_t *ring);

size_t ConsistentHash_size(ConsistentHash_t *ring);

/**
 * number of alive servers
 */
uint32_t ConsistentHash_alive_count(ConsistentHash_t *ring);
/**
 * does ring use handle for access?
 */
CH_use_handle_e ConsistentHash_use_handle(ConsistentHash_t *ring);
/**
 * reset all configuration
 */
void ConsistentHash_clean(ConsistentHash_t *ring);

typedef struct CH_server_list ConsistentHash_ServerList_t;
ConsistentHash_ServerList_t *ConsistentHash_ServerList_new(ConsistentHash_t *ring);
size_t ConsistentHash_ServerList_size(ConsistentHash_ServerList_t *servers);

typedef enum { CH_ADD_OK = 0, CH_NAME_EXISTS = 1, CH_HANDLE_EXISTS = 2 } CH_add_result_e;
/**
 * adds one server's description. name will be copied, so that, fill free to free it after adding.
 * handle - is arbitary data stored with server's description
 * returns CH_ADD_OK if server added
 * returns CH_NAME_EXISTS if server with same name already exists
 * returns CH_HANDLE_EXISTS if server with same handle already exists
 */
CH_add_result_e ConsistentHash_ServerList_add(ConsistentHash_ServerList_t *list, const char *name, size_t name_len, uint32_t weight, CH_aliveness_e alive, CH_handle_t handle);
/**
 * exchange stored servers description in a ring with passed one.
 * (so that, *list will be rewritten with old description)
 */
void ConsistentHash_exchange_server_list(ConsistentHash_t *ring, ConsistentHash_ServerList_t *list);
void ConsistentHash_ServerList_free(ConsistentHash_ServerList_t *list);

/**
 * every "server" has two aliveness value:
 * alive_as_configured - that is set by _exchange_server_list
 * alive_as_updated - that is set by _refresh_alive_by_*
 * resulting alive is computed as:
 * ((alive_as_configured == CH_DEAD) ? CH_DEAD :
 * ((alive_as_updated == CH_DEFAULT) ? alive_as_configured :
 *                                     alive_as_updated))
 */
typedef struct CH_aliveness_by_name ConsistentHash_AliveByName_t;
ConsistentHash_AliveByName_t *ConsistentHash_AliveByName_new(ConsistentHash_t *ring);
void ConsistentHash_AliveByName_add(ConsistentHash_AliveByName_t *list, const char *name, size_t name_len, CH_aliveness_e alive);
void ConsistentHash_refresh_alive_by_name(ConsistentHash_t *ring, ConsistentHash_AliveByName_t *list, CH_aliveness_e default_alive);
size_t ConsistentHash_AliveByName_size(ConsistentHash_AliveByName_t *list);
void ConsistentHash_AliveByName_free(ConsistentHash_AliveByName_t *list);

typedef struct CH_aliveness_by_handle ConsistentHash_AliveByHandle_t;
ConsistentHash_AliveByHandle_t *ConsistentHash_AliveByHandle_new(ConsistentHash_t *ring);
void ConsistentHash_AliveByHandle_add(ConsistentHash_AliveByHandle_t *list, CH_handle_t handle, CH_aliveness_e alive);
void ConsistentHash_refresh_alive_by_handle(ConsistentHash_t *ring, ConsistentHash_AliveByHandle_t *list, CH_aliveness_e default_alive);
size_t ConsistentHash_AliveByHandle_size(ConsistentHash_AliveByHandle_t *list);
void ConsistentHash_AliveByHandle_free(ConsistentHash_AliveByHandle_t *list);

CH_handle_t ConsistentHash_Helper_parse_ipv4_with_port(const char *str, size_t len, uint32_t default_port);

typedef struct CH_iterator {
    ConsistentHash_t *ring;
    struct CH_name *name;
    uint32_t   found;
    uint32_t   visited;
    uint32_t   seed;
    struct {
        uint32_t   capa;
        uint32_t  *buf;
        uint32_t   smallbuf;
    } bitmap;
} ConsistentHash_Iterator_t;

typedef struct CH_iterator_name {
    size_t size;
    const  char *name;
} ConsistentHash_IteratorName_t;

typedef struct CH_iterator_handle {
    CH_handle_t handle;
    int    found;
} ConsistentHash_IteratorHandle_t;

ConsistentHash_Iterator_t *ConsistentHash_Iterator_new(ConsistentHash_t *ring, const char *name, size_t name_len);
void ConsistentHash_Iterator_free(ConsistentHash_Iterator_t *iterator);

/**
 * returns {0, NULL} when no more servers
 */
ConsistentHash_IteratorName_t ConsistentHash_Iterator_next_name(ConsistentHash_Iterator_t *iterator);
/**
 * returns {0, 0} when no more servers
 */
ConsistentHash_IteratorHandle_t ConsistentHash_Iterator_next_handle(ConsistentHash_Iterator_t *iterator);

/* following functions are for custom allocations of iterator.
 * use it if you need maximum speed */
/* default value for iterator allocated on stack */
#define ConsistentHash_Iterator_init_value(ring) {ring, NULL}
/* allocate iterator but not prepare it for work */
ConsistentHash_Iterator_t *ConsistentHash_Iterator_alloc(ConsistentHash_t *ring);
/* initializes already allocated iterator.
 * It's ring field should be set to ring, so that, you ought to use
 * ConsistentHash_Iterator_init_value or ConsistentHash_Iterator_alloc  */
void ConsistentHash_Iterator_init(ConsistentHash_Iterator_t *iterator, const char *name, size_t name_len);
/* release additionally allocated memory of iterator without freeing it */
void ConsistentHash_Iterator_release(ConsistentHash_Iterator_t *iterator);
/* release iterator and initializes it again for same ring */
void ConsistentHash_Iterator_reinit(ConsistentHash_Iterator_t *iterator, const char *name, size_t name_len);


size_t ConsistentHash_Iterator_size(ConsistentHash_Iterator_t *iterator);

#endif

#ifdef CONSISTENT_IMPLEMENTATION
#define DEFAULT_POINTS_PER_SERVER (1000)

#ifndef __unused__
#define __unused__ __attribute__((unused))
#endif

CH_handle_t
ConsistentHash_Helper_parse_ipv4_with_port(const char *str, size_t len, uint32_t default_port)
{
    char buf[24]; /* strlen("xxx.xxx.xxx.xxx:ppppp") == 21 */
    uint32_t ip[4] = {0, 0, 0, 0};
    uint32_t port;
    uint32_t n;
    CH_handle_t result = 0;

    if (len > 23) len = 23;
    memcpy(buf, str, len);
    buf[len] = 0;

    n = sscanf(buf, "%u.%u.%u.%u:%u", ip, ip+1, ip+2, ip+3, &port);
    if (n == 4) {
        port = default_port;
        n = 5;
    }
    if (n == 5 &&
            ip[0] < 256 && ip[1] < 256 && ip[2] < 256 && ip[3] < 256 &&
            port < 65536 ) {
        result = (ip[0] << 24) | (ip[1] << 16) | (ip[2] << 8) | ip[3];
        result = (result << 16) | port;
    }
    return result;
}

void *
default_realloc(__unused__ void *ctx, void *old, size_t new_size)
{
    if (new_size == 0) { /* fix weird behaviour of some implemenations */
        if (old != NULL)
            free(old);
        return NULL;
    }
    return realloc(old, new_size);
}

__attribute__((noreturn)) void
default_abort(__unused__ void *ctx)
{
    abort();
}

int
default_handle_eq(__unused__ void *ctx, CH_handle_t handle_a, CH_handle_t handle_b)
{
    return handle_a == handle_b;
}

static inline uint32_t
fmix64(CH_handle_t k)
{
  k ^= k >> 33;
  k *= 0x53215229;
  k ^= k >> 33;
  k *= 0x53215229;
  return (uint32_t)k ^ (uint32_t)(k >> 33);
}

uint32_t
default_handle_hash(__unused__ void *ctx, CH_handle_t handle)
{
    return fmix64(handle);
}

static void simple_points_hash(void *ctx, const char *server, size_t len, uint32_t seed, uint32_t digest[4]);
static uint32_t murmur_item_hash(void *ctx, const char *item, size_t len, uint32_t seed);

static void
config_set_defaults(CH_config_t *config)
{
    if (config->realloc == NULL) {
        config->realloc = default_realloc;
    }
    if (config->abort == NULL) {
        config->abort = default_abort;
    }
    if (config->handle_eq == NULL) {
        config->handle_eq = default_handle_eq;
        config->handle_hash = default_handle_hash;
    }
    if (config->points_per_server == 0) {
        config->points_per_server = DEFAULT_POINTS_PER_SERVER;
    }
    if (config->use_handle == CH_DEFAULT_IS_USE_HANDLE) {
        config->use_handle = CH_USE_HANDLE;
    }
    if (config->points_hash == NULL) {
        config->points_hash = simple_points_hash;
    }
    if (config->item_hash == NULL) {
        config->item_hash = murmur_item_hash;
    }
}

static inline void
_do_realloc(CH_config_t *config, void **pptr, size_t new_size)
{
    void *new_ptr;
    new_ptr = config->realloc(config->ctx, *pptr, new_size);
    if (new_size != 0 && new_ptr == NULL) {
        config->abort(config->ctx);
        return; /* ha ha */
    }
    *pptr = new_ptr;
}

static inline void
_do_realloca(CH_config_t *config, void **pptr, size_t new_size, size_t old_size)
{
    _do_realloc(config, pptr, new_size);
    if (new_size > old_size) {
        memset(*pptr + old_size, 0, new_size - old_size);
    }
}

static inline void
_do_malloc(CH_config_t *config, void **pptr, size_t size)
{
    *pptr = NULL;
    _do_realloc(config, pptr, size);
}

static inline void
_do_calloc(CH_config_t *config, void **pptr, size_t size)
{
    _do_malloc(config, pptr, size);
    memset(*pptr, 0, size);
}

static inline void
_do_free(CH_config_t *config, void **pptr)
{
    _do_realloc(config, pptr, 0);
    *pptr = NULL;
}

static void
_ensure_capa(CH_config_t *config, void **buf, uint32_t *capa, uint32_t need_capa, size_t elem_size)
{
    if (need_capa > *capa) {
        /* round to nearest 0b0.0100.0 or 0b0.0110.0 */
        /* 4 is minimum, than 8, 12, 16, 24, 32, 48, 64, 96 */
        need_capa >>= 2;
        need_capa |= need_capa >> 2;
        need_capa |= need_capa >> 4;
        need_capa |= need_capa >> 8;
        need_capa |= need_capa >> 16;
        need_capa |= need_capa >> 3;
        need_capa = (need_capa + 1) << 2;
        _do_realloca(config, buf, need_capa * elem_size, *capa * elem_size);
        *capa = need_capa;
    }
}

#define do_realloc(config, buf, count) _do_realloc(config, (void**)buf, count * sizeof(**(buf)))
#define do_realloca(config, buf, new_count, old_count) _do_realloc(config, (void**)buf, new_count * sizeof(**(buf)), old_count * sizeof(**(buf)))
#define do_malloc(config, buf, count)   _do_malloc(config, (void**)buf, count * sizeof(**(buf)))
#define do_calloc(config, buf, count)   _do_calloc(config, (void**)buf, count * sizeof(**(buf)))
#define do_free(config, buf)            _do_free(config, (void**)buf)

#define do_memzero(buf, count) memset(buf, 0, sizeof(*(buf)) * count)

#define ensure_capa(config, arr, need_capa) _ensure_capa(config, (void**)&(arr).buf, &(arr).capa, need_capa, sizeof(*(arr).buf))
#define array_clean(config, arr) do { do_free(config, (void**)&(arr).buf); (arr).capa = (arr).count = 0; } while(0)
#define buf_size(arr) (sizeof(*(arr).buf) * (arr).capa)
#define append_to(config, arr, elem) do { \
    ensure_capa(config, arr, (arr).count + 1); \
    (arr).buf[(arr).count] = elem; \
    (arr).count++; \
} while(0)


#define FASTHASH_LOG 12
#define FASTHASH_SIZE ((1<<FASTHASH_LOG) + 1)
#define FASTHASH_ILOG (32 - FASTHASH_LOG)
#define FASTHASH_STEP (1<<FASTHASH_ILOG)
#define MINIMUM_CONTINUUM (4 * 1024)

typedef struct {
    uint32_t point;
    uint32_t server;
} Point_t;

typedef struct {
    CH_config_t  *config;
    int           sorted;
    struct {
        uint32_t      capa;
        uint32_t      count;
        Point_t      *buf;
    } points;
    uint32_t      hash[FASTHASH_SIZE];
} Continuum_t;

static Continuum_t *
Continuum_new(CH_config_t *config)
{
    Continuum_t *cont;
    do_calloc(config, &cont, 1);
    cont->config = config;
    ensure_capa(config, cont->points, MINIMUM_CONTINUUM);
    return cont;
}

static void
Continuum_clean(Continuum_t *cont)
{
    cont->points.count = 0;
    cont->sorted = 0;
}

static size_t
Continuum_size(Continuum_t *cont)
{
    return sizeof(Continuum_t) + buf_size(cont->points);
}

static void
Continuum_free(Continuum_t *cont)
{
    if (cont) {
        array_clean(cont->config, cont->points);
        do_free(cont->config, &cont);
    }
}

static inline int
points_in_order(const Point_t point_a, const Point_t point_b)
{
    return point_a.point < point_b.point ||
        (point_a.point == point_b.point && point_a.server <= point_b.server);
}

static void
Continuum_add_server(Continuum_t *cont, uint32_t server, uint32_t *points, uint32_t points_num)
{
    uint32_t i = 0;
    Point_t *point;

    ensure_capa(cont->config, cont->points, cont->points.count + points_num);
    point = cont->points.buf + cont->points.count;

    for(i = points_num; i; i--, point++, points++) {
        point->point = *points;
        point->server = server;
    }

    cont->points.count += points_num;
    cont->sorted = 0;
}

static void
points_sort(Point_t *points, uint32_t points_num, uint32_t median, uint32_t delta)
{
    uint32_t i, j;
    Point_t tmp;
    if (points_num < 7) { /* insertion sort */
        for(i = 1; i < points_num; i++) {
            if (!points_in_order(points[i-1], points[i])) {
                tmp = points[i];
                points[i] = points[i-1];
                j = i - 1;
                while (j && !points_in_order(points[j-1], tmp)) {
                    points[j] = points[j - 1];
                    j--;
                }
                points[j] = tmp;
            }
        }
    }
    else { /* quick sort */
        Point_t median_point = { median, 0 };
        Point_t *left = points, *now = points;
        i = points_num;
        /* when points are too close to each other, fallback to median selection*/
        if (delta <= 1024) {
            if (points_in_order(median_point, points[0]) ==
                points_in_order(median_point, points[1])) {
                points_sort(points, 4, 0, 0);
                median_point = points[2];
                left = points+3;
                i -= 3;
            }
        }
        for(; i && !points_in_order(median_point, *left); i--, left++) {}
        if (i) {
            now = left + 1;
            i--;
            for(; i; i--, now++) {
                if (!points_in_order(median_point, *now)) {
                    tmp = *now;
                    *now = *left;
                    *left = tmp;
                    left++;
                }
            }
        }
        i = left - points;
        points_sort(points, i, median - delta, delta / 2);
        points_sort(left, points_num - i, median + delta, delta / 2);
    }
}

static uint32_t
points_first_greater_or_equal(Point_t *points, uint32_t point, uint32_t left, uint32_t right)
{
    uint32_t mid, mid_pnt;
    while (left < right) {
        mid = left + (right - left) / 2;
        mid_pnt = points[mid].point;
        if (mid_pnt < point)
            left = mid + 1;
        else if (mid_pnt >= point)
            right = mid;
    }
    return left;
}

static void
Continuum_fill_hash(Continuum_t *cont)
{
    uint32_t left, hash_point;
    uint32_t right, right_step;
    uint32_t i;
    cont->hash[0] = 0;
    cont->hash[FASTHASH_SIZE-1] = cont->points.count;
    left = 0;
    hash_point = FASTHASH_STEP;
    right_step = cont->points.count / (FASTHASH_SIZE - 1) + 1;
    for(i = 1; i < FASTHASH_SIZE - 1; i++, hash_point += FASTHASH_STEP) {
        right = left + right_step;
        while (right < cont->points.count && cont->points.buf[right].point < hash_point) {
            left = right;
            right += right_step;
        }
        if (right > cont->points.count) right = cont->points.count;
        cont->hash[i] = left =
            points_first_greater_or_equal(cont->points.buf, hash_point, left, right);
    }
}

static void
Continuum_sort(Continuum_t *cont)
{
    if (cont->points.count > 0) {
        points_sort(cont->points.buf, cont->points.count, (1<<31), (1<<30));
        cont->sorted = 1;
        Continuum_fill_hash(cont);
    }
}

static inline uint32_t
distance(uint32_t a, uint32_t b)
{
    uint32_t dist = a - b;
    uint32_t close = !(dist & (1 << 31));
    return close * dist + !close * (~dist + 1);
}

static int
Continuum_find_server(Continuum_t *cont, uint32_t point, uint32_t *server)
{
    if (cont->points.count == 0)
        return 0;

    if (!cont->sorted)
        Continuum_sort(cont);

    {
        uint32_t hash_pos = point >> FASTHASH_ILOG;
        uint32_t left = cont->hash[hash_pos];
        uint32_t right = cont->hash[hash_pos + 1];
        uint32_t greater = left == right ? right : points_first_greater_or_equal(cont->points.buf, point, left, right);
        uint32_t lesser = greater + (!greater * cont->points.count) - 1;
        uint32_t dist_greater, dist_lesser;
        greater %= cont->points.count;
        dist_greater = distance(point, cont->points.buf[greater].point);
        dist_lesser = distance(point, cont->points.buf[lesser].point);
        *server = (dist_greater < dist_lesser) ?
            cont->points.buf[greater].server :
            cont->points.buf[lesser].server;
        return 1;
    }
}

static inline uint32_t
fmix_int32(uint32_t key, uint32_t mix)
{
    uint64_t res = (uint64_t)key * (uint64_t)mix;
    return (uint32_t)res ^ (uint32_t)(res >> 32);
}

/* Murmur3_32 hash implementation */
#if defined(_MSC_VER)
#define rotl32(x,y)  _rotl(x,y)
#else
static inline uint32_t
rotl32(uint32_t x, int8_t r) { return (x << r) | (x >> (32 - r)); }
#endif
/* Block read - if your platform needs to do endian-swapping or can only
   handle aligned reads, do the conversion here */
static inline uint32_t
getblock32 (const uint32_t * p, int i) { return p[i]; }
static const uint32_t c1 = 0xcc9e2d51;
static const uint32_t c2 = 0x1b873593;
static const uint32_t cm1 = 0x85ebca6b;
static const uint32_t cm2 = 0xc2b2ae35;
static uint32_t
CH_MurmurHash3( const void * key, int len, uint32_t seed )
{
  const uint8_t * data = (const uint8_t*)key;
  const int nblocks = len / 4;
  uint32_t h1 = seed, k1;
  const uint32_t * blocks = (const uint32_t *)(data + nblocks*4);
  int i;
  for(i = -nblocks; i; i++)
  {
    uint32_t k1 = getblock32(blocks,i);
    k1 *= c1; k1 = rotl32(k1,15); k1 *= c2;
    h1 ^= k1; h1 = rotl32(h1,13); h1 = h1*5+0xe6546b64;
  }
  const uint8_t * tail = (const uint8_t*)(data + nblocks*4);
  k1 = 0;
  switch(len & 3)
  {
  case 3: k1 ^= tail[2] << 16;
  case 2: k1 ^= tail[1] << 8;
  case 1: k1 ^= tail[0];
          k1 *= c1; k1 = rotl32(k1,15); k1 *= c2; h1 ^= k1;
  };
  h1 ^= len;
  h1 = (h1 ^ (h1 >> 16)) * cm1;
  h1 = (h1 ^ (h1 >> 13)) * cm2;
  return h1 ^ (h1 >> 16);
}

static void
simple_points_hash(__unused__ void *ctx, const char *server, size_t len, uint32_t seed, uint32_t digest[4])
{
    uint32_t i = seed * 4;
    digest[0] = CH_MurmurHash3(server, len, i * c1);
    digest[1] = CH_MurmurHash3(server, len, (i+1) * c2);
    digest[2] = CH_MurmurHash3(server, len, (i+2) * cm1);
    digest[3] = CH_MurmurHash3(server, len, (i+3) * cm2);
}

static uint32_t
murmur_item_hash(__unused__ void *ctx, const char *item, size_t len, uint32_t seed)
{
    return CH_MurmurHash3(item, len, seed);
}

/* tired - cause I'm tired to write something more robust :-) */
/* workflow: fill with items => search by key => delete by key => iterate */
/* it is not intended for insertion after deletion nor for insertion of duplicates */
#define TIRED_SET_INIT_CAPA (8)
typedef CH_handle_t (*CH_key_get_t)(void *elem);

typedef struct tired_set {
    CH_config_t  *config;
    CH_key_hash_t key_hash;
    CH_key_eq_t   key_eq;
    CH_key_get_t  key_get;
    uint32_t  capa;
    uint32_t  size;
    uint32_t  busy;
    uint32_t  max;
    uint32_t *hashes;
    void    **elems;
} TiredSet_t;

static TiredSet_t *
TiredSet_new(CH_config_t *config, CH_key_get_t key_get, CH_key_hash_t key_hash, CH_key_eq_t key_eq)
{
    TiredSet_t *set;
    do_calloc(config, &set, 1);
    set->config   = config;
    set->key_hash = key_hash;
    set->key_eq   = key_eq;
    set->key_get  = key_get;
    do_calloc(config, &set->hashes, TIRED_SET_INIT_CAPA);
    do_calloc(config, &set->elems, TIRED_SET_INIT_CAPA);
    set->capa     = TIRED_SET_INIT_CAPA;
    set->max = (set->capa >> 2) | (set->capa >> 1); /* (capa / 4 * 3) */
    return set;
}

static void
TiredSet_free(TiredSet_t *set)
{
    if (set) {
        do_free(set->config, &set->hashes);
        do_free(set->config, &set->elems);
        do_free(set->config, &set);
    }
}

static size_t
TiredSet_size(TiredSet_t *set)
{
    if (set == NULL) return 0;
    return sizeof(*set) + set->capa * (sizeof(*set->hashes) + sizeof(*set->elems));
}

#define TH_EMPTY (0)
#define TH_DELETED (1)

static inline CH_handle_t
th_key_get(TiredSet_t *set, void *elem)
{
    return set->key_get(elem);
}

static inline uint32_t
th_hash(TiredSet_t *set, CH_handle_t key)
{
    uint32_t hash = set->key_hash(set->config->ctx, key);
    return hash | ((!(hash & ~TH_DELETED)) << 1); /* > TH_DELETED */
}

static inline int
th_key_eq(TiredSet_t *set, uint32_t pos, CH_handle_t key)
{
    return set->key_eq(set->config->ctx, th_key_get(set, set->elems[pos]), key);
}

#define search_prepare() \
    uint32_t *hashes = set->hashes; \
    void    **elems = set->elems; \
    uint32_t hash = th_hash(set, key); \
    uint32_t mask = set->capa - 1; \
    uint32_t pos = hash & mask; \
    uint32_t delta

#define search_loop() do { \
    check_pos(); \
    delta = (((hash % (mask >> 2)) | 1) << 2) - 3; \
    for(;;) { \
        pos = (pos + 1) & mask; \
        check_pos(); \
        pos = (pos + 1) & mask; \
        check_pos(); \
        pos = (pos + 1) & mask; \
        check_pos(); \
        pos = (pos + delta) & mask; \
        check_pos(); \
    } \
} while(0)

static void *
TiredSet_insert(TiredSet_t *set, void *elem)
{
    CH_handle_t key = th_key_get(set, elem);
    search_prepare();
    uint32_t deleted_pos = 0;

#define check_pos() do { \
    if (hashes[pos] == hash && th_key_eq(set, pos, key)) \
        goto found; \
    else if (hashes[pos] == TH_EMPTY) \
        goto fresh; \
    else if (hashes[pos] == TH_DELETED) \
        deleted_pos = pos + 1; \
} while(0)

    search_loop();
#undef check_pos
found:
    if (deleted_pos) {
        hashes[pos] = TH_DELETED;
        hashes[deleted_pos - 1] = hash;
        elems[deleted_pos - 1] = elems[pos];
        pos = deleted_pos - 1;
    }
    return elems[pos];
fresh:
    if (!deleted_pos)
        set->busy++;
    else
        pos = deleted_pos - 1;
    set->size++;
    hashes[pos] = hash;
    elems[pos] = elem;
    return elem;
}

static void
TiredSet_increase(TiredSet_t *set)
{
    uint32_t i;
    uint32_t old_capa = set->capa;
    uint32_t capa = old_capa * 2;
    uint32_t *old_hashes = set->hashes;
    void    **old_elems = set->elems;
    uint32_t mask = capa - 1;
    uint32_t *hashes;
    void    **elems;

    do_calloc(set->config, &hashes, capa);
    do_calloc(set->config, &elems, capa);

    for(i = 0; i < old_capa; i++) {
        uint32_t hash = old_hashes[i];
        if (hash > TH_DELETED) {
            uint32_t pos = hash & mask;
            uint32_t delta;
#define check_pos()  if (hashes[pos] == TH_EMPTY) goto insert
            search_loop();
#undef check_pos
        insert:
            hashes[pos] = hash;
            elems[pos] = old_elems[i];
        }
    }

    set->hashes = hashes;
    set->elems = elems;
    set->capa = capa;
    set->max = (capa >> 2) | (capa >> 1);
    set->busy = set->size;
    do_free(set->config, &old_hashes);
    do_free(set->config, &old_elems);
}

static void *
TiredSet_add(TiredSet_t *set, void *elem)
{
    if (set->busy >= set->max) {
        TiredSet_increase(set);
    }
    return TiredSet_insert(set, elem);
}

#define check_pos() do { \
    if (hashes[pos] == hash && th_key_eq(set, pos, key)) \
        goto found; \
    else if (hashes[pos] == TH_EMPTY) \
        goto not_found; \
} while(0)

static void *
TiredSet_get(TiredSet_t *set, CH_handle_t key)
{
    search_prepare();

    search_loop();
found:
    return elems[pos];
not_found:
    return NULL;
}

static void *
TiredSet_delete(TiredSet_t *set, CH_handle_t key)
{
    search_prepare();

    search_loop();
found:
    hashes[pos] = TH_DELETED;
    set->size--;
    return elems[pos];
not_found:
    return NULL;
}
#undef check_pos
#undef search_loop
#undef TH_DELETED
#undef TH_EMPTY
/** END OF TIRED HASH **/

typedef struct CH_name {
    size_t size;
    char   str[];
} CH_Name_t;

static uint32_t
name_hash(__unused__ void *ctx, CH_handle_t key_name)
{
    CH_Name_t *name = (CH_Name_t*)(uintptr_t)key_name;
    return CH_MurmurHash3(name->str, name->size, 0);
}

static int
name_eq(__unused__ void *ctx, CH_handle_t key_name_a, CH_handle_t key_name_b)
{
    CH_Name_t *name_a = (CH_Name_t*)(uintptr_t)key_name_a;
    CH_Name_t *name_b = (CH_Name_t*)(uintptr_t)key_name_b;

    return name_a->size == name_b->size &&
        memcmp(name_a->str, name_b->str, name_a->size) == 0;
}

static CH_Name_t *
CH_Name_new(CH_config_t *config, const char *str, size_t size)
{
    CH_Name_t *name;
    _do_calloc(config, (void**)&name, sizeof(*name) + size + 1);
    name->size = size;
    memcpy(name->str, str, size);
    name->str[size] = 0;
    return name;
}

static inline size_t
CH_Name_size(CH_Name_t *name)
{
    return sizeof(*name) + name->size - sizeof(size_t);
}

static void
CH_Name_free(CH_config_t *config, CH_Name_t *name)
{
    do_free(config, &name);
}

typedef struct CH_server_item {
    CH_Name_t     *name;
    CH_handle_t    handle;
    uint32_t       weight;
    CH_aliveness_e alive_as_configured;
    CH_aliveness_e alive_as_updated;
    uint32_t       used_points;
    struct {
        uint32_t   capa;
        uint32_t   count;
        uint32_t  *buf;
    } points;
} CH_ServerItem_t;

static inline CH_aliveness_e
server_item_alive(CH_ServerItem_t *server)
{
    if (server->alive_as_configured == CH_DEAD)
        return CH_DEAD;
    if (server->alive_as_updated == CH_DEFAULT)
        return server->alive_as_configured;
    return server->alive_as_updated;
}

static CH_ServerItem_t *
ServerItem_new(CH_config_t *config, const char *name, size_t name_len, uint32_t weight, CH_aliveness_e alive, CH_handle_t handle)
{
    CH_ServerItem_t *server;

    do_calloc(config, &server, 1);
    server->name = CH_Name_new(config, name, name_len);
    server->handle = handle;
    server->weight = weight;
    server->alive_as_configured = alive;
    server->alive_as_updated = CH_DEFAULT;
    return server;
}

static size_t
ServerItem_size(CH_ServerItem_t *server)
{
    /* do not count name size, cause it is primary stored in other place */
    return sizeof(*server) + CH_Name_size(server->name) + buf_size(server->points);
}

static void
ServerItem_free(CH_config_t *config, CH_ServerItem_t *server)
{
    if (server) {
        CH_Name_free(config, server->name);
        do_free(config, &server->points.buf);
        do_free(config, &server);
    }
}

static CH_handle_t
ServerItem_name_as_handle(void *server)
{
    return (CH_handle_t)(uintptr_t)((CH_ServerItem_t*)server)->name;
}

static CH_handle_t
ServerItem_handle_as_handle(void *server)
{
    return ((CH_ServerItem_t*)server)->handle;
}

static void
ServerItem_set_used_points(CH_config_t *config, CH_ServerItem_t *server, uint32_t used)
{
    if (server->points.count < used) {
        uint32_t i;
        uint32_t *pnts;
        size_t      name_size;
        const char *name_str;
        uint32_t rounded = ((used + 3) / 4) * 4;

        ensure_capa(config, server->points, rounded);
        name_str = server->name->str;
        name_size = server->name->size;
        i = server->points.count;
        pnts = server->points.buf + i;
        for (; i < rounded; i+=4, pnts+=4) {
            config->points_hash(config->ctx, name_str, name_size, i/4, pnts);
        }
        server->points.count = rounded;
    }
    server->used_points = used;
}

static void
ServerItem_steal_points_and_alive(CH_ServerItem_t *to, CH_ServerItem_t *from)
{
    to->points = from->points;
    to->alive_as_updated = from->alive_as_updated;
    from->points.capa = 0;
    from->points.count = 0;
    from->points.buf = NULL;
}

struct CH_server_list {
    CH_config_t  *config;
    struct {
        uint32_t      capa;
        uint32_t      count;
        CH_ServerItem_t **buf;
    } list;
    TiredSet_t   *by_name;
    TiredSet_t   *by_handle;
};

struct ConsistentHash {
    CH_config_t    config;
    ConsistentHash_ServerList_t servers;
    uint32_t       alive_count;
    uint32_t       visitable_count;
    Continuum_t   *continuum;
};

#define DEFAULT_SERVERS_AMOUNT (8)
ConsistentHash_ServerList_t *
ConsistentHash_ServerList_new(ConsistentHash_t *ring)
{
    ConsistentHash_ServerList_t *servers;
    do_calloc(&ring->config, &servers, 1);
    servers->config = &ring->config;
    do_calloc(&ring->config, &servers->list.buf, DEFAULT_SERVERS_AMOUNT);
    servers->list.capa = DEFAULT_SERVERS_AMOUNT;
    servers->by_name = TiredSet_new(&ring->config, ServerItem_name_as_handle,
                                    name_hash, name_eq);
    if (servers->config->use_handle == CH_USE_HANDLE) {
        servers->by_handle = TiredSet_new(&ring->config, ServerItem_handle_as_handle,
                                    ring->config.handle_hash, ring->config.handle_eq);
    }
    return servers;
}

size_t
ConsistentHash_ServerList_size(ConsistentHash_ServerList_t *servers)
{
    size_t size;
    uint32_t i;

    size = sizeof(*servers) +
        TiredSet_size(servers->by_name) +
        TiredSet_size(servers->by_handle) +
        buf_size(servers->list);
    for(i=0; i < servers->list.count; i++) {
        size += ServerItem_size(servers->list.buf[i]);
    }
    return size;
}

static void
ConsistentHash_ServerList_release(ConsistentHash_ServerList_t *servers)
{
    int i;
    if (servers->list.buf) {
        for(i=0; i < servers->list.count; i++)
            ServerItem_free(servers->config, servers->list.buf[i]);
        array_clean(servers->config, servers->list);
    }
    TiredSet_free(servers->by_name);
    TiredSet_free(servers->by_handle);
    servers->by_name = NULL;
    servers->by_handle = NULL;
}

void
ConsistentHash_ServerList_free(ConsistentHash_ServerList_t *servers)
{
    if (servers) {
        ConsistentHash_ServerList_release(servers);
        do_free(servers->config, &servers);
    }
}

CH_add_result_e
ConsistentHash_ServerList_add(ConsistentHash_ServerList_t *servers,
        const char *name, size_t name_len,
        uint32_t weight, CH_aliveness_e alive, CH_handle_t handle)
{
    CH_ServerItem_t *server;
    server = ServerItem_new(servers->config, name, name_len,
                            weight, alive, handle);
    append_to(servers->config, servers->list, server);
    if (TiredSet_add(servers->by_name, server) != server) {
        servers->list.count--;
        ServerItem_free(servers->config, server);
        return CH_NAME_EXISTS;
    }
    if (servers->config->use_handle == CH_USE_HANDLE &&
            TiredSet_add(servers->by_handle, server) != server) {
        servers->list.count--;
        TiredSet_delete(servers->by_name, ServerItem_name_as_handle(server));
        ServerItem_free(servers->config, server);
        return CH_HANDLE_EXISTS;
    }
    return CH_ADD_OK;
}

typedef struct CH_name2alive {
    CH_aliveness_e alive;
    CH_Name_t *name;
} CH_Name2Alive_t;

struct CH_aliveness_by_name {
    CH_config_t *config;
    struct {
        uint32_t capa;
        uint32_t count;
        CH_Name2Alive_t *buf;
    } list;
};

ConsistentHash_AliveByName_t *
ConsistentHash_AliveByName_new(ConsistentHash_t *ring)
{
    ConsistentHash_AliveByName_t *list;
    do_calloc(&ring->config, &list, 1);
    list->config = &ring->config;
    return list;
}

void
ConsistentHash_AliveByName_add(ConsistentHash_AliveByName_t *list,
                               const char *name, size_t name_len,
                               CH_aliveness_e alive)
{
    CH_Name2Alive_t n2a = {
        alive,
        CH_Name_new(list->config, name, name_len)
    };

    append_to(list->config, list->list, n2a);
}

size_t
ConsistentHash_AliveByName_size(ConsistentHash_AliveByName_t *list)
{
    size_t size;
    uint32_t i;
    size = sizeof(*list) + buf_size(list->list);
    for(i = 0; i < list->list.count; i++) {
        size += CH_Name_size(list->list.buf[i].name);
    }
    return size;
}

void
ConsistentHash_AliveByName_free(ConsistentHash_AliveByName_t *list)
{
    if (list) {
        uint32_t i;
        if (list->list.buf) {
            for(i = 0; i < list->list.count; i++) {
                CH_Name_free(list->config, list->list.buf[i].name);
            }
            array_clean(list->config, list->list);
        }
        do_free(list->config, &list);
    }
}

typedef struct CH_name2handle {
    CH_aliveness_e alive;
    CH_handle_t    handle;
} CH_Handle2Alive_t;

struct CH_aliveness_by_handle {
    CH_config_t *config;
    struct {
        uint32_t capa;
        uint32_t count;
        CH_Handle2Alive_t *buf;
    } list;
};

ConsistentHash_AliveByHandle_t *
ConsistentHash_AliveByHandle_new(ConsistentHash_t *ring)
{
    ConsistentHash_AliveByHandle_t *list;
    if (ring->config.use_handle == CH_DONOT_USE_HANDLE) {
        return NULL;
    }
    do_calloc(&ring->config, &list, 1);
    list->config = &ring->config;
    return list;
}

void
ConsistentHash_AliveByHandle_add(ConsistentHash_AliveByHandle_t *list,
                                 CH_handle_t handle, CH_aliveness_e alive)
{
    CH_Handle2Alive_t n2h = { alive, handle };

    append_to(list->config, list->list, n2h);
}

size_t
ConsistentHash_AliveByHandle_size(ConsistentHash_AliveByHandle_t *list)
{
    return sizeof(*list) + buf_size(list->list);
}

void
ConsistentHash_AliveByHandle_free(ConsistentHash_AliveByHandle_t *list)
{
    if (list) {
        array_clean(list->config, list->list);
        do_free(list->config, &list);
    }
}

/* Ring itself */
ConsistentHash_t *
ConsistentHash_new(CH_config_t config)
{
    ConsistentHash_t *ring;

    config_set_defaults(&config);
    do_calloc(&config, &ring, 1);
    ring->config = config;
    ring->servers.config = &ring->config;
    ring->continuum = Continuum_new(&ring->config);
    return ring;
}

void
ConsistentHash_free(ConsistentHash_t *ring)
{
    if (ring) {
        Continuum_free(ring->continuum);
        ConsistentHash_ServerList_release(&ring->servers);
        do_free(&ring->config, &ring);
    }
}

size_t
ConsistentHash_size(ConsistentHash_t *ring)
{
    return sizeof(*ring) - sizeof(ring->servers) +
        ConsistentHash_ServerList_size(&ring->servers) +
        Continuum_size(ring->continuum);
}

uint32_t
ConsistentHash_alive_count(ConsistentHash_t *ring)
{
    return ring->alive_count;
}

CH_use_handle_e
ConsistentHash_use_handle(ConsistentHash_t *ring)
{
    return ring->config.use_handle;
}

void
ConsistentHash_clean(ConsistentHash_t *ring)
{
    Continuum_clean(ring->continuum);
    ConsistentHash_ServerList_release(&ring->servers);
    ring->alive_count = 0;
}

static void
sort_weights(uint32_t *weights, uint32_t count)
{
    if (count < 7) { /* insertion sort */
        uint32_t i, j;
        for(i = 1; i < count; i++) {
            uint32_t cur = weights[i];
            if (cur < weights[i-1]) {
                weights[i] = weights[i-1];
                j = i - 1;
                while(j && cur < weights[j - 1]) {
                    weights[j] = weights[j - 1];
                    j--;
                }
                weights[j] = cur;
            }
        }
    }
    else { /* quick sort with respect to repetative items */
        uint32_t *lesser, *equal, *more;
        uint32_t one_third = count / 3;
        uint32_t samples[4] = {weights[0], weights[one_third], weights[one_third*2], weights[count-1]};
        uint32_t median, tmp;
        uint32_t i = count;

        sort_weights(samples, 4);
        median = samples[2];

        lesser = weights;
        for(; i && *lesser < median; i--, lesser++) {}

        equal = lesser;
        for(; i && *equal == median; i--, equal++) {}

        if (i) {
            more = equal;
            for(; i; i--, more++) {
                if (*more < median) {
                    tmp = *more;
                    *more = *equal;
                    *equal = *lesser;
                    *lesser = tmp;
                    lesser++;
                    equal++;
                }
                else if (*more == median) {
                    tmp = *more;
                    *more = *equal;
                    *equal = tmp;
                }
            }
        }

        sort_weights(weights, lesser - weights);
        sort_weights(equal, count - (equal - weights));
    }
}

static void
ConsistentHash_update_continuum(ConsistentHash_t *ring)
{
    ConsistentHash_ServerList_t *list;
    CH_ServerItem_t *server;
    uint32_t i;
    uint32_t median, used_points;
    CH_aliveness_e alive;
    float    part;
    struct {
        uint32_t  capa;
        uint32_t  count;
        uint32_t *buf;
    } weights = {0, 0, 0};

    ring->alive_count = 0;
    ring->visitable_count = 0;
    list = &ring->servers;

    for(i = 0; i < list->list.count; i++) {
        server = list->list.buf[i];
        alive = server_item_alive(server);
        if (alive == CH_ALIVE)
            ring->alive_count++;
        if (alive != CH_DEAD) {
            append_to(&ring->config, weights, server->weight);
            ring->visitable_count++;
        }
    }

    Continuum_clean(ring->continuum);

    if (weights.count > 0) {
        sort_weights(weights.buf, weights.count);

        median = weights.buf[weights.count / 2];
        array_clean(&ring->config, weights);

        for(i = 0; i < list->list.count; i++) {
            server = list->list.buf[i];
            alive = server_item_alive(server);
            if (alive != CH_DEAD) {
                part = ((float)server->weight) / median;
                used_points = ring->config.points_per_server * part;
            }
            else
                used_points = 0;
            ServerItem_set_used_points(&ring->config, server, used_points);
            Continuum_add_server(ring->continuum, i, server->points.buf, used_points);
        }
        Continuum_sort(ring->continuum);
    }
}

void
ConsistentHash_exchange_server_list(ConsistentHash_t *ring, ConsistentHash_ServerList_t *list)
{
    ConsistentHash_ServerList_t tmp, *new_list;
    uint32_t i;

    tmp = ring->servers;
    ring->servers = *list;
    *list = tmp;
    new_list = &ring->servers;

    if (tmp.list.count) { /* copy generated points */
        for(i = 0; i < tmp.list.count; i++) {
            CH_ServerItem_t *new_item;
            new_item = TiredSet_get(new_list->by_name, ServerItem_name_as_handle(tmp.list.buf[i]));
            if (new_item)
                ServerItem_steal_points_and_alive(new_item, tmp.list.buf[i]);
        }
    }

    ConsistentHash_update_continuum(ring);
}

void
ConsistentHash_refresh_alive_by_name(ConsistentHash_t *ring, ConsistentHash_AliveByName_t *list,
                                   CH_aliveness_e default_alive)
{
    uint32_t i;
    CH_ServerItem_t *server;
    ConsistentHash_ServerList_t *servers = &ring->servers;

    if (servers->list.count == 0) return;

    for(i = 0; i < servers->list.count; i++) {
        servers->list.buf[i]->alive_as_updated = default_alive;
    }

    if (list != NULL) {
        for(i = 0; i < list->list.count; i++) {
            server = TiredSet_get(servers->by_name, (CH_handle_t)(uintptr_t)list->list.buf[i].name);
            if (server != NULL) {
                server->alive_as_updated = list->list.buf[i].alive;
            }
        }
    }

    ConsistentHash_update_continuum(ring);
}

void
ConsistentHash_refresh_alive_by_handle(ConsistentHash_t *ring, ConsistentHash_AliveByHandle_t *list,
                                     CH_aliveness_e default_alive)
{
    uint32_t i;
    CH_ServerItem_t *server;
    ConsistentHash_ServerList_t *servers = &ring->servers;

    if (servers->list.count == 0) return;

    for(i = 0; i < servers->list.count; i++) {
        servers->list.buf[i]->alive_as_updated = default_alive;
    }

    if (list != NULL) {
        for(i = 0; i < list->list.count; i++) {
            server = TiredSet_get(servers->by_handle, list->list.buf[i].handle);
            if (server != NULL) {
                server->alive_as_updated = list->list.buf[i].alive;
            }
        }
    }

    ConsistentHash_update_continuum(ring);
}

/* ITERATOR */

#define BITS_PER_UINT (32)

static inline void
CH_Iterator_ensure_bitmap(ConsistentHash_Iterator_t *iterator, uint32_t count)
{
    if (count > (iterator->bitmap.capa + 1) * BITS_PER_UINT) {
        ensure_capa(&iterator->ring->config, iterator->bitmap, ((count | (BITS_PER_UINT-1))+1) / BITS_PER_UINT);
    }
}

static inline void
CH_Iterator_bitmap_set(ConsistentHash_Iterator_t *iterator, uint32_t pos)
{
    if (pos < BITS_PER_UINT)
        iterator->bitmap.smallbuf |= 1 << pos;
    else {
        CH_Iterator_ensure_bitmap(iterator, pos + 1);
        pos -= BITS_PER_UINT;
        iterator->bitmap.buf[pos / BITS_PER_UINT] |= 1 << (pos % BITS_PER_UINT);
    }
}

static inline uint32_t
CH_Iterator_bitmap_get(ConsistentHash_Iterator_t *iterator, uint32_t pos)
{
    if (pos < BITS_PER_UINT)
        return iterator->bitmap.smallbuf & (1 << pos);
    else {
        pos -= BITS_PER_UINT;
        return (pos < iterator->bitmap.capa * BITS_PER_UINT) ?
            iterator->bitmap.buf[pos / BITS_PER_UINT] & (1 << (pos % BITS_PER_UINT)) : 0;
    }
}

inline ConsistentHash_Iterator_t *
ConsistentHash_Iterator_alloc(ConsistentHash_t *ring)
{
    ConsistentHash_Iterator_t *iterator;
    do_malloc(&ring->config, &iterator, 1);
    iterator->ring = ring;
    return iterator;
}

inline void
ConsistentHash_Iterator_init(ConsistentHash_Iterator_t *iterator, const char *name, size_t name_len)
{
    ConsistentHash_t *ring = iterator->ring;
    do_memzero(iterator, 1);
    iterator->ring = ring;
    iterator->name = CH_Name_new(&ring->config, name, name_len);
    iterator->seed = ~5;
    CH_Iterator_ensure_bitmap(iterator, ring->servers.list.count);
}

ConsistentHash_Iterator_t *
ConsistentHash_Iterator_new(ConsistentHash_t *ring, const char *name, size_t name_len)
{
    ConsistentHash_Iterator_t *iterator;
    iterator = ConsistentHash_Iterator_alloc(ring);
    ConsistentHash_Iterator_init(iterator, name, name_len);
    return iterator;
}

size_t
ConsistentHash_Iterator_size(ConsistentHash_Iterator_t *iterator)
{
    return sizeof(*iterator) + buf_size(iterator->bitmap);
}

void
ConsistentHash_Iterator_release(ConsistentHash_Iterator_t *iterator)
{
    ConsistentHash_t *ring = iterator->ring;
    CH_Name_free(&iterator->ring->config, iterator->name);
    do_free(&iterator->ring->config, &iterator->bitmap.buf);
    do_memzero(iterator, 1);
    iterator->ring = ring;
}

void
ConsistentHash_Iterator_reinit(ConsistentHash_Iterator_t *iterator, const char *name, size_t name_len)
{
    ConsistentHash_t *ring = iterator->ring;
    CH_Name_free(&iterator->ring->config, iterator->name);
    do_free(&iterator->ring->config, &iterator->bitmap.buf);
    do_memzero(iterator, 1);
    iterator->ring = ring;
    iterator->name = CH_Name_new(&ring->config, name, name_len);
    iterator->seed = ~5;
    CH_Iterator_ensure_bitmap(iterator, ring->servers.list.count);
}

void
ConsistentHash_Iterator_free(ConsistentHash_Iterator_t *iterator)
{
    if (iterator) {
        ConsistentHash_Iterator_release(iterator);
        do_free(&iterator->ring->config, &iterator);
    }
}

static uint32_t
ConsistentHash_Iterator_next_server(ConsistentHash_Iterator_t *iterator)
{
    uint32_t hash, server;
    CH_aliveness_e alive;
    ConsistentHash_t *ring = iterator->ring;
    ConsistentHash_ServerList_t *list = &ring->servers;
    CH_Name_t *name = iterator->name;

    if (ring->alive_count <= iterator->found) {
        return (uint32_t)-1;
    }

    while (iterator->visited < ring->visitable_count) {
        hash = ring->config.item_hash(ring->config.ctx, name->str, name->size, iterator->seed);
        if (!Continuum_find_server(ring->continuum, hash, &server))
            return (uint32_t)-1;
        iterator->seed--;

        if (!CH_Iterator_bitmap_get(iterator, server)) {
            CH_Iterator_bitmap_set(iterator, server);
            iterator->visited++;

            if (server > list->list.count)
                return (uint32_t)-1;

            alive = server_item_alive(list->list.buf[server]);
            if (alive == CH_ALIVE) {
                iterator->found++;
                return server;
            }
        }
    }

    return (uint32_t)-1;
}

/**
 * returns {0, NULL} when no more servers
 */
ConsistentHash_IteratorName_t
ConsistentHash_Iterator_next_name(ConsistentHash_Iterator_t *iterator)
{
    ConsistentHash_IteratorName_t name = {0, NULL};
    uint32_t next_server;
    ConsistentHash_ServerList_t *list = &iterator->ring->servers;

    next_server = ConsistentHash_Iterator_next_server(iterator);
    if (next_server < list->list.count) {
        name.name = list->list.buf[next_server]->name->str;
        name.size = list->list.buf[next_server]->name->size;
    }
    return name;
}

/**
 * returns {0, 0} when no more servers
 */
ConsistentHash_IteratorHandle_t
ConsistentHash_Iterator_next_handle(ConsistentHash_Iterator_t *iterator) {
    ConsistentHash_IteratorHandle_t handle = {0, 0};
    uint32_t next_server;
    ConsistentHash_ServerList_t *list = &iterator->ring->servers;

    if (iterator->ring->config.use_handle == CH_DONOT_USE_HANDLE)
        return handle;

    next_server = ConsistentHash_Iterator_next_server(iterator);
    if (next_server < list->list.count) {
        handle.handle = list->list.buf[next_server]->handle;
        handle.found = 1;
    }
    return handle;
}

#endif
