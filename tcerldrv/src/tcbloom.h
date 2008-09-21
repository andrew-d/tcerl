#ifndef __TC_BLOOM_H_
#define __TC_BLOOM_H_

#include <stdint.h>
#include <sys/types.h>

typedef struct _TcBloom TcBloom;

#ifdef __cplusplus
extern "C"
    {
#endif

TcBloom*
tc_bloom_open (const char*       filename,
               void*           (*alloc) (size_t),
               int               mode,
               ...);
               /* uint64_t num_bytes,
                  uint8_t  num_hashes */

void
tc_bloom_close (TcBloom* filter,
                void   (*dealloc) (void*));

int
tc_bloom_lookup (TcBloom*       filter,
                 const void*    p,
                 size_t         len);

void
tc_bloom_insert (TcBloom*       filter,
                 const void*    p,
                 size_t         len);

void
tc_bloom_vanish (TcBloom*       filter);

#ifdef __cplusplus
    }
#endif

#endif /* __TC_BLOOM_H_ */
