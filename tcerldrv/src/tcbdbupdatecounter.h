#ifndef TCBDB_UPDATE_COUNTER_H_
#define TCBDB_UPDATE_COUNTER_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C"
    {
#endif

int
tcbdb_update_counter (const unsigned char*      source,
                      int                       source_len,
                      unsigned char*            dest,
                      int                       dest_len,
                      uint32_t                  pos,
                      int32_t                   incr,
                      int32_t*                  result);

#ifdef __cplusplus
    }
#endif

#endif /* TCBDB_UPDATE_COUNTER_H_ */
