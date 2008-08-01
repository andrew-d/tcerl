#ifndef __TC_ERL_TYPES_H_
#define __TC_ERL_TYPES_H_

typedef struct _TcDriverData TcDriverData;

#include <stdbool.h>
#include "tcbdbfrom.h"
#include "tcbdbto.h"

typedef void (*handler) (FromEmulator*);

#ifdef __cplusplus
extern "C"
    {
#endif

static bool
is_request_type (unsigned char type)
{
  return type <= EMULATOR_REQUEST_MAX;
}

struct _TcDriverData 
{
  unsigned int  ref_count;
  ErlDrvPort    port;
  TCBDB*        bdb;
  BDBCUR*       cur;
  handler       handlers[1 + EMULATOR_REQUEST_MAX];
  unsigned int  magic;
  unsigned int  open : 1;
  unsigned int  async_threads : 1;
};

static TcDriverData* 
data_ref (TcDriverData* d)
{
  ++d->ref_count;
  return d;
}

static void
data_unref (TcDriverData* d)
{
  if (--d->ref_count == 0)
    {
      tcbdbcurdel (d->cur);
      tcbdbdel (d->bdb);
      driver_free (d);
    }
}

#ifdef __cplusplus
    }
#endif

#endif /* __TC_ERL_TYPES_H_ */
