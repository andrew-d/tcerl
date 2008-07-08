#ifndef __TC_ERL_TYPES_H_
#define __TC_ERL_TYPES_H_

typedef struct _TcDriverData TcDriverData;
enum _RequestType 
{
  EMULATOR_REQUEST_BDB_TUNE = 0,
  EMULATOR_REQUEST_BDB_OPEN = 1,
  EMULATOR_REQUEST_BDB_CLOSE = 2,
  EMULATOR_REQUEST_BDB_PUT = 3,
  EMULATOR_REQUEST_BDB_PUT_DUP = 4,
  EMULATOR_REQUEST_BDB_OUT = 5,
  EMULATOR_REQUEST_BDB_GET = 6,
  EMULATOR_REQUEST_BDB_FIRST = 7,
  EMULATOR_REQUEST_BDB_NEXT = 8,
  EMULATOR_REQUEST_BDB_LAST = 9,
  EMULATOR_REQUEST_BDB_PREV = 10,
  EMULATOR_REQUEST_BDB_RANGE = 11,
  EMULATOR_REQUEST_BDB_FWM = 12,
  EMULATOR_REQUEST_BDB_VANISH = 13,
  EMULATOR_REQUEST_BDB_OUT_EXACT = 14,
  EMULATOR_REQUEST_BDB_INFO = 15,
  EMULATOR_REQUEST_BDB_SYNC = 16,
  EMULATOR_REQUEST_BDB_UPDATE_COUNTER = 17,

  EMULATOR_REQUEST_INVALID = 255
};
typedef enum _RequestType RequestType;

#include <stdbool.h>
#include "tcbdbfrom.h"

typedef void (*handler) (TcDriverData*, FromEmulator);

#ifdef __cplusplus
extern "C"
    {
#endif

static bool
is_request_type (unsigned char type)
{
  return type <= EMULATOR_REQUEST_BDB_UPDATE_COUNTER;
}

struct _TcDriverData 
{
  ErlDrvPort    port;
  TCBDB*        bdb;
  BDBCUR*       cur;
  handler       handlers[1 + EMULATOR_REQUEST_BDB_UPDATE_COUNTER];
  unsigned int  open : 1;
};

#ifdef __cplusplus
    }
#endif

#endif /* __TC_ERL_TYPES_H_ */
