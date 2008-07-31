#ifndef __TC_ERL_TO_H_
#define __TC_ERL_TO_H_

enum _ReplyType 
{
  EMULATOR_REPLY_BINARY = 0,
  EMULATOR_REPLY_BINARY_SINGLETON = 1,
  EMULATOR_REPLY_BINARY_LIST = 2,
  EMULATOR_REPLY_EMPTY_LIST = 3,
  EMULATOR_REPLY_STRING = 4,
  EMULATOR_REPLY_ERROR = 5,

  EMULATOR_REPLY_INVALID = 255
};
typedef enum _ReplyType ReplyType;
typedef struct _ToEmulator ToEmulator;

#include "tcbdbcodec.h"
#include "tcbdbfrom.h"
#include "tcbdbtypes.h"

#ifdef __cplusplus
extern "C"
    {
#endif

struct _ToEmulator 
{
  ReplyType                                     type;

  union
    {
      struct
        {
          void*         data;
          int           len;
        }                       binary;

      struct
        {
          void*         data;
          int           len;
        }                       binary_singleton;

      struct
        {
          TCLIST*       vals;
        }                       binary_list;

      struct
        {
        }                       empty_list;

      struct
        {
          const char*   data;
        }                       string;

      struct
        {
          int           ecode;
        }                       error;
    };
};

static void
to_emulator_destruct (ToEmulator to)
{
  switch (to.type)
    {
      case EMULATOR_REPLY_INVALID:
        break;

      case EMULATOR_REPLY_BINARY:
        driver_free (to.binary.data);

        break;

      case EMULATOR_REPLY_BINARY_SINGLETON:
        driver_free (to.binary_singleton.data);

        break;

      case EMULATOR_REPLY_BINARY_LIST:
        tclistdel (to.binary_list.vals);

        break;

      case EMULATOR_REPLY_EMPTY_LIST:
      case EMULATOR_REPLY_STRING:
      case EMULATOR_REPLY_ERROR:
        break;
    }
}

static void
reply_binary (ErlDrvPort        port,
              ErlDrvTermData    caller,
              const void*       data,
              int               len)
{
  ErlDrvBinary* bin = driver_alloc_binary (len);
  ErlDrvTermData spec[] = { 
      ERL_DRV_PORT, driver_mk_port (port),
        ERL_DRV_ATOM, driver_mk_atom ((char *) "data"),
        ERL_DRV_BINARY, TERM_DATA (bin), len, 0,
      ERL_DRV_TUPLE, 2,
    ERL_DRV_TUPLE, 2
  };

  memcpy (bin->orig_bytes, data, len);

  driver_send_term (port, caller, spec, sizeof (spec) / sizeof (spec[0]));

  driver_free_binary (bin);
}

static void
reply_binary_singleton (ErlDrvPort      port,
                        ErlDrvTermData  caller,
                        const void*     data,
                        int             len)
{
  ErlDrvBinary* bin = driver_alloc_binary (len);
  ErlDrvTermData spec[] = { 
      ERL_DRV_PORT, driver_mk_port (port),
        ERL_DRV_ATOM, driver_mk_atom ((char *) "data"),
          ERL_DRV_BINARY, TERM_DATA (bin), len, 0,
          ERL_DRV_NIL,
        ERL_DRV_LIST, 2,
      ERL_DRV_TUPLE, 2,
    ERL_DRV_TUPLE, 2
  };

  memcpy (bin->orig_bytes, data, len);

  driver_send_term (port, caller, spec, sizeof (spec) / sizeof (spec[0]));

  driver_free_binary (bin);
}

static void
reply_binary_list       (ErlDrvPort     port,
                         ErlDrvTermData caller,
                         TCLIST*        vals)
{
  int i;
  int num_vals = tclistnum (vals);
  ErlDrvTermData spec[4 * num_vals + 11];

  spec[0] = ERL_DRV_PORT;
  spec[1] = driver_mk_port (port);
  spec[2] = ERL_DRV_ATOM;
  spec[3] = driver_mk_atom ((char *) "data");

  for (i = 0; i < num_vals; ++i)
    {
      int vsiz;
      const void* vbuf = tclistval (vals, i, &vsiz);
      ErlDrvBinary* bin = driver_alloc_binary (vsiz);

      memcpy (bin->orig_bytes, vbuf, vsiz);

      spec[4 * i + 4] = ERL_DRV_BINARY;
      spec[4 * i + 5] = TERM_DATA (bin);
      spec[4 * i + 6] = vsiz;
      spec[4 * i + 7] = 0;
    }

  spec[4 * num_vals + 4] = ERL_DRV_NIL;
  spec[4 * num_vals + 5] = ERL_DRV_LIST;
  spec[4 * num_vals + 6] = 1 + num_vals;
  spec[4 * num_vals + 7] = ERL_DRV_TUPLE;
  spec[4 * num_vals + 8] = 2;
  spec[4 * num_vals + 9] = ERL_DRV_TUPLE;
  spec[4 * num_vals + 10] = 2;

  driver_send_term (port, caller, spec, 4 * num_vals + 11);

  for (i = 0; i < num_vals; ++i)
    {
      driver_free_binary ((void*) spec[4 * i + 5]);
    }
}

static void
reply_empty_list (ErlDrvPort     port,
                  ErlDrvTermData caller)
{
  ErlDrvTermData spec[] = { 
      ERL_DRV_PORT, driver_mk_port (port),
        ERL_DRV_ATOM, driver_mk_atom ((char *) "data"),
        ERL_DRV_NIL,
      ERL_DRV_TUPLE, 2,
    ERL_DRV_TUPLE, 2
  };

  driver_send_term (port, caller, spec, sizeof (spec) / sizeof (spec[0]));
}

static void
reply_string (ErlDrvPort        port,
              ErlDrvTermData    caller,
              const char*       string)
{
  reply_binary (port, caller, string, strlen (string));
}

static void
reply_error  (ErlDrvPort        port,
              ErlDrvTermData    caller,
              int               ecode)
{
  reply_string (port, caller, tcbdberrmsg (ecode));
}

#ifdef __cplusplus
    }
#endif

#endif /* __TC_ERL_TO_H_ */
