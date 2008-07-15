#ifndef __TC_ERL_FROM_H_
#define __TC_ERL_FROM_H_

typedef struct _FromEmulator FromEmulator;

#include "tcbdbcodec.h"
#include "tcbdbtypes.h"

#ifdef __cplusplus
extern "C"
    {
#endif

struct _FromEmulator 
{
  RequestType                                   type;

  union
    {
      struct
        {
          int32_t       lmemb; 
          int32_t       nmemb; 
          int64_t       bnum; 
          int8_t        apow; 
          int8_t        fpow; 
          uint8_t       opts;
          int32_t       lcnum; 
          int32_t       ncnum;
          int8_t        raw;
        }                       bdb_tune;

      struct
        {
          const char*   path;
          int           omode;
        }                       bdb_open;

      struct
        {
        }                       bdb_close;

      struct
        {
          const void*   kbuf;
          int           ksiz;
          const void*   vbuf;
          int           vsiz;
        }                       bdb_put;

      struct
        {
          const void*   kbuf;
          int           ksiz;
          const void*   vbuf;
          int           vsiz;
        }                       bdb_put_dup;

      struct
        {
          const void*   kbuf;
          int           ksiz;
        }                       bdb_out;

      struct
        {
          const void*   kbuf;
          int           ksiz;
        }                       bdb_get;

      struct
        {
        }                       bdb_first;

      struct
        {
          const void*   kbuf;
          int           ksiz;
        }                       bdb_next;

      struct
        {
        }                       bdb_last;

      struct
        {
          const void*   kbuf;
          int           ksiz;
        }                       bdb_prev;

      struct
        {
          const void*   bkbuf;
          int           bksiz;
          bool          binc;
          const void*   ekbuf;
          int           eksiz;
          bool          einc;
          int           max;
        }                       bdb_range;

      struct
        {
          const void*   pbuf;
          int           psiz;
          int           max;
        }                       bdb_fwm;

      struct
        {
        }                       bdb_vanish;

      struct
        {
          const void*   kbuf;
          int           ksiz;
          const void*   vbuf;
          int           vsiz;
        }                       bdb_out_exact;

      struct
        {
        }                       bdb_info;

      struct
        {
        }                       bdb_sync;

      struct
        {
          const void*   kbuf;
          int           ksiz;
          uint32_t      pos;
          int32_t       incr;
        }                       bdb_update_counter;
    };
};

static FromEmulator
decode_from (char*              buf,
             int                buflen)
{
  FromEmulator from;
  unsigned char type;

  memset (&from, 0, sizeof (FromEmulator));

  DECODE_BYTE (type);

  if (is_request_type (type))
    {
      from.type = type;

      switch ((RequestType) type)
        {
          case EMULATOR_REQUEST_BDB_TUNE:
            DECODE_NATIVE_64_SIGNED (from.bdb_tune.lmemb);
            DECODE_NATIVE_64_SIGNED (from.bdb_tune.nmemb);
            DECODE_NATIVE_64_SIGNED (from.bdb_tune.bnum);
            DECODE_NATIVE_64_SIGNED (from.bdb_tune.apow);
            DECODE_NATIVE_64_SIGNED (from.bdb_tune.fpow);
            DECODE_TUNE_OPTS (from.bdb_tune.opts);
            DECODE_NATIVE_64_SIGNED (from.bdb_tune.lcnum);
            DECODE_NATIVE_64_SIGNED (from.bdb_tune.ncnum);
            DECODE_BYTE (from.bdb_tune.raw);

            break;

          case EMULATOR_REQUEST_BDB_OPEN:
            DECODE_STRING (from.bdb_open.path);
            DECODE_OMODE (from.bdb_open.omode);

            break;

          case EMULATOR_REQUEST_BDB_CLOSE:

            break;

          case EMULATOR_REQUEST_BDB_PUT:
            DECODE_BINARY (from.bdb_put.ksiz, from.bdb_put.kbuf);
            DECODE_BINARY (from.bdb_put.vsiz, from.bdb_put.vbuf);

            break;

          case EMULATOR_REQUEST_BDB_PUT_DUP:
            DECODE_BINARY (from.bdb_put_dup.ksiz, from.bdb_put_dup.kbuf);
            DECODE_BINARY (from.bdb_put_dup.vsiz, from.bdb_put_dup.vbuf);

            break;

          case EMULATOR_REQUEST_BDB_OUT:
            DECODE_BINARY (from.bdb_out.ksiz, from.bdb_out.kbuf);

            break;

          case EMULATOR_REQUEST_BDB_GET:
            DECODE_BINARY (from.bdb_get.ksiz, from.bdb_get.kbuf);

            break;

          case EMULATOR_REQUEST_BDB_FIRST:

            break;

          case EMULATOR_REQUEST_BDB_NEXT:
            DECODE_BINARY (from.bdb_next.ksiz, from.bdb_next.kbuf);

            break;

          case EMULATOR_REQUEST_BDB_LAST:

            break;

          case EMULATOR_REQUEST_BDB_PREV:
            DECODE_BINARY (from.bdb_prev.ksiz, from.bdb_prev.kbuf);

            break;

          case EMULATOR_REQUEST_BDB_RANGE:
            DECODE_BINARY (from.bdb_range.bksiz, from.bdb_range.bkbuf);
            DECODE_BYTE (from.bdb_range.binc);
            DECODE_BINARY (from.bdb_range.eksiz, from.bdb_range.ekbuf);
            DECODE_BYTE (from.bdb_range.einc);
            DECODE_NATIVE_64_SIGNED (from.bdb_range.max);

            break;

          case EMULATOR_REQUEST_BDB_FWM:
            DECODE_BINARY (from.bdb_fwm.psiz, from.bdb_fwm.pbuf);
            DECODE_NATIVE_64_SIGNED (from.bdb_fwm.max);

            break;

          case EMULATOR_REQUEST_BDB_VANISH:

            break;

          case EMULATOR_REQUEST_BDB_OUT_EXACT:
            DECODE_BINARY (from.bdb_out_exact.ksiz, from.bdb_out_exact.kbuf);
            DECODE_BINARY (from.bdb_out_exact.vsiz, from.bdb_out_exact.vbuf);

            break;

          case EMULATOR_REQUEST_BDB_INFO:

            break;

          case EMULATOR_REQUEST_BDB_SYNC:

            break;

          case EMULATOR_REQUEST_BDB_UPDATE_COUNTER:
            DECODE_BINARY (from.bdb_update_counter.ksiz,
                           from.bdb_update_counter.kbuf);
            DECODE_NATIVE_64_UNSIGNED (from.bdb_update_counter.pos);
            DECODE_NATIVE_64_SIGNED (from.bdb_update_counter.incr);

            break;

          case EMULATOR_REQUEST_INVALID:

            break;
        }
    }
  else
    {
      from.type = EMULATOR_REQUEST_INVALID;
    }

  return from;

ERROR:
  from.type = EMULATOR_REQUEST_INVALID;

  return from;
}

#ifdef __cplusplus
    }
#endif

#endif /* __TC_ERL_FROM_H_ */
