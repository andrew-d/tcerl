#ifndef __TC_ERL_FROM_H_
#define __TC_ERL_FROM_H_

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
  EMULATOR_REQUEST_BDB_OUT_ASYNC = 18,

  EMULATOR_REQUEST_INVALID = 255
};
typedef enum _RequestType RequestType;
typedef struct _FromEmulator FromEmulator;

#define EMULATOR_REQUEST_MAX EMULATOR_REQUEST_BDB_OUT_ASYNC

#include "tcbdbcodec.h"
#include "tcbdbto.h"
#include "tcbdbtypes.h"

#ifdef __cplusplus
extern "C"
    {
#endif

struct _FromEmulator 
{
  RequestType                                   type;

  TcDriverData*                                 d;

  ErlDrvTermData                                caller;

  ToEmulator                                    to;

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
          char*         path;
          int           omode;
        }                       bdb_open;

      struct
        {
        }                       bdb_close;

      struct
        {
          void*         kbuf;
          int           ksiz;
          void*         vbuf;
          int           vsiz;
        }                       bdb_put;

      struct
        {
          void*         kbuf;
          int           ksiz;
          void*         vbuf;
          int           vsiz;
        }                       bdb_put_dup;

      struct
        {
          void*         kbuf;
          int           ksiz;
        }                       bdb_out;

      struct
        {
          void*         kbuf;
          int           ksiz;
        }                       bdb_get;

      struct
        {
        }                       bdb_first;

      struct
        {
          void*         kbuf;
          int           ksiz;
        }                       bdb_next;

      struct
        {
        }                       bdb_last;

      struct
        {
          void*         kbuf;
          int           ksiz;
        }                       bdb_prev;

      struct
        {
          void*         bkbuf;
          int           bksiz;
          bool          binc;
          void*         ekbuf;
          int           eksiz;
          bool          einc;
          int           max;
        }                       bdb_range;

      struct
        {
          void*         pbuf;
          int           psiz;
          int           max;
        }                       bdb_fwm;

      struct
        {
        }                       bdb_vanish;

      struct
        {
          void*         kbuf;
          int           ksiz;
          void*         vbuf;
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
          void*         kbuf;
          int           ksiz;
          uint32_t      pos;
          int32_t       incr;
        }                       bdb_update_counter;

      struct
        {
          void*         kbuf;
          int           ksiz;
        }                       bdb_out_async;
    };
};

static FromEmulator
decode_from (TcDriverData*      d,
             char*              buf,
             int                buflen)
{
  FromEmulator from;
  unsigned char type;

  memset (&from, 0, sizeof (FromEmulator));
  from.d = d;

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

          case EMULATOR_REQUEST_BDB_OUT_ASYNC:
            DECODE_BINARY (from.bdb_out_async.ksiz, from.bdb_out_async.kbuf);

            break;

          case EMULATOR_REQUEST_INVALID:

            break;
        }
    }
  else
    {
      from.type = EMULATOR_REQUEST_INVALID;
      from.to.type = EMULATOR_REPLY_INVALID;
    }

  return from;

ERROR:
  from.type = EMULATOR_REQUEST_INVALID;
  from.to.type = EMULATOR_REPLY_INVALID;

  return from;
}

static void*
my_memdup (const void* src,
           int         len)
{
  if (src == NULL)
    {
      return NULL;
    }
  else
    {
      void* dest = driver_alloc (len);
      memcpy (dest, src, len);
      return dest;
    }
}

static void
my_free (void* p)
{
  if (p != NULL)
    {
      driver_free (p);
    }
}

static FromEmulator*
from_emulator_dup (FromEmulator* from)
{
  from->caller = driver_caller (from->d->port);
  from->to.type = EMULATOR_REPLY_INVALID;

  if (from->d->async_threads)
    {
      FromEmulator* dup = my_memdup (from, sizeof (FromEmulator));

      dup->d = data_ref (from->d);

      switch (from->type)
        {
          case EMULATOR_REQUEST_BDB_TUNE:

            break;

          case EMULATOR_REQUEST_BDB_OPEN:
            dup->bdb_open.path = my_memdup (from->bdb_open.path,
                                            1 + strlen (from->bdb_open.path));

            break;

          case EMULATOR_REQUEST_BDB_CLOSE:
            break;

          case EMULATOR_REQUEST_BDB_PUT:
            dup->bdb_put.kbuf = my_memdup (from->bdb_put.kbuf,
                                           from->bdb_put.ksiz);
            dup->bdb_put.vbuf = my_memdup (from->bdb_put.vbuf,
                                           from->bdb_put.vsiz);

            break;

          case EMULATOR_REQUEST_BDB_PUT_DUP:
            dup->bdb_put_dup.kbuf = my_memdup (from->bdb_put_dup.kbuf,
                                               from->bdb_put_dup.ksiz);
            dup->bdb_put_dup.vbuf = my_memdup (from->bdb_put_dup.vbuf,
                                               from->bdb_put_dup.vsiz);

            break;

          case EMULATOR_REQUEST_BDB_OUT:
            dup->bdb_out.kbuf = my_memdup (from->bdb_out.kbuf,
                                           from->bdb_out.ksiz);

            break;

          case EMULATOR_REQUEST_BDB_GET:
            dup->bdb_get.kbuf = my_memdup (from->bdb_get.kbuf,
                                           from->bdb_get.ksiz);

            break;

          case EMULATOR_REQUEST_BDB_FIRST:

            break;

          case EMULATOR_REQUEST_BDB_NEXT:
            dup->bdb_next.kbuf = my_memdup (from->bdb_next.kbuf,
                                            from->bdb_next.ksiz);

            break;

          case EMULATOR_REQUEST_BDB_LAST:

            break;

          case EMULATOR_REQUEST_BDB_PREV:
            dup->bdb_prev.kbuf = my_memdup (from->bdb_prev.kbuf,
                                            from->bdb_prev.ksiz);

            break;

          case EMULATOR_REQUEST_BDB_RANGE:
            dup->bdb_range.bkbuf = my_memdup (from->bdb_range.bkbuf,
                                              from->bdb_range.bksiz);
            dup->bdb_range.ekbuf = my_memdup (from->bdb_range.ekbuf,
                                              from->bdb_range.eksiz);

            break;

          case EMULATOR_REQUEST_BDB_FWM:
            dup->bdb_fwm.pbuf = my_memdup (from->bdb_fwm.pbuf,
                                           from->bdb_fwm.psiz);

            break;

          case EMULATOR_REQUEST_BDB_VANISH:

            break;

          case EMULATOR_REQUEST_BDB_OUT_EXACT:
            dup->bdb_out_exact.kbuf = my_memdup (from->bdb_out_exact.kbuf,
                                                 from->bdb_out_exact.ksiz);
            dup->bdb_out_exact.vbuf = my_memdup (from->bdb_out_exact.vbuf,
                                                 from->bdb_out_exact.vsiz);

            break;

          case EMULATOR_REQUEST_BDB_INFO:
            
            break;

          case EMULATOR_REQUEST_BDB_SYNC:

            break;

          case EMULATOR_REQUEST_BDB_UPDATE_COUNTER:
            dup->bdb_update_counter.kbuf = 
              my_memdup (from->bdb_update_counter.kbuf,
                         from->bdb_update_counter.ksiz);

            break;

          case EMULATOR_REQUEST_BDB_OUT_ASYNC:
            dup->bdb_out_async.kbuf = my_memdup (from->bdb_out_async.kbuf,
                                                 from->bdb_out_async.ksiz);

            break;

          case EMULATOR_REQUEST_INVALID:

            break;
        }

      return dup;
    }
  else /* ! from->d->async_threads */
    {
      return from;
    }
}

static void
from_emulator_free (FromEmulator* dup)
{
  if (dup->d->async_threads)
    {
      switch (dup->type)
        {
          case EMULATOR_REQUEST_INVALID:

            break;

          case EMULATOR_REQUEST_BDB_TUNE:

            break;

          case EMULATOR_REQUEST_BDB_OPEN:
            my_free (dup->bdb_open.path);

            break;

          case EMULATOR_REQUEST_BDB_CLOSE:

            break;

          case EMULATOR_REQUEST_BDB_PUT:
            my_free (dup->bdb_put.kbuf);
            my_free (dup->bdb_put.vbuf);

            break;

          case EMULATOR_REQUEST_BDB_PUT_DUP:
            my_free (dup->bdb_put_dup.kbuf);
            my_free (dup->bdb_put_dup.vbuf);

            break;

          case EMULATOR_REQUEST_BDB_OUT:
            my_free (dup->bdb_out.kbuf);

            break;

          case EMULATOR_REQUEST_BDB_GET:
            my_free (dup->bdb_get.kbuf);

            break;

          case EMULATOR_REQUEST_BDB_FIRST:

            break;

          case EMULATOR_REQUEST_BDB_NEXT:
            my_free (dup->bdb_next.kbuf);

            break;

          case EMULATOR_REQUEST_BDB_LAST:

            break;

          case EMULATOR_REQUEST_BDB_PREV:
            my_free (dup->bdb_prev.kbuf);

            break;

          case EMULATOR_REQUEST_BDB_RANGE:
            my_free (dup->bdb_range.bkbuf);
            my_free (dup->bdb_range.ekbuf);

            break;

          case EMULATOR_REQUEST_BDB_FWM:
            my_free (dup->bdb_fwm.pbuf);

            break;

          case EMULATOR_REQUEST_BDB_VANISH:

            break;

          case EMULATOR_REQUEST_BDB_OUT_EXACT:
            my_free (dup->bdb_out_exact.kbuf);
            my_free (dup->bdb_out_exact.vbuf);

            break;

          case EMULATOR_REQUEST_BDB_INFO:

            break;

          case EMULATOR_REQUEST_BDB_SYNC:

            break;

          case EMULATOR_REQUEST_BDB_UPDATE_COUNTER:
            my_free (dup->bdb_update_counter.kbuf);

            break;

          case EMULATOR_REQUEST_BDB_OUT_ASYNC:
            my_free (dup->bdb_out_async.kbuf);

            break;
        }

      to_emulator_destruct (dup->to);
      data_unref (dup->d);
      my_free (dup);
    }
}

static void
make_reply_null (FromEmulator* from)
{
  from->to.type = EMULATOR_REPLY_NULL;
}

static void
make_reply_string (FromEmulator* from,
                   const char*   string)
{
  from->to.type = EMULATOR_REPLY_STRING;
  from->to.string.data = string;
}

static void
make_reply_error  (FromEmulator* from,
                   int           ecode)
{
  from->to.type = EMULATOR_REPLY_ERROR;
  from->to.error.ecode = ecode;
}

static void
make_reply_empty_list (FromEmulator* from)
{
  from->to.type = EMULATOR_REPLY_EMPTY_LIST;
}

static void
make_reply_binary (FromEmulator* from,
                   const void*   data,
                   int           len)
{
  from->to.type = EMULATOR_REPLY_BINARY;
  from->to.binary.data = my_memdup (data, len);
  from->to.binary.len = len;
}

static void
make_reply_binary_singleton (FromEmulator* from,
                             const void*   data,
                             int           len)
{
  from->to.type = EMULATOR_REPLY_BINARY_SINGLETON;
  from->to.binary_singleton.data = my_memdup (data, len);
  from->to.binary_singleton.len = len;
}

static void
make_reply_binary_list (FromEmulator* from,
                        TCLIST*       vals)
{
  from->to.type = EMULATOR_REPLY_BINARY_LIST;
  from->to.binary_list.vals = vals;
}

#ifdef __cplusplus
    }
#endif

#endif /* __TC_ERL_FROM_H_ */
