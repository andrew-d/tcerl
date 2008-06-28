#ifndef __TC_ERL_HANDLER_H_
#define __TC_ERL_HANDLER_H_

#include <erl_driver.h>
#include <tcbdbupdatecounter.h>

#ifdef __cplusplus
extern "C"
    {
#endif

static void
reply_binary (ErlDrvPort        port,
              ErlDrvBinary*     bin,
              int               len)
{
  ErlDrvTermData spec[] = { 
      ERL_DRV_PORT, driver_mk_port (port),
        ERL_DRV_ATOM, driver_mk_atom ((char *) "data"),
        ERL_DRV_BINARY, TERM_DATA (bin), len, 0,
      ERL_DRV_TUPLE, 2,
    ERL_DRV_TUPLE, 2
  };

  driver_send_term (port,
                    driver_caller (port),
                    spec,
                    sizeof (spec) / sizeof (spec[0]));
}

static void
reply_binary_singleton (ErlDrvPort      port,
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

  driver_send_term (port,
                    driver_caller (port),
                    spec,
                    sizeof (spec) / sizeof (spec[0]));

  driver_free_binary (bin);
}

static void
reply_binary_list       (ErlDrvPort     port,
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

  driver_send_term (port,
                    driver_caller (port),
                    spec,
                    4 * num_vals + 11);

  for (i = 0; i < num_vals; ++i)
    {
      driver_free_binary ((void*) spec[4 * i + 5]);
    }
}

static void
reply_empty_list (ErlDrvPort port)
{
  ErlDrvTermData spec[] = { 
      ERL_DRV_PORT, driver_mk_port (port),
        ERL_DRV_ATOM, driver_mk_atom ((char *) "data"),
        ERL_DRV_NIL,
      ERL_DRV_TUPLE, 2,
    ERL_DRV_TUPLE, 2
  };

  driver_send_term (port,
                    driver_caller (port),
                    spec,
                    sizeof (spec) / sizeof (spec[0]));
}

static void
reply_string (ErlDrvPort        port,
              const char*       string)
{
  int len = strlen (string);
  ErlDrvBinary* bin = driver_alloc_binary (len);

  memcpy (bin->orig_bytes, string, len);
  reply_binary (port, bin, len);
  driver_free_binary (bin);
}

static void
reply_error  (ErlDrvPort        port,
              TCBDB*            bdb)
{
  int ecode = tcbdbecode (bdb);
  const char* message = tcbdberrmsg (ecode);

  reply_string (port, message);
}

static int
erlang_term_compare (const char* aptr,
                     int         asiz,
                     const char* bptr,
                     int         bsiz,
                     void*       op);

/*
 * bdb_tune
 * 
 * Really starting to become a catch-all config function.
 */

static void
bdb_tune        (TcDriverData* d,
                 FromEmulator  from)
{
  if (! d->open)
    {
      if (   tcbdbtune (d->bdb, 
                        from.bdb_tune.lmemb,
                        from.bdb_tune.nmemb,
                        from.bdb_tune.bnum,
                        from.bdb_tune.apow,
                        from.bdb_tune.fpow,
                        from.bdb_tune.opts)
          && tcbdbsetcache (d->bdb,
                            from.bdb_tune.lcnum,
                            from.bdb_tune.ncnum)
          && (from.bdb_tune.raw ||
              tcbdbsetcmpfunc (d->bdb, erlang_term_compare, NULL)))
        {
          reply_string (d->port, "ok");
        }
      else
        {
          reply_error (d->port, d->bdb);
        }
    }
  else
    {
      reply_string (d->port, "already open");
    }
}

static void
bdb_open        (TcDriverData* d,
                 FromEmulator  from)
{
  if (! d->open)
    {
      if (tcbdbopen (d->bdb, from.bdb_open.path, from.bdb_open.omode))
        {
          reply_string (d->port, "ok");
          d->open = 1;
        }
      else
        {
          reply_error (d->port, d->bdb);
        }
    }
  else
    {
      reply_string (d->port, "already open");
    }
}

static void
bdb_close        (TcDriverData* d,
                  FromEmulator  from)
{
  (void) from;

  if (d->open)
    {
      if (tcbdbclose (d->bdb))
        {
          reply_string (d->port, "ok");
          d->open = 0;
        }
      else
        {
          reply_error (d->port, d->bdb);
        }
    }
  else
    {
      reply_string (d->port, "not open");
    }
}

static void
bdb_put         (TcDriverData* d,
                 FromEmulator  from)
{
  if (d->open)
    {
      if (tcbdbput (d->bdb, 
                    from.bdb_put.kbuf,
                    from.bdb_put.ksiz,
                    from.bdb_put.vbuf,
                    from.bdb_put.vsiz))
        {
          reply_string (d->port, "ok");
        }
      else
        {
          reply_error (d->port, d->bdb);
        }
    }
  else
    {
      reply_string (d->port, "not open");
    }
}

static void
bdb_put_dup     (TcDriverData* d,
                 FromEmulator  from)
{
  if (d->open)
    {
      if (tcbdbputdup (d->bdb, 
                       from.bdb_put.kbuf,
                       from.bdb_put.ksiz,
                       from.bdb_put.vbuf,
                       from.bdb_put.vsiz))
        {
          reply_string (d->port, "ok");
        }
      else
        {
          reply_error (d->port, d->bdb);
        }
    }
  else
    {
      reply_string (d->port, "not open");
    }
}

static void
bdb_out         (TcDriverData* d,
                 FromEmulator  from)
{
  if (d->open)
    {
      if (tcbdbout3 (d->bdb, from.bdb_out.kbuf, from.bdb_out.ksiz))
        {
          reply_string (d->port, "ok");
        }
      else
        {
          reply_error (d->port, d->bdb);
        }
    }
  else
    {
      reply_string (d->port, "not open");
    }
}

static void
bdb_get         (TcDriverData* d,
                 FromEmulator  from)
{
  if (d->open)
    {
      switch (tcbdbvnum (d->bdb, from.bdb_get.kbuf, from.bdb_get.ksiz)) 
        {
          case 0:
            reply_empty_list (d->port);
            break;

          case 1:
            {
              const void *vbuf;
              int vsiz;

              vbuf = tcbdbget3 (d->bdb,
                                from.bdb_get.kbuf,
                                from.bdb_get.ksiz,
                                &vsiz);

              if (vbuf != NULL)
                {
                  reply_binary_singleton (d->port, vbuf, vsiz);
                }
              else /* vbuf == NULL */
                {
                  reply_empty_list (d->port);
                }
            }

            break;

          default:
            {
              TCLIST* vals = tcbdbget4 (d->bdb,
                                        from.bdb_get.kbuf,
                                        from.bdb_get.ksiz);

              if (vals != NULL)
                {
                  reply_binary_list (d->port, vals);
                  tclistdel (vals);
                }
              else /* vals == NULL */
                {
                  reply_empty_list (d->port);
                }
            }

            break;
        }
    }
  else
    {
      reply_string (d->port, "not open");
    }
}

static void
bdb_cur_init (TcDriverData* d,
              bool        (*first) (BDBCUR*))
{
  if (d->open)
    {
      if (first (d->cur))
        {
          const void *kbuf;
          int ksiz;

          kbuf = tcbdbcurkey3 (d->cur, &ksiz);

          if (kbuf != NULL)
            {
              reply_binary_singleton (d->port, kbuf, ksiz);
            }
          else
            {
              reply_empty_list (d->port);
            }
        }
      else
        {
          reply_empty_list (d->port);
        }
    }
  else
    {
      reply_string (d->port, "not open");
    }
}

#define bdb_cur_advance(d, next, oldkbuf, oldksiz, op)  \
  do                                                    \
    {                                                   \
      const void* kbuf;                                 \
      int ksiz;                                         \
                                                        \
      kbuf = tcbdbcurkey3 (d->cur, &ksiz);              \
                                                        \
      /* with ordered_bag semantics, there can be many keys to skip */  \
                                                        \
      while (   kbuf != NULL                            \
             && d->bdb->cmp (oldkbuf,                   \
                             oldksiz,                   \
                             kbuf,                      \
                             ksiz,                      \
                             d->bdb->cmpop) op 0)       \
        {                                               \
          if (! next (d->cur))                          \
            {                                           \
              goto EMPTY_LIST;                          \
            }                                           \
                                                        \
          kbuf = tcbdbcurkey3 (d->cur, &ksiz);          \
        }                                               \
                                                        \
      if (kbuf != NULL)                                 \
        {                                               \
          reply_binary_singleton (d->port, kbuf, ksiz); \
        }                                               \
      else                                              \
        {                                               \
    EMPTY_LIST:                                         \
          reply_empty_list (d->port);                   \
        }                                               \
    }                                                   \
  while (0)

static void
bdb_first   (TcDriverData* d,
             FromEmulator  from)
{
  (void) from;

  bdb_cur_init (d, tcbdbcurfirst);
}

static void
bdb_next   (TcDriverData* d,
            FromEmulator  from)
{
  if (d->open)
    {
      /* match ets:next/2 when ordered set:
       * "If the table is of type ordered_set, the function returns the 
       * next key in order, even if the object does no longer exist."
       */

      if (tcbdbcurjump (d->cur, from.bdb_next.kbuf, from.bdb_next.ksiz))
        {
          bdb_cur_advance (d, 
                           tcbdbcurnext,
                           from.bdb_next.kbuf,
                           from.bdb_next.ksiz,
                           >=);
        }
      else
        {
          reply_empty_list (d->port);
        }

    }
  else
    {
      reply_string (d->port, "not open");
    }
}

static void
bdb_last   (TcDriverData* d,
            FromEmulator  from)
{
  (void) from;

  bdb_cur_init (d, tcbdbcurlast);
}

static void
bdb_prev   (TcDriverData* d,
            FromEmulator  from)
{
  if (d->open)
    {
      /*
       * slightly different than next, because
       * tcbdbcurjump positions either on or right after
       * the probe key, so we might have to 
       * return the last key if it exists
       */

      if (tcbdbcurjump (d->cur, from.bdb_prev.kbuf, from.bdb_prev.ksiz))
        {
          bdb_cur_advance (d,
                           tcbdbcurprev,
                           from.bdb_prev.kbuf,
                           from.bdb_prev.ksiz,
                           <=);
        }
      else 
        {
          bdb_cur_init (d, tcbdbcurlast);
        }
    }
  else
    {
      reply_string (d->port, "not open");
    }
}

static void
bdb_range  (TcDriverData* d,
            FromEmulator  from)
{
  if (d->open)
    {
      if (from.bdb_range.max == 0)
        {
          reply_empty_list (d->port);
        }
      else
        {
          TCLIST* vals = tcbdbrange (d->bdb, 
                                     from.bdb_range.bkbuf, 
                                     from.bdb_range.bksiz,
                                     from.bdb_range.binc,
                                     from.bdb_range.ekbuf, 
                                     from.bdb_range.eksiz,
                                     from.bdb_range.einc,
                                     from.bdb_range.max);

          reply_binary_list (d->port, vals);

          tclistdel (vals);
        }
    }
  else
    {
      reply_string (d->port, "not open");
    }
}

static void
bdb_fwm    (TcDriverData* d,
            FromEmulator  from)
{
  if (d->open)
    {
      if (from.bdb_fwm.max == 0)
        {
          reply_empty_list (d->port);
        }
      else
        {
          TCLIST* vals = tcbdbfwmkeys (d->bdb,
                                       from.bdb_fwm.pbuf,
                                       from.bdb_fwm.psiz,
                                       from.bdb_fwm.max);

          reply_binary_list (d->port, vals);

          tclistdel (vals);
        }
    }
  else
    {
      reply_string (d->port, "not open");
    }
}

static void
bdb_vanish       (TcDriverData* d,
                  FromEmulator  from)
{
  (void) from;

  if (d->open)
    {
      if (tcbdbvanish (d->bdb))
        {
          reply_string (d->port, "ok");
        }
      else
        {
          reply_error (d->port, d->bdb);
        }
    }
  else
    {
      reply_string (d->port, "not open");
    }
}

static void
bdb_out_exact   (TcDriverData* d,
                 FromEmulator  from)
{
  if (d->open)
    {
      if (tcbdbcurjump (d->cur, from.bdb_next.kbuf, from.bdb_next.ksiz))
        {
          const void* kbuf;
          int ksiz;

          kbuf = tcbdbcurkey3 (d->cur, &ksiz);

          while (   kbuf != NULL
                 && d->bdb->cmp (from.bdb_out_exact.kbuf,
                                 from.bdb_out_exact.ksiz,
                                 kbuf,
                                 ksiz,
                                 d->bdb->cmpop) == 0)
            {
              const void* vbuf;
              int vsiz;

              vbuf = tcbdbcurval3 (d->cur, &vsiz);

              if (   vbuf != NULL
                  && d->bdb->cmp (from.bdb_out_exact.vbuf,
                                  from.bdb_out_exact.vsiz,
                                  vbuf,
                                  vsiz,
                                  d->bdb->cmpop) == 0)
                {
                  if (! tcbdbcurout (d->cur))
                    {
                      goto ERROR;
                    }
                }
              else
                {
                  if (! tcbdbcurnext (d->cur))
                    {
                      goto OK;
                    }
                }

              kbuf = tcbdbcurkey3 (d->cur, &ksiz);
            }
OK:
          reply_string (d->port, "ok");
        }
      else
        {
ERROR:
          reply_error (d->port, d->bdb);
        }
    }
  else
    {
      reply_string (d->port, "not open");
    }
}

static void
bdb_info        (TcDriverData* d,
                 FromEmulator  from)
{
  (void) from;

  if (d->open)
    {
      uint64_t size = tcbdbfsiz (d->bdb);
      uint64_t no_records = tcbdbrnum (d->bdb);
      ErlDrvBinary* bin = driver_alloc_binary (16);

      memcpy (bin->orig_bytes, &size, 8);
      memcpy (bin->orig_bytes + 8, &no_records, 8);

      reply_binary (d->port, bin, 16);

      driver_free_binary (bin);
    }
  else
    {
      reply_string (d->port, "not open");
    }
}

static void
bdb_sync         (TcDriverData* d,
                  FromEmulator  from)
{
  (void) from;

  if (d->open)
    {
      if (tcbdbsync (d->bdb))
        {
          reply_string (d->port, "ok");
        }
      else
        {
          reply_error (d->port, d->bdb);
        }
    }
  else
    {
      reply_string (d->port, "not open");
    }
}

static void
bdb_update_counter (TcDriverData* d,
                    FromEmulator  from)
{
  if (d->open)
    {
      const void *vbuf;
      int vsiz;

      vbuf = tcbdbget3 (d->bdb,
                        from.bdb_get.kbuf,
                        from.bdb_get.ksiz,
                        &vsiz);

      if (vbuf != NULL)
        {
          unsigned char dest[vsiz + 3];
          int32_t result;

          if (tcbdb_update_counter (vbuf,
                                    vsiz,
                                    dest,
                                    vsiz + 3, 
                                    from.bdb_update_counter.pos,
                                    from.bdb_update_counter.incr,
                                    &result))
            {
              reply_string (d->port, "invalid update");
            }
          else
            {
              reply_binary_singleton (d->port, &result, 4);
            }
        }
      else /* vbuf == NULL */
        {
          reply_empty_list (d->port);
        }
    }
  else
    {
      reply_string (d->port, "not open");
    }
}

static void
init_handlers (handler handlers[1 + EMULATOR_REQUEST_BDB_PREV])
{
  handlers[EMULATOR_REQUEST_BDB_TUNE] = bdb_tune;
  handlers[EMULATOR_REQUEST_BDB_OPEN] = bdb_open;
  handlers[EMULATOR_REQUEST_BDB_CLOSE] = bdb_close;
  handlers[EMULATOR_REQUEST_BDB_PUT] = bdb_put;
  handlers[EMULATOR_REQUEST_BDB_PUT_DUP] = bdb_put_dup;
  handlers[EMULATOR_REQUEST_BDB_OUT] = bdb_out;
  handlers[EMULATOR_REQUEST_BDB_GET] = bdb_get;
  handlers[EMULATOR_REQUEST_BDB_FIRST] = bdb_first;
  handlers[EMULATOR_REQUEST_BDB_NEXT] = bdb_next;
  handlers[EMULATOR_REQUEST_BDB_LAST] = bdb_last;
  handlers[EMULATOR_REQUEST_BDB_PREV] = bdb_prev;
  handlers[EMULATOR_REQUEST_BDB_RANGE] = bdb_range;
  handlers[EMULATOR_REQUEST_BDB_FWM] = bdb_fwm;
  handlers[EMULATOR_REQUEST_BDB_VANISH] = bdb_vanish;
  handlers[EMULATOR_REQUEST_BDB_OUT_EXACT] = bdb_out_exact;
  handlers[EMULATOR_REQUEST_BDB_INFO] = bdb_info;
  handlers[EMULATOR_REQUEST_BDB_SYNC] = bdb_sync;
  handlers[EMULATOR_REQUEST_BDB_UPDATE_COUNTER] = bdb_update_counter;
}

#ifdef __cplusplus
    }
#endif

#endif /* __TC_ERL_HANDLER_H_ */
