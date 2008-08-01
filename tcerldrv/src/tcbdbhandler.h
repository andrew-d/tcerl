#ifndef __TC_ERL_HANDLER_H_
#define __TC_ERL_HANDLER_H_

#include <erl_driver.h>
#include <tcbdbupdatecounter.h>

#ifdef __cplusplus
extern "C"
    {
#endif

static int
erlang_term_compare (const char* aptr,
                     int         asiz,
                     const char* bptr,
                     int         bsiz,
                     void*       op);

/*=====================================================================*
 *                            Async methods                            *
 *=====================================================================*/

static void
bdb_tune        (FromEmulator* from)
{
  if (! from->d->open)
    {
      if (   tcbdbtune (from->d->bdb, 
                        from->bdb_tune.lmemb,
                        from->bdb_tune.nmemb,
                        from->bdb_tune.bnum,
                        from->bdb_tune.apow,
                        from->bdb_tune.fpow,
                        from->bdb_tune.opts)
          && tcbdbsetcache (from->d->bdb,
                            from->bdb_tune.lcnum,
                            from->bdb_tune.ncnum)
          && (from->bdb_tune.raw ||
              tcbdbsetcmpfunc (from->d->bdb, erlang_term_compare, NULL)))
        {
          make_reply_string (from, "ok");
        }
      else
        {
          make_reply_error (from, tcbdbecode (from->d->bdb));
        }
    }
  else
    {
      make_reply_string (from, "already open");
    }
}

static void
bdb_open        (FromEmulator* from)
{
  if (! from->d->open)
    {
      if (tcbdbopen (from->d->bdb, from->bdb_open.path, from->bdb_open.omode))
        {
          make_reply_string (from, "ok");
          from->d->open = 1;
        }
      else
        {
          make_reply_error (from, tcbdbecode (from->d->bdb));
        }
    }
  else
    {
      make_reply_string (from, "already open");
    }
}

static void
bdb_close        (FromEmulator* from)
{
  if (from->d->open)
    {
      if (tcbdbclose (from->d->bdb))
        {
          make_reply_string (from, "ok");
          from->d->open = 0;
        }
      else
        {
          make_reply_error (from, tcbdbecode (from->d->bdb));
        }
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

static void
bdb_put         (FromEmulator* from)
{
  if (from->d->open)
    {
      if (tcbdbput (from->d->bdb, 
                    from->bdb_put.kbuf,
                    from->bdb_put.ksiz,
                    from->bdb_put.vbuf,
                    from->bdb_put.vsiz))
        {
          make_reply_string (from, "ok");
        }
      else
        {
          make_reply_error (from, tcbdbecode (from->d->bdb));
        }
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

static void
bdb_put_dup     (FromEmulator* from)
{
  if (from->d->open)
    {
      if (tcbdbputdup (from->d->bdb, 
                       from->bdb_put.kbuf,
                       from->bdb_put.ksiz,
                       from->bdb_put.vbuf,
                       from->bdb_put.vsiz))
        {
          make_reply_string (from, "ok");
        }
      else
        {
          make_reply_error (from, tcbdbecode (from->d->bdb));
        }
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

static void
bdb_out         (FromEmulator* from)
{
  if (from->d->open)
    {
      if (tcbdbout3 (from->d->bdb, from->bdb_out.kbuf, from->bdb_out.ksiz))
        {
          make_reply_string (from, "ok");
        }
      else
        {
          make_reply_error (from, tcbdbecode (from->d->bdb));
        }
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

static void
bdb_get         (FromEmulator* from)
{
  if (from->d->open)
    {
      switch (tcbdbvnum (from->d->bdb, from->bdb_get.kbuf, from->bdb_get.ksiz)) 
        {
          case 0:
            make_reply_empty_list (from);
            break;

          case 1:
            {
              const void *vbuf;
              int vsiz;

              vbuf = tcbdbget3 (from->d->bdb,
                                from->bdb_get.kbuf,
                                from->bdb_get.ksiz,
                                &vsiz);

              if (vbuf != NULL)
                {
                  make_reply_binary_singleton (from, vbuf, vsiz);
                }
              else /* vbuf == NULL */
                {
                  make_reply_empty_list (from);
                }
            }

            break;

          default:
            {
              TCLIST* vals = tcbdbget4 (from->d->bdb,
                                        from->bdb_get.kbuf,
                                        from->bdb_get.ksiz);

              if (vals != NULL)
                {
                  make_reply_binary_list (from, vals);
                }
              else /* vals == NULL */
                {
                  make_reply_empty_list (from);
                }
            }

            break;
        }
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

static void
bdb_cur_init (FromEmulator* from,
              bool        (*first) (BDBCUR*))
{
  if (from->d->open)
    {
      if (first (from->d->cur))
        {
          const void *kbuf;
          int ksiz;

          kbuf = tcbdbcurkey3 (from->d->cur, &ksiz);

          if (kbuf != NULL)
            {
              make_reply_binary_singleton (from, kbuf, ksiz);
            }
          else
            {
              make_reply_empty_list (from);
            }
        }
      else
        {
          make_reply_empty_list (from);
        }
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

#define bdb_cur_advance(from, next, oldkbuf, oldksiz, op)       \
  do                                                            \
    {                                                           \
      const void* kbuf;                                         \
      int ksiz;                                                 \
                                                                \
      kbuf = tcbdbcurkey3 (from->d->cur, &ksiz);                \
                                                                \
      /* with ordered_bag semantics, there can be many keys to skip */  \
                                                                \
      while (   kbuf != NULL                                    \
             && from->d->bdb->cmp (oldkbuf,                     \
                                   oldksiz,                     \
                                   kbuf,                        \
                                   ksiz,                        \
                                   from->d->bdb->cmpop) op 0)   \
        {                                                       \
          if (! next (from->d->cur))                            \
            {                                                   \
              goto EMPTY_LIST;                                  \
            }                                                   \
                                                                \
          kbuf = tcbdbcurkey3 (from->d->cur, &ksiz);            \
        }                                                       \
                                                                \
      if (kbuf != NULL)                                         \
        {                                                       \
          make_reply_binary_singleton (from, kbuf, ksiz);       \
        }                                                       \
      else                                                      \
        {                                                       \
    EMPTY_LIST:                                                 \
          make_reply_empty_list (from);                         \
        }                                                       \
    }                                                           \
  while (0)

static void
bdb_first   (FromEmulator* from)
{
  bdb_cur_init (from, tcbdbcurfirst);
}

static void
bdb_next   (FromEmulator* from)
{
  if (from->d->open)
    {
      /* match ets:next/2 when ordered set:
       * "If the table is of type ordered_set, the function returns the 
       * next key in order, even if the object does no longer exist."
       */

      if (tcbdbcurjump (from->d->cur, from->bdb_next.kbuf, from->bdb_next.ksiz))
        {
          bdb_cur_advance (from,
                           tcbdbcurnext,
                           from->bdb_next.kbuf,
                           from->bdb_next.ksiz,
                           >=);
        }
      else
        {
          make_reply_empty_list (from);
        }
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

static void
bdb_last   (FromEmulator* from)
{
  bdb_cur_init (from, tcbdbcurlast);
}

static void
bdb_prev   (FromEmulator* from)
{
  if (from->d->open)
    {
      /*
       * slightly different than next, because
       * tcbdbcurjump positions either on or right after
       * the probe key, so we might have to 
       * return the last key if it exists
       */

      if (tcbdbcurjump (from->d->cur, from->bdb_prev.kbuf, from->bdb_prev.ksiz))
        {
          bdb_cur_advance (from,
                           tcbdbcurprev,
                           from->bdb_prev.kbuf,
                           from->bdb_prev.ksiz,
                           <=);
        }
      else 
        {
          bdb_cur_init (from, tcbdbcurlast);
        }
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

static void
bdb_range  (FromEmulator* from)
{
  if (from->d->open)
    {
      if (from->bdb_range.max == 0)
        {
          make_reply_empty_list (from);
        }
      else
        {
          TCLIST* vals = tcbdbrange (from->d->bdb, 
                                     from->bdb_range.bkbuf, 
                                     from->bdb_range.bksiz,
                                     from->bdb_range.binc,
                                     from->bdb_range.ekbuf, 
                                     from->bdb_range.eksiz,
                                     from->bdb_range.einc,
                                     from->bdb_range.max);

          make_reply_binary_list (from, vals);
        }
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

static void
bdb_fwm    (FromEmulator* from)
{
  if (from->d->open)
    {
      if (from->bdb_fwm.max == 0)
        {
          make_reply_empty_list (from);
        }
      else
        {
          TCLIST* vals = tcbdbfwmkeys (from->d->bdb,
                                       from->bdb_fwm.pbuf,
                                       from->bdb_fwm.psiz,
                                       from->bdb_fwm.max);

          make_reply_binary_list (from, vals);
        }
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

static void
bdb_vanish       (FromEmulator* from)
{
  if (from->d->open)
    {
      if (tcbdbvanish (from->d->bdb))
        {
          make_reply_string (from, "ok");
        }
      else
        {
          make_reply_error (from, tcbdbecode (from->d->bdb));
        }
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

static void
bdb_out_exact   (FromEmulator* from)
{
  if (from->d->open)
    {
      if (tcbdbcurjump (from->d->cur,
                        from->bdb_out_exact.kbuf,
                        from->bdb_out_exact.ksiz))
        {
          const void* kbuf;
          int ksiz;

          kbuf = tcbdbcurkey3 (from->d->cur, &ksiz);

          while (   kbuf != NULL
                 && from->d->bdb->cmp (from->bdb_out_exact.kbuf,
                                       from->bdb_out_exact.ksiz,
                                       kbuf,
                                       ksiz,
                                       from->d->bdb->cmpop) == 0)
            {
              const void* vbuf;
              int vsiz;

              vbuf = tcbdbcurval3 (from->d->cur, &vsiz);

              if (   vbuf != NULL
                  && from->d->bdb->cmp (from->bdb_out_exact.vbuf,
                                        from->bdb_out_exact.vsiz,
                                        vbuf,
                                        vsiz,
                                        from->d->bdb->cmpop) == 0)
                {
                  if (! tcbdbcurout (from->d->cur))
                    {
                      goto ERROR;
                    }
                }
              else
                {
                  if (! tcbdbcurnext (from->d->cur))
                    {
                      goto OK;
                    }
                }

              kbuf = tcbdbcurkey3 (from->d->cur, &ksiz);
            }
OK:
          make_reply_string (from, "ok");
        }
      else
        {
ERROR:
          make_reply_error (from, tcbdbecode (from->d->bdb));
        }
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

static void
bdb_info        (FromEmulator* from)
{
  if (from->d->open)
    {
      uint64_t data[2] = { tcbdbfsiz (from->d->bdb), tcbdbrnum (from->d->bdb) };

      make_reply_binary (from, data, 16);
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

static void
bdb_sync         (FromEmulator* from)
{
  if (from->d->open)
    {
      if (tcbdbsync (from->d->bdb))
        {
          make_reply_string (from, "ok");
        }
      else
        {
          make_reply_error (from, tcbdbecode (from->d->bdb));
        }
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

static void
bdb_update_counter (FromEmulator* from)
{
  if (from->d->open)
    {
      const void *vbuf;
      int vsiz;

      vbuf = tcbdbget3 (from->d->bdb,
                        from->bdb_get.kbuf,
                        from->bdb_get.ksiz,
                        &vsiz);

      if (vbuf != NULL)
        {
          int dest_len = vsiz + 3;
          unsigned char dest[dest_len];
          int32_t result;

          if (tcbdb_update_counter (vbuf,
                                    vsiz,
                                    dest,
                                    &dest_len,
                                    from->bdb_update_counter.pos,
                                    from->bdb_update_counter.incr,
                                    &result))
            {
              if (tcbdbput (from->d->bdb,
                            from->bdb_update_counter.kbuf,
                            from->bdb_update_counter.ksiz,
                            dest,
                            dest_len))
                {
                  make_reply_binary_singleton (from, &result, 4);
                }
              else
                {
                  make_reply_error (from, tcbdbecode (from->d->bdb));
                }
            }
          else
            {
              make_reply_string (from, "invalid update");
            }
        }
      else /* vbuf == NULL */
        {
          make_reply_empty_list (from);
        }
    }
  else
    {
      make_reply_string (from, "not open");
    }
}

static void
bdb_out_async   (FromEmulator* from)
{
  if (from->d->open)
    {
      tcbdbout3 (from->d->bdb, from->bdb_out.kbuf, from->bdb_out.ksiz);
    }

  make_reply_null (from);
}

static void
bdb_put_async   (FromEmulator* from)
{
  if (from->d->open)
    {
      tcbdbput (from->d->bdb,
                from->bdb_put.kbuf,
                from->bdb_put.ksiz,
                from->bdb_put.vbuf,
                from->bdb_put.vsiz);
    }

  make_reply_null (from);
}

static void
bdb_out_exact_async   (FromEmulator* from)
{
  if (from->d->open)
    {
      if (tcbdbcurjump (from->d->cur,
                        from->bdb_out_exact_async.kbuf,
                        from->bdb_out_exact_async.ksiz))
        {
          const void* kbuf;
          int ksiz;

          kbuf = tcbdbcurkey3 (from->d->cur, &ksiz);

          while (   kbuf != NULL
                 && from->d->bdb->cmp (from->bdb_out_exact_async.kbuf,
                                       from->bdb_out_exact_async.ksiz,
                                       kbuf,
                                       ksiz,
                                       from->d->bdb->cmpop) == 0)
            {
              const void* vbuf;
              int vsiz;

              vbuf = tcbdbcurval3 (from->d->cur, &vsiz);

              if (   vbuf != NULL
                  && from->d->bdb->cmp (from->bdb_out_exact_async.vbuf,
                                        from->bdb_out_exact_async.vsiz,
                                        vbuf,
                                        vsiz,
                                        from->d->bdb->cmpop) == 0)
                {
                  if (! tcbdbcurout (from->d->cur))
                    {
                      goto ERROR;
                    }
                }
              else
                {
                  if (! tcbdbcurnext (from->d->cur))
                    {
                      goto OK;
                    }
                }

              kbuf = tcbdbcurkey3 (from->d->cur, &ksiz);
            }
OK:
          (void) 0;
        }
      else
        {
ERROR:
          (void) 0;
        }
    }

  make_reply_null (from);
}

static void
init_handlers (handler* handlers)
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
  handlers[EMULATOR_REQUEST_BDB_OUT_ASYNC] = bdb_out_async;
  handlers[EMULATOR_REQUEST_BDB_PUT_ASYNC] = bdb_put_async;
  handlers[EMULATOR_REQUEST_BDB_OUT_EXACT_ASYNC] = bdb_out_exact_async;
}

#ifdef __cplusplus
    }
#endif

#endif /* __TC_ERL_HANDLER_H_ */
