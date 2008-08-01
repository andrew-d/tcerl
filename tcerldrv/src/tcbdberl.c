#include "config.h"
#undef ERL_SYS_DRV

#include <assert.h>
#if HAVE_BACKTRACE_SYMBOLS
#include <execinfo.h>
#endif
#include <erl_driver.h>
#include <tcutil.h>
#include <tcbdb.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "tcbdbfrom.h"
#include "tcbdbto.h"
#include "tcbdbhandler.h"

#include "my_erl_marshal.h"

static int
erlang_term_compare (const char* aptr,
                     int         asiz,
                     const char* bptr,
                     int         bsiz,
                     void*       op)
{
  (void) asiz;
  (void) bsiz;
  (void) op;
  
  return my_erl_compare_ext ((unsigned char *) aptr, (unsigned char *) bptr);
}

#if HAVE_BACKTRACE_SYMBOLS
static void
print_trace (void)
{
  void *array[20];
  size_t size;
  char **strings;
  size_t i;

  size = backtrace (array, 20);
  strings = backtrace_symbols (array, size);

  fprintf (stderr, "Obtained %zd stack frames.\n", size);

  for (i = 0; i < size; i++)
     fprintf (stderr, "%s\n", strings[i]);

  free (strings);
}
#else
static void
print_trace (void)
{
  fprintf (stderr, "Can't print backtrace.\n");
}
#endif

static void
my_fatal_func (const char *message)
{
  fprintf (stderr, "fatal error in tcerl: %s\n", message);
  print_trace ();

  /* tokyocabinet calls exit (1) ...
   * instead call abort to get a core file 
   */

  abort ();
}

/*=====================================================================*
 *                         Erl Driver callbacks                        *
 *=====================================================================*/

static int
tcbdb_init (void)
{
  my_erl_init_marshal ();

  tcfatalfunc = my_fatal_func;

  return 0;
}

static ErlDrvData
tcbdb_start    (ErlDrvPort     port,
                char*          buf)
{
  ErlDrvSysInfo info;
  TcDriverData* d = (TcDriverData*) driver_alloc (sizeof (TcDriverData));
  (void) buf;

  memset (d, 0, sizeof (TcDriverData));

  d->ref_count = 1;
  d->port = port;
  d->bdb = tcbdbnew ();
  driver_system_info (&info, sizeof (info));
  if (info.thread_support && info.async_threads > 0)
    {
      d->async_threads = 1;
    }
  d->cur = tcbdbcurnew (d->bdb);
  init_handlers (d->handlers);

  d->magic = ((intptr_t) d) >> 8;

  return (ErlDrvData) d;
}

static void
async_invoke (void* data)
{
  FromEmulator* from = (FromEmulator *) data;

  switch (from->type)
    {
      case EMULATOR_REQUEST_INVALID:
        break;

      case EMULATOR_REQUEST_BDB_TUNE:
      case EMULATOR_REQUEST_BDB_OPEN:
      case EMULATOR_REQUEST_BDB_CLOSE:
      case EMULATOR_REQUEST_BDB_PUT:
      case EMULATOR_REQUEST_BDB_PUT_DUP:
      case EMULATOR_REQUEST_BDB_OUT:
      case EMULATOR_REQUEST_BDB_GET:
      case EMULATOR_REQUEST_BDB_FIRST:
      case EMULATOR_REQUEST_BDB_NEXT:
      case EMULATOR_REQUEST_BDB_LAST:
      case EMULATOR_REQUEST_BDB_PREV:
      case EMULATOR_REQUEST_BDB_RANGE:
      case EMULATOR_REQUEST_BDB_FWM:
      case EMULATOR_REQUEST_BDB_VANISH:
      case EMULATOR_REQUEST_BDB_OUT_EXACT:
      case EMULATOR_REQUEST_BDB_INFO:
      case EMULATOR_REQUEST_BDB_SYNC:
      case EMULATOR_REQUEST_BDB_UPDATE_COUNTER:
      case EMULATOR_REQUEST_BDB_OUT_ASYNC:
      case EMULATOR_REQUEST_BDB_PUT_ASYNC:
      case EMULATOR_REQUEST_BDB_OUT_EXACT_ASYNC:
        from->d->handlers[from->type] (from);
    }
}

static void
async_free (void* data)
{
  FromEmulator* from = (FromEmulator*) data;

  switch (from->to.type)
    {
      case EMULATOR_REPLY_INVALID:
        break;

      case EMULATOR_REPLY_BINARY:
        reply_binary (from->d->port,
                      from->caller,
                      from->to.binary.data,
                      from->to.binary.len);

        break;

      case EMULATOR_REPLY_BINARY_SINGLETON:
        reply_binary_singleton (from->d->port,
                                from->caller,
                                from->to.binary_singleton.data,
                                from->to.binary_singleton.len);

        break;

      case EMULATOR_REPLY_BINARY_LIST:
        reply_binary_list (from->d->port,
                           from->caller,
                           from->to.binary_list.vals);

        break;

      case EMULATOR_REPLY_EMPTY_LIST:
        reply_empty_list (from->d->port, from->caller);

        break;

      case EMULATOR_REPLY_STRING:
        reply_string (from->d->port, from->caller, from->to.string.data);

        break;

      case EMULATOR_REPLY_ERROR:
        reply_error (from->d->port, from->caller, from->to.error.ecode);

        break;

      case EMULATOR_REPLY_NULL:

        break;
    }

  from_emulator_free (from);
}

static void
tcbdb_output           (ErlDrvData     handle,
                        char*          buf,
                        int            buflen)
{
  TcDriverData* d = (TcDriverData*) handle;
  FromEmulator from = decode_from (d, buf, buflen);

  switch (from.type)
    {
      case EMULATOR_REQUEST_INVALID:
        reply_string (d->port, driver_caller (d->port), "invalid request");

        break;

      default:
        driver_async (d->port,
                      &d->magic,
                      async_invoke,
                      from_emulator_dup (&from),
                      async_free);

        break;
    }
}

static void
tcbdb_stop      (ErlDrvData      handle)
{
  TcDriverData* d = (TcDriverData*) handle;

  if (d->open)
    {
      tcbdbclose (d->bdb);
    }

  data_unref (d);
}

/*=====================================================================*
 *                             Entry Point                             *
 *=====================================================================*/

static ErlDrvEntry tcbdb_driver_entry = 
{
  .init = tcbdb_init,
  .start = tcbdb_start, 
  .stop = tcbdb_stop,
  .output = tcbdb_output,
  .driver_name = (char*) "libtcbdberl",
  .extended_marker = ERL_DRV_EXTENDED_MARKER,
  .major_version = ERL_DRV_EXTENDED_MAJOR_VERSION,
  .minor_version = ERL_DRV_EXTENDED_MINOR_VERSION,
  .driver_flags = ERL_DRV_FLAG_USE_PORT_LOCKING
};

DRIVER_INIT (libtcbdberl);

DRIVER_INIT (libtcbdberl) /* must match name in driver_entry */
{
  return &tcbdb_driver_entry;
}
