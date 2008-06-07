#include "config.h"
#undef ERL_SYS_DRV

#include <assert.h>
#include <erl_driver.h>
#include <tcutil.h>
#include <tcbdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "tcbdbfrom.h"
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

/*=====================================================================*
 *                         Erl Driver callbacks                        *
 *=====================================================================*/

static int
tcbdb_init (void)
{
  my_erl_init_marshal ();

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

  d->port = port;
  d->bdb = tcbdbnew ();
  driver_system_info (&info, sizeof (info));
  if (info.smp_support)
    {
      tcbdbsetmutex (d->bdb);
    }
  d->cur = tcbdbcurnew (d->bdb);
  init_handlers (d->handlers);

  return (ErlDrvData) d;
}

static void
tcbdb_output           (ErlDrvData     handle,
                        char*          buf,
                        int            buflen)
{
  TcDriverData* d = (TcDriverData*) handle;
  FromEmulator from_emulator = decode_from (buf, buflen);

  switch (from_emulator.type)
    {
      case EMULATOR_REQUEST_INVALID:
        reply_string (d->port, "invalid request");

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
        d->handlers[from_emulator.type] (d, from_emulator);

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

  tcbdbcurdel (d->cur);
  tcbdbdel (d->bdb);
  driver_free (d);
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
  .driver_name = (char*) "libtcbdb",
  .extended_marker = ERL_DRV_EXTENDED_MARKER,
  .major_version = ERL_DRV_EXTENDED_MAJOR_VERSION,
  .minor_version = ERL_DRV_EXTENDED_MINOR_VERSION,
  .driver_flags = ERL_DRV_FLAG_USE_PORT_LOCKING
};

DRIVER_INIT (libtcbdb);

DRIVER_INIT (libtcbdb) /* must match name in driver_entry */
{
  return &tcbdb_driver_entry;
}
