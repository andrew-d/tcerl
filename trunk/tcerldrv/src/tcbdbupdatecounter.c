#include "tcbdbupdatecounter.h"

#include <stdint.h>
#include <string.h>

#include <erl_interface.h>
#include <ei.h>

#define ERL_MAGIC 131

enum _ExternalType
{
  SMALL_INT = 97,
  LARGE_INT = 98,
  FLOAT = 99,
  ATOM = 100,
  REFERENCE = 101,
  PORT = 102,
  PID = 103,
  SMALL_TUPLE = 104,
  LARGE_TUPLE = 105,
  NIL = 106,
  STRING = 107,
  LIST = 108,
  BINARY = 109,
  SMALL_BIGNUM = 110,
  LARGE_BIGNUM = 111,
  NEW_CACHE = 78,
  CACHED_ATOM = 67,
  NEW_REFERENCE = 114,
  FUN = 117,
  NEW_FUN = 112,
  EXPORT = 113,
  BIT_BINARY = 77,
  NEW_FLOAT = 70
};
typedef enum _ExternalType ExternalType;

static int
copy_element    (const unsigned char**  source,
                 int*                   source_len,
                 unsigned char**        dest,
                 int*                   dest_len);

static int
is_small_integer(const unsigned char*   source,
                 int                    source_len);

static int
is_large_integer(const unsigned char*   source,
                 int                    source_len);

static int
is_small_tuple  (const unsigned char*   source,
                 int                    source_len);

static int
is_large_tuple  (const unsigned char*   source,
                 int                    source_len);

static uint8_t
small_integer   (const unsigned char*   source);

static int32_t
large_integer   (const unsigned char*   source);

static uint32_t
unsigned_large_integer (const unsigned char*   source);

static void
output_integer (int32_t         val,
                unsigned char** dest,
                int*            dest_len);

static int
update_tuple   (uint32_t                  elems,
                const unsigned char*      source,
                int                       source_len,
                unsigned char*            dest,
                int                       dest_len,
                uint32_t                  pos,
                int32_t                   incr,
                int32_t*                  result);

/*=====================================================================*
 *                                Public                               *
 *=====================================================================*/

int
tcbdb_update_counter (const unsigned char*      source,
                      int                       source_len,
                      unsigned char*            dest,
                      int                       dest_len,
                      uint32_t                  pos,
                      int32_t                   incr,
                      int32_t*                  result)
{
  if (   source_len < 1
      || dest_len < source_len
      || *source != ERL_MAGIC)
    {
      return 1;
    }

  ++source;
  --source_len;

  *dest++ = ERL_MAGIC;
  dest_len = 1;

  if (is_small_tuple (source, source_len))
    {
      ++source;
      --source_len;

      *dest++ = SMALL_TUPLE;
      --dest_len;

      uint8_t elems = small_integer (source);

      ++source;
      --source_len;

      *dest++ = elems;
      --dest_len;

      return update_tuple (elems,
                           source,
                           source_len,
                           dest,
                           dest_len,
                           pos,
                           incr,
                           result);
    }
  else if (is_large_tuple (source, source_len))
    {
      ++source;
      --source_len;

      *dest++ = LARGE_TUPLE;
      --dest_len;

      uint32_t elems = unsigned_large_integer (source);

      source += 4;
      source_len -= 4;

      memcpy (dest, source, 4);
      dest_len -= 4;

      return update_tuple (elems,
                           source,
                           source_len,
                           dest,
                           dest_len,
                           pos,
                           incr,
                           result);
    }
  else
    {
      return 1;
    }
}

/*=====================================================================*
 *                               Private                               *
 *=====================================================================*/

static int
copy_element    (const unsigned char**  source,
                 int*                   source_len,
                 unsigned char**        dest,
                 int*                   dest_len)
{
  int source_index = 0;

  if (   ei_skip_term ((const char *) *source, &source_index) < 0
      || *dest_len < source_index)
    {
      return 1;
    }

  memcpy (*dest, *source, source_index);

  *dest += source_index;
  *source += source_index;
  *dest_len -= source_index;
  *source_len -= source_index;

  return 0;
}

static int
is_small_integer (const unsigned char*    source,
                  int                     source_len)
{
  return (source_len >= 2 && *source == SMALL_INT);
}

static int
is_large_integer (const unsigned char*    source,
                  int                     source_len)
{
  return (source_len >= 5 && *source == LARGE_INT);
}

static int
is_small_tuple (const unsigned char*    source,
                int                     source_len)
{
  return (source_len >= 2 && *source == SMALL_TUPLE);
}

static int
is_large_tuple (const unsigned char*    source,
                int                     source_len)
{
  return (source_len >= 5 && *source == LARGE_TUPLE);
}

static uint8_t
small_integer   (const unsigned char*   source)
{
  return *source;
}

static int32_t
large_integer   (const unsigned char*   source)
{
  int32_t val;

  val = source[0] & 0x7F;
  val <<= 8;
  val |= source[1];
  val <<= 8;
  val |= source[2];
  val <<= 8;
  val |= source[3];

  if (source[0] & 0x80)
    {
      val *= -1;
    }

  return val;
}

static uint32_t
unsigned_large_integer   (const unsigned char*   source)
{
  uint32_t val;

  val = source[0];
  val <<= 8;
  val |= source[1];
  val <<= 8;
  val |= source[2];
  val <<= 8;
  val |= source[3];

  return val;
}

static void
output_integer (int32_t         val,
                unsigned char** dest,
                int*            dest_len)
{
  if (val >= 0 && val < 256)
    {
      *(*dest)++ = SMALL_INT;
      --(*dest_len);

      *(*dest)++ = val;
      --(*dest_len);
    }
  else 
    {
      *(*dest)++ = LARGE_INT;
      --(*dest_len);

      *dest[3] = (val & 0xFF000000) >> 24;
      *dest[2] = (val & 0x00FF0000) >> 16;
      *dest[1] = (val & 0x0000FF00) >> 8;
      *dest[0] = (val & 0x000000FF);

      (*dest_len) -= 4;
    }
}

static int
update_tuple   (uint32_t                  elems,
                const unsigned char*      source,
                int                       source_len,
                unsigned char*            dest,
                int                       dest_len,
                uint32_t                  pos,
                int32_t                   incr,
                int32_t*                  result)
{
  uint32_t i;

  if (pos > elems)
    {
      return 1;
    }

  for (i = 1; i < pos; ++i)
    {
      if (copy_element (&source, &source_len, &dest, &dest_len))
        {
          return 1;
        }
    }

  if (is_small_integer (source, source_len))
    {
      ++source;
      --source_len;

      int32_t val = small_integer (source);

      ++source;
      --source_len;

      val += incr;

      output_integer (val, &dest, &dest_len);

      *result = val;
    }
  else if (is_large_integer (source, source_len))
    {
      ++source;
      --source_len;

      int32_t val = large_integer (source);

      source += 4;
      source_len -= 4;

      val += incr;

      output_integer (val, &dest, &dest_len);

      *result = val;
    }
  else
    {
      /* punt on bignums for now */
      /* TODO: use ei_decode_term here to grab the bignum field */

      return 1;
    }

  for (i = pos; i < elems; ++i)
    {
      if (copy_element (&source, &source_len, &dest, &dest_len))
        {
          return 1;
        }
    }

  return 0;
}
