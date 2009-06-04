#include "config.h"
#include "tcbloom.h"

#include <stdarg.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif

struct _TcBloom 
{
  uint8_t* start;
  uint64_t num_bytes;
  uint8_t  num_hashes;
  int      fd;
  mode_t   mode;
};

/*=====================================================================*
 *                               Private                               *
 *=====================================================================*/

static uint64_t
read_64le (int      fd)
{
  uint64_t val = 0;
  uint8_t bytes[8];
  int i;

  read (fd, bytes, 8);

  for (i = 0; i < 8; ++i)
    {
      val <<= 8;
      val |= bytes[7 - i];
    }

  return val;
}

static void
write_64le (int      fd,
            uint64_t val)
{
  int i;
  uint8_t bytes[8];

  for (i = 0; i < 8; ++i)
    {
      bytes[i] = val;
      val >>= 8;
    }

  write (fd, bytes, 8);
}

static uint64_t
djb2_hash  (const uint8_t* p,
            uint64_t       len,
            uint64_t       hash)
{
  uint64_t i;

  for (i = 0; i < len; ++i, ++p)
    {
      hash = ((hash << 5) + hash) ^ *p; 
    }
 
  return hash;
}

static unsigned int
get_bit (uint8_t* start,
         uint64_t bit)
{
  uint64_t byte = bit / 8;
  uint8_t offset = bit % 8;

  return start[byte] & (1U << offset);
}

static void
set_bit (uint8_t* start,
         uint64_t bit)
{
  uint64_t byte = bit / 8;
  uint8_t offset = bit % 8;

  start[byte] |= 1U << offset;
}

/*=====================================================================*
 *                                Public                               *
 *=====================================================================*/

TcBloom*
tc_bloom_open (const char*       filename,
               void*           (*alloc) (size_t),
               int               mode,
               ...)
               /* uint64_t num_bytes,
                  uint8_t  num_hashes */
{
  TcBloom* rv;
  int fd;
  int prot = PROT_READ;
  uint64_t num_bytes;
  uint8_t num_hashes;
  uint8_t* start;

  fd = open (filename, mode & ~O_CREAT & ~O_TRUNC);

  if (fd < 0)
    {
      if (    (mode & O_ACCMODE) == O_RDWR 
           && (mode & O_CREAT) == O_CREAT)
        {
          va_list ap;

          va_start (ap, mode);

          num_bytes = va_arg (ap, uint64_t);
          num_hashes = va_arg (ap, unsigned int);

          va_end (ap);

          fd = open (filename,
                     mode,
                     S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);

          if (fd >= 0)
            {
              lseek (fd, num_bytes + 1, SEEK_SET);
              write_64le (fd, num_bytes);
              write (fd, &num_hashes, 1);
            }
        }
    }

  if (fd < 0)
    {
      return NULL;
    }

  lseek (fd, -9, SEEK_END);
  num_bytes = read_64le (fd);
  read (fd, &num_hashes, 1);

  if ((mode & O_ACCMODE) == O_RDWR)
    {
      prot |= PROT_WRITE;
    }

  start = mmap (NULL, num_bytes, prot, MAP_SHARED | MAP_POPULATE, fd, 0);

  if (start == MAP_FAILED)
    {
      close (fd);
      return NULL;
    }

  rv = alloc (sizeof (TcBloom));
  rv->start = start;
  rv->num_bytes = num_bytes;
  rv->num_hashes = num_hashes;
  rv->fd = fd;
  rv->mode = mode;

  if ((mode & O_TRUNC) == O_TRUNC)
    {
      tc_bloom_vanish (rv);
    }

  return rv;
}

void
tc_bloom_close (TcBloom* filter,
                void   (*dealloc) (void*))
{
  if (filter != NULL)
    {
      munmap (filter->start, filter->num_bytes);
      close (filter->fd);
      dealloc (filter);
    }
}

int
tc_bloom_lookup (TcBloom*       filter,
                 const void*    p,
                 size_t         len)
{
  if (filter != NULL)
    {
      uint64_t i;
      uint64_t x = djb2_hash (p, len, 5381);
      uint64_t y = djb2_hash ((uint8_t*) "saltygoodness",
                              sizeof ("saltygoodness"),
                              x);
      uint64_t num_bits = 8 * filter->num_bytes;

      for (i = 0; i < filter->num_hashes; ++i)
        {
          if (! get_bit (filter->start, x % num_bits))
            {
              return 0;
            }

          x = (x + y) % num_bits;
          y = (y + i) % num_bits;
        }

      return 1;
    }
  else
    {
      return 1;
    }
}

void
tc_bloom_insert (TcBloom*       filter,
                 const void*    p,
                 size_t         len)
{
  if (filter != NULL)
    {
      uint64_t i;
      uint64_t x = djb2_hash (p, len, 5381);
      uint64_t y = djb2_hash ((uint8_t*) "saltygoodness",
                              sizeof ("saltygoodness"),
                              x);
      uint64_t num_bits = 8 * filter->num_bytes;

      for (i = 0; i < filter->num_hashes; ++i)
        {
          set_bit (filter->start, x % num_bits);

          x = (x + y) % num_bits;
          y = (y + i) % num_bits;
        }
    }
}

void
tc_bloom_sync (TcBloom*       filter)
{
  if (filter != NULL)
    {
      msync (filter->start, filter->num_bytes, MS_ASYNC);
    }
}

void
tc_bloom_vanish (TcBloom*       filter)
{
  if (   filter != NULL
      && (filter->mode & O_ACCMODE) == O_RDWR)
    {
      ftruncate (filter->fd, 0);
      lseek (filter->fd, filter->num_bytes + 1, SEEK_SET);
      write_64le (filter->fd, filter->num_bytes);
      write (filter->fd, &filter->num_hashes, 1);
    }
}
