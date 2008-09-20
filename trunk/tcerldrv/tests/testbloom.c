#include "../src/tcbloom.c"

#include <assert.h>

static void
test_bloom ()
{
  TcBloom* f;
  
  unlink ("dild");
  
  f = tc_bloom_open ("dild", malloc, O_RDWR | O_CREAT, 1ULL << 16, 3);

  assert (f != NULL);

  assert (! tc_bloom_lookup (f, "flass", sizeof ("flass")));
  assert (! tc_bloom_lookup (f, "turg", sizeof ("turg")));
  assert (! tc_bloom_lookup (f, "warez", sizeof ("warez")));

  tc_bloom_insert (f, "flass", sizeof ("flass"));
  tc_bloom_insert (f, "turg", sizeof ("turg"));

  assert (tc_bloom_lookup (f, "flass", sizeof ("flass")));
  assert (tc_bloom_lookup (f, "turg", sizeof ("turg")));
  assert (! tc_bloom_lookup (f, "warez", sizeof ("warez")));

  tc_bloom_close (f, free);
  f = tc_bloom_open ("dild", malloc, O_RDWR | O_CREAT, 1ULL << 18, 5);

  assert (tc_bloom_lookup (f, "flass", sizeof ("flass")));
  assert (tc_bloom_lookup (f, "turg", sizeof ("turg")));
  assert (! tc_bloom_lookup (f, "warez", sizeof ("warez")));

  tc_bloom_close (f, free);

  unlink ("dild");
  assert (tc_bloom_open ("dild", malloc, O_RDWR) == NULL);
}

int 
main (void)
{
  test_bloom ();

  return 0;
}
