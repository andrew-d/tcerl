#include <assert.h>
#include "../src/my_erl_marshal.c"

static void 
test_order_bug_zero ()
{
  unsigned char term_one[] = 
    /*    {     "cl"                        , 1  , '_' } (smallest) */
    { 131,104,3,108,0,0,0,2,97,99,97,108,106,97,1,0 };
  unsigned char term_two[] = 
    /*    {     "cl"          , 1  , <<0,4,82,4,122,47,243,186>> } */
    { 131,104,3,107,0,2,99,108,97,1,109,0,0,0,8,0,4,82,4,122,47,243,186 };
  unsigned char term_three[] = 
    /*    {     "cl"                        , 1  , '_' } (largest) */
    { 131,104,3,108,0,0,0,2,97,99,97,108,106,97,1,255 };

  assert (my_erl_compare_ext (term_one, term_two) < 0);
  assert (my_erl_compare_ext (term_two, term_three) < 0);
}

int 
main ()
{
  test_order_bug_zero ();
}
