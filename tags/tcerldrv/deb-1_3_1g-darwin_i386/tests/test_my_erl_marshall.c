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

static void 
test_order_bug_one ()
{
  unsigned char term_one[] = 
    /*    -1 */
    { 131,98,255,255,255,255 };

  unsigned char term_two[] = 
    /*    -1.00000e-2 */
    { 131,70,191,132,122,225,71,174,20,123 };

  assert (my_erl_compare_ext (term_one, term_two) < 0);
  assert (my_erl_compare_ext (term_two, term_one) > 0);
}

static void
test_order_bug_two () 
{
  unsigned char term_one[] = 
    /* { -1, -1 } */
    { 131,104,2,98,255,255,255,255,98,255,255,255,255 };
  unsigned char term_two[] =
    /* { -1.0, 0.5 } */
    { 131,104,2,70,191,240,0,0,0,0,0,0,70,63,224,0,0,0,0,0,0 };

  assert (my_erl_compare_ext (term_one, term_two) < 0);
  assert (my_erl_compare_ext (term_two, term_one) > 0);
}

static void
test_issue_four ()
{
  unsigned char term_one[] = 
    /* 9999999999 */
    { 131,110,5,0,255,227,11,84,2 };
  unsigned char term_two [] = 
    /* 9999999999 */
    { 131,110,5,0,255,227,11,84,2 };

  assert (my_erl_compare_ext (term_one, term_two) == 0);
}

int 
main ()
{
  my_erl_init_marshal ();

  test_issue_four ();
  test_order_bug_two ();
  test_order_bug_one ();
  test_order_bug_zero ();

  return 0;
}
