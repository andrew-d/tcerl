/* ``The contents of this file are subject to the Erlang Public License,
 * Version 1.1, (the "License"); you may not use this file except in
 * compliance with the License. You should have received a copy of the
 * Erlang Public License along with this software. If not, it can be
 * retrieved via the world wide web at http://www.erlang.org/.
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Initial Developer of the Original Code is Ericsson Utvecklings AB.
 * Portions created by Ericsson are Copyright 1999, Ericsson Utvecklings
 * AB. All Rights Reserved.''
 * 
 *     $Id: my_erl_marshal.c,v 1.1 2008/06/03 23:43:47 pmineiro Exp $
 */
/*
 * Purpose: Decoding and encoding Erlang terms.
 */  

/* erl_compare_ext extracted and modified to accept ERL_SMALLEST and
 * ERL_LARGEST.  other code removed.
 */

#include "eidef.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/types.h>
#include <string.h>

#include "erl_interface.h"
#include "erl_marshal.h"
#include "erl_eterm.h"
#include "erl_malloc.h"
#include "erl_error.h"
#include "erl_internal.h"

#include "eiext.h" /* replaces external.h */
#include "putget.h"

#include "my_erl_marshal.h"

#if defined(VXWORKS) && CPU == PPC860
static int erl_fp_compare(unsigned *a, unsigned *b);
static void erl_long_to_fp(long l, unsigned *d);
#endif

/* Used when comparing two encoded byte arrays */
/* this global data is ok (from threading point of view) since it is
 * initialized once and never changed
 */

#define CMP_ARRAY_SIZE 256
/* FIXME problem for threaded ? */
static char cmp_array[CMP_ARRAY_SIZE]; 
static int init_cmp_array_p=1; /* initialize array, the first time */

#if defined(VXWORKS) && CPU == PPC860
#include <limits.h>
#endif

#if defined(__GNUC__)
#  define INLINE __inline__
#elif defined(__WIN32__)
#  define INLINE __inline
#else
#  define INLINE
#endif

static int cmp_floats(double f1, double f2);
static INLINE double to_float(long l);

#define ERL_NUM_CMP 1
#define ERL_REF_CMP 3

#define IS_ERL_NUM(t) (cmp_array[t]==ERL_NUM_CMP)

#define CMP_NUM_CLASS_SIZE 256
static unsigned char cmp_num_class[CMP_NUM_CLASS_SIZE]; 
static int init_cmp_num_class_p=1; /* initialize array, the first time */

#define MK_CMP_NUM_CODE(x,y)    (((x)<<2)|(y))
#define CMP_NUM_CLASS(x)        (cmp_num_class[x] & 0x03)
#define CMP_NUM_CODE(x,y)       (MK_CMP_NUM_CODE(CMP_NUM_CLASS(x),CMP_NUM_CLASS(y)))

#define SMALL 1
#define FLOAT 2
#define BIG   3

#define SMALL_SMALL    MK_CMP_NUM_CODE(SMALL,SMALL)
#define SMALL_FLOAT    MK_CMP_NUM_CODE(SMALL,FLOAT)
#define SMALL_BIG      MK_CMP_NUM_CODE(SMALL,BIG)
#define FLOAT_SMALL    MK_CMP_NUM_CODE(FLOAT,SMALL)
#define FLOAT_FLOAT    MK_CMP_NUM_CODE(FLOAT,FLOAT)
#define FLOAT_BIG      MK_CMP_NUM_CODE(FLOAT,BIG)
#define BIG_SMALL      MK_CMP_NUM_CODE(BIG,SMALL)
#define BIG_FLOAT      MK_CMP_NUM_CODE(BIG,FLOAT)
#define BIG_BIG        MK_CMP_NUM_CODE(BIG,BIG)

#define ERL_SMALLEST 0
#define ERL_LARGEST 255
#define ERL_NEW_FLOAT_EXT 70

void my_erl_init_marshal(void)
{
  if (init_cmp_array_p) {
    memset(cmp_array, 0, CMP_ARRAY_SIZE);
    cmp_array[ERL_SMALLEST]          = 0;
    cmp_array[ERL_SMALL_INTEGER_EXT] = 1;
    cmp_array[ERL_INTEGER_EXT]       = 1;
    cmp_array[ERL_FLOAT_EXT]         = 1;
    cmp_array[ERL_NEW_FLOAT_EXT]     = 1;
    cmp_array[ERL_SMALL_BIG_EXT]     = 1;
    cmp_array[ERL_LARGE_BIG_EXT]     = 1;
    cmp_array[ERL_ATOM_EXT]          = 2;
    cmp_array[ERL_REFERENCE_EXT]     = 3;
    cmp_array[ERL_NEW_REFERENCE_EXT] = 3;
    cmp_array[ERL_FUN_EXT]           = 4;
    cmp_array[ERL_NEW_FUN_EXT]       = 4;
    cmp_array[ERL_PORT_EXT]          = 5;
    cmp_array[ERL_PID_EXT]           = 6;
    cmp_array[ERL_SMALL_TUPLE_EXT]   = 7;
    cmp_array[ERL_LARGE_TUPLE_EXT]   = 7;
    cmp_array[ERL_NIL_EXT]           = 8;
    cmp_array[ERL_STRING_EXT]        = 9;
    cmp_array[ERL_LIST_EXT]          = 9;
    cmp_array[ERL_BINARY_EXT]        = 10;
    cmp_array[ERL_LARGEST]           = 11;
    init_cmp_array_p = 0;
  }
  if (init_cmp_num_class_p) {
    memset(cmp_num_class, 0, CMP_NUM_CLASS_SIZE);
    cmp_num_class[ERL_SMALL_INTEGER_EXT] = SMALL;
    cmp_num_class[ERL_INTEGER_EXT]       = SMALL;
    cmp_num_class[ERL_FLOAT_EXT]         = FLOAT;
    cmp_num_class[ERL_NEW_FLOAT_EXT]     = FLOAT;
    cmp_num_class[ERL_SMALL_BIG_EXT]     = BIG;
    cmp_num_class[ERL_LARGE_BIG_EXT]     = BIG;
    init_cmp_num_class_p = 0;
  }
}

/*
 * broken in erlang, incorporating patch from here
 * hat tip to Paul Guyot <pguyot@kallisys.net>
 * http://erlang.org/pipermail/erlang-bugs/2008-October/001023.html
 */

static int my_ei_decode_big(const char *buf, int *index, erlang_big *b)
{
  long digit_bytes;
  const char *s = buf + *index;
  const char *s0 = s;

  switch ( get8(s) ) {
    case ERL_SMALL_BIG_EXT:
      digit_bytes = get8(s);
      break;
    case ERL_LARGE_BIG_EXT:
      digit_bytes = get32be(s);
      break;
    default:
      return -1;
  }
  if ( b ) {
      unsigned short *dt = b->digits;
      int i;
      unsigned char *u;

      if ( ((digit_bytes+1)/2) != b->arity ) {
          return -1;
      }
      b->is_neg = get8(s);
      u = (unsigned char *) s;
      for (i = 0; i < b->arity; ++i) {
	  dt[i] = u[i*2];
          if ((i*2 + 1) < digit_bytes)
            {
              dt[i] |= ((unsigned short) u[(i*2)+1]) << 8;
            }
      }
  } else {
      s++; /* skip sign byte */
  }

  s += digit_bytes;

  *index += s-s0; 
  
  return 0; 
}

/*==============================================================
 * Marshalling routines.
 *==============================================================
 */

/*
 * A nice macro to make it look cleaner in the 
 * cases of PID's,PORT's and REF's below. 
 * It reads the NODE name from a buffer.
 */
#define READ_THE_NODE(ext,cp,len,i) \
/* eat first atom, repr. the node */ \
if (**ext != ERL_ATOM_EXT) \
  return (ETERM *) NULL; \
*ext += 1; \
i = (**ext << 8) | (*ext)[1]; \
cp = (char *) *(ext) + 2; \
*ext += (i + 2); \
len = i

#define STATIC_NODE_BUF_SZ 30

#define SET_NODE(node,node_buf,cp,len) \
if (len >= STATIC_NODE_BUF_SZ) node = malloc(len+1); \
else node = node_buf; \
memcpy(node, cp, len); \
node[len] = '\0'

#define RESET_NODE(node,len) \
if (len >= STATIC_NODE_BUF_SZ) free(node)


/*==============================================================
 * Ok, here comes routines for inspecting/manipulating 
 * an encoded buffer of bytes.
 *==============================================================
 */

/* 
 * Lexically compare two strings of bytes,
 * (string s1 length l1 and s2 l2).
 * Return: -1 if s1 < s2
 *	    0 if s1 = s2
 *	    1 if s1 > s2 
 */
static int cmpbytes(unsigned char* s1,int l1,unsigned char* s2,int l2)
{
  int i;
  i = 0;
  while((i < l1) && (i < l2)) {
    if (s1[i] < s2[i]) return(-1);
    if (s1[i] > s2[i]) return(1);
    i++;
  }
  if (l1 < l2) return(-1);
  if (l1 > l2) return(1);
  return(0);

} /* cmpbytes */

#define CMP_EXT_ERROR_CODE 4711

#define CMP_EXT_INT32_BE(AP, BP)				\
do {								\
    if ((AP)[0] != (BP)[0]) return (AP)[0] < (BP)[0] ? -1 : 1;	\
    if ((AP)[1] != (BP)[1]) return (AP)[1] < (BP)[1] ? -1 : 1;	\
    if ((AP)[2] != (BP)[2]) return (AP)[2] < (BP)[2] ? -1 : 1;	\
    if ((AP)[3] != (BP)[3]) return (AP)[3] < (BP)[3] ? -1 : 1;	\
} while (0)

#define CMP_EXT_SKIP_ATOM(EP)					\
do {								\
    if ((EP)[0] != ERL_ATOM_EXT)				\
	return CMP_EXT_ERROR_CODE;				\
    (EP) += 3 + ((EP)[1] << 8 | (EP)[2]);			\
} while (0)

/* 
 * We now know that both byte arrays are of the same type.
 */
static int compare_top_ext(unsigned char**, unsigned char **); /* forward */
static int cmp_exe2(unsigned char **e1, unsigned char **e2);

static int cmp_refs(unsigned char **e1, unsigned char **e2)
{
    int tmp, n1, n2;
    unsigned char *node1, *node2, *id1, *id2, cre1, cre2;

    if (*((*e1)++) == ERL_REFERENCE_EXT) {
	node1 = *e1;
	CMP_EXT_SKIP_ATOM(*e1);
	n1 = 1;
	id1 = *e1;
	cre1 = (*e1)[4];
	*e1 += 5;
    } else {
	n1 = get16be(*e1);
	node1 = *e1;
	CMP_EXT_SKIP_ATOM(*e1);
	cre1 = **e1;
	id1 = (*e1) + 1 + (n1 - 1)*4;
	*e1 = id1 + 4;
    }

    if (*((*e2)++) == ERL_REFERENCE_EXT) {
	node2 = *e2;
	CMP_EXT_SKIP_ATOM(*e2);
	n2 = 1;
	id2 = *e2;
	cre2 = (*e2)[4];
	*e2 += 5;
    } else {
	n2 = get16be(*e2);
	node2 = *e2;
	CMP_EXT_SKIP_ATOM(*e2);
	cre2 = **e2;
	id2 = (*e2) + 1 + (n2 - 1)*4;
	*e2 = id2 + 4;
    }

    /* First compare node names... */
    tmp = cmp_exe2(&node1, &node2);
    if (tmp != 0)
	return tmp;

    /* ... then creations ... */
    if (cre1 != cre2)
	return cre1 < cre2 ? -1 : 1;

    /* ... and then finaly ids. */
    if (n1 != n2) {
	unsigned char zero[] = {0, 0, 0, 0};
	if (n1 > n2)
	    do {
		CMP_EXT_INT32_BE(id1, zero);
		id1 -= 4;
		n1--;
	    } while (n1 > n2);
	else
	    do {
		CMP_EXT_INT32_BE(zero, id2);
		id2 -= 4;
		n2--;
	    } while (n2 > n1);
    }
    
    for (; n1 > 0; n1--, id1 -= 4, id2 -= 4)
	CMP_EXT_INT32_BE(id1, id2);

    return 0;
}

static int cmp_string_list(unsigned char **e1, unsigned char **e2)
{
  
  /* we need to compare a string in **e1 and a list in **e2               */
  /* convert the string to list representation and convert that with e2   */
  /* we need a temporary buffer of:                                       */
  /* 5 (list tag + length) + 2*string length + 1 (end of list tag)        */
  /* for short lists we use a stack allocated buffer, otherwise we malloc */

  unsigned char *bp_orig;
  unsigned char *bp;
  unsigned char buf[5+2*255+1]; /* used for short lists */
  int i,e1_len;
  int res;

  e1_len = ((*e1)[1] << 8) | ((*e1)[2]);
  if ( e1_len < 256 ) {
    bp = buf;
  } else {
    bp = malloc(5+(2*e1_len)+1);
  }

  bp_orig = bp;

  bp[0] = ERL_LIST_EXT;
  bp[1] = bp[2] = 0;
  bp[3] = (*e1)[1];
  bp[4] = (*e1)[2];

  for(i=0;i<e1_len;i++) {
    bp[5+2*i] = ERL_SMALL_INTEGER_EXT;
    bp[5+2*i+1] = (*e1)[3+i];
  }

  bp[5+2*e1_len] = ERL_NIL_EXT;

  res = cmp_exe2(&bp, e2);

  *e1 += 3 + e1_len;

  if ( e1_len >= 256 ) free(bp_orig);

  return res;
}

static int cmp_exe2(unsigned char **e1, unsigned char **e2)
{
  int min,  ret,i,j,k;
  double ff1, ff2;
  unsigned char *tmp1, *tmp2;

  if ( ((*e1)[0] == ERL_STRING_EXT) && ((*e2)[0] == ERL_LIST_EXT) ) {
    return cmp_string_list(e1, e2);
  } else if ( ((*e1)[0] == ERL_LIST_EXT) && ((*e2)[0] == ERL_STRING_EXT) ) {
    return -cmp_string_list(e2, e1);
  }

  *e2 += 1;
  switch (*(*e1)++) 
    {
    case ERL_SMALLEST:
      return -1;
    case ERL_SMALL_INTEGER_EXT:
      if (**e1 < **e2) ret = -1;
      else if (**e1 > **e2) ret = 1;
      else ret = 0;
      *e1 += 1; *e2 += 1;
      return ret;
    case ERL_INTEGER_EXT:
      i = (int) (**e1 << 24) | ((*e1)[1] << 16) |((*e1)[2] << 8) | (*e1)[3];
      j = (int) (**e2 << 24) | ((*e2)[1] << 16) |((*e2)[2] << 8) | (*e2)[3];
      if ( i < j) 
	ret = -1;
      else if ( i > j) 
	ret = 1;
      else 
	ret = 0;
      *e1 += 4; *e2 += 4;
      return ret;
    case ERL_ATOM_EXT:
      i = (**e1 << 8) | (*e1)[1];
      j = (**e2 << 8) | (*e2)[1];
      ret = cmpbytes(*e1 +2, i, *e2 +2, j);
      *e1 += (i + 2);
      *e2 += (j + 2);
      return ret;
    case ERL_PID_EXT: {
      unsigned char *n1 = *e1;
      unsigned char *n2 = *e2;
      CMP_EXT_SKIP_ATOM(*e1); CMP_EXT_SKIP_ATOM(*e2);
      *e1 += 9; *e2 += 9;

      /* First compare serials ... */
      tmp1 = *e1 - 5; tmp2 = *e2 - 5;
      CMP_EXT_INT32_BE(tmp1, tmp2);

      /* ... then ids ... */
      tmp1 -= 4; tmp2 -= 4;
      CMP_EXT_INT32_BE(tmp1, tmp2);

      /* ... then node names ... */
      ret = cmp_exe2(&n1, &n2);
      if (ret != 0)
	  return ret;

      /* ... and then finaly creations. */
      tmp1 += 8; tmp2 += 8;
      if (*tmp1 != *tmp2)
	  return *tmp1 < *tmp2 ? -1 : 1;
      return 0;
    }
    case ERL_PORT_EXT:
      /* First compare node names ... */
      if (**e1 != ERL_ATOM_EXT || **e2 != ERL_ATOM_EXT)
	  return CMP_EXT_ERROR_CODE;
      ret = cmp_exe2(e1, e2);
      *e1 += 5; *e2 += 5;
      if (ret != 0)
	  return ret;
      /* ... then creations ... */
      tmp1 = *e1 - 1; tmp2 = *e2 - 1;
      if (*tmp1 != *tmp2)
	  return *tmp1 < *tmp2 ? -1 : 1;
      /* ... and then finaly ids. */
      tmp1 -= 4; tmp2 -= 4;
      CMP_EXT_INT32_BE(tmp1, tmp2);
      return 0;
    case ERL_NIL_EXT: return 0;
    case ERL_LIST_EXT:
      i = (**e1 << 24) | ((*e1)[1] << 16) |((*e1)[2] << 8) | (*e1)[3];
      *e1 += 4;
      j = (**e2 << 24) | ((*e2)[1] << 16) |((*e2)[2] << 8) | (*e2)[3];
      *e2 += 4;
      if ( i == j && j == 0 ) return 0;
      min = (i < j) ? i : j;
      k = 0;
      while (1) {
	if (k++ == min)
	  return compare_top_ext(e1 , e2);
	if ((ret = compare_top_ext(e1 , e2)) == 0) 
	  continue;
	return ret;
      }
    case ERL_STRING_EXT:
      i = (**e1 << 8) | ((*e1)[1]);
      *e1 += 2;
      j = (**e2 << 8) | ((*e2)[1]);
      *e2 += 2;
      ret = cmpbytes(*e1, i, *e2, j);
      *e1 += i;
      *e2 += j;
      return ret;
    case ERL_SMALL_TUPLE_EXT:
      i = *(*e1)++; 	j = *(*e2)++;
      if (i < j) return -1;
      if (i > j) return 1;
      while (i--) {
	if ((j = compare_top_ext(e1, e2))) return j;
      }
      return 0;
    case ERL_LARGE_TUPLE_EXT:
      i = (**e1 << 24) | ((*e1)[1]) << 16| ((*e1)[2]) << 8| ((*e1)[3]) ;	
      *e1 += 4;
      j = (**e2 << 24) | ((*e2)[1]) << 16| ((*e2)[2]) << 8| ((*e2)[3]) ;	
      *e2 += 4;
      if (i < j) return -1;
      if (i > j) return 1;
      while (i--) {
	if ((j = compare_top_ext(e1, e2))) return j;
      }
      return 0;
    case ERL_FLOAT_EXT:
      if (sscanf((char *) *e1, "%lf", &ff1) != 1)
	return -1;
      *e1 += 31;
      if (sscanf((char *) *e2, "%lf", &ff2) != 1)
	return -1;
      *e2 += 31;
      return cmp_floats(ff1,ff2);

    case ERL_NEW_FLOAT_EXT:
      { 
        unsigned char tmp_one[8];
        unsigned char tmp_two[8];
        int memcmp_result;

        /* sign bit */
        if ((**e1 & 0x80) > (**e2 & 0x80)) { return -1; }
        if ((**e1 & 0x80) < (**e2 & 0x80)) { return 1; }
        /* ok sign bits are the same */
        memcpy (tmp_one, *e1, 8);
        memcpy (tmp_two, *e2, 8);
        tmp_one[0] &= 0x7F;
        tmp_two[0] &= 0x7F;

        memcmp_result = memcmp (tmp_one, tmp_two, 8);

        return (**e1 & 0x80) ? -memcmp_result : memcmp_result;
      }

    case ERL_BINARY_EXT:
      i = (**e1 << 24) | ((*e1)[1] << 16) |((*e1)[2] << 8) | (*e1)[3];
      *e1 += 4;
      j = (**e2 << 24) | ((*e2)[1] << 16) |((*e2)[2] << 8) | (*e2)[3];
      *e2 += 4;
      ret = cmpbytes(*e1, i , *e2 , j);
      *e1 += i; *e2 += j;
      return ret;

    case ERL_FUN_EXT:  /* FIXME: */
    case ERL_NEW_FUN_EXT:  /* FIXME: */
      return -1;

    case ERL_LARGEST:
      return 1;

    default:
      return cmpbytes(*e1, 1, *e2, 1);

    } /* switch */
  
} /* cmp_exe2 */

/* Number compare */

static int cmp_floats(double f1, double f2)
{
#if defined(VXWORKS) && CPU == PPC860
      return erl_fp_compare((unsigned *) &f1, (unsigned *) &f2);
#else
      if (f1<f2) return -1;
      else if (f1>f2) return 1;
      else return 0;
#endif
}

static INLINE double to_float(long l) 
{
    double f;
#if defined(VXWORKS) && CPU == PPC860
    erl_long_to_fp(l, (unsigned *) &f);
#else
    f = l;
#endif
    return f;
}


static int cmp_small_big(unsigned char**e1, unsigned char **e2)
{
    int i1,i2;
    int t2;
    int n2;
    long l1;
    int res;

    erlang_big *b1,*b2;

    i1 = i2 = 0;
    if ( ei_decode_long(*e1,&i1,&l1) < 0 ) return -1;
    
    ei_get_type(*e2,&i2,&t2,&n2);
    
    /* any small will fit in two digits */
    if ( (b1 = ei_alloc_big(2)) == NULL ) return -1;
    if ( ei_small_to_big(l1,b1) < 0 ) {
        ei_free_big(b1);
        return -1;
    }
    
    if ( (b2 = ei_alloc_big(n2)) == NULL ) {
        ei_free_big(b1);
        return 1;
    }

    if ( my_ei_decode_big(*e2,&i2,b2) < 0 ) {
        ei_free_big(b1);
        ei_free_big(b2);
        return 1;
    }
    
    res = ei_big_comp(b1,b2);
    
    ei_free_big(b1);
    ei_free_big(b2);

    *e1 += i1;
    *e2 += i2;

    return res;
}

static double
from_ieee_double_big_endian (const unsigned char *buf)
{
  int sign = (buf[0] & 0x80) ? -1 : 1;
  uint32_t exponent;
  uint64_t mantissa;

  exponent = buf[0] & 0x7F;
  exponent <<= 4;
  exponent |= (buf[1] & 0xF0) >> 4;

   if (exponent == 0) /* denormalized */
     { 
       exponent = -1022;
       mantissa = 0;
     }
   else
     { 
       exponent -= 1023;
       mantissa = 1;
     }

  mantissa <<= 4;
  mantissa |= buf[1] & 0x0F;
  mantissa <<= 8;
  mantissa |= buf[2];
  mantissa <<= 8;
  mantissa |= buf[3];
  mantissa <<= 8;
  mantissa |= buf[4];
  mantissa <<= 8;
  mantissa |= buf[5];
  mantissa <<= 8;
  mantissa |= buf[6];
  mantissa <<= 8;
  mantissa |= buf[7];

  return sign * ldexp (mantissa, exponent - 52);
}

static int
my_ei_decode_double (unsigned char *e1, int *i1, double *f1)
{
  if (*e1 == ERL_NEW_FLOAT_EXT)
    {
      *i1 += 9;
      *f1 = from_ieee_double_big_endian (e1 + 1);
    }
  else
    {
      if ( ei_decode_double(e1,i1,f1) < 0 ) return -1;
    }

  return 0;
}

static int cmp_small_float(unsigned char**e1, unsigned char **e2)
{
    int i1,i2;
    long l1;
    double f1,f2;

    /* small -> float -> float_comp */

    i1 = i2 = 0;
    if ( ei_decode_long(*e1,&i1,&l1) < 0 ) return -1;
    if ( my_ei_decode_double(*e2,&i2,&f2) < 0 ) return 1;

    f1 = to_float(l1);

    *e1 += i1;
    *e2 += i2;

    return cmp_floats(f1,f2);
}

static int cmp_float_big(unsigned char**e1, unsigned char **e2)
{
    int res;
    int i1,i2;
    int t2,n2;
    double f1,f2;
    erlang_big *b2;
    
    /* big -> float if overflow return big sign else float_comp */
    
    i1 = i2 = 0;
    if ( my_ei_decode_double(*e1,&i1,&f1) < 0 ) return -1;
    
    if (ei_get_type(*e2,&i2,&t2,&n2) < 0) return 1;
    if ((b2 = ei_alloc_big(n2)) == NULL) return 1;
    if (my_ei_decode_big(*e2,&i2,b2) < 0) return 1;
    
    /* convert the big to float */
    if ( ei_big_to_double(b2,&f2) < 0 ) {
        /* exception look at the sign */
        res = b2->is_neg ? 1 : -1;
        ei_free_big(b2);
        return res;
    }
    
    ei_free_big(b2);

    *e1 += i1;
    *e2 += i2;

    return cmp_floats(f1,f2);
}

static int cmp_small_small(unsigned char**e1, unsigned char **e2)
{
    int i1,i2;
    long l1,l2;

    i1 = i2 = 0;
    if ( ei_decode_long(*e1,&i1,&l1) < 0 ) {
        fprintf(stderr,"Failed to decode 1\r\n");
        return -1;
    }
    if ( ei_decode_long(*e2,&i2,&l2) < 0 ) {
        fprintf(stderr,"Failed to decode 2\r\n");
        return 1;
    }
    
    *e1 += i1;
    *e2 += i2;

    if ( l1 < l2 ) return -1;
    else if ( l1 > l2 ) return 1;
    else return 0;
}

static int cmp_float_float(unsigned char**e1, unsigned char **e2)
{
    int i1,i2;
    double f1,f2;

    i1 = i2 = 0;
    if ( my_ei_decode_double(*e1,&i1,&f1) < 0 ) return -1;
    if ( my_ei_decode_double(*e2,&i2,&f2) < 0 ) return -1;

    *e1 += i1;
    *e2 += i2;

    return cmp_floats(f1,f2);
}

static int cmp_big_big(unsigned char**e1, unsigned char **e2)
{
    int res;
    int i1,i2;
    int t1,t2;
    int n1,n2;
    erlang_big *b1,*b2;

    i1 = i2 = 0;
    ei_get_type(*e1,&i1,&t1,&n1);
    ei_get_type(*e2,&i2,&t2,&n2);

    b1 = ei_alloc_big(n1);
    b2 = ei_alloc_big(n2);
    
    my_ei_decode_big(*e1,&i1,b1);
    my_ei_decode_big(*e2,&i2,b2);

    res = ei_big_comp(b1,b2);
    
    ei_free_big(b1);
    ei_free_big(b2);

    *e1 += i1;
    *e2 += i2;

    return res;
}

static int cmp_number(unsigned char**e1, unsigned char **e2)
{
    switch (CMP_NUM_CODE(**e1,**e2)) {

      case SMALL_BIG:
        /* fprintf(stderr,"compare small_big\r\n"); */
        return cmp_small_big(e1,e2);

      case BIG_SMALL:
        /* fprintf(stderr,"compare sbig_small\r\n"); */
        return -cmp_small_big(e2,e1);

      case SMALL_FLOAT:
        /* fprintf(stderr,"compare small_float\r\n"); */
        return cmp_small_float(e1,e2);
        
      case FLOAT_SMALL:
        /* fprintf(stderr,"compare float_small\r\n"); */
        return -cmp_small_float(e2,e1);

      case FLOAT_BIG:
        /* fprintf(stderr,"compare float_big\r\n"); */
        return cmp_float_big(e1,e2);

      case BIG_FLOAT:
        /* fprintf(stderr,"compare big_float\r\n"); */
        return -cmp_float_big(e2,e1);

      case SMALL_SMALL:
        /* fprintf(stderr,"compare small_small\r\n"); */
        return cmp_small_small(e1,e2);

      case FLOAT_FLOAT:
        /* fprintf(stderr,"compare float_float\r\n"); */
        return cmp_float_float(e1,e2);

      case BIG_BIG:
        /* fprintf(stderr,"compare big_big\r\n"); */
        return cmp_big_big(e1,e2);

      default:
        /* should never get here ... */
        /* fprintf(stderr,"compare standard\r\n"); */
        return cmp_exe2(e1,e2);
    }

}

/* 
 * If the arrays are of the same type, then we
 * have to do a real compare.
 */
/* 
 * COMPARE TWO encoded BYTE ARRAYS e1 and e2.
 * Return: -1 if e1 < e2
 *          0 if e1 == e2 
 *          1 if e2 > e1   
 */
static int compare_top_ext(unsigned char**e1, unsigned char **e2)
{
  if (**e1 == ERL_VERSION_MAGIC) (*e1)++;
  if (**e2 == ERL_VERSION_MAGIC) (*e2)++;

  if (cmp_array[**e1] < cmp_array[**e2]) return -1;
  if (cmp_array[**e1] > cmp_array[**e2]) return 1;

  /*
   * our h4x rule: any ERL_SMALLEST or ERL_LARGEST terminates comparison
   * if doing this "for real", we would keep going; 
   * but then we should send in lengths in order to support prefix
   * matching ... and i'm too lazy to do that.
   *
   * this is ok for the case we are using it for, namely, only one
   * of the two will contain either ERL_SMALLEST or ERL_LARGEST
   */

  if (**e1 == ERL_SMALLEST || **e2 == ERL_LARGEST)
    {
      fprintf (stderr, "wtf %hhu %hhu!\r\n", **e1, **e2);
      return -1;
    }

  if (**e2 == ERL_SMALLEST || **e1 == ERL_LARGEST)
    {
      fprintf (stderr, "wtf %hhu %hhu!\r\n", **e1, **e2);
      return 1;
    }

  if (IS_ERL_NUM(**e1)) 
      return cmp_number(e1,e2);

  if (cmp_array[**e1] == ERL_REF_CMP)
      return cmp_refs(e1, e2);

  return cmp_exe2(e1, e2);
}

int my_erl_compare_ext(unsigned char *e1, unsigned char *e2)
{
  return compare_top_ext(&e1, &e2); 
} /* erl_compare_ext */

#if defined(VXWORKS) && CPU == PPC860
/* FIXME we have no floating point but don't we have emulation?! */
static int erl_fp_compare(unsigned *a, unsigned *b) 
{
    /* Big endian mode of powerPC, IEEE floating point. */
    unsigned a_split[4] = {a[0] >> 31,             /* Sign bit */
                           (a[0] >> 20) & 0x7FFU,  /* Exponent */
                           a[0] & 0xFFFFFU,        /* Mantissa MS bits */
                           a[1]};                  /* Mantissa LS bits */
    unsigned b_split[4] = {b[0] >> 31,
                           (b[0] >> 20) & 0x7FFU,
                           b[0] & 0xFFFFFU,
                           b[1]};
    int a_is_infinite, b_is_infinite;
    int res;


    /* Make -0 be +0 */
    if (a_split[1] == 0 && a_split[2] == 0 && a_split[3] == 0)
        a_split[0] = 0;
    if (b_split[1] == 0 && b_split[2] == 0 && b_split[3] == 0)
        b_split[0] = 0;
    /* Check for infinity */
    a_is_infinite = (a_split[1] == 0x7FFU && a_split[2] == 0 && 
                     a_split[3] == 0);
    b_is_infinite = (b_split[1] == 0x7FFU && b_split[2] == 0 && 
                     b_split[3] == 0);

    if (a_is_infinite && !b_is_infinite)
        return (a_split[0]) ? -1 : 1;
    if (b_is_infinite && !a_is_infinite)
        return (b_split[0]) ? 1 : -1;
    if (a_is_infinite && b_is_infinite)
        return b[0] - a[0]; 
    /* Check for indeterminate or nan, infinite is already handled, 
     so we only check the exponent. */
    if((a_split[1] == 0x7FFU) || (b_split[1] == 0x7FFU))
        return INT_MAX; /* Well, they are not equal anyway, 
                           abort() could be an alternative... */

    if (a_split[0] && !b_split[0])
        return -1;
    if (b_split[0] && !a_split[0])
        return 1;
    /* Compare */
    res = memcmp(a_split + 1, b_split + 1, 3 * sizeof(unsigned));
    /* Make -1, 0 or 1 */
    res = (!!res) * ((res < 0) ? -1 : 1); 
    /* Turn sign if negative values */
    if (a_split[0]) /* Both are negative */
        res = -1 * res;
    return res;
}

static void join(unsigned d_split[4], unsigned *d)
{
    d[0] = (d_split[0] << 31) |         /* Sign bit */
	((d_split[1] & 0x7FFU) << 20) | /* Exponent */
	(d_split[2] & 0xFFFFFU);        /* Mantissa MS bits */
    d[1] = d_split[3];                  /* Mantissa LS bits */
}

static int blength(unsigned long l)
{
    int i;
    for(i = 0; l; ++i)
	l >>= 1;
    return i;
}

static void erl_long_to_fp(long l, unsigned *d) 
{
    unsigned d_split[4];
    unsigned x;
    if (l < 0) {
	d_split[0] = 1;
	x = -l;
    } else {
	d_split[0] = 0;
	x = l;
    }

    if (!l) {
	memset(d_split,0,sizeof(d_split));
    } else {
	int len = blength(x);
	x <<= (33 - len);
	d_split[2] = (x >> 12);
	d_split[3] = (x << 20);
	d_split[1] = 1023 + len - 1;
    }
    join(d_split,d);
}

#endif
