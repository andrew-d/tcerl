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
 *     $Id$
 */

/* Modified to support NEW_FLOAT */
#include "eidef.h"
#include "eiext.h"
#include "my_decode_skip.h"

#define ERL_NEW_FLOAT_EXT 70

static int
my_ei_decode_new_float (const char* buf, int* index, void* p)
{
  (void) buf;
  (void) p;
  *index += 9;
  return 0;
}

int my_ei_skip_term(const char* buf, int* index)
{
    int i, n, ty;
    long long_n;

    /* ASSERT(ep != NULL); */

    ei_get_type_internal(buf, index, &ty, &n);
    switch (ty) {
    case ERL_ATOM_EXT:
	/* FIXME: what if some weird locale is in use? */
	if (ei_decode_atom(buf, index, NULL) < 0) return -1;
	break;
    case ERL_PID_EXT:
	if (ei_decode_pid(buf, index, NULL) < 0) return -1;
	break;
    case ERL_PORT_EXT:
	if (ei_decode_port(buf, index, NULL) < 0) return -1;
	break;
    case ERL_NEW_REFERENCE_EXT:
    case ERL_REFERENCE_EXT:
	if (ei_decode_ref(buf, index, NULL) < 0) return -1;
	break;
    case ERL_NIL_EXT:
	if (ei_decode_list_header(buf, index, &n) < 0) return -1;
	break;
    case ERL_LIST_EXT:
	if (ei_decode_list_header(buf, index, &n) < 0) return -1;
	for (i = 0; i < n; ++i)
	    my_ei_skip_term(buf, index);
	if (ei_get_type_internal(buf, index, &ty, &n) < 0) return -1;
	if (ty != ERL_NIL_EXT)
	    my_ei_skip_term(buf, index);
	else
	    if (ei_decode_list_header(buf, index, &n) < 0) return -1;
	break;
    case ERL_STRING_EXT:
	if (ei_decode_string(buf, index, NULL) < 0) return -1;
	break;
    case ERL_SMALL_TUPLE_EXT:
    case ERL_LARGE_TUPLE_EXT:
	if (ei_decode_tuple_header(buf, index, &n) < 0) return -1;	
	for (i = 0; i < n; ++i)
	    my_ei_skip_term(buf, index);
	break;
    case ERL_BINARY_EXT:
        long_n = n;
	if (ei_decode_binary(buf, index, NULL, &long_n) < 0)
	    return -1;
	break;
    case ERL_SMALL_INTEGER_EXT:
    case ERL_INTEGER_EXT:
	if (ei_decode_long(buf, index, NULL) < 0) return -1;
	break;
    case ERL_SMALL_BIG_EXT:
	if (ei_decode_ulong(buf, index, NULL) < 0) return -1;
	break;
    case ERL_LARGE_BIG_EXT:
	*index += n; /* !! Funkar detta? */
	break;
    case ERL_FLOAT_EXT:
	if (ei_decode_double(buf, index, NULL) < 0) return -1;
	break;
    case ERL_FUN_EXT:
	if (ei_decode_fun(buf, index, NULL) < 0) return -1;
	break;
    case ERL_NEW_FLOAT_EXT:
        if (my_ei_decode_new_float(buf, index, NULL) < 0) return -1;
        break;
    default:
	return -1;
    }
    return 0;
}
