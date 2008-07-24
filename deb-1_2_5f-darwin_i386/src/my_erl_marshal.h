#ifndef _MY_ERL_MARSHAL_H_
#define _MY_ERL_MARSHAL_H_

#ifdef __cplusplus
extern "C"
    {
#endif

void 
my_erl_init_marshal (void);

int 
my_erl_compare_ext  (unsigned char *e1, 
                     unsigned char *e2);

#ifdef __cplusplus
    }
#endif

#endif /* _MY_ERL_MARSHAL_H_ */
