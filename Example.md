# Prerequisities #

  * [mnesiaex](http://code.google.com/p/mnesiaex/wiki/InstallationHowto) installed
  * [tcerl](http://code.google.com/p/tcerl/wiki/InstallationHowto) installed

# Example #

```
% erl
Erlang (BEAM) emulator version 5.6.5 [source] [smp:2] [async-threads:0] [kernel-poll:false]

Eshell V5.6.5  (abort with ^G)
1> tcerl:start ().
ok
2> mnesia:start ().
ok
3> mnesia:change_table_copy_type (schema, node (), disc_copies).
{atomic,ok}
4> mnesia:create_table (testtab, [ { type, { external, ordered_set, tcbdbtab } }, { external_copies, [ node () ] }, { user_properties, [ { deflate, true }, { bucket_array_size, 10000 } ] } ]).
{atomic,ok}
5> mnesia:table_info (testtab, type).
ordered_set
6> mnesia:dirty_write ({ testtab, hello, world }), mnesia:dirty_read (testtab, hello).
[{testtab,hello,world}]
7> f (), Start = now (), [ mnesia:dirty_write ({ testtab, N, N }) || N <- lists:seq (1, 100000) ], End = now (), timer:now_diff (End, Start).
5590639
8> {ok, Port} = tcbdbsrv:get_tab (testtab), tcbdbets:sync (Port). 
ok
9>              
User switch command
 --> q
% ls -l Mnesia.nonode@nohost
total 1872
-rw-r--r--   1 pmineiro  admin     154 Jun 12 20:45 DECISION_TAB.LOG
-rw-r--r--   1 pmineiro  admin   48231 Jun 12 20:46 LATEST.LOG
-rw-r--r--   1 pmineiro  admin    9339 Jun 12 20:45 schema.DAT
-rw-r--r--   1 pmineiro  admin  892672 Jun 12 20:46 testtab.tcb
```