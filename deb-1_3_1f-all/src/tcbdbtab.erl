%% @doc Mnesia table driver for tcbdbets.
%% Example: 
%% <br/><pre>
%% mnesia:create_table (testtab, 
%%                      [ { type, { external, ordered_set, tcbdbtab } },
%%                        { external_copies, [ node () ] },
%%                        { attributes, [ key, count ] },
%%                        { user_properties, [ { deflate, true },
%%                                             { async_write, true },
%%                                             { bucket_array_size, 10000 },
%%                                             { bloom, 1 bsl 20, 7 } ] } ]),
%% </pre><br/>
%% Options to tcbdbets:open_file/1 are passed via user_properties.  Since
%% user_properties must contain tuples, the non-tuple options to 
%% tcbdbets:open_file/1 are indicated via { Arg, true } tuples, e.g.,
%% in the above example the { deflate, true } tuple activates the 
%% deflate option to tcbdbets:open_file/1.  Unlike 
%% tcbdbets:open_file/1, the bloom option does not take a filename 
%% as this is computed from the table name.
%% @end

-module (tcbdbtab).

-export ([ clear_stats/1,
           get_stats/1 ]).
-ifdef (HAVE_MNESIA_EXT).
-behaviour (mnesia_ext).
-endif.
-export ([ info/2,
           lookup/2,
           insert/2,
           match_object/2,
           select/1,
           select/2,
           select/3,
           delete/2,
           match_delete/2,
           first/1,
           next/2,
           last/1,
           prev/2,
           slot/2,
           update_counter/3,
           create_table/2,
           delete_table/1,
           init_index/2,
           add_index/2,
           delete_index/2,
           fixtable/2,
           init_table/3
         ]).

-ifdef (HAVE_EUNIT).
-include_lib ("eunit/include/eunit.hrl").
-endif.

-include_lib ("mnesia/src/mnesia.hrl").

% { Tab, Name } is a checkpoint retainer
-define (is_tabid (X), (is_atom (X) orelse 
                        (is_tuple (X) andalso
                         is_atom (element (1, X))))).

-define (stat (What, X), (begin
                          Start = now (),
                          R = X,
                          End = now (),
                          update_stat (What, Tab, timer:now_diff (End, Start)),
                          R
                          end)).

-define (stat_keys, [ info,
                      lookup,
                      insert,
                      match_object,
                      select,
                      delete,
                      match_delete,
                      first,
                      next,
                      last,
                      prev,
                      update_counter ]).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

clear_stats (Tab) when ?is_tabid (Tab) ->
  lists:foreach (fun (X) -> mnesia_lib:unset ({ Tab, X, stats }) end,
                 ?stat_keys).


get_stats (Tab) when ?is_tabid (Tab) ->
  [ { What, Max, Sum, Count } || What <- ?stat_keys,
                                 { Max, Sum, Count } <- 
                                   [ try mnesia_lib:val ({ Tab, What, stats })
                                     catch _ : _ -> []
                                     end ] ].

%% @hidden

info (Tab, What) when ?is_tabid (Tab) ->
  ?stat (info, tcbdbets:info (get_port (Tab), What)).

%% @hidden

lookup (Tab, Key) when ?is_tabid (Tab) ->
  ?stat (lookup, tcbdbets:lookup (get_port (Tab), Key)).

%% @hidden

insert (Tab, Objects) when ?is_tabid (Tab) ->
  ?stat (insert, tcbdbets:insert (get_port (Tab), Objects)).

%% @hidden

match_object (Tab, Pattern) when ?is_tabid (Tab) ->
  ?stat (match_object, tcbdbets:match_object (get_port (Tab), Pattern)).

%% @hidden

select (Cont) ->
  tcbdbets:select (Cont).

%% @hidden

select (Tab, MatchSpec) when ?is_tabid (Tab) ->
  ?stat (select, tcbdbets:select (get_port (Tab), MatchSpec)).

%% @hidden

select (Tab, MatchSpec, Count) when ?is_tabid (Tab) ->
  ?stat (select, tcbdbets:select (get_port (Tab), MatchSpec, Count)).

%% @hidden

delete (Tab, Key) when ?is_tabid (Tab) ->
  ?stat (delete, tcbdbets:delete (get_port (Tab), Key)).

%% @hidden

match_delete (Tab, Pattern) when ?is_tabid (Tab) ->
  ?stat (match_delete, tcbdbets:match_delete (get_port (Tab), Pattern)).

%% @hidden

first (Tab) when ?is_tabid (Tab) ->
  ?stat (first, tcbdbets:first (get_port (Tab))).

%% @hidden

next (Tab, Key) when ?is_tabid (Tab) ->
  ?stat (next, tcbdbets:next (get_port (Tab), Key)).

%% @hidden

last (Tab) when ?is_tabid (Tab) ->
  ?stat (last, tcbdbets:last (get_port (Tab))).

%% @hidden

prev (Tab, Key) when ?is_tabid (Tab) ->
  ?stat (prev, tcbdbets:prev (get_port (Tab), Key)).

%% @hidden

% ??? 
slot (Tab, _N) when ?is_tabid (Tab) ->
  { error, slot_not_supported }.

%% @hidden

update_counter (Tab, Key, Incr) when ?is_tabid (Tab) ->
  ?stat (update_counter, tcbdbets:update_counter (get_port (Tab), Key, Incr)).

%% @hidden

create_table (Tab, Cs) when is_atom (Tab) ->
  create_table (Tab, Cs, atom_to_list (Tab) ++ ".tcb");
create_table (T = { Tab, Name }, Cs) when is_atom (Tab) -> % checkpoint retainer
  create_table (T,
                Cs, 
                atom_to_list (Tab) 
                ++ "_" 
                ++ lists:flatten (io_lib:write (Name))
                ++ ".tcbret").

%% @hidden

delete_table (Tab) when ?is_tabid (Tab) ->
  tcbdbsrv:delete_tab (Tab),
  ok.

% TODO: indices

%% @hidden

init_index (Tab, _Pos) when ?is_tabid (Tab) ->
  ok.

%% @hidden

add_index (Tab, _Pos) when ?is_tabid (Tab) ->
  ok.

%% @hidden

delete_index (Tab, _Pos) when ?is_tabid (Tab) ->
  ok.

%% @hidden

fixtable (Tab, _Bool) when ?is_tabid (Tab) ->
  true.

%% @hidden

init_table (Tab, InitFun, _Sender) when ?is_tabid (Tab) ->
  tcbdbets:init_table (get_port (Tab), InitFun).

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-

create_table (Tab, Cs, FileName) ->
  Dir = mnesia_lib:val (dir),
  File = filename:join ([ Dir, FileName ]),
  { _, Type, _ } = Cs#cstruct.type,
  UserProps = [ case X of { Foo, true } when Foo =:= uncompressed orelse
                                             Foo =:= deflate orelse
                                             Foo =:= async_write orelse
                                             Foo =:= tcbs -> Foo;
                          { bloom, Bytes, Hashes } ->
                            { bloom, [ File, "_bloom" ], Bytes, Hashes };
                          _ -> X
                end || X <- Cs#cstruct.user_properties ],
  { ok, _ } = tcbdbsrv:create_tab (Tab, 
                                   [ { access, read_write },
                                     { file, File },
                                     { keypos, 2 },
                                     { type, Type } ] 
                                   ++ UserProps),
  Tab.

get_port (Tab) ->
  case tcbdbsrv:get_tab (Tab) of
    { ok, Port } -> 
      Port;
    unknown ->
      Cstruct = mnesia_lib:val ({ Tab, cstruct }),
      Tab = create_table (Tab, Cstruct),
      { ok, Port } = tcbdbsrv:get_tab (Tab),
      Port
  end.

max (A, B) when A > B -> A;
max (_, B) -> B.

update_stat (What, Tab, Micros) ->
  try mnesia_lib:val ({ Tab, What, stats }) of
    { Max, Sum, Count } ->
      mnesia_lib:set ({ Tab, What, stats },
                      { max (Max, Micros),
                        Sum + Micros,
                        Count + 1 })
  catch
    exit : { no_exists, { Tab, What, stats } } ->
      mnesia_lib:set ({ Tab, What, stats }, { Micros, Micros, 1 })
  end.

-ifdef (HAVE_EUNIT).
-ifdef (HAVE_MNESIA_EXT).

% ok, don't want to depend upon quickcheck, so here's some cheese

-define (FORALL (Var, Gen, Cond), fun (A) -> Var = (Gen) (A), Cond end).

flasscheck (N, Limit, P) -> flasscheck (1, N, math:log (Limit), P).

flasscheck (M, N, LogLimit, P) when M =< N -> 
  Size = trunc (math:exp (LogLimit * M / N)),
  true = P (Size),
  io:format (".", []),
  flasscheck (M + 1, N, LogLimit, P);
flasscheck (_, N, _, _) -> 
  io:format ("~n~p tests passed~n", [ N ]),
  ok.

random_integer () ->
  random:uniform (100000).

random_float () ->
  random:uniform ().

random_binary () ->
  list_to_binary ([ random:uniform (255) || _ <- lists:seq (1, 5) ]).

random_atom () ->
  list_to_atom ([ random:uniform ($z - $a) + $a || _ <- lists:seq (1, 5) ]).

random_list () ->
  [ random_term () || _ <- lists:seq (1, 3) ].

random_tuple () ->
  list_to_tuple (random_list ()).

random_term () ->
  case random:uniform (10) of
    1 -> random_integer ();
    2 -> random_integer ();
    3 -> random_float ();
    4 -> random_float ();
    5 -> random_binary ();
    6 -> random_binary ();
    7 -> random_atom ();
    8 -> random_atom ();
    9 -> random_list ();
    10 -> random_tuple ()
  end.

random_variable () ->
  case random:uniform (10) of
    1 -> 
      '_';
    2 ->
      list_to_atom ([ $$ ] ++ [ random:uniform ($z - $a) + $a || _ <- lists:seq (1, 5) ]); % not actually a variable
    _ ->
      list_to_atom ([ $$ ] ++ integer_to_list (random:uniform (100) - 1))
  end.

random_variables () ->
  [ random_variable () || _ <- lists:seq (1, random:uniform (5)) ].

random_matchhead () ->
  random_matchhead (random_variables ()).

random_matchhead ([ H ]) ->
  case random:uniform (2) of
    1 -> { [ H ], H };
    2 -> { [], random_term () }
  end;
random_matchhead ([ H | T ]) ->
  { V, M } = random_matchhead (T),
  case random:uniform (3) of
    1 -> { [ H | V ], { H, M } };
    2 -> { V, M };
    3 -> { V, { random_term (), M } }
  end.

random_guard (_Variables) ->
  [].   % TODO

random_matchbody (Variables) ->
  case { random:uniform (3), Variables } of
    { _, [] } -> [ '$_' ];
    { 1, _ } -> [ '$$' ];
    { 2, _ } -> [ '$_' ];
    { 3, _ } -> [ hd (Variables) | lists:filter (fun (_) -> random:uniform (2) =:= 1 end, tl (Variables)) ]
  end.

random_matchspec () ->
  [ (fun () -> { V, M } = random_matchhead (), 
               { M, random_guard (V), random_matchbody (V) } end) () ||
    _ <- lists:seq (1, random:uniform (3)) ].

random_variable_or_term () ->
  case random:uniform (2) of
    1 -> random_term ();
    2 -> random_variable ()
  end.

random_pattern (N) ->
  list_to_tuple ([ random_variable_or_term () || _ <- lists:seq (1, N) ]).

%-=====================================================================-
%-                                Tests                                -
%-=====================================================================-

delete_test_ () ->
  F = fun (R) ->
    { ok, _ } = 
      file:read_file_info 
        (filename:join ([ mnesia:system_info (directory),
                          atom_to_list (R) ++ ".tcb" ])),
    mnesia:stop (),
    mnesia:start (),
    { atomic, ok } = mnesia:delete_table (R),
    { error, enoent } = 
      file:read_file_info 
        (filename:join ([ mnesia:system_info (directory),
                          atom_to_list (R) ++ ".tcb" ])),
    ok
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      mnesia:stop (),
      os:cmd ("rm -rf Mnesia.*"),
      mnesia:start (),
      mnesia:change_table_copy_type (schema, node (), disc_copies),
      mnesia:create_table (testtab, 
                           [ { type, { external, ordered_set, ?MODULE } },
                             { external_copies, [ node () ] } ]),
      testtab
    end,
    fun (_) ->
      mnesia:stop (),
      tcerl:stop (),
      os:cmd ("rm -rf Mnesia.*")
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

% todo: this test fails in R12 because mnesia guards against 
% variables in delete_object (also, this test doesn't make sense)
% delete_object_test_ () ->
%   F = fun ({ Tab, R }) ->
%     T = 
%       ?FORALL (X,
%                fun (_) -> 
%                 { random_term (), random_term (), random_pattern (2) }
%                end,
%                (fun ({ Key, Value, Pattern }) ->
%                   RPattern = list_to_tuple ([ R | tuple_to_list (Pattern) ]),
%                   TabPattern = list_to_tuple ([ Tab | tuple_to_list (Pattern) ]),
%                   ok = mnesia:dirty_delete_object (RPattern),
%                   ok = mnesia:dirty_delete_object (TabPattern),
% 
%                   RConts = mnesia:dirty_match_object ({ R, '_', '_' }),
%                   TabConts = mnesia:dirty_match_object ({ Tab, '_', '_' }),
%                   TabFix = [ { R, K, V } || { _, K, V } <- TabConts ],
%                   RConts = TabFix,
% 
%                   ok = mnesia:dirty_write ({ R, Key, Value }),
%                   ok = mnesia:dirty_write ({ Tab, Key, Value }),
% 
%                   true
%                 end) (X)),
% 
%     ok = flasscheck (1000, 10, T)
%   end,
% 
%   { setup,
%     fun () -> 
%       tcerl:start (),
%       mnesia:stop (),
%       os:cmd ("rm -rf Mnesia.*"),
%       mnesia:start (),
%       mnesia:change_table_copy_type (schema, node (), disc_copies),
%       mnesia:create_table (testtab, 
%                            [ { type, { external, ordered_set, ?MODULE } },
%                              { external_copies, [ node () ] } ]),
%       mnesia:create_table (etstab, 
%                            [ { type, ordered_set },
%                              { ram_copies, [ node () ] } ]),
%       { etstab, testtab }
%     end,
%     fun (_) ->
%       mnesia:stop (),
%       tcerl:stop (),
%       os:cmd ("rm -rf Mnesia.*")
%     end,
%     fun (X) -> { timeout, 60, fun () -> F (X) end } end
%   }.

first_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  EtsFirst = mnesia:dirty_first (Tab),
                  TcbdbFirst = mnesia:dirty_first (R),
                  EtsFirst = TcbdbFirst,

                  ok = mnesia:dirty_write ({ Tab, Key, Value }),
                  ok = mnesia:dirty_write ({ R, Key, Value }),

                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      mnesia:stop (),
      os:cmd ("rm -rf Mnesia.*"),
      mnesia:start (),
      mnesia:change_table_copy_type (schema, node (), disc_copies),
      mnesia:create_table (testtab, 
                           [ { type, { external, ordered_set, ?MODULE } },
                             { external_copies, [ node () ] } ]),
      mnesia:create_table (etstab, 
                           [ { type, ordered_set },
                             { ram_copies, [ node () ] } ]),
      { etstab, testtab }
    end,
    fun (_) ->
      mnesia:stop (),
      tcerl:stop (),
      os:cmd ("rm -rf Mnesia.*")
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

info_test_ () ->
  F = fun (R) ->
    true = is_list (mnesia:table_info (R, all)),
    read_write = mnesia:table_info (R, access_mode),
    0 = mnesia:table_info (R, size),
    true = is_integer (mnesia:table_info (R, memory)),
    ordered_set = mnesia:table_info (R, type),
    { external_copies, ?MODULE } = mnesia:table_info (R, storage_type),
    ok
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      mnesia:stop (),
      os:cmd ("rm -rf Mnesia.*"),
      mnesia:start (),
      mnesia:change_table_copy_type (schema, node (), disc_copies),
      mnesia:create_table (testtab, 
                           [ { type, { external, ordered_set, ?MODULE } },
                             { external_copies, [ node () ] } ]),
      testtab
    end,
    fun (_) ->
      mnesia:stop (),
      tcerl:stop (),
      os:cmd ("rm -rf Mnesia.*")
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

last_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  EtsLast = mnesia:dirty_last (Tab),
                  TcbdbLast = mnesia:dirty_last (R),
                  EtsLast = TcbdbLast,

                  ok = mnesia:dirty_write ({ Tab, Key, Value }),
                  ok = mnesia:dirty_write ({ R, Key, Value }),

                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      mnesia:stop (),
      os:cmd ("rm -rf Mnesia.*"),
      mnesia:start (),
      mnesia:change_table_copy_type (schema, node (), disc_copies),
      mnesia:create_table (testtab, 
                           [ { type, { external, ordered_set, ?MODULE } },
                             { external_copies, [ node () ] } ]),
      mnesia:create_table (etstab, 
                           [ { type, ordered_set },
                             { ram_copies, [ node () ] } ]),
      { etstab, testtab }
    end,
    fun (_) ->
      mnesia:stop (),
      tcerl:stop (),
      os:cmd ("rm -rf Mnesia.*")
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

match_object_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> 
                { random_term (), random_term (), random_pattern (2) }
               end,
               (fun ({ Key, Value, Pattern }) ->
                  RPattern = list_to_tuple ([ R | tuple_to_list (Pattern) ]),
                  TabPattern = list_to_tuple ([ Tab | tuple_to_list (Pattern) ]),
                  TcbdbMatch = mnesia:dirty_match_object (RPattern),
                  EtsMatch = mnesia:dirty_match_object (TabPattern),
                  FixEtsMatch = [ { R, K, V } || { _, K, V } <- EtsMatch ],
                  TcbdbMatch = FixEtsMatch,

                  ok = mnesia:dirty_write ({ R, Key, Value }),
                  ok = mnesia:dirty_write ({ Tab, Key, Value }),

                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      mnesia:stop (),
      os:cmd ("rm -rf Mnesia.*"),
      mnesia:start (),
      mnesia:change_table_copy_type (schema, node (), disc_copies),
      mnesia:create_table (testtab, 
                           [ { type, { external, ordered_set, ?MODULE } },
                             { external_copies, [ node () ] } ]),
      mnesia:create_table (etstab, 
                           [ { type, ordered_set },
                             { ram_copies, [ node () ] } ]),
      { etstab, testtab }
    end,
    fun (_) ->
      mnesia:stop (),
      tcerl:stop (),
      os:cmd ("rm -rf Mnesia.*")
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

next_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  EtsPrev = mnesia:dirty_next (Tab, Key),
                  TcbdbPrev = mnesia:dirty_next (R, Key),
                  EtsPrev = TcbdbPrev,

                  ok = mnesia:dirty_write ({ Tab, Key, Value }),
                  ok = mnesia:dirty_write ({ R, Key, Value }),

                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      mnesia:stop (),
      os:cmd ("rm -rf Mnesia.*"),
      mnesia:start (),
      mnesia:change_table_copy_type (schema, node (), disc_copies),
      mnesia:create_table (testtab, 
                           [ { type, { external, ordered_set, ?MODULE } },
                             { external_copies, [ node () ] } ]),
      mnesia:create_table (etstab, 
                           [ { type, ordered_set },
                             { ram_copies, [ node () ] } ]),
      { etstab, testtab }
    end,
    fun (_) ->
      mnesia:stop (),
      tcerl:stop (),
      os:cmd ("rm -rf Mnesia.*")
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

prev_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  EtsPrev = mnesia:dirty_prev (Tab, Key),
                  TcbdbPrev = mnesia:dirty_prev (R, Key),
                  EtsPrev = TcbdbPrev,

                  ok = mnesia:dirty_write ({ Tab, Key, Value }),
                  ok = mnesia:dirty_write ({ R, Key, Value }),

                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      mnesia:stop (),
      os:cmd ("rm -rf Mnesia.*"),
      mnesia:start (),
      mnesia:change_table_copy_type (schema, node (), disc_copies),
      mnesia:create_table (testtab, 
                           [ { type, { external, ordered_set, ?MODULE } },
                             { external_copies, [ node () ] } ]),
      mnesia:create_table (etstab, 
                           [ { type, ordered_set },
                             { ram_copies, [ node () ] } ]),
      { etstab, testtab }
    end,
    fun (_) ->
      mnesia:stop (),
      tcerl:stop (),
      os:cmd ("rm -rf Mnesia.*")
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

roundtrip_test_ () ->
  F = fun (Tab) ->
    Dir = mnesia_lib:val (dir),
    BloomFile = filename:join ([ Dir, atom_to_list (Tab) ++ ".tcb_bloom" ]),
    { ok, _ } = file:read_file_info (BloomFile),

    T =
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  ok = mnesia:dirty_write ({ Tab, Key, Value }),
                  [ { Tab, Key, Value } ] = mnesia:dirty_read ({ Tab, Key }),
                  ok = mnesia:dirty_delete ({ Tab, Key }),
                  [] = mnesia:dirty_read ({ Tab, Key }),
                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      mnesia:stop (),
      os:cmd ("rm -rf Mnesia.*"),
      mnesia:start (),
      mnesia:change_table_copy_type (schema, node (), disc_copies),
      mnesia:create_table (testtab, 
                           [ { type, { external, ordered_set, ?MODULE } },
                             { external_copies, [ node () ] },
                             { user_properties, [ { bloom, 1 bsl 20, 7 } ] } ]),
      testtab
    end,
    fun (_) ->
      mnesia:stop (),
      tcerl:stop (),
      os:cmd ("rm -rf Mnesia.*")
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

roundtrip_async_test_ () ->
  F = fun (Tab) ->
    T =
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  ok = mnesia:dirty_write ({ Tab, Key, Value }),
                  [ { Tab, Key, Value } ] = mnesia:dirty_read ({ Tab, Key }),
                  ok = mnesia:dirty_delete ({ Tab, Key }),
                  [] = mnesia:dirty_read ({ Tab, Key }),
                  [ { delete, _, _, 1 },
                    { insert, _, _, 1 },
                    { lookup, _, _, 2 } ] = lists:sort (get_stats (Tab)),
                  ok = clear_stats (Tab),
                  [] = get_stats (Tab),
                  ok = clear_stats (Tab),
                  [] = get_stats (Tab),
                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      mnesia:stop (),
      os:cmd ("rm -rf Mnesia.*"),
      mnesia:start (),
      mnesia:change_table_copy_type (schema, node (), disc_copies),
      mnesia:create_table (testtab, 
                           [ { type, { external, ordered_set, ?MODULE } },
                             { external_copies, [ node () ] },
                             { user_properties, [ { async_write, true } ] } ]),
      testtab
    end,
    fun (_) ->
      mnesia:stop (),
      tcerl:stop (),
      os:cmd ("rm -rf Mnesia.*")
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

select_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> 
                { random_term (), random_term (), random_matchspec () }
               end,
               (fun ({ Key, Value, MatchSpec }) ->
                  TcbdbSelect = mnesia:dirty_select (R, MatchSpec),
                  EtsSelect = mnesia:dirty_select (Tab, MatchSpec),
                  FixEtsSelect = [ (fun ({ _, K, V }) -> { R, K, V };
                                        ([ { _, K, V } ]) -> [ { R, K, V } ];
                                        (Y) -> Y
                                    end) (Z) || Z <- EtsSelect ],
                  TcbdbSelect = FixEtsSelect,

                  mnesia:async_dirty
                    (fun () ->
                       case length (TcbdbSelect) of
                         N when N < 2 -> ok;
                         N ->
                           { TcbdbSelectSmall, TcBdbCont } = 
                             mnesia:select (R, MatchSpec, N - 1, read),
                           { EtsSelectSmall, EtsCont } = 
                             mnesia:select (Tab, MatchSpec, N - 1, read),
                           FixEtsSelectSmall = 
                             [ (fun ({ _, K, V }) -> { R, K, V };
                                    ([ { _, K, V } ]) -> [ { R, K, V } ];
                                    (Y) -> Y
                                end) (Z) || Z <- EtsSelectSmall ],
                           TcbdbSelectSmall = FixEtsSelectSmall,

                           { TcBdbMore, TcBdbAgain } = 
                             mnesia:select (TcBdbCont),
                           { EtsMore, _ } = 
                             mnesia:select (EtsCont),
                           FixEtsMore = [ (fun ({ _, K, V }) -> { R, K, V };
                                                ([ { _, K, V } ]) -> [ { R, K, V } ];
                                               (Y) -> Y
                                           end) (Z) || Z <- EtsMore ],

                           TcBdbMore = FixEtsMore,

                           '$end_of_table' = mnesia:select (TcBdbAgain)
                       end
                     end),

                  ok = mnesia:dirty_write ({ R, Key, Value }),
                  ok = mnesia:dirty_write ({ Tab, Key, Value }),

                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      mnesia:stop (),
      os:cmd ("rm -rf Mnesia.*"),
      mnesia:start (),
      mnesia:change_table_copy_type (schema, node (), disc_copies),
      mnesia:create_table (testtab, 
                           [ { type, { external, ordered_set, ?MODULE } },
                             { external_copies, [ node () ] } ]),
      mnesia:create_table (etstab, 
                           [ { type, ordered_set },
                             { ram_copies, [ node () ] } ]),
      { etstab, testtab }
    end,
    fun (_) ->
      mnesia:stop (),
      tcerl:stop (),
      os:cmd ("rm -rf Mnesia.*")
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.


% TODO: this does not appear to work at this time
% to_ets_test_ () ->
%   { setup,
%     fun () -> 
%       tcerl:start (),
%       mnesia:stop (),
%       os:cmd ("rm -rf Mnesia.*"),
%       mnesia:start (),
%       mnesia:change_table_copy_type (schema, node (), disc_copies),
%       mnesia:create_table (testtab, 
%                            [ { type, { external, ordered_set, ?MODULE } },
%                              { external_copies, [ node () ] },
%                              { attributes, [ key, count ] } ]),
%       testtab
%     end,
%     fun (_) ->
%       mnesia:stop (),
%       tcerl:stop (),
%       os:cmd ("rm -rf Mnesia.*")
%     end,
%     fun (X) -> mnesia:change_table_copy_type (X, node (), ram_copies) end
%   }.

update_counter_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), 
                            random_integer (),
                            random_integer () } end,
               (fun ({ Key, Count, Incr }) ->
                  ok = mnesia:dirty_write ({ R, Key, Count }),
                  ok = mnesia:dirty_write ({ Tab, Key, Count }),

                  TcbdbUp = mnesia:dirty_update_counter (R, Key, Incr),
                  EtsUp = mnesia:dirty_update_counter (Tab, Key, Incr),

                  TcbdbUp = EtsUp,

                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      mnesia:stop (),
      os:cmd ("rm -rf Mnesia.*"),
      mnesia:start (),
      mnesia:change_table_copy_type (schema, node (), disc_copies),
      mnesia:create_table (testtab, 
                           [ { type, { external, ordered_set, ?MODULE } },
                             { external_copies, [ node () ] },
                             { attributes, [ key, count ] },
                             { user_properties, [ { deflate, true },
                                                  { bucket_array_size, 10000 } ] } ]),
      mnesia:create_table (etstab, 
                           [ { type, ordered_set },
                             { ram_copies, [ node () ] },
                             { attributes, [ key, count ] } ]),
      { etstab, testtab }
    end,
    fun (_) ->
      mnesia:stop (),
      tcerl:stop (),
      os:cmd ("rm -rf Mnesia.*")
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

-endif.
-endif.
