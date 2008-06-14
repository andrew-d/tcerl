-module (tcbdbtab).

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
           fixtable/2 
         ]).

-ifdef (HAVE_EUNIT).
-include_lib ("eunit/include/eunit.hrl").
-endif.

-include_lib ("mnesia/src/mnesia.hrl").

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

info (Tab, What) when is_atom (Tab) ->
  tcbdbets:info (get_port (Tab), What).

lookup (Tab, Key) when is_atom (Tab) ->
  tcbdbets:lookup (get_port (Tab), Key).

insert (Tab, Objects) when is_atom (Tab) ->
  tcbdbets:insert (get_port (Tab), Objects).

match_object (Tab, Pattern) when is_atom (Tab) ->
  tcbdbets:match_object (get_port (Tab), Pattern).

select (Cont) ->
  tcbdbets:select (Cont).

select (Tab, MatchSpec) when is_atom (Tab) ->
  tcbdbets:select (get_port (Tab), MatchSpec).

select (Tab, MatchSpec, Count) when is_atom (Tab) ->
  tcbdbets:select (get_port (Tab), MatchSpec, Count).

delete (Tab, Key) when is_atom (Tab) ->
  tcbdbets:delete (get_port (Tab), Key).

match_delete (Tab, Pattern) when is_atom (Tab) ->
  tcbdbets:match_delete (get_port (Tab), Pattern).

first (Tab) when is_atom (Tab) ->
  tcbdbets:first (get_port (Tab)).

next (Tab, Key) when is_atom (Tab) ->
  tcbdbets:next (get_port (Tab), Key).

last (Tab) when is_atom (Tab) ->
  tcbdbets:last (get_port (Tab)).

prev (Tab, Key) when is_atom (Tab) ->
  tcbdbets:prev (get_port (Tab), Key).

% ??? 
slot (Tab, _N) when is_atom (Tab) ->
  { error, slot_not_supported }.

update_counter (Tab, Key, Incr) when is_atom (Tab) ->
  tcbdbets:update_counter (get_port (Tab), Key, Incr).

create_table (Tab, Cs) when is_atom (Tab) ->
  Dir = mnesia_lib:val (dir),
  { _, Type, _ } = Cs#cstruct.type,
  UserProps = [ case X of { Foo, true } when Foo =:= uncompressed orelse
                                             Foo =:= deflate orelse
                                             Foo =:= tcbs -> Foo;
                          _ -> X
                end || X <- Cs#cstruct.user_properties ],
  File = filename:join ([ Dir, atom_to_list (Tab) ++ ".tcb" ]),
  { ok, Port } = tcbdbets:open_file ([ { access, read_write },
                                       { file, File },
                                       { keypos, 2 },
                                       { type, Type } ] 
                                     ++ UserProps),
  tcbdbets:unlink (Port),
  mnesia_lib:set ({ Tab, tcbdb_port }, Port),
  Tab.

delete_table (Tab) when is_atom (Tab) ->
  Port = get_port (Tab),
  FileName = tcbdbets:info (Port, filename),
  tcbdbets:close (Port),
  file:delete (FileName).

% TODO: indices

init_index (Tab, _Pos) when is_atom (Tab) ->
  ok.

add_index (Tab, _Pos) when is_atom (Tab) ->
  ok.

delete_index (Tab, _Pos) when is_atom (Tab) ->
  ok.

fixtable (Tab, _Bool) when is_atom (Tab) ->
  true.

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-

get_port (Tab) ->
  try mnesia_lib:val ({ Tab, tcbdb_port }) 
  catch
    exit : { no_exists, { Tab, tcbdb_port } } ->
      Cstruct = mnesia_lib:val ({ Tab, cstruct }),
      Tab = create_table (Tab, Cstruct),
      mnesia_lib:val ({ Tab, tcbdb_port })
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

%-=====================================================================-
%-                                Tests                                -
%-=====================================================================-

roundtrip_test_ () ->
  F = fun (Tab) ->
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

%flass (_File, _Line) -> ok.

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
