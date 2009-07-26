%% @doc Analyze match specifications.

-module (tcbdbmsutil).
-export ([ analyze/2 ]).

-ifdef (HAVE_EUNIT).
-include_lib ("eunit/include/eunit.hrl").
-endif.

%-=====================================================================-
%-                                Types                                -
%-=====================================================================-

% these are from the http://erlang.org/doc/apps/erts/match_spec.html
% with some modifications to 1) ignore stuff for tracing and 
% 2) work within the type syntax.

-type action_term () :: any (). 
-type bool_function () :: is_atom | is_constant | is_float | is_integer | is_list | is_number | is_pid | is_port | is_reference | is_tuple | is_binary | is_function | is_record | is_seq_trace | 'and' | 'or' | 'not' | 'xor' | 'andalso' | 'orelse'.
-type guard_function () :: bool_function () | abs | element | hd | length | node | round | size | tl | trunc | '+' | '-' | '*' | 'div' | 'rem' | 'band' | 'bor' | 'bxor' | 'bnot' | 'bsl' | 'bsr' | '>' | '>=' | '<' | '=<' | '=:=' | '==' | '=/=' | '/=' | self | get_tcw.
-type match_variable () :: '$1' | '$2' | '$3'.  % ...
-type expression_match_variable () :: match_variable ()  | '$_' | '$$'.
-type non_composite_term () :: pid () | port () | ref () | atom () | binary () | float () | integer ().
-type match_constant () :: { const, any () }.
-type term_construct () :: {{}} | {{ any () }} | non_composite_term () | match_constant () | [ any () ].
-type condition_expression () :: expression_match_variable () | term_construct ().
-type match_condition () :: { guard_function () } | 
                            { guard_function (), condition_expression () } |
                            { guard_function (), 
                              condition_expression (),
                              condition_expression () }.
-type match_head () :: any ().
-type match_function () :: { match_head (), [ match_condition () ], [ action_term () ] }.
-type match_expression () :: [ match_function () ].

% these are for me

% erlang terms, extended with a smallest and largest term

%-type extended_term () :: smallest | largest | { literal, non_composite_term () } | [ extended_term () ] | { tuple, integer (), [ extended_term () ] }.
-type extended_term () :: smallest | largest | { literal, non_composite_term () } | [ any () ] | { tuple, integer (), [ any () ] }.

% intervals of extended erlang terms

-type interval () :: { interval, extended_term (), extended_term () } |
                     none.

-type bindings () :: { bindings, any () }.

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

%% @doc Return an union of intervals which is guaranteed to contain all keys
%% in tuples that contribute to the results of the match spec.  
%% @end

-spec analyze (match_expression (), integer ()) -> [ interval () ].

analyze (Expression, KeyPos) when is_list (Expression),
                                  is_integer (KeyPos) ->
  interval_union ([ analyze_match_function (M, KeyPos) || M <- Expression ]).

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-

-spec adjust_lower_bound (match_variable (), extended_term (), bindings ()) -> bindings ().

adjust_lower_bound (Variable, ExtendedTerm, Bindings = { bindings, Dict }) ->
  case dict:find (Variable, Dict) of
    { ok, { interval, Lower, Upper } } ->
      NewLower = extended_max (Lower, ExtendedTerm),
      case extended_leq (NewLower, Upper) of
        true ->
          NewInterval = { interval, NewLower, Upper },
          { bindings, dict:store (Variable, NewInterval, Dict) };
        false ->
          { bindings, dict:store (Variable, none, Dict) }
      end;
    { ok, none } -> 
      Bindings;
    error ->
      NewInterval = { interval, ExtendedTerm, largest },
      { bindings, dict:store (Variable, NewInterval, Dict) }
  end.

-spec adjust_upper_bound (match_variable (), extended_term (), bindings ()) -> bindings ().

adjust_upper_bound (Variable, ExtendedTerm, Bindings = { bindings, Dict }) ->
  case dict:find (Variable, Dict) of
    { ok, { interval, Lower, Upper } } ->
      NewUpper = extended_min (Upper, ExtendedTerm),
      case extended_leq (Lower, NewUpper) of
        true ->
          NewInterval = { interval, Lower, NewUpper },
          { bindings, dict:store (Variable, NewInterval, Dict) };
        false ->
          { bindings, dict:store (Variable, none, Dict) }
      end;
    { ok, none } -> 
      Bindings;
    error ->
      NewInterval = { interval, smallest, ExtendedTerm },
      { bindings, dict:store (Variable, NewInterval, Dict) }
  end.

-spec analyze_keypos (match_head (), bindings ()) -> interval ().

analyze_keypos (MatchHead, Bindings) when is_tuple (MatchHead) ->
  case analyze_keypos_elements (tuple_to_list (MatchHead), Bindings, []) of
    { interval, Lower, Upper } when is_list (Lower), is_list (Upper) -> 
      Size = size (MatchHead),
      { interval, 
        { tuple, Size, fixed_width (Lower, Size, smallest) }, 
        { tuple, Size, fixed_width (Upper, Size, largest) }
      };
    none -> 
      none
  end;
analyze_keypos (MatchHead, Bindings) when is_list (MatchHead) ->
  analyze_keypos_elements (MatchHead, Bindings, []);
analyze_keypos ('_', _Bindings) ->
  { interval, smallest, largest };
analyze_keypos (MatchHead, Bindings) when is_atom (MatchHead) ->
  case is_match_variable (MatchHead) of
    true -> 
      bound_interval (MatchHead, Bindings);
    false -> 
      { interval, { literal, MatchHead }, { literal, MatchHead } }
  end;
analyze_keypos (MatchHead, _Bindings) ->
  { interval, { literal, MatchHead }, { literal, MatchHead } }.

-spec analyze_keypos_elements ([ match_head () ],
                               bindings (),
                               [ any () ]) -> interval ().

analyze_keypos_elements ([], _Bindings, Acc) ->
  Point = lists:reverse (Acc),
  { interval, Point, Point };
analyze_keypos_elements ([ H | T ], Bindings, Acc) ->
  case analyze_keypos (H, Bindings) of
    { interval, X, X } ->
      analyze_keypos_elements (T, Bindings, [ X | Acc ]);
    { interval, Lower, Upper } ->
      { interval,
        lists:reverse ([ Lower | Acc ]),
        lists:reverse ([ Upper | Acc ]) };
    none ->
      none
  end.

-spec analyze_op_arguments (condition_expression (), condition_expression ()) -> { match_variable (), extended_term () } | { swapped, match_variable (), extended_term () } | false.

analyze_op_arguments (A, B) ->
  case { is_match_variable (A), is_constant_expression (B) } of
    { true, true } ->
      { A, match_condition_to_extended_term (B) };
    _ ->
      case { is_match_variable (B), is_constant_expression (A) } of
        { true, true } ->
          { swapped, B, match_condition_to_extended_term (A) };
        _ ->
          false
      end
  end.

-spec analyze_match_conditions ([ match_condition () ]) -> bindings ().

analyze_match_conditions (Conditions) ->
  lists:foldl (fun analyze_match_conditions/2, new_bindings (), Conditions).

-spec analyze_match_conditions (match_condition (), bindings ()) -> bindings ().

analyze_match_conditions ({ Op, A, B }, Bindings) when Op =:= '>';
                                                       Op =:= '>=' ->
  case analyze_op_arguments (A, B) of
    { Variable, ExtendedTerm } ->
      adjust_lower_bound (Variable, ExtendedTerm, Bindings);
    { swapped, Variable, ExtendedTerm } ->
      adjust_upper_bound (Variable, ExtendedTerm, Bindings);
    false ->
      Bindings
  end;
analyze_match_conditions ({ Op, A, B }, Bindings) when Op =:= '<';
                                                       Op =:= '=<' ->
  case analyze_op_arguments (A, B) of
    { Variable, ExtendedTerm } ->
      adjust_upper_bound (Variable, ExtendedTerm, Bindings);
    { swapped, Variable, ExtendedTerm } ->
      adjust_lower_bound (Variable, ExtendedTerm, Bindings);
    false ->
      Bindings
  end;
analyze_match_conditions ({ Op, A, B }, Bindings) when Op =:= '=:=';
                                                       Op =:= '==' ->
  lists:foldl (fun analyze_match_conditions/2, 
               Bindings,
               [ { '>=', A, B }, { '=<', A, B } ]);
analyze_match_conditions (_, Bindings) ->
  Bindings.

-spec analyze_match_function (match_function (), integer ()) -> interval ().

analyze_match_function ({ MatchHead, MatchConditions, _ }, 
                        KeyPos) when is_tuple (MatchHead),
                                     size (MatchHead) >= KeyPos,
                                     is_integer (KeyPos) ->
  analyze_keypos (element (KeyPos, MatchHead), 
                  analyze_match_conditions (MatchConditions));
analyze_match_function ({ '_', _, _ }, _) ->
  { interval, smallest, largest };
analyze_match_function ({ MatchHead, _, _ }, _) when is_atom (MatchHead) ->
  case is_match_variable (MatchHead) of
    true ->
      { interval, smallest, largest };
    false ->
      none
  end;
analyze_match_function (_, _) ->
  none.

-spec bound_interval (match_variable (), bindings ()) -> interval ().

bound_interval (Variable, { bindings, Dict }) ->
  case dict:find (Variable, Dict) of
    { ok, Interval } -> Interval;
    error -> { interval, smallest, largest }
  end.

-spec extended_leq (extended_term (), extended_term ()) -> bool ().

extended_leq (smallest, _) -> true;
extended_leq (_, smallest) -> false;
extended_leq (largest, _) -> false;
extended_leq (_, largest) -> true;
% ok those were the easy cases.  for the rest note
% number < atom < reference < fun < port < pid < tuple < list < bit string
% so first deal with lists
extended_leq (X, { literal, Y }) when is_list (X), is_bitstring (Y) -> true;
extended_leq (X, Y) when is_list (X), is_list (Y) -> extended_leq_list (X, Y);
extended_leq (X, { tuple, _, _ }) when is_list (X) -> false;
extended_leq (X, { literal, _ }) when is_list (X) -> false;
% now deal with tuples
extended_leq ({ tuple, _, _ }, { literal, Y }) when is_bitstring (Y) -> true;
extended_leq ({ tuple, _, _ }, Y) when is_list (Y) -> true;
extended_leq ({ tuple, X, _ }, { tuple, Y, _ }) when X < Y -> true;
extended_leq ({ tuple, X, _ }, { tuple, Y, _ }) when X > Y -> false;
extended_leq ({ tuple, N, X }, { tuple, N, Y }) -> extended_leq (X, Y);
extended_leq ({ tuple, _, _ }, { literal, _ }) -> false;
% now deal with other types
extended_leq ({ literal, X }, Y) when is_bitstring (X), is_list (Y) -> false;
extended_leq ({ literal, X }, { tuple, _, _ }) when is_bitstring (X) -> false;
extended_leq ({ literal, _ }, Y) when is_list (Y) -> true;
extended_leq ({ literal, _ }, { tuple, _, _ }) -> true;
extended_leq ({ literal, X }, { literal, Y }) -> X =< Y.

-spec extended_leq_list ([ extended_term () ], [ extended_term () ]) -> bool ().

extended_leq_list ([], []) ->
  true;
extended_leq_list ([ X | Xs ], [ Y | Ys ]) ->
  case extended_leq (X, Y) of
    false -> false;
    true ->
      case extended_leq (Y, X) of
        true -> extended_leq_list (Xs, Ys);
        false -> true
      end
  end.

-spec extended_min (extended_term (), extended_term ()) -> extended_term ().

extended_min (A, B) ->
  case extended_leq (A, B) of
    true -> A;
    false -> B
  end.

-spec extended_max (extended_term (), extended_term ()) -> extended_term ().

extended_max (A, B) ->
  case extended_leq (A, B) of
    true -> B;
    false -> A
  end.

-spec fixed_width ([ any () ], integer (), any ()) -> [ any () ].

fixed_width (List, N, Elem) when length (List) < N ->
  lists:append (List, lists:duplicate (N - length (List), Elem));
fixed_width (List, _N, _Elem) ->
  List.

-spec interval_union ([ interval () ]) -> [ interval () ].

interval_union (Intervals) ->
  lists:foldl (fun merge_interval/2, [], Intervals).

-spec is_constant_expression (condition_expression ()) -> bool ().

is_constant_expression (X) when is_atom (X) -> 
  not is_match_variable (X);
is_constant_expression ({ const, _ }) -> 
  true;
is_constant_expression (X) when is_tuple (X),
                                size (X) =:= 1,
                                is_tuple (element (1, X)) ->
  is_constant_expression (tuple_to_list (element (1, X)));
is_constant_expression (X) when is_tuple (X) ->
  false;
is_constant_expression (X) when is_list (X) ->
  lists:all (fun is_constant_expression/1, X);
is_constant_expression (_) -> 
  true.

-spec is_match_variable (atom ()) -> bool ().

is_match_variable (Atom) when is_atom (Atom) ->
  List = atom_to_list (Atom),
  case { catch hd (List), catch erlang:list_to_integer (tl (List)) } of
    { $$, X } when is_integer (X), X >= 0, X =< 100000000 -> true;
    _ -> false
  end;
is_match_variable (_) ->
  false.

-spec match_condition_to_extended_term (condition_expression ()) -> extended_term ().

% this is only called with arguments such that is_constant_expression/1 is true

match_condition_to_extended_term (X) when is_tuple (X),
                                          size (X) =:= 1,
                                          is_tuple (element (1, X)) ->
  { tuple, 
    size (element (1, X)),
    match_condition_to_extended_term (tuple_to_list (element (1, X))) 
  };
match_condition_to_extended_term ({ const, X }) when is_tuple (X) ->
  { tuple, size (X), match_condition_to_extended_term (tuple_to_list (X)) };
match_condition_to_extended_term ({ const, X }) ->
  match_condition_to_extended_term (X);
match_condition_to_extended_term (X) when is_list (X) ->
  [ match_condition_to_extended_term (Y) || Y <- X ];
match_condition_to_extended_term (X) when not is_tuple (X) ->
  { literal, X }.

-spec merge_interval (interval (), [ interval () ]) -> [ interval () ].

merge_interval (none, Acc) ->
  Acc;
merge_interval (Interval, Acc) ->
  merge_interval (Interval, Acc, []).

-spec merge_interval (interval (), [ interval () ], [ interval () ]) -> [ interval () ].

merge_interval (Cur, [], Prev) ->
  [ Cur | Prev ];
merge_interval (Cur, [ H | T ], Prev) ->
  case overlap (Cur, H) of
    { true, New } -> merge_interval (New, T ++ Prev);
    false -> merge_interval (Cur, T, [ H | Prev ])
  end.

-spec new_bindings () -> bindings ().

new_bindings () -> { bindings, dict:new () }.

-spec overlap (interval (), interval ()) -> { true, interval () } | false.

overlap ({ interval, A, B }, { interval, C, D }) ->
  case (extended_leq (C, A) andalso extended_leq (A, D)) orelse
       (extended_leq (C, B) andalso extended_leq (B, D)) orelse
       (extended_leq (A, C) andalso extended_leq (C, B)) orelse
       (extended_leq (A, D) andalso extended_leq (D, B)) of 
    true ->
      { true, { interval, extended_min (A, C), extended_max (B, D) } };
    false ->
      false
  end.

-ifdef (EUNIT).

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

random_gt () ->
  case random:uniform (2) of
    1 -> '>';
    2 -> '>='
  end.

random_lt () ->
  case random:uniform (2) of
    1 -> '<';
    2 -> '=<'
  end.

random_eq () ->
  case random:uniform (2) of
    1 -> '=:=';
    2 -> '=='
  end.

-spec term_to_extended_term (any ()) -> extended_term ().

term_to_extended_term (X) when is_list (X) ->
  [ term_to_extended_term (Y) || Y <- X ];
term_to_extended_term (X) when is_tuple (X) ->
  { tuple, size (X), term_to_extended_term (tuple_to_list (X)) };
term_to_extended_term (X) ->
  { literal, X }.

-spec term_to_match_condition (any ()) -> match_condition ().

term_to_match_condition (X) when is_list (X) ->
  [ term_to_match_condition (Y) || Y <- X ];
term_to_match_condition (X) when is_tuple (X) ->
  { list_to_tuple ([ term_to_match_condition (Y) || Y <- tuple_to_list (X) ]) };
term_to_match_condition (X) ->
  X.

-spec term_interval_union ([ any () ], any (), any (), any (), any ()) -> [ interval () ].

term_interval_union (Prefix, A, B, C, D) when A =< C, C =< B;
                                              A =< D, D =< B;
                                              C =< A, A =< D;
                                              C =< B, B =< D ->
  [ 
    { interval, 
      term_to_extended_term 
        (list_to_tuple (Prefix ++ [ lists:min ([ A, C ]) ])), 
      term_to_extended_term 
        (list_to_tuple (Prefix ++ [ lists:max ([ B, D ]) ]))
    }
  ];
term_interval_union (Prefix, A, B, C, D) ->
  [ 
    { interval, 
      term_to_extended_term (list_to_tuple (Prefix ++ [ C ])),
      term_to_extended_term (list_to_tuple (Prefix ++ [ D ]))
    },
    { interval, 
      term_to_extended_term (list_to_tuple (Prefix ++ [ A ])),
      term_to_extended_term (list_to_tuple (Prefix ++ [ B ]))
    }
  ].

%-=====================================================================-
%-                                Tests                                -
%-=====================================================================-

all_test () ->
  [] = analyze ([ { foo, [], [ '$_' ] } ], 1),
  [] = analyze ([ { 7, [], [ '$_' ] } ], 1),
  [ { interval, smallest, largest } ] = analyze ([ { '_', [], [ '$_' ] } ], 1),
  [ { interval, smallest, largest } ] = analyze ([ { '_', [], [ '$_' ] },
                                                   { foo, [], [ '$_' ] } 
                                                 ],
                                                 1),
  [ { interval, smallest, largest } ] = analyze ([ { '_', [], [ '$_' ] },
                                                   { foo, [], [ '$_' ] },
                                                   { '$1', [], [ 6 ] } 
                                                 ],
                                                 2),
  [ { interval, smallest, largest } ] = analyze ([ { '_', [], [ '$_' ] },
                                                   { foo, [], [ '$_' ] },
                                                   { '$1', [], [ 6 ] },
                                                   { '$2', [], [ 9 ] }
                                                 ],
                                                 3),
  ok.

all_keys_test () ->
  [ { interval, { literal, foo }, { literal, foo } } ] = 
    analyze ([ { { foo, '_' }, [], [ '$_' ] } ], 1),
  [ { interval, smallest, largest } ] = 
    analyze ([ { { foo, '_' }, [], [ '$_' ] } ], 2),
  [ { interval, { literal, bar }, { literal, bar } },
    { interval, { literal, foo }, { literal, foo } } ] = 
    analyze ([ { { foo, '_' }, [], [ '$_' ] },
               { { bar, '$1' }, [], [ '$_' ] } ], 1),
  [ { interval, smallest, largest } ] = 
    analyze ([ { { foo, '_' }, [], [ '$_' ] },
               { { bar, '$1' }, [], [ '$_' ] } ], 2),
  ok.

prefix_tuple_test_ () ->
  { timeout,
    60,
    fun () ->
      T = ?FORALL (X,
                   fun (_) -> random_tuple () end,
                   (fun (Prefix) ->
                      PrefixList = tuple_to_list (Prefix),
                      LitPrefixList = term_to_extended_term (PrefixList),
                      BindingTuple = list_to_tuple (PrefixList ++ [ '$1' ]),
                      LowerBound = LitPrefixList ++ [ smallest ],
                      UpperBound = LitPrefixList ++ [ largest ],
                      Result = 
                        [ { interval,
                            { tuple, size (BindingTuple), LowerBound },
                            { tuple, size (BindingTuple), UpperBound }
                          } 
                        ],
                      Result = analyze ([ { { foo, BindingTuple, bar }, 
                                            [], 
                                            [ '$1' ] 
                                          }
                                        ],
                                        2),
                      true
                   end) (X)),
  
      ok = flasscheck (250, 10, T)
    end
  }.

prefix_tuple_with_guard_test_ () ->
  { timeout,
    60,
    fun () ->
      T = ?FORALL (X,
                   fun (_) -> { random_tuple (),
                                random_term (),
                                random_term (),
                                random_gt (),
                                random_lt () } 
                    end,
                   (fun ({ Prefix, R, S, Gt, Lt }) ->
                      [ L, U ] = lists:sort ([ R, S ]),
                      Lower = term_to_match_condition (L),
                      Upper = term_to_match_condition (U),
                      PrefixList = tuple_to_list (Prefix),
                      LitPrefixList = term_to_extended_term (PrefixList),
                      BindingTuple = list_to_tuple (PrefixList ++ [ '$1' ]),
                      LowerBound = LitPrefixList ++ term_to_extended_term ([ L ]),
                      UpperBound = LitPrefixList ++ term_to_extended_term ([ U ]),
                      Result = 
                        [ { interval,
                            { tuple, size (BindingTuple), LowerBound },
                            { tuple, size (BindingTuple), UpperBound }
                          }
                        ],

                      Result = analyze ([ { { foo, BindingTuple, bar }, 
                                            [ { Gt, '$1', Lower },
                                              { Lt, '$1', Upper },
                                              { Gt, 69, { const, 69 } },
                                              { Lt, 69, 69 },
                                              { 'and', true, true }
                                            ],
                                            [ '$1' ] 
                                          }
                                        ],
                                        2),
                      true
                   end) (X)),
  
      ok = flasscheck (250, 10, T)
    end
  }.

prefix_tuple_with_double_guard_test_ () ->
  { timeout,
    60,
    fun () ->
      T = ?FORALL (X,
                   fun (_) -> { random_tuple (),
                                random_term (),
                                random_term (),
                                random_term (),
                                random_term (),
                                random_gt (),
                                random_lt () } 
                   end,
                   (fun ({ Prefix, R, S, RDeux, SDeux, Gt, Lt }) ->
                      [ L, U ] = lists:sort ([ R, S ]),
                      Lower = term_to_match_condition (L),
                      Upper = term_to_match_condition (U),
                      [ LDeux, UDeux ] = lists:sort ([ RDeux, SDeux ]),
                      LowerDeux = term_to_match_condition (LDeux),
                      UpperDeux = term_to_match_condition (UDeux),
                      PrefixList = tuple_to_list (Prefix),
                      BindingTuple = list_to_tuple (PrefixList ++ [ '$1' ]),
                      Result = 
                        term_interval_union (PrefixList, L, U, LDeux, UDeux),
                      Result = 
                          analyze ([ { { foo, BindingTuple, bar }, 
                                       [ { Gt, '$1', Lower },
                                         { Lt, '$1', Upper },
                                         { Lt, '$1', Upper } 
                                       ],
                                       [ '$1' ] 
                                     },
                                     { { bar, BindingTuple, foo },
                                       [ { Lt, LowerDeux, '$1' },
                                         { Gt, UpperDeux, '$1' },
                                         { Gt, UpperDeux, '$1' } 
                                       ],
                                       [ '$1' ] 
                                     }
                                   ],
                                   2),
                      true
                   end) (X)),
  
      ok = flasscheck (250, 10, T)
    end
  }.

prefix_tuple_with_double_impossible_guard_test_ () ->
  { timeout,
    60,
    fun () ->
      T = ?FORALL (X,
                   fun (_) -> { random_tuple (),
                                random_term (),
                                random_term (),
                                random_term (),
                                random_term (),
                                random_gt (),
                                random_lt () } 
                   end,
                   (fun ({ Prefix, R, S, RDeux, SDeux, Gt, Lt }) ->
                      [ L, U ] = lists:sort ([ R, S ]),
                      Lower = term_to_match_condition (L),
                      Upper = term_to_match_condition (U),
                      [ UDeux, LDeux ] = lists:sort ([ RDeux, SDeux ]),
                      LowerDeux = term_to_match_condition (LDeux),
                      UpperDeux = term_to_match_condition (UDeux),
                      PrefixList = tuple_to_list (Prefix),
                      BindingTuple = list_to_tuple (PrefixList ++ [ '$1' ]),
                      Result = 
                        [ { interval,
                            { tuple,
                              size (BindingTuple),
                              term_to_extended_term (PrefixList ++ [ L ])
                            },
                            { tuple,
                              size (BindingTuple),
                              term_to_extended_term (PrefixList ++ [ U ])
                            }
                          } 
                        ],
                      Result = 
                          analyze ([ { { foo, BindingTuple, bar }, 
                                       [ { Gt, '$1', Lower },
                                         { Lt, '$1', Upper },
                                         { Lt, '$1', Upper }
                                       ],
                                       [ '$1' ] 
                                     },
                                     { { bar, BindingTuple, foo },
                                       [ { Gt, UpperDeux, '$1' },
                                         { Lt, LowerDeux, '$1' },
                                         { Lt, LowerDeux, '$1' }
                                       ],
                                       [ '$1' ] 
                                     }
                                   ],
                                   2),
                      true
                   end) (X)),
  
      ok = flasscheck (250, 10, T)
    end
  }.

prefix_tuple_with_double_impossible_guard_swap_test_ () ->
  { timeout,
    60,
    fun () ->
      T = ?FORALL (X,
                   fun (_) -> { random_tuple (),
                                random_term (),
                                random_term (),
                                random_term (),
                                random_term (),
                                random_gt (),
                                random_lt () } 
                   end,
                   (fun ({ Prefix, R, S, RDeux, SDeux, Gt, Lt }) ->
                      [ L, U ] = lists:sort ([ R, S ]),
                      Lower = term_to_match_condition (L),
                      Upper = term_to_match_condition (U),
                      [ UDeux, LDeux ] = lists:sort ([ RDeux, SDeux ]),
                      LowerDeux = term_to_match_condition (LDeux),
                      UpperDeux = term_to_match_condition (UDeux),
                      PrefixList = tuple_to_list (Prefix),
                      BindingTuple = list_to_tuple (PrefixList ++ [ '$1' ]),
                      Result = 
                        [ { interval,
                            { tuple,
                              size (BindingTuple),
                              term_to_extended_term (PrefixList ++ [ L ])
                            },
                            { tuple,
                              size (BindingTuple),
                              term_to_extended_term (PrefixList ++ [ U ])
                            }
                          } 
                        ],
                      Result = 
                          analyze ([ { { foo, BindingTuple, bar }, 
                                       [ { Gt, Upper, '$1' },
                                         { Lt, Lower, '$1' },
                                         { Gt, Upper, '$1' }
                                       ],
                                       [ '$1' ] 
                                     },
                                     { { bar, BindingTuple, foo },
                                       [ { Gt, '$1', LowerDeux },
                                         { Lt, '$1', UpperDeux },
                                         { Lt, '$1', UpperDeux }
                                       ],
                                       [ '$1' ] 
                                     }
                                   ],
                                   2),
                      true
                   end) (X)),
  
      ok = flasscheck (250, 10, T)
    end
  }.

prefix_tuple_with_double_gteq_guard_test_ () ->
  { timeout,
    60,
    fun () ->
      T = ?FORALL (X,
                   fun (_) -> { random_tuple (),
                                random_term (),
                                random_term (),
                                random_term (),
                                random_gt (),
                                random_lt (),
                                random_eq () } 
                   end,
                   (fun ({ Prefix, R, S, EDeux, Gt, Lt, Eq }) ->
                      [ L, U ] = lists:sort ([ R, S ]),
                      Lower = term_to_match_condition (L),
                      Upper = term_to_match_condition (U),
                      EqualDeux = term_to_match_condition (EDeux),
                      PrefixList = tuple_to_list (Prefix),
                      BindingTuple = list_to_tuple (PrefixList ++ [ '$1' ]),
                      Result = 
                        term_interval_union (PrefixList, L, U, EDeux, EDeux),
                      Result = 
                          analyze ([ { { foo, BindingTuple, bar }, 
                                       [ { Gt, '$1', Lower },
                                         { Lt, '$1', Upper } 
                                       ],
                                       [ '$1' ] 
                                     },
                                     { { bar, BindingTuple, foo },
                                       [ { Eq, EqualDeux, '$1' },
                                         { Eq, 69, 69 }
                                       ],
                                       [ '$1' ] 
                                     }
                                   ],
                                   2),
                      true
                   end) (X)),
  
      ok = flasscheck (250, 10, T)
    end
  }.

prefix_tuple_with_double_halfgt_guard_test_ () ->
  { timeout,
    60,
    fun () ->
      T = ?FORALL (X,
                   fun (_) -> { random_tuple (),
                                random_term (),
                                random_term (),
                                random_gt (),
                                random_lt () }
                   end,
                   (fun ({ Prefix, L, LDeux, Gt, Lt }) ->
                      Lower = term_to_match_condition (L),
                      LowerDeux = term_to_match_condition (LDeux),
                      PrefixList = tuple_to_list (Prefix),
                      BindingTuple = list_to_tuple (PrefixList ++ [ '$1' ]),
  
                      Result = 
                        [ { interval, 
                          term_to_extended_term (list_to_tuple (PrefixList ++ [ lists:min ([ L, LDeux ]) ])),
                          { tuple,
                            size (BindingTuple),
                            term_to_extended_term (PrefixList) ++ [ largest ]
                          }
                        } ],
  
                      Result = 
                          analyze ([ { { foo, BindingTuple, bar }, 
                                       [ { Gt, '$1', Lower } ],
                                       [ '$1' ] 
                                     },
                                     { { bar, BindingTuple, foo },
                                       [ { Lt, LowerDeux, '$1' } ],
                                       [ '$1' ] 
                                     }
                                   ],
                                   2),
                      true
                   end) (X)),
  
      ok = flasscheck (250, 10, T)
    end
  }.

prefix_tuple_with_double_halflt_guard_test_ () ->
  { timeout,
    60,
    fun () ->
      T = ?FORALL (X,
                   fun (_) -> { random_tuple (),
                                random_term (),
                                random_term (),
                                random_gt (),
                                random_lt () } 
                   end,
                   (fun ({ Prefix, U, UDeux, Gt, Lt }) ->
                      Upper = term_to_match_condition (U),
                      UpperDeux = term_to_match_condition (UDeux),
                      PrefixList = tuple_to_list (Prefix),
                      BindingTuple = list_to_tuple (PrefixList ++ [ '$1' ]),
  
                      Result = 
                        [ { interval, 
                          { tuple,
                            size (BindingTuple),
                            term_to_extended_term (PrefixList) ++ [ smallest ]
                          },
                          term_to_extended_term (list_to_tuple (PrefixList ++ [ lists:max ([ U, UDeux ]) ]))
                        } ],
  
                      Result = 
                          analyze ([ { { foo, BindingTuple, bar }, 
                                       [ { Lt, '$1', Upper } ],
                                       [ '$1' ] 
                                     },
                                     { { bar, BindingTuple, foo },
                                       [ { Gt, UpperDeux, '$1' } ],
                                       [ '$1' ] 
                                     }
                                   ],
                                   2),
                      true
                   end) (X)),
  
      ok = flasscheck (250, 10, T)
    end
  }.

prefix_tuple_with_double_eq_guard_test_ () ->
  { timeout,
    60,
    fun () ->
      T = ?FORALL (X,
                   fun (_) -> { random_tuple (),
                                random_term (),
                                random_term (),
                                random_eq () }
                   end,
                   (fun ({ Prefix, E, EDeux, Eq }) ->
                      Equal = term_to_match_condition (E),
                      EqualDeux = term_to_match_condition (EDeux),
                      PrefixList = tuple_to_list (Prefix),
                      BindingTuple = list_to_tuple (PrefixList ++ [ '$1' ]),

                      Result = 
                        [ { interval,
                            { tuple,
                              size (BindingTuple),
                              term_to_extended_term (PrefixList ++ [ EDeux ])
                            },
                            { tuple,
                              size (BindingTuple),
                              term_to_extended_term (PrefixList ++ [ EDeux ])
                            }
                          },
                          { interval,
                            { tuple,
                              size (BindingTuple),
                              term_to_extended_term (PrefixList ++ [ E ])
                            },
                            { tuple,
                              size (BindingTuple),
                              term_to_extended_term (PrefixList ++ [ E ])
                            }
                          }
                        ],

                      Result =
                          analyze ([ { { foo, BindingTuple, bar }, 
                                       [ { Eq, '$1', Equal } ],
                                       [ '$1' ] 
                                     },
                                     { { bar, BindingTuple, foo },
                                       [ { Eq, EqualDeux, '$1' } ],
                                       [ '$1' ] 
                                     }
                                   ],
                                   2),
                      true
                   end) (X)),

      ok = flasscheck (250, 10, T)
    end
  }.

-endif.
