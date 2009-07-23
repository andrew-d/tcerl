%% @doc Analyze match specifications.

-module (tcbdbmsutil).
-export ([ analyze/2 ]).

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
-type term_construct () :: {{}} | {{ any () }} | non_composite_term () | match_constant ().
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

%-type extended_term () :: smallest | largest | { literal, non_composite_term () } | [ extended_term () ] | { tuple, [ extended_term () ] }.
-type extended_term () :: smallest | largest | { literal, non_composite_term () } | [ any () ] | { tuple, [ any () ] }.

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
    { interval, Lower, Upper } -> 
      { interval, tupliz (Lower), tupliz (Upper) };
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

-spec analyze_op_arguments (condition_expression (), condition_expression ()) -> { match_variable (), extended_term () } | false.

analyze_op_arguments (A, B) ->
  case { is_match_variable (A), is_constant_expression (B) } of
    { true, true } ->
      { A, term_to_extended_term (B) };
    _ ->
      case { is_match_variable (B), is_constant_expression (A) } of
        { true, true } ->
          { B, term_to_extended_term (A) };
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
    false ->
      Bindings
  end;
analyze_match_conditions ({ Op, A, B }, Bindings) when Op =:= '<';
                                                       Op =:= '=<' ->
  case analyze_op_arguments (A, B) of
    { Variable, ExtendedTerm } ->
      adjust_upper_bound (Variable, ExtendedTerm, Bindings);
    false ->
      Bindings
  end;
analyze_match_conditions ({ '=:=', A, B }, Bindings) ->
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
extended_leq (X, { tuple, _ }) when is_list (X) -> false;
extended_leq (X, { literal, _ }) when is_list (X) -> false;
% now deal with tuples
extended_leq ({ tuple, _ }, { literal, Y }) when is_bitstring (Y) -> true;
extended_leq ({ tuple, _ }, Y) when is_list (Y) -> true;
extended_leq ({ tuple, X }, { tuple, Y }) -> extended_leq_list (X, Y);
extended_leq ({ tuple, _ }, { literal, _ }) -> false;
% now deal with other types
extended_leq ({ literal, X }, Y) when is_bitstring (X), is_list (Y) -> false;
extended_leq ({ literal, X }, { tuple, _ }) when is_bitstring (X) -> false;
extended_leq ({ literal, _ }, Y) when is_list (Y) -> true;
extended_leq ({ literal, _ }, { tuple, _ }) -> true;
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
       (extended_leq (A, C) andalso extended_leq (D, B)) of
    true ->
      { true, { interval, extended_min (A, C), extended_max (B, D) } };
    false ->
      false
  end;
overlap (_, _) ->
  false.

-spec term_to_extended_term (any ()) -> extended_term ().

term_to_extended_term (X) when is_list (X) -> 
  [ term_to_extended_term (Y) || Y <- X ];
term_to_extended_term (X) when is_tuple (X) -> 
  { tuple, term_to_extended_term (tuple_to_list (X)) };
term_to_extended_term (X) ->
  { literal, X }.

-spec tupliz (extended_term ()) -> extended_term ().

tupliz (X) when is_list (X) -> { tuple, X };
tupliz (X) -> X.
