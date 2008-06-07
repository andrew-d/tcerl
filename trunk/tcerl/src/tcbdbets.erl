%% @doc tcbdb-based erlang term storage.  Has either ordered_set or 
%% ordered_duplicate_bag semantics.

-module (tcbdbets).

% Stuff I don't really understand.

% -export ([ all/0,
%            bchunk/2,
%            is_compatible_bchunk_format/2
%            pid2name/1
%            safe_fixtable/2
%            slot/2

% Stuff that is either implemented or is reasonable to consider.

-export ([ close/1,
           delete/2,
           delete_all_objects/1,
           delete_object/2,
           first/1,
           foldl/3,
           foldr/3,
           % from_ets/2,
           info/1,
           info/2,
           % init_table/2,
           % init_table/3,
           insert/2,
           % insert_new/2
           % is_tcbdb_file/1
           last/1,
           lookup/2,
           match/1,
           match/2,
           match/3,
           match_delete/2,
           match_object/1,
           match_object/2,
           match_object/3,
           member/2,
           next/2,
           prev/2,
           open_file/1,
           open_file/2,
           % repair_continuation/2,
           select/1,
           select/2,
           select/3,
           select_delete/2,
           sync/1,
           % table/1
           % table/2
           % to_ets/2
           traverse/2 
           % update_counter/3   
         ]).

-ifdef (HAVE_EUNIT).
-include_lib ("flasscheck/include/quickcheck.hrl").
-include_lib ("eunit/include/eunit.hrl").
-endif.

% well, not perfect, but iodata is a recursively defined datastructure
% so can't be captured by a guard
-define (is_iodata (X), (is_list (X) orelse is_binary (X))).

%% @type access() = read | read_write.
%% @type type() = ordered_set | ordered_duplicate_bag. 
%% @type object () = tuple ().
%% @end

-record (tcbdbets, { tcerl, filename, keypos, type, access }).
-record (tcbdbselectcont, { ts, intervals, endbin, max, matchspec, keys }).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

%% @spec close (tcbdbets ()) -> ok | { error, Reason }
%% @doc Close a tcbdbets.  
%% @end

close (TcBdbEts = #tcbdbets{}) ->
  tcbdb:close (TcBdbEts#tcbdbets.tcerl).

%% @spec delete (tcbdbets (), any ()) -> ok | { error, Reason }
%% @doc Delete all records associated with Key.  
%% @end

delete (TcBdbEts = #tcbdbets{}, Key) ->
  tcbdb:out (TcBdbEts#tcbdbets.tcerl, 
             erlang:term_to_binary (Key, [ { minor_version, 1 } ])).

%% @spec delete_all_objects (tcbdbets ()) -> ok | { error, Reason }
%% @doc Deletes all objects from a table.
%% @end

delete_all_objects (TcBdbEts = #tcbdbets{}) ->
  tcbdb:vanish (TcBdbEts#tcbdbets.tcerl).

%% @spec delete_object (tcbdbets (), object ()) -> ok | { error, Reason }
%% @doc Deletes all instances of a given object from a table..  
%% With ordered_duplicate_bag semantics this can be used to delete some of the 
%% objects with a given key.
%% @end

delete_object (TcBdbEts = #tcbdbets{}, Object) when is_tuple (Object) ->
  Key = element (TcBdbEts#tcbdbets.keypos, Object),
  tcbdb:out_exact (TcBdbEts#tcbdbets.tcerl,
                   erlang:term_to_binary (Key, [ { minor_version, 1 } ]),
                   erlang:term_to_binary (Object, [ { minor_version, 1 } ])).


%% @spec first (tcbdbets ()) -> Key | '$end_of_table'
%% @doc Returns the first key stored in the table according to erlang
%% term order, or the atom '$end_of_table' if the table is empty.
%% @end

first (TcBdbEts = #tcbdbets{}) ->
  case tcbdb:first (TcBdbEts#tcbdbets.tcerl) of
    [] -> '$end_of_table';      % in-band signaling :(
                                % however we uphold the tradition to 
                                % match dets as much as possible
    [ Key ] -> erlang:binary_to_term (Key)
  end.

%% @spec foldl (function(), Acc0::acc (), tcbdbets ()) -> Acc1::acc () | { error, Reason }
%%   where
%%      function () = (any (), acc ()) -> acc ()
%%      acc () = any ()
%% @doc Calls Function on successive elements of the
%% table together with an extra argument AccIn. The
%% order in which the elements of the table is erlang term order of key,
%% and unspecified for objects with the same key (in ordered_duplicate_bag).
%% Function must return a new accumulator which
%% is passed to the next call. Acc0 is returned if the table
%% is empty.

foldl (Function, Acc0, TcBdbEts = #tcbdbets{}) when is_function (Function, 2) ->
  foldl (Function, Acc0, tcbdb:first (TcBdbEts#tcbdbets.tcerl), TcBdbEts).

%% @spec foldr (function(), Acc0::acc (), tcbdbets ()) -> Acc1::acc () | { error, Reason }
%%   where
%%      function () = (any (), acc ()) -> acc ()
%%      acc () = any ()
%% @doc Calls Function on successive elements of the
%% table together with an extra argument AccIn. The
%% order in which the elements of the table is reverse erlang term order 
%% of key, and unspecified for objects with the same key (in ordered_duplicate_bag).
%% Function must return a new accumulator which
%% is passed to the next call. Acc0 is returned if the table
%% is empty.

foldr (Function, Acc0, TcBdbEts = #tcbdbets{}) when is_function (Function, 2) ->
  foldr (Function, Acc0, tcbdb:last (TcBdbEts#tcbdbets.tcerl), TcBdbEts).

%% @spec info (tcbdbets ()) -> [ info () ] | undefined
%% where
%%   info () = { file_size, integer () } |
%%             { filename, string () } |
%%             { keypos, integer () } |
%%             { size, integer () } |
%%             { type, type () }
%% @doc Returns information about the table as a list of { Item, Value }
%% tuples.
%% @end

info (TcBdbEts = #tcbdbets{}) ->
  case tcbdb:info (TcBdbEts#tcbdbets.tcerl) of
    List when is_list (List) ->
      FileSize = get_value (file_size, List),
      NoObjects = get_value (no_objects, List),
      [ { file_size, FileSize },
        { filename, TcBdbEts#tcbdbets.filename },
        { keypos, TcBdbEts#tcbdbets.keypos },
        { size, NoObjects },
        { type, TcBdbEts#tcbdbets.type } ];
    { error, _Reason } -> 
      undefined
  end.

%% @spec info (tcbdbets (), what ()) -> Value | undefined
%% where
%%   what () = access |
%%             filename |
%%             file_size |
%%             keypos |
%%             memory |
%%             no_objects |
%%             size |
%%             type
%% @doc Returns the information associated with Item for the table.
%% @end

info (TcBdbEts = #tcbdbets{}, Item) ->
  case Item of
    memory -> info (TcBdbEts, file_size);
    no_objects -> info (TcBdbEts, size);
    X when X =:= size orelse X =:= file_size ->
      case info (TcBdbEts) of
        List when is_list (List) -> get_value (X, List);
        undefined -> undefined
      end;
    filename -> TcBdbEts#tcbdbets.filename;     % hmmm ... parse transform (?)
    keypos -> TcBdbEts#tcbdbets.keypos;
    type -> TcBdbEts#tcbdbets.type;
    access -> TcBdbEts#tcbdbets.access;
    _ -> undefined
  end.

%% @spec insert (tcbdbets (), objects ()) -> ok | { error, Reason }
%% where
%%   objects () = object () | [ object () ]
%% @doc Inserts one or more objects into the table.  If there already exists 
%% an object with a key comparing equal to the key of some of the given objects 
%% and the table type is ordered_set, the old object will be replaced.
%% @end

insert (TcBdbEts = #tcbdbets{}, Object) when is_tuple (Object) ->
  insert (TcBdbEts, [ Object ]);
insert (_TcBdbEts, []) ->
  ok;
insert (TcBdbEts = #tcbdbets{}, [ H | T ]) ->
  Key = element (TcBdbEts#tcbdbets.keypos, H),
  KeyBin = erlang:term_to_binary (Key, [ { minor_version, 1 } ]),
  ValueBin = erlang:term_to_binary (H, [ { minor_version, 1 } ]),

  case TcBdbEts#tcbdbets.type of
    ordered_set ->
      case tcbdb:put (TcBdbEts#tcbdbets.tcerl, KeyBin, ValueBin) of
        ok ->
          insert (TcBdbEts, T);
        R = { error, _Reason } ->
          R
      end;
    ordered_duplicate_bag ->
      case tcbdb:put_dup (TcBdbEts#tcbdbets.tcerl, KeyBin, ValueBin) of
        ok ->
          insert (TcBdbEts, T);
        R = { error, _Reason } ->
          R
      end
  end.

%% @spec last (tcbdbets ()) -> Key | '$end_of_table'
%% @doc Returns the last key stored in the table according to erlang
%% term order, or the atom '$end_of_table' if the table is empty.
%% @end

last (TcBdbEts = #tcbdbets{}) ->
  case tcbdb:last (TcBdbEts#tcbdbets.tcerl) of
    [] -> '$end_of_table';      % in-band signaling :(
                                % however we uphold the tradition to 
                                % match dets as much as possible
    [ Key ] -> erlang:binary_to_term (Key)
  end.

%% @spec lookup (tcbdbets (), any ()) -> [ Object ] | { error, Reason }
%% @doc Returns a list of all objects with the key Key stored in the table.
%% If the table is of type ordered_set, the function returns either the 
%% empty list or a list with one object, as there cannot be more than 
%% one object with a given key.
%% The order of objects returned is unspecified.

lookup (TcBdbEts = #tcbdbets{}, Key) ->
  case tcbdb:get (TcBdbEts#tcbdbets.tcerl, 
                  erlang:term_to_binary (Key, [ { minor_version, 1 } ])) of
    List when is_list (List) ->
      [ erlang:binary_to_term (L) || L <- List ];
    R = { error, _Reason } ->
      R
  end.

%% @spec match (match_continuation ()) -> { [ match () ], match_continuation () } | '$end_of_table' | { error, Reason }
%% @doc Matches some objects stored in a table and returns a 
%% list of the bindings that match a given pattern in the same
%% order as first/next traversal. The table, the pattern, and the 
%% number of objects that are matched are all defined by Continuation, 
%% which has been returned by a prior call to match/1, match/2, or match/3.
%% When all objects of the table have been matched, 
%% '$end_of_table' is returned. 

match (Continuation) ->
  select (Continuation).

%% @spec match (tcbdbets (), pattern ()) -> [ match () ] | { error, Reason }
%% @doc Like match/3, but matches all objects.
%% @end

match (TcBdbEts, Pattern) ->
  case match (TcBdbEts, Pattern, -1) of
    { Matches, _Continue } when is_list (Matches) -> Matches;
    '$end_of_table' -> [];
    R = { error, _Reason } -> R
  end.

%% @spec match (tcbdbets (), pattern (), integer ()) -> { [ match () ], match_continuation () } | '$end_of_table' | { error, Reason }
%% @doc 
%% Matches some or all objects of the table and returns
%% a list of the bindings that match Pattern in
%% some unspecified order. See ets(3) for a description
%% of patterns.
%% 
%% A tuple of the bindings and a continuation is returned,
%% unless the table is empty, in which case '$end_of_table'
%% is returned. The continuation is to be used when matching
%% further objects by calling match/1.
%% 
%% All objects with the same key are always
%% matched at the same time which implies that, for ordered_duplicate_bag,
%% more than N matches may sometimes be matched.
%%
%% The result is in the same order as in a first/next traversal.
%% @end

match (TcBdbEts = #tcbdbets{}, Pattern, N) when is_integer (N) orelse
                                                N =:= default ->
  select (TcBdbEts, [ { Pattern, [], [ '$$' ] } ], N).

%% @spec match_delete (tcbdbets (), pattern ()) -> N | { error, Reason }
%% @doc
%% Deletes all objects that match Pattern from the table,
%% and returns the number of deleted objects. See ets(3)
%% for a description of patterns.
%% 
%% If the keypos'th element of Pattern is bound, only the
%% objects with the right key are matched.
%% @end

match_delete (TcBdbEts = #tcbdbets{}, Pattern) ->
  select_delete (TcBdbEts, [ { Pattern, [], [ true ] } ]).

%% @spec match_object (match_object_cont ()) -> { [ object () ], match_object_cont () } | { error, Reason }
%% @doc Returns a list of some objects stored in a table that match 
%% a given pattern in first/next traversal order.
%% The table, the pattern, and the number of objects that
%% are matched are all defined by Continuation, which
%% has been returned by a prior call to match_object/1 or
%% match_object/3.
%% 
%% When all objects of the table have been matched,
%% '$end_of_table' is returned.
%% @end

match_object (Continuation) ->
  select (Continuation).

%% @spec match_object (tcbdbets (), pattern ()) -> [ object () ] | { error, Reason }
%% @doc 
%% Returns a list of all objects of the table that
%% match Pattern in some unspecified order. See ets(3) for
%% a description of patterns.
%% 
%% If the keypos'th element of Pattern is unbound, all objects
%% of the table are matched. If the keypos'th element of
%% Pattern is bound, only the objects with the right key
%% are matched.
%% @end

match_object (TcBdbEts = #tcbdbets{}, Pattern) ->
  case match_object (TcBdbEts, Pattern, -1) of
    { Objects, _Continue } when is_list (Objects) -> Objects;
    '$end_of_table' -> [];
    R = { error, _Reason } -> R
  end.

%% @spec match_object (tcbdbets (), pattern (), integer ()) -> { [ object () ], match_object_cont } | '$end_of_table' | { error, Reason }
%% @doc 
%% Matches some or all objects stored in the table and
%% returns a list of the objects that match Pattern
%% in first/next traversal order. See ets(3) for a description
%% of patterns.
%% 
%% A list of objects and a continuation is returned, unless
%% the table is empty, in which case '$end_of_table' is
%% returned. The continuation is to be used when matching
%% further objects by calling match_object/1.
%% 
%% All objects with the same key are always
%% matched at the same time which implies that, for ordered_duplicate_bag,
%% more than N objects may sometimes be matched.
%% @end

match_object (TcBdbEts = #tcbdbets{}, Pattern, Max) ->
  select (TcBdbEts, [ { Pattern, [], [ '$_' ] } ], Max).

%% @spec member (tcbdbets (), any ()) -> true | false | { error, Reason }
%% @doc Works like lookup/2, but does not return the objects. 
%% The function returns true if one or more elements of the table has the key 
%% Key, false otherwise.

member (TcBdbEts = #tcbdbets{}, Key) ->
  case tcbdb:get (TcBdbEts#tcbdbets.tcerl, 
                  erlang:term_to_binary (Key, [ { minor_version, 1 } ])) of
    [] -> false;
    List when is_list (List) -> true;
    R = { error, _Reason } -> R
  end.

%% @spec next (tcbdbets (), any ()) -> any () | '$end_of_table'
%% @doc Returns the key following Key in the table
%% according to erlang term order, or '$end_of_table' if there is
%% no next key.
%% 
%% Should an error occur, the process is exited with an error
%% tuple {error, Reason}.
%% 
%% Use first/1 to find the first key in the table.

next (TcBdbEts = #tcbdbets{}, Key) ->
  case tcbdb:next (TcBdbEts#tcbdbets.tcerl, 
                   erlang:term_to_binary (Key, [ { minor_version, 1 } ])) of
    [] -> '$end_of_table';
    [ Next ] -> erlang:binary_to_term (Next);
    R = { error, _Reason } -> exit (R)  % don't ask me why, i'm just trying to
                                        % match dets as closely as possible
  end.

%% @spec prev (tcbdbets (), any ()) -> any () | '$end_of_table'
%% @doc Returns the key preceeding Key in the table
%% according to erlang term order, or '$end_of_table' if there is
%% no prev key.
%% 
%% Should an error occur, the process is exited with an error
%% tuple {error, Reason}.
%% 
%% Use last/1 to find the last key in the table.

prev (TcBdbEts = #tcbdbets{}, Key) ->
  case tcbdb:prev (TcBdbEts#tcbdbets.tcerl, 
                   erlang:term_to_binary (Key, [ { minor_version, 1 } ])) of
    [] -> '$end_of_table';
    [ Next ] -> erlang:binary_to_term (Next);
    R = { error, _Reason } -> exit (R)  % don't ask me why, i'm just trying to
                                        % match dets as closely as possible
  end.

%% @spec open_file (iodata ()) -> { ok, tcbdbets () } | { error, Reason }
%% @equiv open_file (Filename, [])
%% @end

open_file (Filename) ->
  open_file (Filename, []).

%% @spec open_file (iodata (), [ arg () ]) -> { ok, tcbdbets () } | { error, Reason }
%% where
%%   arg () = { access, access () } | { file, iodata () } | 
%%            { keypos, integer () } | { type, type () } |
%%            { leaf_members, integer () } |
%%            { non_leaf_members, integer () } |
%%            { bucket_array_size, integer () } |
%%            { record_alignment, integer () } |
%%            { free_block_pool, integer () } |
%%            { leaf_node_cache, integer () } |
%%            { nonleaf_node_cache, integer () } |
%%            uncompressed | deflate | tcbs 
%% @doc Opens a table. An empty table is created if no file exists.
%% 
%% The Args argument is a list of { Key, Val } tuples where
%% the following values are recognized:
%%    * { access, access () }: either read or read_write; default read_write
%%    * { file, iodata () }: this can override the filename specified as the first argument; default Filename
%%    * { keypos, integer () }, the position of the element of each object to be used as key. The default value is 1. The ability to explicitly state the key position is most convenient when we want to store Erlang records in which the first position of the record is the name of the record type.
%%    * { type, type () }, the type of the table (ordered_set or ordered_duplicate_bag). The default value is ordered_set.
%%    * Other tuples indicated above: same interpretation as tcbdb:open/2.  NB: The tcbdb:open/2 options [ term_store, large, nolock ] are always present.
%% @end

open_file (Filename, Args) ->
  WithDefaults = set_defaults (Filename, Args),
  Access = get_value (access, WithDefaults),
  File = get_value (file, WithDefaults),
  KeyPos = get_value (keypos, WithDefaults),
  Type = get_value (type, WithDefaults),

  Mode = case Access of 
           read -> [ reader ];
           read_write -> [ reader, writer, create ]
         end,

  case tcbdb:open (File,
                   [ term_store, large, nolock ] ++ Mode ++ WithDefaults) of
    { ok, Tcerl } ->
      { ok, #tcbdbets{ tcerl = Tcerl,
                       filename = File,
                       keypos = KeyPos,
                       type = Type,
                       access = Access } };
    R = { error, _Reason } -> 
      R
  end.

%% @spec select (select_continuation ()) -> { [ Match ], select_continuation () } | '$end_of_table' | { error, Reason }
%% @doc Applies a match specification to some objects
%% stored in a table and returns a list of the
%% results. The table, the match specification, and the
%% number of objects that are matched are all defined by
%% Continuation, which has been returned by a prior call to
%% select/1, select/2, or select/3.
%% 
%% When all objects of the table have been matched,
%% '$end_of_table' is returned.
%% @end

select (_Continuation = finished) ->
  '$end_of_table';
select (Continuation = #tcbdbselectcont{}) ->
  % TODO: Compile every time until repair_continuation is called in mnesia_ext
  Compiled = ets:match_spec_compile (Continuation#tcbdbselectcont.matchspec),
  select (Continuation#tcbdbselectcont.ts,
          Continuation#tcbdbselectcont.intervals,
          Continuation#tcbdbselectcont.endbin,
          Continuation#tcbdbselectcont.max,
          Continuation#tcbdbselectcont.matchspec,
          Compiled,
          Continuation#tcbdbselectcont.keys,
          []).

%% @spec select (tcbdbets (), matchspec ()) -> [ match () ] | { error, Reason }
%% @doc Like select/3, but matches all objects.
%% @end

select (TcBdbEts = #tcbdbets{}, MatchSpec) ->
  case select (TcBdbEts, MatchSpec, -1) of 
    { Matches, _Continue } when is_list (Matches) -> Matches;
    '$end_of_table' -> [];
    R = { error, _Reason } -> R
  end.

%% @spec select (tcbdbets (), matchspec (), integer ()) -> { [ match () ], select_continuation () } | '$end_of_table' | { error, Reason }
%% @doc Returns the results of applying the match specification
%% MatchSpec to all or some objects stored in the table
%% Name. The order of the objects is not specified. See
%% the ERTS User's Guide for a description of match
%% specifications.
%% 
%% If the keypos'th element of MatchSpec is unbound, the match
%% specification is applied to all objects of the table. If
%% the keypos'th element is bound, the match specification
%% is applied to the objects with the right key(s) only.
%% @end

select (TcBdbEts = #tcbdbets{}, MatchSpec, N) when is_integer (N) orelse 
                                                   N =:= default ->
  Intervals = analyze_matchspec (MatchSpec, TcBdbEts#tcbdbets.keypos),

  case Intervals of
    [] -> 
      '$end_of_table';
    _ ->
      { StartBin, EndBin } = hd (Intervals),
      Max = case N of default -> 10; _ -> N end,
      Compiled = ets:match_spec_compile (MatchSpec),

      case tcbdb:range (TcBdbEts#tcbdbets.tcerl,
                        StartBin,
                        true,
                        EndBin,
                        true,
                        Max) of
        Keys when is_list (Keys) ->
         select (TcBdbEts,
                 tl (Intervals),
                 EndBin,
                 Max,
                 MatchSpec,
                 Compiled,
                 Keys,
                 []);
        R = { error, _Reason } ->
          R
      end
  end.

%% @spec select_delete (tcbdbets (), matchspec ()) -> N | { error, Reason }
%% @doc Deletes each object from the table Name such that
%% applying the match specification MatchSpec to the object
%% returns the value true. See the ERTS User's Guide for a
%% description of match specifications. Returns the number
%% of deleted objects.
%% If the keypos'th element of MatchSpec is bound, the match
%% specification is applied to the objects with the right
%% key(s) only.
%% @end

% TODO: the spirit of select_delete, presumably, is to be faster
% than selecting and deleting.  to take advantage, however, we'd
% need to move the match spec implementation into the driver.

select_delete (TcBdbEts = #tcbdbets{}, MatchSpec) ->
  Intervals = analyze_matchspec (MatchSpec, TcBdbEts#tcbdbets.keypos),

  case Intervals of
    [] ->  
      0;
    _ ->
      { StartBin, EndBin } = hd (Intervals),
      Compiled = ets:match_spec_compile (MatchSpec),

      case tcbdb:range (TcBdbEts#tcbdbets.tcerl, StartBin, true, EndBin, true, 10) of
        Keys when is_list (Keys) ->
         select_delete (TcBdbEts, tl (Intervals), EndBin, Compiled, Keys, 0);
        R = { error, _Reason } ->
          R
      end
  end.

%% @spec sync (tcbdbets ()) -> ok | { error, Reason }
%% @doc Ensures that all updates made to the table are written to disk.
%% @end

sync (TcBdbEts = #tcbdbets{}) ->
  tcbdb:sync (TcBdbEts#tcbdbets.tcerl).

%% @spec traverse (tcbdbets (), traverse_func ()) -> Acc | { error, Reason }
%% @doc 
%% Applies Fun to each object stored in the table Name in some
%% unspecified order. Different actions are taken depending
%% on the return value of Fun. The following Fun return values
%% are allowed:
%% 
%% continue
%%     Continue to perform the traversal. For example,
%%     the following function can be used to print out the
%%     contents of a table:
%% 
%%     fun(X) -> io:format("~p~n", [X]), continue end.            
%% 
%% {continue, Val}
%%     Continue the traversal and accumulate Val. The
%%     following function is supplied in order to collect
%%     all objects of a table in a list:
%% 
%%     fun(X) -> {continue, X} end.            
%% 
%% {done, Value}
%%     Terminate the traversal and return [Value | Acc].
%% 
%% Any other value returned by Fun terminates the traversal
%% and is immediately returned.
%% @end

traverse (TcBdbEts = #tcbdbets{}, Function) when is_function (Function, 1) ->
  case tcbdb:first (TcBdbEts#tcbdbets.tcerl) of
    [] -> [];
    [ KeyBin ] ->
      case tcbdb:get (TcBdbEts#tcbdbets.tcerl, KeyBin) of
        ValueBins when is_list (ValueBins) ->
          traverse (Function, [], ValueBins, KeyBin, TcBdbEts);
        R = { error, _Reason } ->
          R
      end;
    R = { error, _Reason } ->
      R
  end.

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-

% analyze_matchspec: query planning, such as it is
% the goal is to find the smallest union of disjoint key intervals
% to query

analyze_matchspec (MatchSpec, KeyPos) ->
  interval_union ([ analyze_matchfun (M, KeyPos) || M <- MatchSpec ]).

%% TODO: analyze guard to constrain range of keys applicable
analyze_matchfun ({ Pattern, _Guard, _Result }, KeyPos) 
  when is_tuple (Pattern),
       size (Pattern) >= KeyPos ->
  analyze_pattern (element (KeyPos, Pattern));
analyze_matchfun ({ Pattern, _Guard, _Result }, _KeyPos) when is_atom (Pattern) ->
  case is_variable (Pattern) of
    true -> analyze_pattern (Pattern);
    false -> []
  end;
analyze_matchfun (_MatchSpec, _KeyPos) -> 
  [].

analyze_pattern (Pattern) ->
  case analyze_pattern_complete (Pattern, [ <<131>> ]) of
    { complete, RevExact } -> 
      Exact = lists:reverse (RevExact),
      { Exact, Exact };
    { partial, RevSmallest, RevLargest } -> 
      { lists:reverse (RevSmallest), lists:reverse (RevLargest) }
  end.

% analyze_pattern_complete
%
% the idea here is to walk the pattern until we encounter a variable,
% then replace the variable with ERL_SMALLEST and ERL_LARGEST and stop.
% 
% if we don't find a variable, we just end up computing 
% erlang:term_to_binary/1, except more slowly :)

% tuples ... output header and recurse on elements

analyze_pattern_complete (Pattern, Acc) when is_tuple (Pattern) ->
  Initial = 
    case size (Pattern) of
      N when N < 256 ->
        [ <<104, N:8/unsigned>> ];
      N ->
        [ <<105, N:32/big-unsigned>> ]
    end,

  analyze_pattern_complete_elements (tuple_to_list (Pattern),
                                     false,
                                     [ Initial | Acc ]);

% empty list ... terminate complete
analyze_pattern_complete ([], Acc) ->
  { complete, [ <<106>> | Acc ] };

% non-empty list ... output header and recurse on elements

analyze_pattern_complete (Pattern, Acc) when is_list (Pattern) ->
  Length = erlang:length (Pattern),
  analyze_pattern_complete_elements (Pattern,
                                     true,
                                     [ <<108:8, 
                                         Length:32/big-unsigned>> 
                                       | Acc ]);

% empty atom ... terminate complete
analyze_pattern_complete ('', Acc) ->
  { complete, [ <<100,0,0>> | Acc ] };

% wildcard variable ... terminate partial
analyze_pattern_complete ('_', Acc) ->
  { partial, [ <<0>> | Acc ], [ <<255>> | Acc ] };

% atom ... terminate; if variable, partial, else complete
% from "match specifications in erlang":
% Variables take the form '$<number>' where <number> is an integer 
% between 0 (zero) and 100000000 (1e+8)

analyze_pattern_complete (Pattern, Acc) when is_atom (Pattern) ->
  case is_variable (Pattern) of
    true ->
      { partial, [ <<0>> | Acc ], [ <<255>> | Acc ] };
    false ->
      List = atom_to_list (Pattern),
      Length = length (List),
      { complete, [ List, <<100,Length:16/big-unsigned>> | Acc ] }
  end;

% everything else is literal 
% TODO: binaries and bit strings with variables: allowed in match specs (?)
analyze_pattern_complete (Pattern, Acc) ->
  <<131, Rest/binary>> = erlang:term_to_binary (Pattern, [ { minor_version, 1 } ]),
  { complete, [ Rest | Acc ] }.

% for recursing along lists or tuples
analyze_pattern_complete_elements ([], true, Acc) ->
  { [ <<106>> | Acc ], complete };
analyze_pattern_complete_elements ([], false, Acc) ->
  { Acc, complete };
analyze_pattern_complete_elements ([ H | T ], AddTail, Acc) ->
  case analyze_pattern_complete (H, Acc) of
    { NewAcc, complete } ->
      analyze_pattern_complete_elements (T, AddTail, NewAcc);
    R ->
      R
  end.

% foldl

foldl (_Function, Acc, [], _TcBdbEts) ->
  Acc;
foldl (_Function, _Acc, R = { error, _Reason }, _TcBdbEts) ->
  R;
foldl (Function, Acc, [ KeyBin ], TcBdbEts) ->
  case tcbdb:get (TcBdbEts#tcbdbets.tcerl, KeyBin) of
    Objects when is_list (Objects) -> 
      foldl (Function,
             lists:foldl 
               (fun (X, S) -> Function (erlang:binary_to_term (X), S) end,
                Acc,
                Objects),
             tcbdb:next (TcBdbEts#tcbdbets.tcerl, KeyBin),
             TcBdbEts);
    R = { error, _Reason } ->
      R
  end.

% foldr

foldr (_Function, Acc, [], _TcBdbEts) ->
  Acc;
foldr (_Function, _Acc, R = { error, _Reason }, _TcBdbEts) ->
  R;
foldr (Function, Acc, [ KeyBin ], TcBdbEts) ->
  case tcbdb:get (TcBdbEts#tcbdbets.tcerl, KeyBin) of
    Objects when is_list (Objects) -> 
      foldr (Function,
             lists:foldl 
               (fun (X, S) -> Function (erlang:binary_to_term (X), S) end,
                Acc,
                Objects),
             tcbdb:prev (TcBdbEts#tcbdbets.tcerl, KeyBin),
             TcBdbEts);
    R = { error, _Reason } ->
      R
  end.

% generate_matches

generate_matches (Compiled, Objects, Acc) ->
     [ R
       || O <- Objects,
          R <- ets:match_spec_run ([ erlang:binary_to_term (O) ], Compiled) ] 
  ++ Acc.

% get_value

get_value (What, List) ->
  { value, { What, Value } } = lists:keysearch (What, 1, List),
  Value.

% interval_union

interval_union (Intervals) ->
  lists:foldl (fun merge_interval/2, [], Intervals).

is_variable ('_') -> true;
is_variable (Atom) when is_atom (Atom) ->
  List = atom_to_list (Atom),
  case { hd (List), catch erlang:list_to_integer (tl (List)) } of
    { $$, X } when is_integer (X), X >= 0, X =< 100000000 -> true;
    _ -> false
  end.

% max

max (A, B) when A > B -> A;
max (_, B) -> B.

% min

min (A, B) when A < B -> A;
min (_, B) -> B.

% merge_interval

merge_interval ([], Acc) ->
  Acc;
merge_interval (Interval, Acc) ->
  merge_interval (Interval, Acc, []).

merge_interval (Cur, [], Prev) ->
  [ Cur | Prev ];
merge_interval (Cur, [ H | T ], Prev) ->
  case overlap (Cur, H) of
    { true, New } -> merge_interval (New, T ++ Prev);
    false -> merge_interval (Cur, T, [ H | Prev ])
  end.

% overlap

overlap ({ A, B }, { C, D }) when (C =< A andalso A =< D);
                                  (C =< B andalso B =< D);
                                  (A =< C andalso D =< B) ->
  { true, { min (A, C), max (B, D) } };
overlap (_, _) ->
  false.

% select

select (_TcBdbEts, [], _EndBin, _Max, _MatchSpec, _Compiled, [], []) ->
  '$end_of_table';
select (_TcBdbEts, [], _EndBin, _Max, _MatchSpec, _Compiled, [], Acc) ->
  { lists:reverse (Acc), finished };
select (TcBdbEts,
        Intervals,
        EndBin,
        Max,
        MatchSpec,
        _Compiled,
        Keys,
        Acc) when Max > 0,
                  length (Acc) >= Max ->
  { lists:reverse (Acc), 
    #tcbdbselectcont{ ts = TcBdbEts,
                      intervals = Intervals,
                      endbin = EndBin,
                      max = Max,
                      matchspec = MatchSpec,
                      keys = Keys } };
select (TcBdbEts, [ { StartBin, EndBin } | T ], _, Max, MatchSpec, Compiled, [], Acc) ->
  case tcbdb:range (TcBdbEts#tcbdbets.tcerl, StartBin, true, EndBin, true, Max) of
    NewKeys when is_list (NewKeys) ->
      select (TcBdbEts,
              T,
              EndBin,
              Max,
              MatchSpec,
              Compiled,
              NewKeys,
              Acc);
    R = { error, _Reason } -> 
      R
  end;
select (TcBdbEts, Intervals, EndBin, Max, MatchSpec, Compiled, [ KeyBin ], Acc) ->
  case tcbdb:range (TcBdbEts#tcbdbets.tcerl, KeyBin, false, EndBin, true, Max) of
    NewKeys when is_list (NewKeys) ->
      case tcbdb:get (TcBdbEts#tcbdbets.tcerl, KeyBin) of
        NewVals when is_list (NewVals) ->
          select (TcBdbEts,
                  Intervals,
                  EndBin,
                  Max,
                  MatchSpec,
                  Compiled,
                  NewKeys,
                  generate_matches (Compiled, NewVals, Acc));
        R = { error, _Reason } -> 
          R
      end;
    R = { error, _Reason } -> 
      R
  end;
select (TcBdbEts, Intervals, EndBin, Max, MatchSpec, Compiled, [ KeyBin | Rest ], Acc) ->
  case tcbdb:get (TcBdbEts#tcbdbets.tcerl, KeyBin) of
    NewVals when is_list (NewVals) ->
      select (TcBdbEts,
              Intervals,
              EndBin,
              Max,
              MatchSpec,
              Compiled,
              Rest,
              generate_matches (Compiled, NewVals, Acc));
    R = { error, _Reason } -> 
      R
  end.

% select_delete

select_delete (_TcBdbEts, [], _EndBin, _Compiled, [], N) ->
  N;
select_delete (TcBdbEts, [ { StartBin, EndBin } | T ], _, Compiled, [], N) ->
  case tcbdb:range (TcBdbEts#tcbdbets.tcerl, StartBin, true, EndBin, true, 10) of
    NewKeys when is_list (NewKeys) ->
      select_delete (TcBdbEts,
                     T,
                     EndBin,
                     Compiled,
                     NewKeys,
                     N);
    R = { error, _Reason } -> 
      R
  end;
select_delete (TcBdbEts, Intervals, EndBin, Compiled, [ KeyBin ], N) ->
  case tcbdb:range (TcBdbEts#tcbdbets.tcerl, KeyBin, false, EndBin, true, 10) of
    NewKeys when is_list (NewKeys) ->
      case tcbdb:get (TcBdbEts#tcbdbets.tcerl, KeyBin) of
        NewVals when is_list (NewVals) ->
          ToDelete = 
            [ TermO
              || O <- NewVals,
                 TermO <- [ erlang:binary_to_term (O) ],
                 R <- ets:match_spec_run ([ TermO ], Compiled),
                 R =:= true ],
          case try_delete_objects (TcBdbEts, ToDelete) of
            ok ->
              select_delete (TcBdbEts,
                             Intervals,
                             EndBin,
                             Compiled,
                             NewKeys,
                             N + length (ToDelete));
            R = { error, _Reason } -> 
              R
          end;
        R = { error, _Reason } -> 
          R
      end;
    R = { error, _Reason } -> 
      R
  end;
select_delete (TcBdbEts, Intervals, EndBin, Compiled, [ KeyBin | Rest ], N) ->
  case tcbdb:get (TcBdbEts#tcbdbets.tcerl, KeyBin) of
    NewVals when is_list (NewVals) ->
      ToDelete = 
        [ TermO
          || O <- NewVals,
             TermO <- [ erlang:binary_to_term (O) ],
             R <- ets:match_spec_run ([ TermO ], Compiled),
             R =:= true ],
      case try_delete_objects (TcBdbEts, ToDelete) of
        ok ->
          select_delete (TcBdbEts,
                         Intervals,
                         EndBin,
                         Compiled,
                         Rest,
                         N + length (ToDelete));
        R = { error, _Reason } -> 
          R
      end;
    R = { error, _Reason } -> 
      R
  end.

% set_defaults

set_defaults (Filename, Options) ->
  Compression = case { lists:member (deflate, Options), 
                       lists:member (tcbs, Options) } of
                  { false, false } -> [ deflate ];
                  _ -> []
                end,

  Access = case lists:member (access, Options) of
             false -> [ { access, read_write } ];
             _ -> []
           end,

  File = case lists:member (file, Options) of
             false -> [ { file, Filename } ];
             _ -> []
         end,

  Keypos = case lists:member (keypos, Options) of
             false -> [ { keypos, 1 } ];
             _ -> []
           end,

  Type = case lists:member (type, Options) of
             false -> [ { type, ordered_set } ];
             _ -> []
           end,

  lists:reverse ([ reader, 
                   { leaf_node_cache, -1 },
                   { nonleaf_node_cache, -1 },
                   { leaf_members, -1 },
                   { non_leaf_members, - 1 },
                   { bucket_array_size, -1 },
                   { record_alignment, -1 },
                   { free_block_pool, -1 } ] ++ 
                   Type ++ Keypos ++ File ++ Access ++ Compression ++ Options).


% traverse

traverse (Function, Acc, [ HBin | T ], Last, TcBdbEts) when is_binary (Last) ->
  case Function (erlang:binary_to_term (HBin)) of
    continue ->
      traverse (Function, Acc, T, Last, TcBdbEts);
    { continue, Val } ->
      traverse (Function, [ Val | Acc ], T, Last, TcBdbEts);
    { done, Val } ->
      [ Val | Acc ];
    _Other ->
     Acc
  end;
traverse (Function, Acc, [], Last, TcBdbEts) when is_binary (Last) ->
  case tcbdb:next (TcBdbEts#tcbdbets.tcerl, Last) of
    [] -> Acc;
    [ KeyBin ] ->
      case tcbdb:get (TcBdbEts#tcbdbets.tcerl, KeyBin) of
        ValueBins when is_list (ValueBins) ->
          traverse (Function, Acc, ValueBins, KeyBin, TcBdbEts);
        R = { error, _Reason } ->
          R
      end;
    R = { error, _Reason } ->
      R
  end;
traverse (_Function, _Acc, _Objects, R = { error, _Reason }, _TcBdbEts) ->
  R.

% try_delete_objects

try_delete_objects (_TcBdbEts, []) ->
  ok;
try_delete_objects (TcBdbEts, [ H | T ]) ->
  Key = element (TcBdbEts#tcbdbets.keypos, H),
  KeyBin = erlang:term_to_binary (Key, [ { minor_version, 1 } ]),
  HBin = erlang:term_to_binary (H, [ { minor_version, 1 } ]),
  case tcbdb:out_exact (TcBdbEts#tcbdbets.tcerl, KeyBin, HBin) of
    ok ->
      try_delete_objects (TcBdbEts, T);
    R = { error, _Reason } -> 
      R
  end.

-ifdef (EUNIT).

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

close_test_ () ->
  { setup,
    fun tcerl:start/0,
    fun (_) -> tcerl:stop () end,
    fun () -> 
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      ok = tcbdbets:close (R),
      file:delete ("flass" ++ os:getpid ())
    end }.

delete_all_objects_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> 
                [ { random_term (), 
                    random_term () } || _ <- lists:seq (1, 10) ]
               end,
               (fun (Objects) ->
                  ok = tcbdbets:insert (R, Objects),
                  [ _ ] = tcbdbets:lookup (R, element (1, hd (Objects))),
                  ok = tcbdbets:delete_all_objects (R),
                  [] = tcbdbets:lookup (R, element (1, hd (Objects))),
                  ok = tcbdbets:delete_all_objects (R),
                  true
                end) (X)),

    ok = fc:flasscheck (20, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

delete_object_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  false = tcbdbets:member (R, Key),
                  ok = tcbdbets:insert (R, { Key, Value }),
                  [ { Key, Value } ] = tcbdbets:lookup (R, Key),
                  true = tcbdbets:member (R, Key),

                  ok = tcbdbets:insert (R, { Key, [ Value ] }),
                  [ { Key, Value },
                    { Key, [ Value ] } ] = tcbdbets:lookup (R, Key),
                  true = tcbdbets:member (R, Key),

                  ok = tcbdbets:insert (R, { Key, Value }),

                  [ { Key, Value },
                    { Key, [ Value ] },
                    { Key, Value } ] = tcbdbets:lookup (R, Key),
                  true = tcbdbets:member (R, Key),

                  ok = tcbdbets:delete_object (R, { Key, { Value, 0 } }),
                  [ { Key, Value },
                    { Key, [ Value ] },
                    { Key, Value } ] = tcbdbets:lookup (R, Key),
                  true = tcbdbets:member (R, Key),

                  ok = tcbdbets:delete_object (R, { Key, Value }),
                  [ { Key, [ Value ] } ] = tcbdbets:lookup (R, Key),
                  true = tcbdbets:member (R, Key),

                  ok = tcbdbets:delete_object (R, { Key, [ Value ] }),
                  [] = tcbdbets:lookup (R, Key),
                  false = tcbdbets:member (R, Key),

                  true
                end) (X)),

    ok = fc:flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid (),
                                      [ { type, ordered_duplicate_bag } ]),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

first_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  case erlang:get (minsofar) of
                    undefined -> '$end_of_table' = tcbdbets:first (R);
                    { defined, Min } -> Min = tcbdbets:first (R)
                  end,

                  ok = tcbdbets:insert (R, { Key, Value }),

                  case erlang:get (minsofar) of
                    undefined -> erlang:put (minsofar, { defined, Key });
                    { defined, TheMin } ->
                      if Key < TheMin -> erlang:put (minsofar, { defined, Key });
                         true -> ok
                      end
                  end,
                  true
                end) (X)),

    ok = fc:flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

foldl_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  EtsFoldl = ets:foldl (fun (Y, Acc) -> [ Y | Acc ] end,
                                        [],
                                        Tab),
                  TcbdbFoldl = tcbdbets:foldl (fun (Y, Acc) -> [ Y | Acc ] end,
                                               [],
                                               R),

                  EtsFoldl = TcbdbFoldl,

                  true = ets:insert (Tab, { Key, Value }),
                  ok = tcbdbets:insert (R, { Key, Value }),

                  true
                end) (X)),

    ok = fc:flasscheck (100, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

foldr_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  EtsFoldr = ets:foldr (fun (Y, Acc) -> [ Y | Acc ] end,
                                        [],
                                        Tab),
                  TcbdbFoldr = tcbdbets:foldr (fun (Y, Acc) -> [ Y | Acc ] end,
                                               [],
                                               R),

                  EtsFoldr = TcbdbFoldr,

                  true = ets:insert (Tab, { Key, Value }),
                  ok = tcbdbets:insert (R, { Key, Value }),

                  true
                end) (X)),

    ok = fc:flasscheck (100, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

info_test_ () ->
  F = fun (R) ->
    true = is_list (tcbdbets:info (R)),
    read_write = tcbdbets:info (R, access),
    true = is_list (tcbdbets:info (R, filename)),
    true = is_integer (tcbdbets:info (R, file_size)),
    true = is_integer (tcbdbets:info (R, memory)),
    0 = tcbdbets:info (R, size),
    0 = tcbdbets:info (R, no_objects),
    1 = tcbdbets:info (R, keypos),
    ordered_set = tcbdbets:info (R, type),
    ok
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

last_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  case erlang:get (maxsofar) of
                    undefined -> '$end_of_table' = tcbdbets:last (R);
                    { defined, Max } -> Max = tcbdbets:last (R)
                  end,

                  ok = tcbdbets:insert (R, { Key, Value }),

                  case erlang:get (maxsofar) of
                    undefined -> erlang:put (maxsofar, { defined, Key });
                    { defined, TheMax } ->
                      if Key > TheMax -> erlang:put (maxsofar, { defined, Key });
                         true -> ok
                      end
                  end,
                  true
                end) (X)),

    ok = fc:flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

match_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> 
                { random_term (), random_term (), random_matchhead (random_variables ()) }
               end,
               (fun ({ Key, Value, { _, Pattern } }) ->
                  TcbdbMatch = tcbdbets:match (R, Pattern),
                  EtsMatch = ets:match (Tab, Pattern),
                  TcbdbMatch = EtsMatch,

                  case length (TcbdbMatch) of
                    N when N < 2 -> ok;
                    N ->
                      { TcbdbMatchSmall, TcBdbCont } = tcbdbets:match (R, Pattern, N - 1),
                      { EtsMatchSmall, EtsCont } = ets:match (Tab, Pattern, N - 1),
                      TcbdbMatchSmall = EtsMatchSmall,

                      { TcBdbMore, TcBdbAgain } = tcbdbets:match (TcBdbCont),
                      { EtsMore, _ } = ets:match (EtsCont),

                      TcBdbMore = EtsMore,

                      '$end_of_table' = tcbdbets:match (TcBdbAgain)
                  end,

                  ok = tcbdbets:insert (R, { Key, Value }),
                  true = ets:insert (Tab, { Key, Value }),

                  true
                end) (X)),

    ok = fc:flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

match_delete_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> 
                { random_term (), random_term (), random_matchhead (random_variables ()) }
               end,
               (fun ({ Key, Value, MatchSpec }) ->
                  % in r11b-5, ets:match_delete/2 returns true, rather than the
                  % number of elements deleted, so call ets:select_delete/2

                  TcbdbMatch = tcbdbets:match_delete (R, MatchSpec),
                  EtsMatch = 
                    ets:select_delete (Tab, [ { MatchSpec, [], [ true ] } ]),
                  TcbdbMatch = EtsMatch,

                  ok = tcbdbets:insert (R, { Key, Value }),
                  true = ets:insert (Tab, { Key, Value }),

                  true
                end) (X)),

    ok = fc:flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

match_object_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> 
                { random_term (), random_term (), random_matchhead (random_variables ()) }
               end,
               (fun ({ Key, Value, { _, Pattern } }) ->
                  TcbdbMatch = tcbdbets:match_object (R, Pattern),
                  EtsMatch = ets:match_object (Tab, Pattern),
                  TcbdbMatch = EtsMatch,

                  case length (TcbdbMatch) of
                    N when N < 2 -> ok;
                    N ->
                      { TcbdbMatchSmall, TcBdbCont } = tcbdbets:match_object (R, Pattern, N - 1),
                      { EtsMatchSmall, EtsCont } = ets:match_object (Tab, Pattern, N - 1),
                      TcbdbMatchSmall = EtsMatchSmall,

                      { TcBdbMore, TcBdbAgain } = tcbdbets:match_object (TcBdbCont),
                      { EtsMore, _ } = ets:match_object (EtsCont),

                      TcBdbMore = EtsMore,

                      '$end_of_table' = tcbdbets:match_object (TcBdbAgain)
                  end,

                  ok = tcbdbets:insert (R, { Key, Value }),
                  true = ets:insert (Tab, { Key, Value }),

                  true
                end) (X)),

    ok = fc:flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

next_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  EtsNext = ets:next (Tab, Key),
                  TcbdbNext = tcbdbets:next (R, Key),
                  EtsNext = TcbdbNext,

                  true = ets:insert (Tab, { Key, Value }),
                  ok = tcbdbets:insert (R, { Key, Value }),

                  true
                end) (X)),

    ok = fc:flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

prev_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  EtsPrev = ets:prev (Tab, Key),
                  TcbdbPrev = tcbdbets:prev (R, Key),
                  EtsPrev = TcbdbPrev,

                  true = ets:insert (Tab, { Key, Value }),
                  ok = tcbdbets:insert (R, { Key, Value }),

                  true
                end) (X)),

    ok = fc:flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

roundtrip_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  ok = tcbdbets:insert (R, { Key, Value }),
                  [ { Key, Value } ] = tcbdbets:lookup (R, Key),
                  true = tcbdbets:member (R, Key),
                  ok = tcbdbets:delete (R, Key),
                  [] = tcbdbets:lookup (R, Key),
                  false = tcbdbets:member (R, Key),
                  ok = tcbdbets:delete (R, Key),
                  true
                end) (X)),

    ok = fc:flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid (), []),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
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
                  TcbdbSelect = tcbdbets:select (R, MatchSpec),
                  EtsSelect = ets:select (Tab, MatchSpec),
                  TcbdbSelect = EtsSelect,

                  case length (TcbdbSelect) of
                    N when N < 2 -> ok;
                    N ->
                      { TcbdbSelectSmall, TcBdbCont } = tcbdbets:select (R, MatchSpec, N - 1),
                      { EtsSelectSmall, EtsCont } = ets:select (Tab, MatchSpec, N - 1),
                      TcbdbSelectSmall = EtsSelectSmall,

                      { TcBdbMore, TcBdbAgain } = tcbdbets:select (TcBdbCont),
                      { EtsMore, _ } = ets:select (EtsCont),

                      TcBdbMore = EtsMore,

                      '$end_of_table' = tcbdbets:select (TcBdbAgain)
                  end,

                  ok = tcbdbets:insert (R, { Key, Value }),
                  true = ets:insert (Tab, { Key, Value }),

                  true
                end) (X)),

    ok = fc:flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

select_delete_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> 
                { random_term (), random_term (), random_matchspec () }
               end,
               (fun ({ Key, Value, MatchSpec }) ->
                  NewMS = 
                    [ { Head, Guard, [ random:uniform (2) =:= 1 ] } ||
                      { Head, Guard, _ } <- MatchSpec ],
                  TcbdbSelect = tcbdbets:select_delete (R, NewMS),
                  EtsSelect = ets:select_delete (Tab, NewMS),
                  TcbdbSelect = EtsSelect,

                  ok = tcbdbets:insert (R, { Key, Value }),
                  true = ets:insert (Tab, { Key, Value }),

                  true
                end) (X)),

    ok = fc:flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

sync_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> 
                [ { random_term (), 
                    random_term () } || _ <- lists:seq (1, 10) ]
               end,
               (fun (Objects) ->
                  ok = tcbdbets:insert (R, Objects),
                  [ _ ] = tcbdbets:lookup (R, element (1, hd (Objects))),
                  ok = tcbdbets:sync (R),
                  [ _ ] = tcbdbets:lookup (R, element (1, hd (Objects))),
                  ok = tcbdbets:delete_all_objects (R),
                  [ ] = tcbdbets:lookup (R, element (1, hd (Objects))),
                  true
                end) (X)),

    ok = fc:flasscheck (20, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

traverse_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), random_term () } end,
               (fun ({ Key, Value }) ->
                  put (result, []),

                  Traverse = 
                    traverse (R, 
                              fun (Y) ->
                                case random:uniform (6) of
                                  Z when Z =:= 1 orelse Z =:= 2 -> 
                                    continue;
                                  Z when Z =:= 3 orelse Z =:= 4 ->
                                    Extra = random:uniform (45),
                                    put (result, 
                                         [ { Y, Extra } | get (result) ]),
                                    { continue, { Y, Extra } };
                                  5 ->
                                    Extra = random:uniform (45),
                                    put (result, 
                                         [ { Y, Extra } | get (result) ]),
                                    { done, { Y, Extra } };
                                  6 ->
                                    wazzup
                                end
                              end),

                  Traverse = get (result),
                  ok = tcbdbets:insert (R, { Key, Value }),

                  true
                end) (X)),

    ok = fc:flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ("flass" ++ os:getpid ()),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.


-endif.
