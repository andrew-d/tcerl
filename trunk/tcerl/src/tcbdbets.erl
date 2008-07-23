%% @doc tcbdb-based erlang term storage.  Has either ordered_set or 
%% ordered_duplicate_bag semantics.
%% @end

% TODO: ordered_bag (requires driver help)
% more TODOs sprinkled inline

-module (tcbdbets).

% Stuff I don't really understand where it fits in right now.

% -export ([ all/0,
%            pid2name/1
%            safe_fixtable/2
%            slot/2

% Stuff that is either implemented or is reasonable to consider.

-export ([ bchunk/2,
           close/1,
           delete/2,
           delete_all_objects/1,
           delete_object/2,
           first/1,
           foldl/3,
           foldr/3,
           from_ets/2,
           is_compatible_bchunk_format/2,
           info/1,
           info/2,
           init_table/2,
           init_table/3,
           insert/2,
           insert_new/2,
           is_tcbdbets_file/1,
           unlink/1,
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
           repair_continuation/2,
           select/1,
           select/2,
           select/3,
           select_delete/2,
           sync/1,
           % table/1
           % table/2
           to_ets/2,
           traverse/2,
           update_counter/3   
         ]).

-ifdef (HAVE_EUNIT).
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
-record (tcbdbselectcont, { ts, intervals, endbin, max, matchspec, compiled, keys }).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

%% @spec bchunk (tcbdbets (), bchunk_continuation ()) -> { bchunk_continuation (), data () } | '$end_of_table' | { error, Reason }
%% @doc Returns a list of objects stored in a table. The
%% exact representation of the returned objects is not
%% public. The lists of data can be used for initializing a
%% table by giving the value bchunk to the format option of
%% the init_table/3 function. The Mnesia application uses
%% this function for copying open tables.
%% @end

% TODO: there is a chance to be more efficient here, since we don't have 
% to run everything through binary_to_term and back again.  however,
% hold off on tweaking this until we see whether mnesia_ext will use this.

bchunk (TcBdbEts, start) ->
  case select (TcBdbEts, [ { '_', [], [ '$_' ] } ], default) of
    { Data, Continuation } when is_list (Data) ->
      { Continuation, Data };
    R ->
      R
  end;
bchunk (_TcBdbEts, Continuation) ->
  case select (Continuation) of
    { Data, More } when is_list (Data) ->
      { More, Data };
    R ->
      R
  end.

%% @spec close (tcbdbets ()) -> ok | { error, Reason }
%% @doc Close a tcbdbets.  
%% @end

close (TcBdbEts = #tcbdbets{}) ->
  tcbdb:close (TcBdbEts#tcbdbets.tcerl).

%% @spec delete (tcbdbets (), any ()) -> ok | { error, Reason }
%% @doc Delete all records associated with Key.  
%% @end

delete (_TcBdbEts = #tcbdbets{ access = read }, _Key) ->
  { error, read_only };
delete (TcBdbEts = #tcbdbets{}, Key) ->
  tcbdb:out (TcBdbEts#tcbdbets.tcerl, 
             erlang:term_to_binary (Key, [ { minor_version, 1 } ])).

%% @spec delete_all_objects (tcbdbets ()) -> ok | { error, Reason }
%% @doc Deletes all objects from a table.
%% @end

delete_all_objects (_TcBdbEts = #tcbdbets{ access = read }) ->
  { error, read_only };
delete_all_objects (TcBdbEts = #tcbdbets{}) ->
  tcbdb:vanish (TcBdbEts#tcbdbets.tcerl).

%% @spec delete_object (tcbdbets (), object ()) -> ok | { error, Reason }
%% @doc Deletes all instances of a given object from a table..  
%% With bag semantics this can be used to delete some of the 
%% objects with a given key.
%% @end

delete_object (_TcBdbEts = #tcbdbets{ access = read }, _Object) ->
  { error, read_only };
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
%% and unspecified for objects with the same key (if type is not ordered_set).
%% Function must return a new accumulator which
%% is passed to the next call. Acc0 is returned if the table
%% is empty.

foldl (Function, Acc0, TcBdbEts = #tcbdbets{}) when is_function (Function, 2) ->
  foldl (Function, Acc0, tcbdb:first (TcBdbEts#tcbdbets.tcerl), TcBdbEts).

%% @spec foldr (function (), Acc0::acc (), tcbdbets ()) -> Acc1::acc () | { error, Reason }
%%   where
%%      function () = (any (), acc ()) -> acc ()
%%      acc () = any ()
%% @doc Calls Function on successive elements of the
%% table together with an extra argument AccIn. The
%% order in which the elements of the table is reverse erlang term order 
%% of key, and unspecified for objects with the same key (if type is not ordered_set).
%% Function must return a new accumulator which
%% is passed to the next call. Acc0 is returned if the table
%% is empty.

foldr (Function, Acc0, TcBdbEts = #tcbdbets{}) when is_function (Function, 2) ->
  foldr (Function, Acc0, tcbdb:last (TcBdbEts#tcbdbets.tcerl), TcBdbEts).

%% @spec from_ets (tcbdbets (), ets_table ()) -> ok | { error, Reason }
%% @doc Deletes all objects of the table and then
%% inserts all the objects of the Ets table Tab. The order
%% in which the objects are inserted is not specified. Since
%% ets:safe_fixtable/2 is called the Ets table must be public
%% or owned by the calling process.
%% TODO: perhaps the order *is* specified, if the ets table is
%% ordered_set ?
%% @end

from_ets (_TcBdbEts = #tcbdbets{ access = read }, _Tab) ->
  { error, read_only };
from_ets (TcBdbEts = #tcbdbets{}, Tab) ->
  ets:safe_fixtable (Tab, true),
  init_table (TcBdbEts, from_ets_init (start, Tab)).

%% @spec is_compatible_bchunk_format (tcbdbets (), bchunk_format ()) -> bool ()
%% @doc Returns true if it would be possible to initialize
%% the table, using init_table/3 with the option
%% { format, bchunk }, with objects read with bchunk/2 from
%% some table T such that calling info (T, bchunk_format)
%% returns Format.
%% @end

is_compatible_bchunk_format (_TcBdbEts, _Format = <<1>>) -> true;
is_compatible_bchunk_format (_TcBdbEts, _Format) -> false.

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
    filename -> TcBdbEts#tcbdbets.filename; 
    keypos -> TcBdbEts#tcbdbets.keypos;
    type -> TcBdbEts#tcbdbets.type;
    access -> TcBdbEts#tcbdbets.access;
    bchunk_format -> <<1>>;
    _ -> undefined
  end.

%% @spec init_table (tcbdbets (), initfun ()) -> ok | { error, Reason }
%% @equiv init_table (tcbdbets (), initfun (), [ { format, term } ])
%% @end

init_table (TcBdbEts, InitFun) ->
  init_table (TcBdbEts, InitFun, [ { format, term } ]).

%% @spec init_table (tcbdbets (), initfun (), [ option () ]) -> ok | { error, Reason }
%%  where
%%    option () = { format, Format }
%% @doc Replaces the existing objects of the table with
%% objects created by calling the input function InitFun,
%% see below. The reason for using this function rather than
%% calling insert/2 is that of efficiency. (TODO: does that apply here?)
%% 
%% When called with the argument read the function InitFun is
%% assumed to return end_of_input when there is no more input,
%% or { Objects, Fun }, where Objects is a list of objects
%% and Fun is a new input function. Any other value Value is
%% returned as an error { error, { init_fun, Value } }. Each input
%% function will be called exactly once, and should an error
%% occur, the last function is called with the argument close,
%% the reply of which is ignored.
%% 
%% If the type of the table is ordered_set and there is more than
%% one object with a given key, one of the objects is
%% chosen. This is not necessarily the last object with the
%% given key in the sequence of objects returned by the input
%% functions. Extra objects should be avoided, or the file
%% will be unnecessarily fragmented. This holds also for
%% duplicated objects stored in tables of type bag.
%%
%% The Options argument is a list of { Key, Val } tuples where
%% the following values are allowed:
%% 
%%    * { format, Format }. Specifies the format of the objects
%%    returned by the function InitFun. If Format is term
%%    (the default), InitFun is assumed to return a list
%%    of tuples. If Format is bchunk, InitFun is assumed
%%    to return Data as returned by bchunk/2. 
%% @end

init_table (_TcBdbEts = #tcbdbets{ access = read }, InitFun, _Options) when is_function (InitFun, 1) ->
  catch InitFun (close),
  { error, read_only };
init_table (TcBdbEts, InitFun, Options) when is_function (InitFun, 1),
                                             is_list (Options) ->
  delete_all_objects (TcBdbEts),
  case lists:keysearch (format, 1, Options) of
    { value, { format, term } } ->
      init_table_term (TcBdbEts, InitFun);
    { value, { format, bchunk } } ->
      % TODO: actual bchunk format
      init_table_term (TcBdbEts, InitFun);
    false ->
      init_table_term (TcBdbEts, InitFun)
  end.

%% @spec insert (tcbdbets (), objects ()) -> ok | { error, Reason }
%% where
%%   objects () = object () | [ object () ]
%% @doc Inserts one or more objects into the table.  If there already exists 
%% an object with a key comparing equal to the key of some of the given objects 
%% and the table type is ordered_set, the old object will be replaced.
%% @end

insert (_TcBdbEts = #tcbdbets{ access = read }, _Object) ->
  { error, read_only };
insert (TcBdbEts = #tcbdbets{}, Object) when is_tuple (Object) ->
  insert (TcBdbEts, [ Object ]);
insert (_TcBdbEts, []) ->
  ok;
insert (TcBdbEts = #tcbdbets{ keypos = KeyPos }, 
        [ H | T ]) when is_tuple (H),
                        size (H) >= KeyPos ->
  Key = element (KeyPos, H),
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

%% @spec insert_new (tcbdbets (), objects ()) -> true | false | { error, Reason }
%% @doc Inserts one or more objects into the table. If
%% there already exists some object with a key matching the
%% key of any of the given objects the table is not updated
%% and false is returned, otherwise the objects are inserted
%% and true returned.
%% @end

%% TODO: this could be made faster at the driver layer ...

insert_new (_TcBdbEts = #tcbdbets{ access = read }, _Object) ->
  { error, read_only };
insert_new (TcBdbEts, Object) when is_tuple (Object) ->
  insert_new (TcBdbEts, [ Object ]);
insert_new (TcBdbEts = #tcbdbets{ keypos = KeyPos }, 
            Objects) when is_list (Objects) ->
  case lists:any (fun (O) when is_tuple (O), size (O) >= KeyPos -> 
                    Key = element (KeyPos, O),
                    tcbdbets:member (TcBdbEts, Key)
                  end,
                  Objects) of
    false -> 
      case insert (TcBdbEts, Objects) of
        ok -> true;
        R = { error, _Reason } -> R
      end;
    true -> false
  end.

%% @spec is_tcbdbets_file (iodata ()) -> true | false | { error, Reason }
%% @doc Returns true if the file Filename is a tcbdbets store, false otherwise.
%% @end

% TODO: this only indicates it is a tcbdb file, not that it contains
% an Erlang Term Store.  is there some way to indicate/detect this?

is_tcbdbets_file (Filename) when ?is_iodata (Filename) ->
  tcbdb:is_tcbdb_file (Filename).

%% @spec unlink (tcbdbets ()) -> true
%% @doc Unlinks the port underlying the term store from the current process.
%% @end

unlink (TcBdbEts = #tcbdbets{}) ->
  tcbdb:unlink (TcBdbEts#tcbdbets.tcerl).

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
%% matched at the same time which implies that, unless type is ordered_set,
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

match_delete (_TcBdbEts = #tcbdbets{ access = read }, _Pattern) ->
  { error, read_only };
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
%% matched at the same time which implies that, unless type is ordered_set,
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

%% @spec open_file ([ arg () ]) -> { ok, tcbdbets () } | { error, Reason }
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
%%    * { file, iodata () }: required
%%    * { keypos, integer () }, the position of the element of each object to be used as key. The default value is 1. The ability to explicitly state the key position is most convenient when we want to store Erlang records in which the first position of the record is the name of the record type.
%%    * { type, type () }, the type of the table (ordered_set or ordered_duplicate_bag). The default value is ordered_set.
%%    * Other tuples indicated above: same interpretation as tcbdb:open/2.  NB: The tcbdb:open/2 options [ term_store, large, nolock ] are always present.
%% @end

open_file (Args) ->
  WithDefaults = set_defaults (Args),
  case lists:keysearch (file, 1, WithDefaults) of
    false -> { error, file_not_specified };
    { value, { file, File } } ->
      Access = get_value (access, WithDefaults),
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
      end
  end.

%% @spec repair_continuation (select_continuation (), matchspec ()) -> select_continuation ()
%% @doc This function can be used to restore an opaque continuation
%% returned by select/3 or select/1 if the continuation has
%% passed through external term format (been sent between
%% nodes or stored on disk).
%% 
%% The reason for this function is that continuation terms
%% contain compiled match specifications and therefore will
%% be invalidated if converted to external term format. Given
%% that the original match specification is kept intact, the
%% continuation can be restored, meaning it can once again
%% be used in subsequent select/1 calls even though it has
%% been stored on disk or on another node.
%% 
%% See also ets(3) for further explanations and examples.
%% @end

repair_continuation (finished, _MatchSpec) -> 
  finished;
repair_continuation (Continuation = #tcbdbselectcont{}, MatchSpec) ->
  Continuation#tcbdbselectcont{ compiled = ets:match_spec_compile (MatchSpec) }.

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
%% MatchSpec to all or some objects stored in the table.
%% The order of the objects is not specified. See
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
%% @doc Deletes each object from the table such that
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

select_delete (_TcBdbEts = #tcbdbets{ access = read }, _MatchSpec) ->
  { error, read_only };
select_delete (TcBdbEts = #tcbdbets{}, [ { '_', [], [ true ] } ]) ->
  % efficient special case
  delete_all_objects (TcBdbEts);
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

%% @spec to_ets (tcbdbets (), ets_table ()) -> ets_table () | { error, Reason }
%% @doc Inserts the objects of the table into
%% the Ets table Tab. Elements are inserted in erlang term order of
%% their keys, and is unspecified for records with the same key 
%% (when type is not ordered_set).  The existing objects of the
%% Ets table are kept unless overwritten.
%% @end

to_ets (TcBdbEts = #tcbdbets{}, Tab) ->
  case traverse (TcBdbEts, fun (X) -> ets:insert (Tab, X), continue end) of
    [] -> Tab;
    R = { error, _Reason } -> R
  end.

%% @spec traverse (tcbdbets (), traverse_func ()) -> Acc | { error, Reason }
%% @doc 
%% Applies Fun to each object stored in the table in some
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

%% @spec update_counter (tcbdbets (), any (), increment ()) -> integer ()
%% where
%%   increment () = { position (), delta () } | delta ()
%%   position () = integer ()
%%   delta () = integer ()
%% @doc Updates the object with key Key stored in the
%% table of type set by adding Incr to the element at
%% the Pos:th position. The new counter value is returned. If
%% no position is specified, the element directly following
%% the key is updated.
%%
%% TcBdbEts must be of type ordered_set, the key must exist,
%% and the position being updated cannot be the key position.
%% Errors are indicated via exceptions with this routine.
%%
%% Another caveat: bignums currently cannot be used as counter
%% fields, and if there is underflow or overflow the value will be 
%% silently incorrect.
%%
%% Exits (throws an exception) if preconditions are violated.
%% @end

update_counter (_TcBdbEts = #tcbdbets{ access = read }, _Key, _Increment) ->
  { error, read_only };
update_counter (TcBdbEts = #tcbdbets{}, 
                Key,
                Increment) when is_integer (Increment) ->
  update_counter (TcBdbEts, Key, { TcBdbEts#tcbdbets.keypos + 1, Increment });
update_counter (TcBdbEts = #tcbdbets{ keypos = KeyPos, type = ordered_set },
                Key,
                { Pos, Incr }) when is_integer (Pos),
                                    is_integer (Incr),
                                    Pos =/= KeyPos ->
  KeyBin = erlang:term_to_binary (Key, [ { minor_version, 1 } ]),
  tcbdb:update_counter (TcBdbEts#tcbdbets.tcerl, KeyBin, Pos, Incr).

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
  { complete, [ <<106>> | Acc ] };
analyze_pattern_complete_elements ([], false, Acc) ->
  { complete, Acc };
analyze_pattern_complete_elements ([ H | T ], AddTail, Acc) ->
  case analyze_pattern_complete (H, Acc) of
    { complete, NewAcc } ->
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

% from_ets_init

from_ets_init (start, Tab) ->
  from_ets_init_finish (ets:select (Tab, [ { '_', [], [ '$_' ] } ], 100), Tab);
from_ets_init (Continuation, Tab) ->
  from_ets_init_finish (ets:select (Continuation), Tab).

from_ets_init_finish ('$end_of_table', Tab) ->
  fun (read) -> end_of_input;
      (close) -> ets:safe_fixtable (Tab, false)
  end;
from_ets_init_finish ({ Matches, Continuation }, Tab) ->
  fun (read) -> { Matches, from_ets_init (Continuation, Tab) };
      (close) -> ets:safe_fixtable (Tab, false)
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

% init_table_term

init_table_term (TcBdbEts, InitFun) ->
  try InitFun (read) of
    end_of_input -> catch InitFun (close), ok;
    { Objects, Fun } -> case insert (TcBdbEts, Objects) of
                          ok -> init_table_term (TcBdbEts, Fun);
                          R = { error, _Reason } -> catch InitFun (close), R
                        end;
    Value -> 
      catch InitFun (close), 
      { error, { init_fun, Value } }
  catch
    X : Y ->
      catch InitFun (close),
      { error, { init_fun_exception, { X, Y } } }
  end.

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

set_defaults (Options) ->
  Compression = case { lists:member (deflate, Options), 
                       lists:member (tcbs, Options) } of
                  { false, false } -> [ deflate ];
                  _ -> []
                end,

  Access = case lists:member (access, Options) of
             false -> [ { access, read_write } ];
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

  Options ++
  Compression ++
  Access ++
  Keypos ++
  Type ++
  [ reader, 
    { leaf_node_cache, -1 },
    { nonleaf_node_cache, -1 },
    { leaf_members, -1 },
    { non_leaf_members, - 1 },
    { bucket_array_size, -1 },
    { record_alignment, -1 },
    { free_block_pool, -1 } ].

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

%-=====================================================================-
%-                                Tests                                -
%-=====================================================================-

close_test_ () ->
  { setup,
    fun tcerl:start/0,
    fun (_) -> tcerl:stop () end,
    fun () -> 
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () } ]),
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

    ok = flasscheck (20, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid ()} ]),
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

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () },
                                        { type, ordered_duplicate_bag } ]),
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

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
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

    ok = flasscheck (100, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
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

    ok = flasscheck (100, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

from_ets_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> [ { random_term (), random_term () } || 
                            _ <- lists:seq (1, random:uniform (20)) ] end,
               (fun (Objects) ->
                  true = ets:delete_all_objects (Tab),
                  true = ets:insert (Tab, Objects),
                  ok = tcbdbets:from_ets (R, Tab),

                  RFoldl = 
                    tcbdbets:foldl (fun (Y, Acc) -> [ Y | Acc ] end, [], R),
                  EtsFoldl = 
                    ets:foldl (fun (Y, Acc) -> [ Y | Acc ] end, [], Tab),

                  RFoldl = EtsFoldl,
                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
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
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

init_table_test_ () ->
  Copy = fun (Tab, Cont, F) -> 
           case bchunk (Tab, Cont) of
             '$end_of_table' -> 
                fun (read) -> end_of_input; (close) -> ok end;
             { More, Data } when More =/= error, is_list (Data) -> 
                fun (read) -> { Data, F (Tab, More, F) }; (close) -> ok end;
             R = { error, _Reason } -> 
               fun (read) -> R; (close) -> R end
           end
         end,
  
  F = fun ({ R, D }) ->
    T = 
      ?FORALL (X,
               fun (_) -> [ { random_term (), random_term () } || 
                            _ <- lists:seq (1, random:uniform (20)) ] end,
               (fun (Objects) ->
                  ok = tcbdbets:delete_all_objects (R),
                  ok = tcbdbets:insert (R, Objects),
                  ok = tcbdbets:init_table (D, Copy (R, start, Copy), [ { format, bchunk } ]),

                  RFoldl = 
                    tcbdbets:foldl (fun (Y, Acc) -> [ Y | Acc ] end, [], R),
                  DFoldl = 
                    tcbdbets:foldl (fun (Y, Acc) -> [ Y | Acc ] end, [], D),

                  RFoldl = DFoldl,
                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      file:delete ("turg" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
      { ok, D } = tcbdbets:open_file ([ { file, "turg" ++ os:getpid () }]),
      { R, D }
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ()),
      file:delete ("turg" ++ os:getpid ())
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

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
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

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
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

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
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

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
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

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
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

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
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
                  false = tcbdbets:insert_new (R, { Key, Value }),
                  [ { Key, Value } ] = tcbdbets:lookup (R, Key),
                  true = tcbdbets:member (R, Key),
                  ok = tcbdbets:delete (R, Key),
                  true = tcbdbets:insert_new (R, { Key, Value }),
                  [ { Key, Value } ] = tcbdbets:lookup (R, Key),
                  ok = tcbdbets:delete (R, Key),
                  [] = tcbdbets:lookup (R, Key),
                  false = tcbdbets:member (R, Key),
                  ok = tcbdbets:delete (R, Key),
                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
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

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
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

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
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

    ok = flasscheck (20, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

to_ets_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> [ { random_term (), random_term () } || 
                            _ <- lists:seq (1, random:uniform (20)) ] end,
               (fun (Objects) ->
                  ok = tcbdbets:insert (R, Objects),
                  Tab = tcbdbets:to_ets (R, Tab),

                  RFoldl = 
                    tcbdbets:foldl (fun (Y, Acc) -> [ Y | Acc ] end, [], R),
                  EtsFoldl = 
                    ets:foldl (fun (Y, Acc) -> [ Y | Acc ] end, [], Tab),

                  RFoldl = EtsFoldl,
                  true
                end) (X)),

    ok = flasscheck (100, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
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

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

update_counter_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_term (), 
                            case random:uniform (5) of
                              1 -> -1;
                              2 -> 255;
                              3 -> random_integer ();
                              4 -> -1 * random_integer ();
                              5 -> 0
                            end,
                            case random:uniform (2) of
                              1 -> random_integer ();
                              2 -> -1 * random_integer ()
                            end,
                            random_term () } end,
               (fun ({ Key, Count, Incr, Value }) ->
                  ok = tcbdbets:insert (R, { Key, Count, Value }),
                  true = ets:insert (Tab, { Key, Count, Value }),

                  TcbdbUp = tcbdbets:update_counter (R, Key, Incr),
                  EtsUp = ets:update_counter (Tab, Key, Incr),
                  TcbdbUp = EtsUp,

                  true = (tcbdbets:lookup (R, Key) =:= ets:lookup (Tab, Key)),

                  TcbdbUpDeux = tcbdbets:update_counter (R, Key, Incr),
                  EtsUpDeux = ets:update_counter (Tab, Key, Incr),

                  TcbdbUpDeux = EtsUpDeux,
                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      file:delete ("flass" ++ os:getpid ()),
      { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () }]),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    fun (X) -> { timeout, 60, fun () -> F (X) end } end
  }.

select_bug_test () ->
  tcerl:start (),
  file:delete ("flass" ++ os:getpid ()),
  { ok, R } = tcbdbets:open_file ([ { file, "flass" ++ os:getpid () },
                                    { keypos, 2 } ]),
  try
    Record = { userdatum, { "cl", 1, <<0,4,82,4,122,47,243,186>> }, "sx" },
    ok = tcbdbets:insert (R, Record),
    GoodMs = [ { { userdatum, { "cl", '_', '_' }, '_' }, [], [ '$_' ] } ],
    BadMs = [ { { userdatum, { "cl", 1, '_' }, '_' }, [], [ '$_' ] } ],

    [ Record ] = tcbdbets:select (R, GoodMs),
    [ Record ] = tcbdbets:select (R, BadMs),
    true
  after
    tcerl:stop (),
    file:delete ("flass" ++ os:getpid ())
  end.

-endif.
