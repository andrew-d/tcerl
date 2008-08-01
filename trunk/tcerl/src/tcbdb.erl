%% @doc Low level interface to tcbdb.  This exposes most of the tcbdb 
%% interface directly operating on binaries.
%% @end

-module (tcbdb).
-export ([ close/1,
           first/1,
           get/2,
           info/1, 
           is_tcbdb_file/1,
           last/1,
           open/2,
           next/2,
           out/2,
           out_async/2,
           out_exact/3,
           out_exact_async/3,
           prefix/3,
           prev/2,
           put/3,
           put_async/3,
           put_dup/3,
           put_dup_async/3,
           range/6,
           range_lb/4,
           range_ub/4,
           sync/1,
           unlink/1,
           update_counter/4,
           vanish/1
         ]).

-ifdef (HAVE_EUNIT).
-include_lib ("eunit/include/eunit.hrl").
-endif.

-define (BDB_TUNE, 0).
-define (BDB_OPEN, 1).
-define (BDB_CLOSE, 2).
-define (BDB_PUT, 3).
-define (BDB_PUT_DUP, 4).
-define (BDB_OUT, 5).
-define (BDB_GET, 6).
-define (BDB_FIRST, 7).
-define (BDB_NEXT, 8).
-define (BDB_LAST, 9).
-define (BDB_PREV, 10).
-define (BDB_RANGE, 11).
-define (BDB_FWM, 12).
-define (BDB_VANISH, 13).
-define (BDB_OUT_EXACT, 14).
-define (BDB_INFO, 15).
-define (BDB_SYNC, 16).
-define (BDB_UPDATE_COUNTER, 17).
-define (BDB_OUT_ASYNC, 18).
-define (BDB_PUT_ASYNC, 19).
-define (BDB_OUT_EXACT_ASYNC, 20).
-define (BDB_PUT_DUP_ASYNC, 21).

% well, not perfect, but iodata is a recursively defined datastructure
% so can't be captured by a guard
-define (is_iodata (X), (is_list (X) orelse is_binary (X))).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

%% @spec close (Tcerl::tcerl ()) -> ok | { error, Reason }
%% @doc Close a tcerl database.  Corresponds to tcbdbclose ().
%% @end

close ({ tcerl, Port }) when is_port (Port) ->
  true = port_command (Port, <<?BDB_CLOSE:8>>),
  receive
    { Port, { data, <<"ok">> } } -> 
      port_close (Port),
      ok;
    { Port, { data, Other } } ->
      port_close (Port),
      { error, Other }
  end.

%% @spec first (Tcerl::tcerl ()) -> [ binary () ] | { error, Reason }
%% @doc Get the first key in the database.
%% Empty list is returned if there are no keys.
%% Corresponds to tcbdbcurfirst ().
%% @end

first ({ tcerl, Port }) when is_port (Port) ->
  true = port_command (Port, <<?BDB_FIRST:8>>),

  receive
    { Port, { data, List } } when is_list (List) -> List;
    { Port, { data, Other } } -> { error, Other }
  end.

%% @spec get (Tcerl::tcerl (), iodata ()) -> [ binary () ] | { error, Reason }
%% @doc Get (possibly multiple) values associated with a Key.  Key
%% is flattened to binary before lookup. Corresponds to tcbdbget4 ().
%% @end

get ({ tcerl, Port }, Key) when is_port (Port), ?is_iodata (Key) ->
  KeySize = erlang:iolist_size (Key),
  true = port_command (Port, [ <<?BDB_GET:8>>, 
                               <<KeySize:64/native-unsigned>>,
                               Key ]),
  receive
    { Port, { data, List } } when is_list (List) -> List;
    { Port, { data, Other } } -> { error, Other }
  end.

%% @spec info (Tcerl::tcerl ()) -> [ info () ] | { error, Reason }
%%   where 
%%     info () = { file_size, integer () } | 
%%               { no_objects, integer () }
%% @doc Returns information about the data store.  file_size is the size
%% of the file in bytes.  no_objects in the number of different objects
%% stored in the table.
%% Corresponds to tcbdbrnum () and tcbdbfsiz ().  
%% @end

info ({ tcerl, Port }) when is_port (Port) ->
  true = port_command (Port, <<?BDB_INFO:8>>),

  receive
    { Port, { data, <<FileSize:64/native-unsigned,
                      NoObjects:64/native-unsigned>> } } -> 
      [ { file_size, FileSize },
        { no_objects, NoObjects } ];
    { Port, { data, Other } } -> 
      { error, Other }
  end.

%% @spec is_tcbdb_file (iodata ()) -> true | false | { error, Reason }
%% @doc Returns true if file Filename is a tcbdb file, false otherwise.
%% @end

is_tcbdb_file (Filename) when ?is_iodata (Filename) ->
  case file:open (Filename, [ read, raw, binary ]) of
    { ok, Fh } ->
      try file:read (Fh, 33) of
        { ok, <<"ToKyO CaBiNeT\n", _FormatAndVersion:18/binary, 1:8>> } ->
          true;
        { ok, _Data } ->
          false;
        eof ->
          false;
        R = { error, _Reason } ->
          R
      after
        file:close (Fh)
      end;
    R = { error, _Reason } ->
      R
  end.

%% @spec last (Tcerl::tcerl ()) -> [ binary () ] | { error, Reason }
%% @doc Get the last key in the database.
%% Empty list is returned if there are no keys.
%% Corresponds to tcbdbcurlast ().
%% @end

last ({ tcerl, Port }) when is_port (Port) ->
  true = port_command (Port, <<?BDB_LAST:8>>),

  receive
    { Port, { data, List } } when is_list (List) -> List;
    { Port, { data, Other } } -> { error, Other }
  end.

%% @spec open (iodata (), [ option () ]) -> { ok, tcerl () } | { error, Reason }
%%   where
%%     option () = reader | writer | create | truncate | 
%%                 { leaf_members, integer () } |
%%                 { non_leaf_members, integer () } |
%%                 { bucket_array_size, integer () } |
%%                 { record_alignment, integer () } |
%%                 { free_block_pool, integer () } |
%%                 { leaf_node_cache, integer () } |
%%                 { nonleaf_node_cache, integer () } |
%%                 large | small | 
%%                 uncompressed | deflate | tcbs | 
%%                 nolock | lock_nonblocking | lock |
%%                 raw | term_store
%% @doc Open a tcerl database.  This function corresponds to 
%% tcbdbtune () followed by tcbdbopen ().  raw uses the default
%% comparison function for keys (memcmp); term_store uses 
%% erlang term order and requires valid external term formats as keys.
%% @end

open (Name, Options) when ?is_iodata (Name),
                          is_list (Options) ->
  Opts = set_defaults (Options),

  OMode = 
    (case lists:member (reader, Opts) of true -> 1; false -> 0 end) bor
    (case lists:member (writer, Opts) of true -> 2; false -> 0 end) bor
    (case lists:member (create, Opts) of true -> 4; false -> 0 end) bor
    (case lists:member (truncate, Opts) of true -> 8; false -> 0 end) bor
    (case lists:member (nolock, Opts) of true -> 16; false -> 0 end) bor
    (case lists:member (lock_nonblocking, Opts) of true -> 32; false -> 0 end),

  LMemb = get_value (leaf_members, Opts),
  NMemb = get_value (non_leaf_members, Opts),
  BNum = get_value (bucket_array_size, Opts),
  APow = get_value (record_alignment, Opts),
  FPow = get_value (free_block_pool, Opts),
  LCNum = get_value (leaf_node_cache, Opts),
  NCNum = get_value (nonleaf_node_cache, Opts),
  Raw = case lists:member (raw, Opts) of true -> 1; false -> 0 end,

  Extra = 
    (case lists:member (large, Opts) of true -> 1; false -> 0 end) bor
    (case lists:member (deflate, Opts) of true -> 2; false -> 0 end) bor
    (case lists:member (tcbs, Opts) of true -> 4; false -> 0 end),

  Port = make_port (),
  true = port_command (Port, <<?BDB_TUNE:8,
                               LMemb:64/native-signed,
                               NMemb:64/native-signed,
                               BNum:64/native-signed,
                               APow:64/native-signed,
                               FPow:64/native-signed,
                               Extra:64/native-unsigned,
                               LCNum:64/native-signed,
                               NCNum:64/native-signed,
                               Raw:8>>),

  receive
    { Port, { data, <<"ok">> } } -> 
      NameSize = 1 + erlang:iolist_size (Name),
      true = port_command (Port, [ <<?BDB_OPEN:8>>,
                                   <<NameSize:64/native-unsigned>>,
                                   Name,
                                   <<0:8>>,
                                   <<OMode:64/native-unsigned>> ]),
      receive
        { Port, { data, <<"ok">> } } -> 
          { ok, { tcerl, Port } };
        { Port, { data, Other } } ->
          port_close (Port),
          { error, Other }
      end;
    { Port, { data, Other } } ->
      port_close (Port),
      { error, Other }
  end.

%% @spec next (Tcerl::tcerl (), iodata ()) -> [ binary () ] | { error, Reason }
%% @doc Get the next key in the database.  Key is flattened to binary 
%% before lookup.
%% Empty list is returned if there are no more keys.  Like ets:next/2,
%% Key need not be an element of the table, the next key in order in the
%% table (if any) will be returned.
%% Corresponds to tcbdbcurjump ().
%% @end

next ({ tcerl, Port }, Key) when is_port (Port), ?is_iodata (Key) ->
  KeySize = erlang:iolist_size (Key),
  true = port_command (Port, [ <<?BDB_NEXT:8>>, 
                               <<KeySize:64/native-unsigned>>,
                               Key ]),

  receive
    { Port, { data, List } } when is_list (List) -> List;
    { Port, { data, Other } } -> { error, Other }
  end.

%% @spec out (Tcerl::tcerl (), iodata ()) -> ok | { error, Reason }
%% @doc Delete all records associated with Key.  
%% Key is flattened to binary before lookup.
%% Corresponds to tcbdbout3 ().
%% @end

out ({ tcerl, Port }, Key) when is_port (Port), ?is_iodata (Key) ->
  KeySize = erlang:iolist_size (Key),
  true = port_command (Port, [ <<?BDB_OUT:8>>, 
                               <<KeySize:64/native-unsigned>>,
                               Key ]),
  receive
    { Port, { data, <<"ok">> } } -> ok;
    { Port, { data, <<"no record found">> } } -> ok;
    { Port, { data, Other } } -> { error, Other }
  end.

%% @spec out_async (Tcerl::tcerl (), iodata ()) -> ok
%% @doc Asynchronously delete all records associated with Key.  
%% Returns immediately, and does not do error checking.
%% Key is flattened to binary before lookup.
%% Corresponds to tcbdbout3 ().
%% @end

out_async ({ tcerl, Port }, Key) when is_port (Port), ?is_iodata (Key) ->
  KeySize = erlang:iolist_size (Key),
  true = port_command (Port, [ <<?BDB_OUT_ASYNC:8>>, 
                               <<KeySize:64/native-unsigned>>,
                               Key ]),
  ok.

%% @spec out_exact (Tcerl::tcerl (), iodata (), iodata ()) -> ok | { error, Reason }
%% @doc Delete all records associated with Key that compare equal to Value.
%% Both Key and Value are flattened to binary before lookup.
%% Corresponds to tcbdbout3 () coupled with tcbdbcurjump () and 
%% tcbdbcurnext ().
%% @end

out_exact ({ tcerl, Port }, Key, Value) when is_port (Port), 
                                             ?is_iodata (Key),
                                             ?is_iodata (Value) ->
  KeySize = erlang:iolist_size (Key),
  ValueSize = erlang:iolist_size (Value),
  true = port_command (Port, [ <<?BDB_OUT_EXACT:8>>, 
                               <<KeySize:64/native-unsigned>>,
                               Key,
                               <<ValueSize:64/native-unsigned>>,
                               Value ]),
  receive
    { Port, { data, <<"ok">> } } -> ok;
    { Port, { data, <<"no record found">> } } -> ok;
    { Port, { data, Other } } -> { error, Other }
  end.

%% @spec out_exact_async (Tcerl::tcerl (), iodata (), iodata ()) -> ok
%% @doc Asynchronously delete all records associated with Key that 
%% compare equal to Value.
%% Returns immediately, and does not do error checking.
%% Both Key and Value are flattened to binary before lookup.
%% Corresponds to tcbdbout3 () coupled with tcbdbcurjump () and 
%% tcbdbcurnext ().
%% @end

out_exact_async ({ tcerl, Port }, Key, Value) when is_port (Port), 
                                                   ?is_iodata (Key),
                                                   ?is_iodata (Value) ->
  KeySize = erlang:iolist_size (Key),
  ValueSize = erlang:iolist_size (Value),
  true = port_command (Port, [ <<?BDB_OUT_EXACT_ASYNC:8>>, 
                               <<KeySize:64/native-unsigned>>,
                               Key,
                               <<ValueSize:64/native-unsigned>>,
                               Value ]),
  ok.

%% @spec prefix (Tcerl::tcerl (), iodata (), integer ()) -> [ binary () ] | { error, Reason }
%% @doc Retrieve keys which have Key as a prefix.
%% Key is flattened to binary before lookup.
%% If Max is non-negative, no more than Max results are returned.
%% Corresponds to tcbdbprefix ().
%% @end

prefix ({ tcerl, Port }, Prefix, Max) when is_port (Port),
                                           ?is_iodata (Prefix),
                                           is_integer (Max) ->
  PrefixSize = erlang:iolist_size (Prefix),
  true = port_command (Port, [ <<?BDB_FWM:8>>,
                               <<PrefixSize:64/native-unsigned>>,
                               Prefix,
                               <<Max:64/native-signed>> ]),

  receive
    { Port, { data, List } } when is_list (List) -> List;
    { Port, { data, Other } } -> { error, Other }
  end.

%% @spec prev (Tcerl::tcerl (), iodata ()) -> [ binary () ] | { error, Reason }
%% @doc Get the previous key in the database.
%% Key is flattened to binary before lookup.
%% Empty list is returned if there are no more keys.  Like ets:prev/2,
%% Key need not be an element of the table, the previous key in order in the
%% table (if any) will be returned.
%% Corresponds to tcbdbcurjump ().
%% @end

prev ({ tcerl, Port }, Key) when is_port (Port), ?is_iodata (Key) ->
  KeySize = erlang:iolist_size (Key),
  true = port_command (Port, [ <<?BDB_PREV:8>>, 
                               <<KeySize:64/native-unsigned>>,
                               Key ]),

  receive
    { Port, { data, List } } when is_list (List) -> List;
    { Port, { data, Other } } -> { error, Other }
  end.

%% @spec put (Tcerl::tcerl (), iodata (), iodata ()) -> ok | { error, Reason }
%% @doc Enter a key-value pair into the database.  
%% Both Key and Value are flattened to binary before insertion.
%% If a record exists %% for the key it will be replaced.  
%% Corresponds to tcbdbput ().
%% @end

put ({ tcerl, Port }, Key, Value) when is_port (Port), 
                                       ?is_iodata (Key),
                                       ?is_iodata (Value) ->
  KeySize = erlang:iolist_size (Key),
  ValueSize = erlang:iolist_size (Value),
  true = port_command (Port, [ <<?BDB_PUT:8>>, 
                               <<KeySize:64/native-unsigned>>,
                               Key,
                               <<ValueSize:64/native-unsigned>>,
                               Value ]),
  receive
    { Port, { data, <<"ok">> } } -> ok;
    { Port, { data, Other } } -> { error, Other }
  end.

%% @spec put_async (Tcerl::tcerl (), iodata (), iodata ()) -> ok 
%% @doc Asynchronously enter a key-value pair into the database.
%% Returns immediately, and does not do error checking.
%% Both Key and Value are flattened to binary before insertion.
%% If a record exists for the key it will be replaced.  
%% Corresponds to tcbdbput ().
%% @end

put_async ({ tcerl, Port }, Key, Value) when is_port (Port), 
                                             ?is_iodata (Key),
                                             ?is_iodata (Value) ->
  KeySize = erlang:iolist_size (Key),
  ValueSize = erlang:iolist_size (Value),
  true = port_command (Port, [ <<?BDB_PUT_ASYNC:8>>, 
                               <<KeySize:64/native-unsigned>>,
                               Key,
                               <<ValueSize:64/native-unsigned>>,
                               Value ]),
  ok.

%% @spec put_dup (Tcerl::tcerl (), iodata (), iodata ()) -> ok | { error, Reason }
%% @doc Enter a key-value pair into the database, allowing duplicate keys.
%% Both Key and Value are flattened to binary before insertion.
%% Corresponds to tcbdbputdup ().
%% @end

put_dup ({ tcerl, Port }, Key, Value) when is_port (Port),
                                           ?is_iodata (Key),
                                           ?is_iodata (Value) ->
  KeySize = erlang:iolist_size (Key),
  ValueSize = erlang:iolist_size (Value),
  true = port_command (Port, [ <<?BDB_PUT_DUP:8>>, 
                               <<KeySize:64/native-unsigned>>,
                               Key,
                               <<ValueSize:64/native-unsigned>>,
                               Value ]),
  receive
    { Port, { data, <<"ok">> } } -> ok;
    { Port, { data, Other } } -> { error, Other }
  end.

%% @spec put_dup_async (Tcerl::tcerl (), iodata (), iodata ()) -> ok 
%% @doc Asynchronously enter a key-value pair into the database, 
%% allowing duplicate keys.
%% Returns immediately, and does not do error checking.
%% Both Key and Value are flattened to binary before insertion.
%% Corresponds to tcbdbputdup ().
%% @end

put_dup_async ({ tcerl, Port }, Key, Value) when is_port (Port),
                                                 ?is_iodata (Key),
                                                 ?is_iodata (Value) ->
  KeySize = erlang:iolist_size (Key),
  ValueSize = erlang:iolist_size (Value),
  true = port_command (Port, [ <<?BDB_PUT_DUP_ASYNC:8>>, 
                               <<KeySize:64/native-unsigned>>,
                               Key,
                               <<ValueSize:64/native-unsigned>>,
                               Value ]),
  ok.

%% @spec range (Tcerl::tcerl (), iodata (), bool (), iodata (), bool (), integer ()) -> [ binary () ] | { error, Reason }
%% @doc Retrieve keys between Begin and End, optionally inclusive.
%% Begin and End are flattened to binary before comparison.
%% If Max is non-negative, no more than Max results are returned.
%% Corresponds to tcbdbrange ().
%% @end

range ({ tcerl, Port }, Begin, BeginInc, End, EndInc, Max) 
  when is_port (Port),
       ?is_iodata (Begin),
       is_boolean (BeginInc),
       ?is_iodata (End),
       is_boolean (EndInc),
       is_integer (Max) ->
  BeginSize = erlang:iolist_size (Begin),
  BeginIncByte = if BeginInc -> <<1:8>>; true -> <<0:8>> end,
  EndSize = erlang:iolist_size (End),
  EndIncByte = if EndInc -> <<1:8>>; true -> <<0:8>> end,
  true = port_command (Port, [ <<?BDB_RANGE:8>>,
                               <<BeginSize:64/native-unsigned>>,
                               Begin,
                               BeginIncByte,
                               <<EndSize:64/native-unsigned>>,
                               End,
                               EndIncByte,
                               <<Max:64/native-signed>> ]),

  receive
    { Port, { data, List } } when is_list (List) -> List;
    { Port, { data, Other } } -> { error, Other }
  end.

%% @spec range_lb (Tcerl::tcerl (), iodata (), bool (), integer ()) -> [ binary () ] | { error, Reason }
%% @doc Retrieve keys greater than Begin, optionally inclusive.
%% Begin is flattened to binary before comparison.
%% If Max is non-negative, no more than Max results are returned.
%% Corresponds to tcbdbrange ().
%% @end

range_lb ({ tcerl, Port }, Begin, BeginInc, Max) when is_port (Port),
                                                      ?is_iodata (Begin),
                                                      is_boolean (BeginInc),
                                                      is_integer (Max) ->
  BeginSize = erlang:iolist_size (Begin),
  BeginIncByte = if BeginInc -> <<1:8>>; true -> <<0:8>> end,
  true = port_command (Port, [ <<?BDB_RANGE:8>>,
                               <<BeginSize:64/native-unsigned>>,
                               Begin,
                               BeginIncByte,
                               <<0:64/native-unsigned>>,
                               <<1:8>>,
                               <<Max:64/native-signed>> ]),

  receive
    { Port, { data, List } } when is_list (List) -> List;
    { Port, { data, Other } } -> { error, Other }
  end.

%% @spec range_ub (Tcerl::tcerl (), iodata (), bool (), integer ()) -> [ binary () ] | { error, Reason }
%% @doc Retrieve keys less than End, optionally inclusive.
%% End is flattened to binary before comparison.
%% If Max is non-negative, no more than Max results are returned.
%% Corresponds to tcbdbrange ().
%% @end

range_ub ({ tcerl, Port }, End, EndInc, Max) when is_port (Port),
                                                  ?is_iodata (End),
                                                  is_boolean (EndInc),
                                                  is_integer (Max) ->
  EndSize = erlang:iolist_size (End),
  EndIncByte = if EndInc -> <<1:8>>; true -> <<0:8>> end,
  true = port_command (Port, [ <<?BDB_RANGE:8>>,
                               <<0:64/native-unsigned>>,
                               <<1:8>>,
                               <<EndSize:64/native-unsigned>>,
                               End,
                               EndIncByte,
                               <<Max:64/native-signed>> ]),

  receive
    { Port, { data, List } } when is_list (List) -> List;
    { Port, { data, Other } } -> { error, Other }
  end.

%% @spec sync (Tcerl::tcerl ()) -> ok | { error, Reason }
%% @doc Synchronize contents of a B+ tree database object with the file 
%% and the device.  Corresponds to tcbdbsync ().
%% @end

sync ({ tcerl, Port }) when is_port (Port) ->
  true = port_command (Port, <<?BDB_SYNC:8>>),
  receive
    { Port, { data, <<"ok">> } } -> ok;
    { Port, { data, Other } } -> { error, Other }
  end.

%% @spec update_counter (Tcerl::tcerl (), Key::iodata (), Pos::integer (), Incr::integer ()) -> Result::integer () 
%% @doc Update a counter associated with a tuple record whose Pos-th 
%% position consists of an integer field.  The new counter value
%% is returned.
%% 
%% For compatibility with ets/dets, this method will 
%% exit (throw an exception) if certain conditions are violated:
%% <ol>
%%   <li>An object exists associated with the key.</li>
%%   <li>The value is a tuple with an integer at the Pos-th position.</li>
%% </ol>
%% @end

update_counter ({ tcerl, Port }, Key, Pos, Incr) when is_port (Port),
                                                      ?is_iodata (Key),
                                                      is_integer (Pos),
                                                      is_integer (Incr) ->
  KeySize = erlang:iolist_size (Key),
  true = port_command (Port, [ <<?BDB_UPDATE_COUNTER:8>>,
                               <<KeySize:64/native-unsigned>>,
                               Key,
                               <<Pos:64/native-unsigned>>,
                               <<Incr:64/native-signed>> ]),

  receive
    { Port, { data, [ <<Result:32/native-signed>> ] } } -> Result;
    { Port, { data, [] } } -> exit (badarg);
    { Port, { data, Other } } -> exit (Other)
  end.

%% @spec unlink (Tcerl::tcerl ()) -> true
%% @doc Unlinks the port underlying the table from the current process.
%% @end

unlink ({ tcerl, Port }) when is_port (Port) ->
  erlang:unlink (Port).

%% @spec vanish (Tcerl::tcerl ()) -> ok | { error, Reason }
%% @doc Remove all records.  Corresponds to tcbdbvanish ().
%% @end

vanish ({ tcerl, Port }) ->
  true = port_command (Port, <<?BDB_VANISH:8>>),
  receive
    { Port, { data, <<"ok">> } } -> ok;
    { Port, { data, Other } } -> { error, Other }
  end.

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-
 
get_value (What, List) ->
  { value, { What, Value } } = lists:keysearch (What, 1, List),
  Value.

make_port () ->
  open_port ({ spawn, libtcbdberl }, [ binary ]).

set_defaults (Options) ->
  Compression = case { lists:member (deflate, Options), 
                       lists:member (tcbs, Options) } of
                  { false, false } -> [ uncompressed ];
                  _ -> []
                end,

  Size = case lists:member (small, Options) of 
           false -> [ large ];
           true -> []
         end,

  Lock = case { lists:member (lock, Options),
                lists:member (lock_nonblocking, Options) } of
           { false, false } -> [ nolock ];
           _ -> []
         end,

  Raw = case { lists:member (raw, Options),
               lists:member (term_store, Options) } of
           { false, false } -> [ raw ];
           _ -> []
        end,

  Options ++
  Raw ++
  Lock ++
  Size ++ 
  Compression ++
  [ reader, 
    { leaf_node_cache, -1 },
    { nonleaf_node_cache, -1 },
    { leaf_members, -1 },
    { non_leaf_members, - 1 },
    { bucket_array_size, -1 },
    { record_alignment, -1 },
    { free_block_pool, -1 } ].

-ifdef (EUNIT).

random_binary () ->
  Length = random:uniform (25),
  list_to_binary ([ random:uniform (255) || _ <- lists:seq (1, Length) ]).

random_bool () ->
  random:uniform (2) =:= 1.

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
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      ok = tcbdb:close (R),
      file:delete ("flass" ++ os:getpid ())
    end }.

first_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_binary (), random_binary () } end,
               (fun ({ Key, Value }) ->
                  case erlang:get (minsofar) of
                    undefined -> [] = tcbdb:first (R);
                    { defined, Min } -> [ Min ] = tcbdb:first (R)
                  end,

                  ok = tcbdb:put (R, Key, Value),

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
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.

is_tcbdb_test_ () ->
  { setup,
    fun tcerl:start/0,
    fun (_) -> tcerl:stop () end,
    fun () -> 
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      ok = tcbdb:close (R),
      true = tcbdb:is_tcbdb_file ("flass" ++ os:getpid ()),
      false = tcbdb:is_tcbdb_file ("Makefile.am.local"),
      file:delete ("flass" ++ os:getpid ()),
      { error, enoent } = tcbdb:is_tcbdb_file ("flass" ++ os:getpid ()),
      true
    end }.

last_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_binary (), random_binary () } end,
               (fun ({ Key, Value }) ->
                  case erlang:get (maxsofar) of
                    undefined -> [] = tcbdb:last (R);
                    { defined, Max } -> [ Max ] = tcbdb:last (R)
                  end,

                  ok = tcbdb:put (R, Key, Value),

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
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.

next_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_binary (), random_binary () } end,
               (fun ({ Key, Value }) ->
                  case ets:next (Tab, Key) of
                    '$end_of_table' -> % in-band signaling :(
                      [] = tcbdb:next (R, Key);
                    NextKey ->
                      [ NextKey ] = tcbdb:next (R, Key)
                  end,

                  true = ets:insert (Tab, { Key, Value }),
                  ok = tcbdb:put (R, Key, Value),

                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.

open_fail_test_ () ->
  { setup,
    fun tcerl:start/0,
    fun (_) -> tcerl:stop () end,
    fun () -> { error, <<"file not found">> } = tcbdb:open ("/zzyzx", []) end
  }.

out_exact_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_binary (), random_binary () } end,
               (fun ({ Key, Value }) ->
                  KeySize = erlang:size (Key),
                  ValueSize = erlang:size (Value),

                  ok = tcbdb:put (R, Key, Value),
                  [ Value ] = tcbdb:get (R, Key),

                  ok = tcbdb:put_dup (R, Key, <<Value:ValueSize/binary,
                                                Value:ValueSize/binary>>),
                  [ Value, <<Value:ValueSize/binary,
                             Value:ValueSize/binary>> ] = tcbdb:get (R, Key),
                  ok = tcbdb:put (R, Key, Value),
                  [ Value, <<Value:ValueSize/binary,
                             Value:ValueSize/binary>> ] = tcbdb:get (R, Key),
                  ok = tcbdb:out_exact (R, <<Key:KeySize/binary,
                                             Key:KeySize/binary>>,
                                        Value),
                  [ Value, <<Value:ValueSize/binary,
                             Value:ValueSize/binary>> ] = tcbdb:get (R, Key),
                  ok = tcbdb:out_exact (R, Key, Value),
                  [ <<Value:ValueSize/binary,
                      Value:ValueSize/binary>> ] = tcbdb:get (R, Key),
                  ok = tcbdb:out_exact (R, Key, <<Value:ValueSize/binary,
                                                  0:8>>),
                  [ <<Value:ValueSize/binary,
                      Value:ValueSize/binary>> ] = tcbdb:get (R, Key),
                  ok = tcbdb:out_exact (R, Key, <<Value:ValueSize/binary,
                                                  Value:ValueSize/binary>>),
                  [] = tcbdb:get (R, Key),
                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.

out_exact_async_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_binary (), random_binary () } end,
               (fun ({ Key, Value }) ->
                  KeySize = erlang:size (Key),
                  ValueSize = erlang:size (Value),

                  ok = tcbdb:put (R, Key, Value),
                  [ Value ] = tcbdb:get (R, Key),

                  ok = tcbdb:put_dup (R, Key, <<Value:ValueSize/binary,
                                                Value:ValueSize/binary>>),
                  [ Value, <<Value:ValueSize/binary,
                             Value:ValueSize/binary>> ] = tcbdb:get (R, Key),
                  ok = tcbdb:put (R, Key, Value),
                  [ Value, <<Value:ValueSize/binary,
                             Value:ValueSize/binary>> ] = tcbdb:get (R, Key),
                  ok = tcbdb:out_exact_async (R, <<Key:KeySize/binary,
                                                   Key:KeySize/binary>>,
                                              Value),
                  [ Value, <<Value:ValueSize/binary,
                             Value:ValueSize/binary>> ] = tcbdb:get (R, Key),
                  ok = tcbdb:out_exact_async (R, Key, Value),
                  [ <<Value:ValueSize/binary,
                      Value:ValueSize/binary>> ] = tcbdb:get (R, Key),
                  ok = tcbdb:out_exact_async (R, Key, <<Value:ValueSize/binary,
                                                        0:8>>),
                  [ <<Value:ValueSize/binary,
                      Value:ValueSize/binary>> ] = tcbdb:get (R, Key),
                  ok = tcbdb:out_exact_async (R, Key, <<Value:ValueSize/binary,
                                                        Value:ValueSize/binary>>),
                  [] = tcbdb:get (R, Key),
                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.

put_dup_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_binary (), random_binary () } end,
               (fun ({ Key, Value }) ->
                  ValueSize = erlang:size (Value),

                  ok = tcbdb:put (R, Key, Value),
                  [ Value ] = tcbdb:get (R, Key),

                  ok = tcbdb:put_dup (R, Key, <<Value:ValueSize/binary,
                                                Value:ValueSize/binary>>),
                  [ Value, <<Value:ValueSize/binary,
                             Value:ValueSize/binary>> ] = tcbdb:get (R, Key),
                  ok = tcbdb:put_dup (R, Key, Value),
                  [ Value, 
                    <<Value:ValueSize/binary,
                      Value:ValueSize/binary>>,
                    Value ] = tcbdb:get (R, Key),
                  ok = tcbdb:out_exact (R, Key, Value),
                  [ <<Value:ValueSize/binary,
                      Value:ValueSize/binary>> ] = tcbdb:get (R, Key),
                  ok = tcbdb:out (R, Key),
                  [] = tcbdb:get (R, Key),
                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.

put_dup_async_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_binary (), random_binary () } end,
               (fun ({ Key, Value }) ->
                  ValueSize = erlang:size (Value),

                  ok = tcbdb:put (R, Key, Value),
                  [ Value ] = tcbdb:get (R, Key),

                  ok = tcbdb:put_dup_async (R, Key, <<Value:ValueSize/binary,
                                                      Value:ValueSize/binary>>),
                  [ Value, <<Value:ValueSize/binary,
                             Value:ValueSize/binary>> ] = tcbdb:get (R, Key),
                  ok = tcbdb:put_dup_async (R, Key, Value),
                  [ Value, 
                    <<Value:ValueSize/binary,
                      Value:ValueSize/binary>>,
                    Value ] = tcbdb:get (R, Key),
                  ok = tcbdb:out_exact (R, Key, Value),
                  [ <<Value:ValueSize/binary,
                      Value:ValueSize/binary>> ] = tcbdb:get (R, Key),
                  ok = tcbdb:out (R, Key),
                  [] = tcbdb:get (R, Key),
                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.


prefix_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_binary (), random_binary (),
                            random_binary () } end,
               (fun ({ Key, Value, Prefix }) ->
                  PrefixSize = erlang:size (Prefix),

                  EtsPrefix = 
                    [ V || 
                      V <- ets:select (Tab, 
                                       [ { { '$1', '_' }, [], [ '$1' ] } ]),
                      (fun (<<Warez:PrefixSize/binary, _/binary>>) 
                              when Warez =:= Prefix -> true;
                           (_) -> false
                       end) (V) ],

                  BdbPrefix = tcbdb:prefix (R, Prefix, -1),

                  EtsPrefix = BdbPrefix,

                  Max = case length (BdbPrefix) of
                          0 -> 1;
                          N when N > 0 -> N - 1
                        end,

                  LimitBdbPrefix = tcbdb:prefix (R, Prefix, Max),

                  case length (BdbPrefix) of
                    0 -> LimitBdbPrefix = [];
                    _ -> LimitBdbPrefix = lists:reverse (lists:nthtail (1, lists:reverse (BdbPrefix)))
                  end,

                  true = ets:insert (Tab, { Key, Value }),
                  ok = tcbdb:put (R, Key, Value),

                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.


prev_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_binary (), random_binary () } end,
               (fun ({ Key, Value }) ->
                  case ets:prev (Tab, Key) of
                    '$end_of_table' -> % in-band signaling :(
                      [] = tcbdb:prev (R, Key);
                    NextKey ->
                      [ NextKey ] = tcbdb:prev (R, Key)
                  end,

                  true = ets:insert (Tab, { Key, Value }),
                  ok = tcbdb:put (R, Key, Value),

                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.

range_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_binary (), random_binary (),
                            random_binary (), random_binary (),
                            random_bool (), random_bool () } end,
               (fun ({ Key, Value, Lower, Upper, LowerInc, UpperInc }) ->
                  LowerOp = if LowerInc -> '>='; true -> '>' end,
                  UpperOp = if UpperInc -> '=<'; true -> '<' end,

                  EtsRange = ets:select (Tab, 
                                         [ { { '$1', '_' },
                                             [ { LowerOp, '$1', { const, Lower } },
                                               { UpperOp, '$1', { const, Upper } } ],
                                             [ '$1' ] } ]),

                  BdbRange = 
                    tcbdb:range (R, Lower, LowerInc, Upper, UpperInc, -1),

                  EtsRange = BdbRange,

                  Max = case length (BdbRange) of
                          0 -> 1;
                          N when N > 0 -> N - 1
                        end,
                                            
                  LimitBdbRange = 
                    tcbdb:range (R, Lower, LowerInc, Upper, UpperInc, Max),

                  case length (BdbRange) of
                    0 -> LimitBdbRange = [];
                    _ -> LimitBdbRange = lists:reverse (lists:nthtail (1, lists:reverse (BdbRange)))
                  end,

                  true = ets:insert (Tab, { Key, Value }),
                  ok = tcbdb:put (R, Key, Value),

                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.

range_lb_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_binary (), random_binary (),
                            random_binary (), random_bool () } end,
               (fun ({ Key, Value, Lower, LowerInc }) ->
                  LowerOp = if LowerInc -> '>='; true -> '>' end,

                  EtsRange = ets:select (Tab, 
                                         [ { { '$1', '_' },
                                             [ { LowerOp, '$1', { const, Lower } } ],
                                             [ '$1' ] } ]),

                  BdbRange = tcbdb:range_lb (R, Lower, LowerInc, -1),

                  EtsRange = BdbRange,

                  Max = case length (BdbRange) of
                          0 -> 1;
                          N when N > 0 -> N - 1
                        end,
                                            
                  LimitBdbRange = tcbdb:range_lb (R, Lower, LowerInc, Max),

                  case length (BdbRange) of
                    0 -> LimitBdbRange = [];
                    _ -> LimitBdbRange = lists:reverse (lists:nthtail (1, lists:reverse (BdbRange)))
                  end,

                  true = ets:insert (Tab, { Key, Value }),
                  ok = tcbdb:put (R, Key, Value),

                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.

range_ub_test_ () ->
  F = fun ({ Tab, R }) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_binary (), random_binary (),
                            random_binary (), random_bool () } end,
               (fun ({ Key, Value, Upper, UpperInc }) ->
                  UpperOp = if UpperInc -> '=<'; true -> '<' end,

                  EtsRange = ets:select (Tab, 
                                         [ { { '$1', '_' },
                                             [ { UpperOp, '$1', { const, Upper } } ],
                                             [ '$1' ] } ]),

                  BdbRange = tcbdb:range_ub (R, Upper, UpperInc, -1),

                  EtsRange = BdbRange,

                  Max = case length (BdbRange) of
                          0 -> 1;
                          N when N > 0 -> N - 1
                        end,
                                            
                  LimitBdbRange = tcbdb:range_ub (R, Upper, UpperInc, Max),

                  case length (BdbRange) of
                    0 -> LimitBdbRange = [];
                    _ -> LimitBdbRange = lists:reverse (lists:nthtail (1, lists:reverse (BdbRange)))
                  end,

                  true = ets:insert (Tab, { Key, Value }),
                  ok = tcbdb:put (R, Key, Value),

                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      Tab = ets:new (?MODULE, [ public, ordered_set ]),
      tcerl:start (),
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      { Tab, R }
    end,
    fun ({ Tab, _ }) ->
      ets:delete (Tab),
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.

roundtrip_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_binary (), random_binary () } end,
               (fun ({ Key, Value }) ->
                  ok = tcbdb:put (R, Key, Value),
                  [ Value ] = tcbdb:get (R, Key),
                  ok = tcbdb:out (R, Key),
                  [] = tcbdb:get (R, Key),
                  ok = tcbdb:out (R, Key),
                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.

roundtrip_async_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> { random_binary (), random_binary () } end,
               (fun ({ Key, Value }) ->
                  ok = tcbdb:put_async (R, Key, Value),
                  [ Value ] = tcbdb:get (R, Key),
                  ok = tcbdb:out_async (R, Key),
                  [] = tcbdb:get (R, Key),
                  ok = tcbdb:out_async (R, Key),
                  true
                end) (X)),

    ok = flasscheck (1000, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.

sync_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> 
                [ { random_binary (), 
                    random_binary () } || _ <- lists:seq (1, 10) ]
               end,
               (fun (Objects) ->
                  [ tcbdb:put (R, Key, Value) || { Key, Value } <- Objects ],
                  [ _ ] = tcbdb:get (R, element (1, hd (Objects))),
                  ok = tcbdb:sync (R),
                  [ _ ] = tcbdb:get (R, element (1, hd (Objects))),
                  ok = tcbdb:vanish (R),
                  [ ] = tcbdb:get (R, element (1, hd (Objects))),
                  true
                end) (X)),

    ok = flasscheck (20, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.

vanish_test_ () ->
  F = fun (R) ->
    T = 
      ?FORALL (X,
               fun (_) -> 
                [ { random_binary (), 
                    random_binary () } || _ <- lists:seq (1, 10) ]
               end,
               (fun (Objects) ->
                  [ tcbdb:put (R, Key, Value) || { Key, Value } <- Objects ],
                  [ _ ] = tcbdb:get (R, element (1, hd (Objects))),
                  ok = tcbdb:vanish (R),
                  [] = tcbdb:get (R, element (1, hd (Objects))),
                  ok = tcbdb:vanish (R),
                  true
                end) (X)),

    ok = flasscheck (20, 10, T)
  end,

  { setup,
    fun () -> 
      tcerl:start (),
      { ok, R } = tcbdb:open ("flass" ++ os:getpid (),
                              [ create, truncate, writer ]),
      R
    end,
    fun (_) ->
      tcerl:stop (),
      file:delete ("flass" ++ os:getpid ())
    end,
    { with, [ F ] }
  }.

-endif.
