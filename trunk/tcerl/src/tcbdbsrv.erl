%% @hidden

-module (tcbdbsrv).
-export ([ create_tab/2,
           delete_tab/1,
           get_tab/1,
           start_link/0 ]).

-behaviour (gen_server).
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           code_change/3,
           terminate/2 ]).

-record (state, {}).
-record (statev2, { tabs }).

-oldrecord (state).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

create_tab (Id, OpenSpec) ->
  gen_server:call (?MODULE, { create_tab, Id, OpenSpec }, 30000).

delete_tab (Id) ->
  gen_server:call (?MODULE, { delete_tab, Id }, 30000).

get_tab (Id) ->
  gen_server:call (?MODULE, { get_tab, Id }, 30000).

start_link () ->
  gen_server:start_link ({ local, ?MODULE }, ?MODULE, [], []).

%-=====================================================================-
%-                         gen_server callbacks                        -
%-=====================================================================-

init ([]) ->
  process_flag (trap_exit, true),

  { ok, Dir } = application:get_env (tcerl, tcerldrvprefix),

  case erl_ddll:load_driver (Dir ++ "/lib", libtcbdberl) of
    ok -> ok;
    { error, already_loaded } -> ok;
    { error, permanent } -> ok
  end,

  { ok, #statev2{ tabs = dict:new () } }.

handle_call ({ delete_tab, Id }, _From, State = #statev2{ tabs = Tabs }) ->
  case dict:find (Id, Tabs) of
    { ok, Port } -> 
      FileName = tcbdbets:info (Port, filename),
      tcbdbets:close (Port),
      file:delete (FileName),
      { reply, ok, State#statev2{ tabs = dict:erase (Id, Tabs) } };
    error ->
      { reply, unknown, State }
  end;
handle_call ({ get_tab, Id }, _From, State = #statev2{ tabs = Tabs }) ->
  case dict:find (Id, Tabs) of
    { ok, Value } -> 
      { reply, { ok, Value }, State };
    error ->
      { reply, unknown, State }
  end;
handle_call ({ create_tab, Id, OpenSpec }, 
             _From, 
             State = #statev2{ tabs = Tabs }) ->
  case dict:find (Id, Tabs) of
    { ok, Value } -> 
      { reply, { ok, Value }, State };
    error -> 
      case tcbdbets:open_file (OpenSpec) of
        R = { ok, Port } ->
          { reply, R, State#statev2{ tabs = dict:store (Id, Port, Tabs) } };
        X ->
          { reply, X, State }
      end
  end;
handle_call (_Msg, _From, State) ->
  { noreply, State }.

handle_cast (_Msg, State) ->
  { noreply, State }.

handle_info (_Msg, State) ->
  { noreply, State }.

code_change (_Vsn, #state{}, _Extra) ->
  { ok, #statev2{ tabs = dict:new () } };
code_change (_Vsn, State, _Extra) ->
  { ok, State }.

terminate (_Reason, #statev2{ tabs = Tabs }) ->
  lists:foreach (fun (T) -> catch tcbdbets:close (T) end,
                 [ V || { _, V } <- dict:to_list (Tabs) ]),
  ok.
