%% @hidden

-module (tcbdbsrv).
-export ([ start_link/0 ]).

-behaviour (gen_server).
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           code_change/3,
           terminate/2 ]).

-record (state, {}).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start_link () ->
  gen_server:start_link ({ local, ?MODULE }, ?MODULE, [], []).

%-=====================================================================-
%-                         gen_server callbacks                        -
%-=====================================================================-

init ([]) ->
  { ok, Dir } = application:get_env (tcerl, tcerldrvprefix),

  case erl_ddll:load_driver (Dir ++ "/lib", libtcbdberl) of
    ok -> ok;
    { error, already_loaded } -> ok
  end,

  { ok, #state{} }.

handle_call (_Msg, _From, State) ->
  { noreply, State }.

handle_cast (_Msg, State) ->
  { noreply, State }.

handle_info (_Msg, State) ->
  { noreply, State }.

code_change (_Vsn, State, _Extra) ->
  { ok, State }.

terminate (_Reason, _State) ->
  ok.
