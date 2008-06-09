%% @doc The tcerl application.  It must be started before any of the 
%% functions provided by this library can be used.
%% @end

-module (tcerl).
-export ([ start/0,
           stop/0 ]).
-behaviour (application).
-export ([ start/2,
           stop/1 ]).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

%% @spec start () -> ok | { error, Reason }
%% @equiv application:start (tcerl)
%% @end

start () ->
  application:start (tcerl).

%% @spec stop () -> ok | { error, Reason }
%% @equiv application:stop (tcerl)
%% @end

stop () ->
  application:stop (tcerl).

%-=====================================================================-
%-                        application callbacks                        -
%-=====================================================================-

%% @hidden

start (_Type, _Args) ->
  tcerlsup:start_link ().

%% @hidden

stop (_Args) ->
  ok.
