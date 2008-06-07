-module (tcerl).
-export ([ start/0,
           stop/0 ]).
-behaviour (application).
-export ([ start/2,
           stop/1 ]).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start () ->
  application:start (tcerl).

stop () ->
  application:stop (tcerl).

%-=====================================================================-
%-                        application callbacks                        -
%-=====================================================================-

start (_Type, _Args) ->
% TODO: tcerlsup to process which owns linked-in driver
  tcerlsup:start_link ().

stop (_Args) ->
  ok.
