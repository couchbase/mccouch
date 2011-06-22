-module (mc_tcp_listener).

-export([start_link/1, init/1]).

% Starting the server

start_link(PortNum) ->
    {ok, spawn_link(?MODULE, init, [PortNum])}.

%
% The server itself
%

% server self-init
init(PortNum) ->
    {ok, LS} = gen_tcp:listen(PortNum, [binary,
                                        {reuseaddr, true},
                                        {packet, raw},
                                        {active, false}]),
    accept_loop(LS).

% Accept incoming connections
accept_loop(LS) ->
    {ok, NS} = gen_tcp:accept(LS),
    mc_conn_sup:start_connection(NS),
    accept_loop(LS).
