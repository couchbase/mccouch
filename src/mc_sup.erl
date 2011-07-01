-module(mc_sup).

-behaviour(supervisor).

-export([start_link/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(PortNum) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [PortNum]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([PortNum]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = permanent,
    Shutdown = 2000,
    Type = worker,

    TcpListener = {mc_tcp_listener, {mc_tcp_listener, start_link, [PortNum]},
                   Restart, Shutdown, Type, [mc_tcp_listener]},

    ConnSup = {mc_conn_sup, {mc_conn_sup, start_link, []},
               Restart, Shutdown, supervisor, dynamic},

    {ok, {SupFlags, [TcpListener, ConnSup]}}.
