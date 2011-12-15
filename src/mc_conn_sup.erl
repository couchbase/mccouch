-module(mc_conn_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-export([start_connection/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    Shutdown = 2000,
    {ok, {{simple_one_for_one, 0, 1},
          [{mc_connection, {mc_connection, start_link, []},
            temporary, Shutdown, worker, [mc_connection, mc_daemon]}]}}.

start_connection(NS) ->
    {ok, _Pid} = supervisor:start_child(?MODULE, [NS]).
