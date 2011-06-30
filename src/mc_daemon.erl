-module(mc_daemon).

-behaviour(gen_fsm).

%% API
-export([start_link/0]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

%% My states
-export([processing/2, processing/3,
        batching/2,
        committing/2]).

%% Kind of ugly to export these, but modularity win.
-export([with_open_db/3, db_name/2, db_prefix/1]).

-define(SERVER, ?MODULE).

-include("couch_db.hrl").
-include("mc_constants.hrl").

-record(state, {db, json_mode=true, batch_ops=0, terminal_opaque=0}).

start_link() ->
    gen_fsm:start_link(?MODULE, [], []).

init([]) ->
    DbName = <<"default">>,
    {ok, processing, #state{db=DbName}}.

db_name(VBucket, State)->
    iolist_to_binary([State#state.db, $/, integer_to_list(VBucket)]).

db_prefix(State) -> State#state.db.

with_open_db(F, VBucket, State) ->
    {ok, Db} = couch_db:open(db_name(VBucket, State), []),
    NewState = F(Db),
    couch_db:close(Db),
    NewState.

handle_get_call(Db, Key) ->
    case mc_couch_kv:get(Db, Key) of
        {ok, Flags, _Expiration, Cas, Data} ->
            FlagsBin = <<Flags:32>>,
            #mc_response{extra=FlagsBin, cas=Cas, body=Data};
        _ ->
            #mc_response{status=1, body="Does not exist"}
    end.

handle_set_call(Db, Key, Flags, Expiration, Value, JsonMode) ->
    NewCas = mc_couch_kv:set(Db,
                             Key, Flags,
                             Expiration, Value,
                             JsonMode),
    #mc_response{cas=NewCas}.

%% Fun receives an open Db
do_batch_item(Cmd, Fun, VBucket, Opaque, Socket, State) ->
    Me = self(),
    spawn_link(fun() ->
                       gen_fsm:send_event(Me, {item_complete, Cmd, Opaque, Socket,
                                               with_open_db(Fun, VBucket, State)})
               end),
    State#state{batch_ops=State#state.batch_ops + 1}.

handle_setq_call(VBucket, Key, Flags, Expiration, Value, _CAS, Opaque, Socket, State) ->
    do_batch_item(?SETQ,
                  fun(Db) ->
                          case catch(mc_couch_kv:set(Db, Key,
                                                     Flags,
                                                     Expiration,
                                                     Value,
                                                     State#state.json_mode)) of
                              CAS when is_integer(CAS) ->
                                  #mc_response{cas=CAS};
                              Error ->
                                  ?LOG_INFO("Error persisting=~p.", [Error]),
                                  Message = io_lib:format("~p", [Error]),
                                  #mc_response{status=?EINTERNAL,
                                               body=Message}
                          end
                  end,
                  VBucket, Opaque, Socket, State).

handle_delq_call(VBucket, Key, _CAS, Opaque, Socket, State) ->
    do_batch_item(?DELETEQ,
                  fun(Db) ->
                          case catch(mc_couch_kv:delete(Db, Key)) of
                              ok ->
                                  #mc_response{};
                              not_found ->
                                  #mc_response{status=?KEY_ENOENT};
                              Error ->
                                  Message = io_lib:format("~p", [Error]),
                                  #mc_response{status=?EINTERNAL,
                                               body=Message}
                          end
                  end,
                  VBucket, Opaque, Socket, State).

handle_delete_call(Db, Key) ->
    case mc_couch_kv:delete(Db, Key) of
        ok -> #mc_response{};
        not_found -> #mc_response{status=1, body="Not found"}
    end.

delete_db(Key) ->
    lists:foreach(fun(N) ->
                          DbName = lists:flatten(io_lib:format("~s/~p",
                                                               [Key, N])),
                          couch_server:delete(list_to_binary(DbName), [])
                  end, lists:seq(0, 1023)).

processing({?GET, VBucket, <<>>, Key, <<>>, _CAS}, _From, State) ->
    with_open_db(fun(Db) -> {reply, handle_get_call(Db, Key), processing, State} end,
                 VBucket, State);
processing({?GET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?SET, VBucket, <<Flags:32, Expiration:32>>, Key, Value, _CAS},
           _From, State) ->
    with_open_db(fun(Db) -> {reply, handle_set_call(Db, Key, Flags,
                                                    Expiration, Value,
                                                    State#state.json_mode),
                             processing, State}
                 end, VBucket, State);
processing({?SET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?DELETE, VBucket, <<>>, Key, <<>>, _CAS}, _From, State) ->
    with_open_db(fun(Db) -> {reply, handle_delete_call(Db, Key), processing, State} end,
                 VBucket, State);
processing({?DELETE, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?DELETE_BUCKET, _VBucket, <<>>, Key, <<>>, 0}, _From, State) ->
    delete_db(Key),
    {reply, #mc_response{body="Done!"}, processing, State};
processing({?DELETE_BUCKET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?SELECT_BUCKET, _VBucket, <<>>, Name, <<>>, 0}, _From, State) ->
    {reply, #mc_response{}, processing, State#state{db=Name}};
processing({?SELECT_BUCKET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?SET_VBUCKET_STATE, VBucket, <<VBState:32>>, <<>>, <<>>, 0}, _From, State) ->
    mc_couch_vbucket:handle_set_state(VBucket, VBState, State);
processing({?SET_VBUCKET_STATE, _, _, _, _, _} = Msg, _From, State) ->
    ?LOG_INFO("Error handling set vbucket state: ~p.", [Msg]),
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?DELETE_VBUCKET, VBucket, <<>>, <<>>, <<>>, 0}, _From, State) ->
    {reply, mc_couch_vbucket:handle_delete(VBucket, State), processing, State};
processing({?DELETE_VBUCKET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({OpCode, VBucket, Header, Key, Body, CAS}, _From, State) ->
    ?LOG_INFO("MC daemon: got unhandled call: ~p/~p/~p/~p/~p/~p.",
               [OpCode, VBucket, Header, Key, Body, CAS]),
    {reply, #mc_response{status=?UNKNOWN_COMMAND, body="WTF, mate?"}, processing, State};
processing(Msg, _From, _State) ->
    ?LOG_INFO("Got unknown thing in processing/3: ~p", [Msg]),
    exit("WTF").

processing({?STAT, _Extra, <<"vbucket">>, _Body, _CAS, Socket, Opaque}, State) ->
    mc_couch_vbucket:handle_stats(Socket, Opaque, State),
    {next_state, processing, State};
processing({?TAP_CONNECT, Extra, _Key, Body, _CAS, Socket, Opaque}, State) ->
    mc_tap:run(State, Opaque, Socket, Extra, Body),
    {next_state, processing, State};
processing({?STAT, _Extra, _Key, _Body, _CAS, Socket, Opaque}, State) ->
    mc_couch_stats:stats(Socket, Opaque),
    {next_state, processing, State};
processing({?NOOP, Socket, Opaque}, State) ->
    mc_connection:respond(Socket, ?NOOP, Opaque, #mc_response{}),
    {next_state, processing, State};
processing({?SETQ, VBucket, <<Flags:32, Expiration:32>>, Key, Value,
             CAS, Socket, Opaque}, State) ->
    {next_state, batching, handle_setq_call(VBucket, Key, Flags, Expiration, Value,
                                            CAS, Opaque, Socket, State)};
processing({?DELETEQ, VBucket, <<>>, Key, <<>>,
             CAS, Socket, Opaque}, State) ->
    {next_state, batching, handle_delq_call(VBucket, Key, CAS, Opaque, Socket, State)};
processing(Msg, _State) ->
    ?LOG_INFO("Got unknown thing in processing/2: ~p", [Msg]),
    exit("WTF").

%%
%% Batch stuff
%%

batching({?SETQ, VBucket, <<Flags:32, Expiration:32>>, Key, Value,
             CAS, Socket, Opaque}, State) ->
    {next_state, batching, handle_setq_call(VBucket, Key, Flags, Expiration, Value,
                                            CAS, Opaque, Socket, State)};
batching({?DELETEQ, VBucket, <<>>, Key, <<>>,
             CAS, Socket, Opaque}, State) ->
    {next_state, batching, handle_delq_call(VBucket, Key, CAS, Opaque, Socket, State)};
batching({item_complete, Cmd, Opaque, Socket, Res}, State) ->
    respond_if_fail(Socket, Cmd, Opaque, Res),
    {next_state, batching, State#state{batch_ops=State#state.batch_ops - 1}};
batching({?NOOP, _Socket, Opaque}, State) ->
    {next_state, committing, State#state{terminal_opaque=Opaque}};
batching(Msg, _State) ->
    ?LOG_INFO("Got unknown thing in batching/2: ~p", [Msg]),
    exit("WTF").

%%
%% Committing a transaction
%%

respond_if_fail(_Socket, _Op, _Opaque, _Res=#mc_response{status=?SUCCESS}) ->
    ok;
respond_if_fail(Socket, Op, Opaque, Res) ->
    mc_connection:respond(Socket, Op, Opaque, Res).

committing({item_complete, Cmd, Opaque, Socket, Res}, State=#state{batch_ops=1}) ->
    respond_if_fail(Socket, Cmd, Opaque, Res),
    mc_connection:respond(Socket, ?NOOP, State#state.terminal_opaque, #mc_response{}),
    {next_state, processing, State#state{batch_ops=0}};
committing({item_complete, Cmd, Opaque, Socket, Res}, State) ->
    respond_if_fail(Socket, Cmd, Opaque, Res),
    {next_state, committing, State#state{batch_ops=State#state.batch_ops - 1}};
committing(Msg, _State) ->
    ?LOG_INFO("Got unknown thing in committing/2: ~p", [Msg]),
    exit("WTF").

%% Everything else

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.
