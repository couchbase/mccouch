-module(mc_daemon).

-behaviour(gen_fsm).

%% API
-export([start_link/1]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

%% My states
-export([processing/2, processing/3, batching/3]).

%% Kind of ugly to export these, but modularity win.
-export([with_open_db/3, db_name/2, db_prefix/1]).

-define(SERVER, ?MODULE).

-include("couch_db.hrl").
-include("mc_constants.hrl").

-record(state, {
          db,
          json_mode = true,
          batch_ops = 0,
          terminal_opaque = nil,
          errors = [],
          worker_batch_size,
          batch,
          batch_size = 0,
          max_workers,
          workers = [],
          caller = nil,
          socket
         }).

start_link(Socket) ->
    gen_fsm:start_link(?MODULE, Socket, []).

init(Socket) ->
    process_flag(trap_exit, true),
    WorkerBatchSize = list_to_integer(
                        couch_config:get("mc_couch", "write_worker_batch_size", "100")),
    MaxWorkers = list_to_integer(
                   couch_config:get("mc_couch", "write_workers", "4")),
    {ok, processing, #state{db = <<"default">>,
                            socket = Socket,
                            worker_batch_size = WorkerBatchSize,
                            max_workers = MaxWorkers
                           }}.

db_name(VBucket, State)->
    iolist_to_binary([State#state.db, $/, integer_to_list(VBucket)]).

db_prefix(State) -> State#state.db.

with_open_db(F, VBucket, State) ->
    case couch_db:open(db_name(VBucket, State), []) of
        {ok, Db} ->
            try
                F(Db)
            after
                couch_db:close(Db)
            end;
        Other ->
            ?LOG_ERROR("MC daemon: Error opening vb ~p in ~p: ~p",
                       [VBucket, db_prefix(State), Other]),
            throw({open_db_error, db_prefix(State), VBucket, Other})
    end.

with_open_db(F, VBucket, State, Def) ->
    case catch(with_open_db(F, VBucket, State)) of
        {open_db_error, _Prefix, VBucket, _Other} -> Def;
        X -> X
    end.

with_open_db_or_einval(F, VBucket, State) ->
    with_open_db(F, VBucket, State,
                 {reply,
                  #mc_response{status=?EINVAL, body= <<"Error opening DB">>},
                  processing, State}).

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

handle_delete_call(Db, Key) ->
    case mc_couch_kv:delete(Db, Key) of
        ok -> #mc_response{};
        not_found -> #mc_response{status=1, body="Not found"}
    end.

delete_db(State, Key) ->
    lists:map(fun({N, _VBucketState} = VBucketAndState) ->
                      DbName = lists:flatten(io_lib:format("~s/~p",
                                                               [Key, N])),
                      couch_server:delete(list_to_binary(DbName), []),
                      VBucketAndState
              end, mc_couch_vbucket:list_vbuckets(State)).

create_async_batch(State, VBucket, Opaque, Op, Doc) ->
    Batch = dict:append(VBucket, {Opaque, Op, Doc}, dict:new()),
    State#state{
      batch = Batch,
      batch_size = 1,
      workers = [],
      caller = nil,
      terminal_opaque = nil
     }.

processing({?SETQ = Op, VBucket, <<Flags:32, Expiration:32>>, Key, Value,
            _CAS, Opaque}, From, State) ->
    gen_fsm:reply(From, ok),
    Doc = mc_couch_kv:mk_doc(Key, Flags, Expiration, Value, State#state.json_mode),
    NewState = create_async_batch(State, VBucket, Opaque, Op, Doc),
    {next_state, batching, NewState};

processing({?DELETEQ = Op, VBucket, <<>>, Key, <<>>, _CAS, Opaque}, From, State) ->
    gen_fsm:reply(From, ok),
    Doc = #doc{id = Key, deleted = true, body = {[]}},
    NewState = create_async_batch(State, VBucket, Opaque, Op, Doc),
    {next_state, batching, NewState};

processing({?GET, VBucket, <<>>, Key, <<>>, _CAS}, _From, State) ->
    with_open_db_or_einval(fun(Db) -> {reply, handle_get_call(Db, Key), processing, State} end,
                           VBucket, State);
processing({?GET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?SET, VBucket, <<Flags:32, Expiration:32>>, Key, Value, _CAS},
           _From, State) ->
    with_open_db_or_einval(fun(Db) -> {reply, handle_set_call(Db, Key, Flags,
                                                              Expiration, Value,
                                                              State#state.json_mode),
                             processing, State}
                 end, VBucket, State);
processing({?SET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?DELETE, VBucket, <<>>, Key, <<>>, _CAS}, _From, State) ->
    with_open_db_or_einval(fun(Db) -> {reply, handle_delete_call(Db, Key), processing, State} end,
                           VBucket, State);
processing({?DELETE, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?DELETE_BUCKET, _VBucket, <<>>, Key, <<>>, 0}, _From, State) ->
    delete_db(State, Key),
    {reply, #mc_response{body="Done!"}, processing, State};
processing({?DELETE_BUCKET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?SELECT_BUCKET, _VBucket, <<>>, Name, <<>>, 0}, _From, State) ->
    {reply, #mc_response{}, processing, State#state{db=Name}};
processing({?SELECT_BUCKET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?DELETE_VBUCKET, VBucket, <<>>, <<>>, <<>>, 0}, _From, State) ->
    {reply, mc_couch_vbucket:handle_delete(VBucket, State), processing, State};
processing({?DELETE_VBUCKET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?FLUSH, _, _, _, _, _}, _From, State) ->
    ?LOG_INFO("FLUSHING ALL THE THINGS!", []),
    lists:foreach(fun({VB, VBState}) ->
                          mc_couch_vbucket:set_vbucket(VB, VBState, State)
                  end, delete_db(State, State#state.db)),
    gen_event:notify(mc_couch_events,
                     {flush_all, binary_to_list(State#state.db)}),
    {reply, #mc_response{}, processing, State};
processing({OpCode, VBucket, Header, Key, Body, CAS}, _From, State) ->
    ?LOG_INFO("MC daemon: got unhandled call: ~p/~p/~p/~p/~p/~p.",
               [OpCode, VBucket, Header, Key, Body, CAS]),
    {reply, #mc_response{status=?UNKNOWN_COMMAND, body="WTF, mate?"}, processing, State};
processing({?NOOP, _Opaque}, _From, State) ->
    {reply, #mc_response{},  processing, State};
processing(Msg, _From, _State) ->
    ?LOG_INFO("Got unknown thing in processing/3: ~p", [Msg]),
    exit("WTF").

processing({?STAT, _Extra, <<"vbucket">>, _Body, _CAS, Opaque}, State) ->
    mc_couch_vbucket:handle_stats(State#state.socket, Opaque, State),
    {next_state, processing, State};
processing({?TAP_CONNECT, Extra, _Key, Body, _CAS, Opaque}, State) ->
    mc_tap:run(State, Opaque, State#state.socket, Extra, Body),
    {next_state, processing, State};
processing({?STAT, _Extra, _Key, _Body, _CAS, Opaque}, State) ->
    mc_couch_stats:stats(State#state.socket, Opaque),
    {next_state, processing, State};
processing({?NOOP, Opaque}, State) ->
    mc_connection:respond(State#state.socket, ?NOOP, Opaque, #mc_response{}),
    {next_state, processing, State};
processing(Msg, _State) ->
    ?LOG_INFO("Got unknown thing in processing/2: ~p", [Msg]),
    exit("WTF").

%%
%% Batch stuff
%%

add_async_job(State, From, VBucket, Opaque, Op, Doc) ->
    #state{
          batch = Batch, batch_size = BatchSize,
          worker_batch_size = WorkerBatchSize, workers = Workers,
          max_workers = MaxWorkers
    } = State,
    case (BatchSize < WorkerBatchSize) orelse (length(Workers) < MaxWorkers) of
        true ->
            gen_fsm:reply(From, ok);
        false ->
            ok
    end,
    Batch2 = dict:append(VBucket, {Opaque, Op, Doc}, Batch),
    BatchSize2 = BatchSize + 1,
    State2 = State#state{batch = Batch2, batch_size = BatchSize2},
    case BatchSize2 >= WorkerBatchSize of
        true ->
            case (length(Workers) >= MaxWorkers) of
                true ->
                    State2#state{caller = From};
                false ->
                    maybe_start_worker(State2)
            end;
        false ->
            State2
    end.

batching({?SETQ = Op, VBucket, <<Flags:32, Expiration:32>>, Key, Value,
          _CAS, Opaque}, From, State) ->
    Doc = mc_couch_kv:mk_doc(Key, Flags, Expiration, Value, State#state.json_mode),
    NewState = add_async_job(State, From, VBucket, Opaque, Op, Doc),
    {next_state, batching, NewState};

batching({?DELETEQ = Op, VBucket, <<>>, Key, <<>>, _CAS, Opaque}, From, State) ->
    Doc = #doc{id = Key, deleted = true, body = {[]}},
    NewState = add_async_job(State, From, VBucket, Opaque, Op, Doc),
    {next_state, batching, NewState};

batching({?NOOP, Opaque}, From, State) ->
    #state{
          batch = Batch, batch_size = BatchSize, workers = Workers, socket = Socket
    } = State,
    case BatchSize > 0 of
        true ->
            update_docs(Batch, State#state.db, Socket);
        false ->
            ok
    end,
    case Workers of
        [] ->
            mc_connection:respond(Socket, ?NOOP, Opaque, #mc_response{}),
            gen_fsm:reply(From, ok),
            {next_state, processing, State};
        _ ->
            NewState = State#state{terminal_opaque = Opaque, caller = From},
            {next_state, batch_ending, NewState}
    end.

%% Everything else

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event({?SET_VBUCKET_STATE, VBucket, <<VBState:32>>, <<>>, <<>>, 0},
                  _From, _StateName, State) ->
    mc_couch_vbucket:handle_set_state(VBucket, VBState, State);
handle_sync_event({?SET_VBUCKET_STATE, _, _, _, _, _} = Msg, _From, _StateName, State) ->
    ?LOG_INFO("Error handling set vbucket state: ~p.", [Msg]),
    {reply, #mc_response{status=?EINVAL}, processing, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

maybe_start_worker(#state{batch_size = 0} = State) ->
    State;
maybe_start_worker(State) ->
    #state{
            workers = Workers, batch = Batch, socket = Socket
          } = State,
    WorkerPid = spawn_link(fun() ->
                                   update_docs(Batch, State#state.db, Socket)
                           end),
    State#state{
      workers = [WorkerPid | Workers],
      batch = dict:new(),
      batch_size = 0
     }.

handle_info({'EXIT', Pid, normal}, batching, State) ->
    #state{
            workers = Workers, caller = From
    } = State,
    case Workers -- [Pid] of
        Workers ->
            {next_state, batching, State};
        Workers2 ->
        case From =:= nil of
            true ->
                {next_state, batching, State#state{workers = Workers2}};
            false ->
                gen_fsm:reply(From, ok),
                NewState = maybe_start_worker(State#state{workers = Workers2}),
                {next_state, batching, NewState#state{caller = nil}}
        end
    end;

handle_info({'EXIT', Pid, normal}, batch_ending, State) ->
    #state{
            workers = Workers, caller = From,
            terminal_opaque = Opaque, socket = Socket
    } = State,
    case Workers -- [Pid] of
        Workers ->
            {next_state, batch_ending, State};
        Workers2 ->
            case Workers2 of
                [] ->
                    mc_connection:respond(Socket, ?NOOP, Opaque, #mc_response{}),
                    gen_fsm:reply(From, ok),
                    {next_state, processing, State};
                _ ->
                    {next_state, batch_ending, State#state{workers = Workers2}}
            end
    end;

handle_info({'EXIT', _Pid, normal}, StateName, State) ->
    % tap stream process terminated successfully
    {next_state, StateName, State}.

terminate(_Reason, _StateName, State) ->
    gen_tcp:close(State#state.socket),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.


update_docs(Batch, BucketName, Socket) ->
    dict:fold(
        fun(VBucketId, Docs, _Acc) ->
            DbName = iolist_to_binary([<<BucketName/binary, $/>>, integer_to_list(VBucketId)]),
            case couch_db:open_int(DbName, []) of
            {ok, Db} ->
                {ok, Results} = couch_db:update_docs(
                    Db, [Doc || {_Opaque, _Op, Doc} <- Docs], [clobber]),
                lists:foreach(
                    fun({{ok, _}, _}) ->
                            ok;
                        ({Error, {Opaque, Op, #doc{id = Key}}}) ->
                            ErrorResp = #mc_response{
                                status = ?EINTERNAL,
                                body = io_lib:format(
                                    "Error persisting key ~s in database ~s: ~p",
                                    [Key, DbName, Error])
                            },
                            mc_connection:respond(Socket, Op, Opaque, ErrorResp)
                    end,
                    lists:zip(Results, Docs)),
                couch_db:close(Db);
            Error ->
                ErrorResp = #mc_response{
                    status = ?EINVAL,
                    body = io_lib:format("Error opening database ~s: ~s",
                        [DbName, couch_util:to_binary(Error)])
                },
                lists:foreach(
                    fun({Opaque, Op, _Doc}) ->
                        mc_connection:respond(Socket, Op, Opaque, ErrorResp)
                    end,
                    Docs)
            end
        end,
        [], Batch).
