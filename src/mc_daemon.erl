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
          next_vb_batch,
          current_vbucket,
          current_vbucket_list = [],
          batched_vbuckets = [],
          max_workers = 4,
          worker_sup,
          caller = nil,
          worker_refs = [],
          socket
         }).

start_link(Socket) ->
    gen_fsm:start_link(?MODULE, Socket, []).

init(Socket) ->
    link(Socket),
    {ok, WorkerSup} = mc_batch_sup:start_link(),
    {ok, processing, #state{db = <<"default">>,
                            socket = Socket,
                            worker_sup = WorkerSup
                           }}.

db_name(VBucket, Prefix) when is_binary(Prefix) ->
    iolist_to_binary([Prefix, $/, integer_to_list(VBucket)]);
db_name(VBucket, State)->
    db_name(VBucket, db_prefix(State)).

db_prefix(State) -> State#state.db.

with_open_db(F, VBucket, Prefix) when is_binary(Prefix) ->
    case couch_db:open(db_name(VBucket, Prefix), []) of
        {ok, Db} ->
            try
                F(Db)
            after
                couch_db:close(Db)
            end;
        Other ->
            ?LOG_ERROR("MC daemon: Error opening vb ~p in ~p: ~p",
                       [VBucket, Prefix, Other]),
            throw({open_db_error, Prefix, VBucket, Other})
    end;
with_open_db(F, VBucket, State) ->
    with_open_db(F, VBucket, db_prefix(State)).

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
        {ok, Flags, _Expiration, Data} ->
            FlagsBin = <<Flags:32>>,
            #mc_response{extra=FlagsBin, body=Data};
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
    ok = mc_couch_kv:delete(Db, Key).

delete_db(State, Key) ->
    lists:map(fun({N, _VBucketState} = VBucketAndState) ->
                      DbName = lists:flatten(io_lib:format("~s/~p",
                                                               [Key, N])),
                      couch_server:delete(list_to_binary(DbName), []),
                      VBucketAndState
              end, mc_couch_vbucket:list_vbuckets(State)).

create_async_batch(State, VBucket, Opaque, Op, Job) ->
    State#state{
      current_vbucket = VBucket,
      current_vbucket_list = [{Opaque, Op, Job}],
      batched_vbuckets = [VBucket],
      caller = nil,
      terminal_opaque = nil
     }.

processing({?SETQ = Op, VBucket, <<Flags:32, Expiration:32>>, Key, Value,
            _CAS, Opaque}, From, State) ->
    gen_fsm:reply(From, ok),
    NewState = create_async_batch(State, VBucket, Opaque, Op,
                                 {set, Key, Flags, Expiration, Value, State#state.json_mode}),
    {next_state, batching, NewState};

processing({?SETQWITHMETA = Op, VBucket, <<MetaDataLen:32, Flags:32, Expiration:32>>, ValueLen,
            Key, Value, _CAS, Opaque}, From, State) ->
    ValueLen2 = ValueLen - MetaDataLen,
    case Value of
        <<Value2:ValueLen2/binary, MetaData:MetaDataLen/binary>> ->
            gen_fsm:reply(From, ok),
            NewState = create_async_batch(State, VBucket, Opaque, Op,
                {set, Key, Flags, Expiration, Value2, MetaData, State#state.json_mode}),
            {next_state, batching, NewState};
        _ ->
            {reply, #mc_response{status=?EINVAL}, processing, State}
    end;

processing({?DELETEQ = Op, VBucket, <<>>, Key, <<>>, _CAS, Opaque}, From, State) ->
    gen_fsm:reply(From, ok),
    NewState = create_async_batch(State, VBucket, Opaque, Op,
                                  {delete, Key}),
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

    BucketStr = binary_to_list(State#state.db),

    gen_event:sync_notify(mc_couch_events, {pre_flush_all, BucketStr}),
    lists:foreach(fun({VB, VBState}) ->
                          {Parsed} = ?JSON_DECODE(VBState),
                          StateName = proplists:get_value(<<"state">>, Parsed),
                          mc_couch_vbucket:set_vbucket(VB, StateName, 0, State)
                  end, delete_db(State, State#state.db)),
    gen_event:sync_notify(mc_couch_events, {post_flush_all, BucketStr}),
    {reply, #mc_response{}, processing, State};
processing({OpCode, VBucket, Header, Key, Body, CAS}, _From, State) ->
    ?LOG_INFO("MC daemon: got unhandled call: ~p/~p/~p/~p/~p/~p.",
               [OpCode, VBucket, Header, Key, Body, CAS]),
    {reply, #mc_response{status=?UNKNOWN_COMMAND, body="WTF, mate?"}, processing, State};
processing({?NOOP, Opaque}, _From, State) ->
    mc_connection:respond(State#state.socket, ?NOOP, Opaque, #mc_response{}),
    {reply, ok, processing, State};
processing(Msg, _From, _State) ->
    ?LOG_INFO("Got unknown thing in processing/3: ~p", [Msg]),
    exit("WTF").

processing({?STAT, _Extra, <<"vbucket">>, _Body, _CAS, Opaque}, State) ->
    mc_couch_vbucket:handle_stats(State#state.socket, Opaque, State),
    {next_state, processing, State};
processing({?STAT, _Extra, <<"db">>, _Body, _CAS, Opaque}, State) ->
    mc_couch_vbucket:handle_db_stats(State#state.socket, Opaque, State),
    {next_state, processing, State};
processing({?STAT, _Extra, _Key, _Body, _CAS, Opaque}, State) ->
    mc_couch_stats:stats(State#state.socket, Opaque),
    {next_state, processing, State};
processing({?TAP_CONNECT, Extra, _Key, Body, _CAS, Opaque}, State) ->
    mc_tap:run(State, Opaque, State#state.socket, Extra, Body),
    {next_state, processing, State};
processing(Msg, _State) ->
    ?LOG_INFO("Got unknown thing in processing/2: ~p", [Msg]),
    exit("WTF").

%%
%% Batch stuff
%%

num_workers(State) ->
    #state{worker_refs = WorkerRefs} = State,
    length(WorkerRefs).

add_async_job(State, From, VBucket, Opaque, Op, Job) ->
    #state{
          current_vbucket = CurrentVBucket,
          current_vbucket_list = CurrentList,
          max_workers = MaxWorkers
    } = State,

    if VBucket == CurrentVBucket ->
        gen_fsm:reply(From, ok),
        CurrentList2 = [{Opaque, Op, Job} | CurrentList],
        NewState = State#state{current_vbucket_list = CurrentList2},
        NewState;
    true ->
        State2 = State#state{
            next_vb_batch = {CurrentVBucket, CurrentList},
            current_vbucket = VBucket,
            current_vbucket_list = [{Opaque, Op, Job}],
            batched_vbuckets = [VBucket | State#state.batched_vbuckets]
        },
        case (num_workers(State) >= MaxWorkers) of
            true ->
                State2#state{caller = From};
            false ->
                gen_fsm:reply(From, ok),
                maybe_start_worker(State2)
        end
    end.

batching({?SETQ = Op, VBucket, <<Flags:32, Expiration:32>>, Key, Value,
          _CAS, Opaque}, From, State) ->
    NewState = add_async_job(State, From, VBucket, Opaque, Op,
                             {set, Key, Flags, Expiration, Value, State#state.json_mode}),
    {next_state, batching, NewState};

batching({?SETQWITHMETA = Op, VBucket, <<MetaDataLen:32, Flags:32, Expiration:32>>, ValueLen,
          Key, Value, _CAS, Opaque}, From, State) ->
    ValueLen2 = ValueLen - MetaDataLen,
    case Value of
        <<Value2:ValueLen2/binary, MetaData:MetaDataLen/binary>> ->
            NewState = add_async_job(State, From, VBucket, Opaque, Op,
                {set, Key, Flags, Expiration, Value2, MetaData, State#state.json_mode}),
            {next_state, batching, NewState};
        _ ->
            {reply, #mc_response{status=?EINVAL}, batching, State}
    end;

batching({?DELETEQ = Op, VBucket, <<>>, Key, <<>>, _CAS, Opaque}, From, State) ->
    NewState = add_async_job(State, From, VBucket, Opaque, Op,
                             {delete, Key}),
    {next_state, batching, NewState};

batching({?NOOP, Opaque}, From, State) ->
    #state{
          socket = Socket, current_vbucket = CurrentVBucket,
          current_vbucket_list = CurrentList,
          batched_vbuckets = BatchedVBuckets
    } = State,
    State2 = maybe_start_worker(State#state{next_vb_batch = {CurrentVBucket, CurrentList},
                                            current_vbucket = 0, current_vbucket_list = []}),
    case num_workers(State2) of
        0 ->
            ensure_full_commit(State#state.db, BatchedVBuckets),
            mc_connection:respond(Socket, ?NOOP, Opaque, #mc_response{}),
            gen_fsm:reply(From, ok),
            {next_state, processing, State2};
        _ ->
            {next_state, batch_ending, State2#state{terminal_opaque = Opaque, caller = From}}
    end.

%% Everything else

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event({?SET_VBUCKET_STATE, VBucket, <<VBState:32>>, <<>>, <<>>, 0},
                  _From, _StateName, State) ->
    mc_couch_vbucket:handle_set_state(VBucket, VBState, 0, State),
    {reply, #mc_response{}, processing, State};
handle_sync_event({?SET_VBUCKET_STATE, _, _, _, _, _} = Msg, _From, _StateName, State) ->
    ?LOG_ERROR("Error handling set vbucket state: ~p.", [Msg]),
    {reply, #mc_response{status=?EINVAL}, processing, State};
handle_sync_event({?SNAPSHOT_VB_STATES, <<>>, _} = Msg, _From, _StateName, State) ->
    ?LOG_ERROR("Empty body in snapshot vb states: ~p", [Msg]),
    {reply, #mc_response{status=?EINVAL}, processing, State};
handle_sync_event({?SNAPSHOT_VB_STATES, Body, BodyLen},
                  _From, _StateName, State) ->
    case byte_size(Body) == BodyLen of
    true ->
        mc_couch_vbucket:handle_snapshot_states(Body, State),
        {reply, #mc_response{}, processing, State};
    false ->
        ?LOG_ERROR("Body length mismatch in snapshot vb states: ~p, ~p",
                   [byte_size(Body), BodyLen]),
        {reply, #mc_response{status=?EINVAL}, processing, State}
    end;
handle_sync_event({?VBUCKET_BATCH_COUNT, <<>>} = Msg, _From, StateName, State) ->
    ?LOG_ERROR("Missing batch count value in VBUCKET_BATCH_COUNT command: ~p", [Msg]),
    {reply, #mc_response{status=?EINVAL}, StateName, State};
handle_sync_event({?VBUCKET_BATCH_COUNT, <<BatchCounter:32>>},
                  _From, StateName, State) ->
   case BatchCounter =< 0 of
   true ->
       ?LOG_ERROR("Invalid batch count value in VBUCKET_BATCH_COUNT command: ~p",
                  [BatchCounter]),
       {reply, #mc_response{status=?EINVAL}, StateName, State};
   false ->
       NewState = State#state{max_workers=BatchCounter},
       {reply, #mc_response{}, StateName, NewState}
   end;
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

maybe_start_worker(State) ->
    #state{
            worker_sup = WorkerSup, socket = Socket,
            worker_refs = WorkerRefs,
            next_vb_batch = {VBucket, ItemList}
          } = State,
    case ItemList /= [] of
        true ->
            {ok, WorkerRef} = mc_batch_sup:start_worker(WorkerSup, VBucket, ItemList,
                                                        State#state.db, Socket),
            State#state{
                next_vb_batch = {VBucket, []},
                worker_refs = [WorkerRef|WorkerRefs]
            };
        false ->
            State
    end.

ensure_full_commit(_BucketName, []) ->
    ok;
ensure_full_commit(BucketName, [VBucket|Rest]) ->
    DbName = iolist_to_binary([<<BucketName/binary, $/>>,
                              integer_to_list(VBucket)]),
    case couch_db:open_int(DbName, []) of
        {ok, Db} ->
            couch_db:ensure_full_commit(Db),
            couch_db:close(Db);
        {not_found,no_db_file} ->
            error_logger:info_msg("~s vbucket ~p file deleted or missing.~n",
                [BucketName, VBucket]),
            ok
        end,
    ensure_full_commit(BucketName, Rest).

handle_info({'DOWN', Ref, process, _Pid, normal}, batching, State) ->
    #state{caller = From, worker_refs = WorkerRefs} = State,
    case From =:= nil of
        true ->
            {next_state, batching, State#state{worker_refs = WorkerRefs -- [Ref]}};
        false ->
            gen_fsm:reply(From, ok),
            NewState = maybe_start_worker(State),
            #state{worker_refs = NewWorkerRefs} = NewState,
            {next_state, batching, NewState#state{caller = nil, worker_refs = NewWorkerRefs -- [Ref]}}
    end;

handle_info({'DOWN', Ref, process, _Pid, normal}, batch_ending, State) ->
    #state{
            caller = From, terminal_opaque = Opaque, socket = Socket,
            worker_refs = WorkerRefs, batched_vbuckets = BatchedVBuckets
          } = State,
    WorkerRefs2 = WorkerRefs -- [Ref],
    case length(WorkerRefs2) of
        0 ->
            ensure_full_commit(State#state.db, BatchedVBuckets),
            mc_connection:respond(Socket, ?NOOP, Opaque, #mc_response{}),
            gen_fsm:reply(From, ok),
            {next_state, processing, State#state{worker_refs = WorkerRefs2,
                                                 batched_vbuckets = []}};
        _ ->
            {next_state, batch_ending, State#state{worker_refs = WorkerRefs2}}
    end.

terminate(_Reason, _StateName, State) ->
    gen_tcp:close(State#state.socket),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

