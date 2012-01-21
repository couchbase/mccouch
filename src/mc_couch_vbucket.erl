-module(mc_couch_vbucket).

-export([get_state/2,
         set_vbucket/4,
         handle_delete/2,
         handle_stats/3,
         handle_db_stats/3,
         handle_set_state/4,
         handle_snapshot_states/2,
         list_vbuckets/1]).

-include("couch_db.hrl").
-include("mc_constants.hrl").

get_state(VBucket, Prefix) when is_binary(Prefix) ->
    mc_daemon:with_open_db(fun(Db) ->
                                   case mc_couch_kv:get(Db, <<"_local/vbstate">>) of
                                       {ok, _Flags, _Expiration, StateDoc} ->
                                           StateDoc;
                                       not_found ->
                                           <<"{\"state\": \"dead\", \"checkpoint_id\": \"0\"}">>
                                   end
                           end,
                           VBucket, Prefix);

get_state(VBucket, State) ->
    get_state(VBucket, mc_daemon:db_prefix(State)).

set_vbucket(VBucket, StateName, CheckpointId, State) ->
    DbName = mc_daemon:db_name(VBucket, State),
    Options = [{user_ctx, #user_ctx{roles = [<<"_admin">>]}}],
    {ok, Db} = case couch_db:create(DbName, Options) of
                   {ok, D} ->
                       ok = couch_db:set_revs_limit(D, 1),
                       {ok, D};
                   _ ->
                       couch_db:open(DbName, Options)
               end,
    CheckpointId2 = integer_to_list(CheckpointId),
    StateJson = iolist_to_binary(["{\"state\": \"", StateName,
                                  "\", \"checkpoint_id\": \"",
                                  CheckpointId2, "\"}"]),

    Bucket = binary_to_list(mc_daemon:db_prefix(State)),
    StateAtom = erlang:binary_to_atom(StateName, latin1),

    mc_couch_kv:set(Db, <<"_local/vbstate">>, 0, 0,
                    StateJson, true),
    couch_db:close(Db),

    gen_event:sync_notify(mc_couch_events,
                          {set_vbucket,
                           Bucket, VBucket, StateAtom, CheckpointId}).

handle_delete(VBucket, State) ->
    DbName = mc_daemon:db_name(VBucket, State),
    couch_server:delete(DbName, []),
    #mc_response{}.


vbucket_from_name(DBPrefix, Len, Name, State) ->
    case catch binary:split(Name, <<$/>>, [{scope, {Len,size(Name)-Len}}]) of
        [DBPrefix, VB] ->
            VBStr = binary_to_list(VB),
            case catch list_to_integer(VBStr) of
                VBInt when is_integer(VBInt) ->
                    StatVal = get_state(VBInt, State),
                    {VBInt, StatVal};
                _ ->
                    invalid_vbucket
            end;
         _ ->
             invalid_vbucket
    end.

list_vbuckets(State) ->

    {ok, DBs} = couch_server:all_databases(),
    DBPrefix = mc_daemon:db_prefix(State),
    Len = size(DBPrefix),

    VBuckets = fun(Name, Acc) ->
        case vbucket_from_name(DBPrefix, Len, Name, State) of
            {Int, Stat} ->
                [{Int, Stat} | Acc];
            invalid_vbucket ->
                Acc
        end
    end,

    lists:foldl(VBuckets, [], DBs).

handle_db_stats(Socket, Opaque, State) ->
    lists:foreach(fun({VBInt, _}) ->
                          {ok, Info} = mc_daemon:with_open_db(fun couch_db:get_db_info/1,
                                                              VBInt, State),
                          lists:foreach(fun({K, V}) ->
                                                StatKey = io_lib:format("vb:~p:~p", [VBInt, K]),
                                                mc_connection:respond(Socket, ?STAT, Opaque,
                                                                      mc_couch_stats:mk_stat(StatKey, V))
                                        end, Info)
                  end, list_vbuckets(State)),
    mc_connection:respond(Socket, ?STAT, Opaque,
                          mc_couch_stats:mk_stat("", "")).

handle_stats(Socket, Opaque, State) ->
    lists:foreach(fun({VBInt, V}) ->
                          StatKey = io_lib:format("vb_~p", [VBInt]),
                          mc_connection:respond(Socket, ?STAT, Opaque,
                                                mc_couch_stats:mk_stat(StatKey, V))
                  end, list_vbuckets(State)),
    mc_connection:respond(Socket, ?STAT, Opaque,
                          mc_couch_stats:mk_stat("", "")).

handle_set_state(VBucket, ?VB_STATE_ACTIVE, CheckpointId, State) ->
    set_vbucket(VBucket, <<"active">>, CheckpointId, State);
handle_set_state(VBucket, ?VB_STATE_REPLICA, CheckpointId, State) ->
    set_vbucket(VBucket, <<"replica">>, CheckpointId, State);
handle_set_state(VBucket, ?VB_STATE_PENDING, CheckpointId, State) ->
    set_vbucket(VBucket, <<"pending">>, CheckpointId, State);
handle_set_state(VBucket, ?VB_STATE_DEAD, CheckpointId, State) ->
    set_vbucket(VBucket, <<"dead">>, CheckpointId, State).

%% couch_db:create for lots of databases takes a lot of time
%% (apparently, due to lots of fsync calls during couch database
%% creation). This function parallelizes vbucket databases creation in
%% order to make it quick.
quickly_create_missing_vbuckets(VBuckets, State) ->
    SpawnPreparer =
        fun (VBucket) ->
                DbName = mc_daemon:db_name(VBucket, State),
                Options = [{user_ctx, #user_ctx{roles = [<<"_admin">>]}}],
                case couch_db:open(DbName, Options) of
                    {ok, D} ->
                        couch_db:close(D),
                        %% if db is present, do nothing
                        fun () -> ok end;
                    _ ->
                        %% if db is missing, create it in separate
                        %% process and return function that'll wait
                        %% for it's completion
                        Pid = spawn(
                                fun () ->
                                        case couch_db:create(DbName, Options) of
                                            {ok, D} ->
                                                couch_db:close(D);
                                            _ ->
                                                ok
                                        end
                                end),
                        MRef = erlang:monitor(process, Pid),
                        fun () ->
                                receive
                                    {'DOWN', MRef, _, _, _} -> ok
                                end
                        end
                end
        end,
    WaitFns = [SpawnPreparer(VBucket) || VBucket <- VBuckets],
    lists:foreach(fun (WaitFn) -> WaitFn() end,
                  WaitFns).

handle_snapshot_states(Msg, State) ->
    VBuckets = extract_vbuckets(Msg, []),
    quickly_create_missing_vbuckets(VBuckets, State),
    do_handle_snapshot_states(Msg, State).

extract_vbuckets(<<>>, Acc) ->
    Acc;
extract_vbuckets(<<VBucket:16, _VBState:32, _CheckpointId:64, Rest/binary>>, Acc) ->
    extract_vbuckets(Rest, [VBucket | Acc]).

do_handle_snapshot_states(<<>>, _State) ->
    ok;
do_handle_snapshot_states(<<VBucket:16, VBState:32, CheckpointId:64, Rest/binary>>, State) ->
    ?LOG_DEBUG("Checkpointing vbucket state (~p, ~p, ~p)", [VBucket, VBState, CheckpointId]),
    handle_set_state(VBucket, VBState, CheckpointId, State),
    handle_snapshot_states(Rest, State).
