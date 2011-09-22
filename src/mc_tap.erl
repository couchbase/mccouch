-module(mc_tap).

-include("couch_db.hrl").
-include("mc_constants.hrl").

-define(TAP_FLAG_NO_VALUE, 16#02).

-export([run/5]).

%% We're pretty specific about the type of tap connections we can handle.
run(State, Opaque, Socket, <<Flags:32>>, <<>>) ->
    TapFlags = parse_tap_flags(Flags),
    spawn_link(fun() ->
                       DbName = mc_daemon:db_prefix(State),
                       lists:foreach(fun({VB,_VBState}) ->
                                             process_tap_stream(DbName, Opaque, VB,
                                                                TapFlags, Socket)
                                     end,
                                     mc_couch_vbucket:list_vbuckets(State)),
                        terminate_tap_stream(Socket, Opaque, 0)
               end);
run(State, Opaque, Socket, <<Flags:32>>, <<1:16, VBucketId:16>>) ->
    true = lists:member(list_vbuckets, parse_tap_flags(Flags)),
    spawn_link(fun() -> process_tap_stream(mc_daemon:db_prefix(State), Opaque,
                                           VBucketId, parse_tap_flags(Flags), Socket),
                        terminate_tap_stream(Socket, Opaque, VBucketId)
               end);
run(State, Opaque, Socket, <<Flags:32>>, Extra) ->
    ?LOG_INFO("MC tap: invalid request: ~p/~p/~p/~p/~p",
              [State, Opaque, Socket, Flags, Extra]),
    mc_connection:respond(Socket, ?TAP_CONNECT, Opaque,
                          #mc_response{status=?EINVAL,
                                       body="Only dump+1 vbucket is allowed"}).

parse_tap_flags(Flags) ->
    KnownFlags = [{16#01, backfill},
                  {16#02, dump},
                  {16#04, list_vbuckets},
                  {16#08, takeover},
                  {16#10, support_ack},
                  {16#20, keys_only},
                  {16#40, checkpoint},
                  {16#80, registered_client}],
    [Flag || {Val, Flag} <- KnownFlags, (Val band Flags) == Val].

emit_tap_doc(Socket, TapFlags, Opaque, VBucketId, Key, Flags, Expiration,
             _Cas, Data) ->
    Extras = <<0:16, TapFlags:16,   %% length, flags
               0:8,                 %% TTL
               0:8, 0:8, 0:8,       %% reserved
               Flags:32, Expiration:32>>,
    mc_connection:respond(?REQ_MAGIC, Socket, ?TAP_MUTATION, Opaque,
                          #mc_response{key=Key, status=VBucketId,
                                       extra=Extras, body=Data}).


process_tap_stream(BaseDbName, Opaque, VBucketId, TapFlags, Socket) ->
    ?LOG_INFO("MC tap: processing: ~p/~p/~p/~p/~p", [BaseDbName, Opaque, TapFlags,
                                                     VBucketId, Socket]),

    DbName = lists:flatten(io_lib:format("~s/~p", [BaseDbName, VBucketId])),
    {ok, Db} = couch_db:open(list_to_binary(DbName), []),
    KeysOnly = lists:member(keys_only, TapFlags),

    OutFlags = case KeysOnly of true -> ?TAP_FLAG_NO_VALUE; _ -> 0 end,

    F = fun(#doc_info{revs=[#rev_info{deleted=true}|_]}, Acc) ->
                {ok, Acc}; %% Ignore deleted docs
           (#doc_info{id = <<"_design/",_/binary>>}, Acc) ->
                %% Ignore design documents
                {ok, Acc};
           (#doc_info{id=Id} = DocInfo, Acc) ->
                {ok, Flags, Expiration, Cas, Data} = case KeysOnly of
                                                         true -> {ok, 0, 0, 0, <<>>};
                                                         _ ->
                                                             {ok, Doc} = couch_db:open_doc_int(Db, DocInfo, []),
                                                             mc_couch_kv:grok_doc(Doc)
                                                     end,
                emit_tap_doc(Socket, OutFlags, Opaque, VBucketId, Id,
                             Flags, Expiration, Cas, Data),
                {ok, Acc}
        end,

    {ok, finished} = couch_db:changes_since(Db, 0, F, finished),

    couch_db:close(Db).

terminate_tap_stream(Socket, Opaque, Status) ->
    TerminalExtra = <<8:16, 0:16,      %% length, flags
                      0:8,             %% TTL
                      0:8, 0:8, 0:8>>, %% reserved
    mc_connection:respond(?REQ_MAGIC, Socket, ?TAP_OPAQUE, Opaque,
                          #mc_response{extra=TerminalExtra, status=Status}).
