-module(mc_batch_sup).

-behaviour(supervisor).

-export([start_link/0, start_worker/4]).

%% Internal junk that needs to be exported
-export([start_link_worker/3, sync_update_docs/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-include("couch_db.hrl").
-include("mc_constants.hrl").

start_link() ->
    supervisor:start_link(?MODULE, []).

init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{?MODULE, {?MODULE, start_link_worker, []},
            temporary, 3600000, worker, []}]}}.

%% {ok, Ref}
start_worker(Sup, Batch, BucketName, Socket) ->
   {ok, Pid} = supervisor:start_child(Sup, [Batch, BucketName, Socket]),
   Ref = monitor(process, Pid),
   {ok, Ref}.

start_link_worker(Batch, BucketName, Socket) ->
    {ok, proc_lib:spawn_link(?MODULE, sync_update_docs, [Batch, BucketName, Socket])}.

sync_update_docs(Batch, BucketName, Socket) ->
    UpdateOptions = [clobber] ++ case couch_config:get("mc_couch", "optimistic_writes", "true") of
                        "true" ->
                            [optimistic];
                        _ ->
                            []
                    end ++  case couch_config:get("mc_couch", "presorted", "true") of
                        "true" ->
                            [presorted];
                        _ ->
                            []
                    end,
    dict:fold(
      fun(VBucketId, Jobs, _Acc) ->
              Docs = lists:map(
                  fun({Opaque, Op, {set, Key, Flags, Expiration, Value, JsonMode}}) ->
                         {Opaque, Op,
                          mc_couch_kv:mk_doc(Key, Flags, Expiration, Value, JsonMode)};
                     ({Opaque, Op, {set, Key, Flags, Expiration, Value, MetaData, JsonMode}}) ->
                         {Opaque, Op,
                          mc_couch_kv:mk_doc(Key, Flags, Expiration, Value, MetaData, JsonMode)};
                     ({Opaque, Op, {delete, Key}}) ->
                         {Opaque, Op, #doc{id = Key, deleted = true, body = {[]}}}
                  end, Jobs),
              DbName = iolist_to_binary([<<BucketName/binary, $/>>,
                                         integer_to_list(VBucketId)]),
              case couch_db:open_int(DbName, []) of
                  {ok, Db} ->
                      {ok, Results} = couch_db:update_docs(
                                        Db, [Doc || {_Opaque, _Op, Doc} <- Docs], UpdateOptions),
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
