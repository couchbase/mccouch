-module(mc_batch_sup).

-behaviour(supervisor).

-export([start_link/0, start_worker/5]).

%% Internal junk that needs to be exported
-export([start_link_worker/4, sync_update_docs/4]).

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
start_worker(Sup, CurrentVBucket, CurrentList, BucketName, Socket) ->
   {ok, Pid} = supervisor:start_child(Sup, [CurrentVBucket, CurrentList, BucketName, Socket]),
   Ref = monitor(process, Pid),
   Pid ! can_start,
   {ok, Ref}.

start_link_worker(CurrentVBucket, CurrentList, BucketName, Socket) ->
    {ok, proc_lib:spawn_link(?MODULE, sync_update_docs,
                             [CurrentVBucket, CurrentList, BucketName, Socket])}.

sync_update_docs(CurrentVBucket, CurrentList, BucketName, Socket) ->
    receive
        can_start ->
            ok
    end,

    Docs = lists:map(
        fun({Opaque, Op, {set, Key, Flags, Expiration, Value, JsonMode}}) ->
            {Opaque, Op, mc_couch_kv:mk_doc(Key, Flags, Expiration, Value, JsonMode)};
        ({Opaque, Op, {set, Key, Flags, Expiration, Value, MetaData, JsonMode}}) ->
            {Opaque, Op,mc_couch_kv:mk_doc(Key, Flags, Expiration, Value, MetaData, JsonMode)};
        ({Opaque, Op, {delete, Key}}) ->
            {Opaque, Op, #doc{id = Key, deleted = true}}
        end, CurrentList),
    DbName = iolist_to_binary([<<BucketName/binary, $/>>, integer_to_list(CurrentVBucket)]),
    case couch_db:open_int(DbName, []) of
        {ok, Db} ->
            ok = couch_db:update_docs(Db, [Doc || {_Opaque, _Op, Doc} <- Docs],
                                                 []),
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
                end, Docs)
      end.
