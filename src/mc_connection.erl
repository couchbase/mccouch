-module (mc_connection).

-export([start_link/1, init/1]).
-export([respond/5, respond/4]).

-include("couch_db.hrl").
-include("mc_constants.hrl").

start_link(Socket) ->
    {ok, spawn_link(?MODULE, init, [Socket])}.

bin_size(undefined) -> 0;
bin_size(IoList) -> iolist_size(IoList).

xmit(_Socket, undefined) -> ok;
xmit(Socket, Data) -> gen_tcp:send(Socket, Data).

respond(Magic, Socket, OpCode, Opaque, Res) ->
    KeyLen = bin_size(Res#mc_response.key),
    ExtraLen = bin_size(Res#mc_response.extra),
    EngineSpecificLen = bin_size(Res#mc_response.engine_specific),
    BodyLen = bin_size(Res#mc_response.body) + (KeyLen + ExtraLen + EngineSpecificLen),
    Status = Res#mc_response.status,
    CAS = Res#mc_response.cas,
    ok = gen_tcp:send(Socket, <<Magic, OpCode:8, KeyLen:16,
                               ExtraLen:8, 0:8, Status:16,
                               BodyLen:32, Opaque:32, CAS:64>>),
    ok = xmit(Socket, Res#mc_response.extra),
    ok = xmit(Socket, Res#mc_response.engine_specific),
    ok = xmit(Socket, Res#mc_response.key),
    ok = xmit(Socket, Res#mc_response.body).

respond(Socket, OpCode, Opaque, Res) ->
    respond(?RES_MAGIC, Socket, OpCode, Opaque, Res).

% Read-data special cases a 0 size to just return an empty binary.
read_data(_Socket, 0, _ForWhat) -> <<>>;
read_data(Socket, N, _ForWhat) ->
    {ok, Data} = gen_tcp:recv(Socket, N),
    Data.

read_message(Socket, KeyLen, ExtraLen, BodyLen) ->
    Extra = read_data(Socket, ExtraLen, extra),
    Key = read_data(Socket, KeyLen, key),
    Body = read_data(Socket, BodyLen - (KeyLen + ExtraLen), body),

    {Extra, Key, Body}.

process_message(Socket, StorageServer, <<?REQ_MAGIC:8, ?TAP_CONNECT:8, KeyLen:16,
                                         ExtraLen:8, 0:8, _VBucket:16,
                                         BodyLen:32,
                                         Opaque:32,
                                         CAS:64>>) ->

    {Extra, Key, Body} = read_message(Socket, KeyLen, ExtraLen, BodyLen),

    % Hand the request off to the server.
    gen_fsm:send_event(StorageServer, {?TAP_CONNECT, Extra, Key, Body, CAS, Opaque});
process_message(Socket, StorageServer, <<?REQ_MAGIC:8, ?STAT:8, KeyLen:16,
                                         ExtraLen:8, 0:8, _VBucket:16,
                                         BodyLen:32,
                                         Opaque:32,
                                         CAS:64>>) ->
    error_logger:info_msg("Got a stat request for ~p.~n", [StorageServer]),

    {Extra, Key, Body} = read_message(Socket, KeyLen, ExtraLen, BodyLen),

    % Hand the request off to the server.
    gen_fsm:send_event(StorageServer, {?STAT, Extra, Key, Body, CAS, Opaque});
process_message(Socket, StorageServer, <<?REQ_MAGIC:8, ?SETQ:8, KeyLen:16,
                                         ExtraLen:8, 0:8, VBucket:16,
                                         BodyLen:32,
                                         Opaque:32,
                                         CAS:64>>) ->
    {Extra, Key, Body} = read_message(Socket, KeyLen, ExtraLen, BodyLen),
    gen_fsm:sync_send_event(StorageServer, {?SETQ, VBucket, Extra, Key, Body, CAS, Opaque}, infinity);
process_message(Socket, StorageServer, <<?REQ_MAGIC:8, ?SETQWITHMETA:8, KeyLen:16,
                                         ExtraLen:8, 0:8, VBucket:16,
                                         BodyLen:32,
                                         Opaque:32,
                                         CAS:64>>) ->
    {Extra, Key, Body} = read_message(Socket, KeyLen, ExtraLen, BodyLen),
    BodyLen1 = BodyLen - (KeyLen + ExtraLen),
    gen_fsm:sync_send_event(StorageServer, {?SETQWITHMETA, VBucket, Extra, BodyLen1,
        Key, Body, CAS, Opaque}, infinity);
process_message(Socket, StorageServer, <<?REQ_MAGIC:8, ?DELETEQ:8, KeyLen:16,
                                         ExtraLen:8, 0:8, VBucket:16,
                                         BodyLen:32,
                                         Opaque:32,
                                         CAS:64>>) ->
    {Extra, Key, Body} = read_message(Socket, KeyLen, ExtraLen, BodyLen),
    gen_fsm:sync_send_event(StorageServer, {?DELETEQ, VBucket, Extra, Key, Body, CAS, Opaque}, infinity);
process_message(Socket, StorageServer, <<?REQ_MAGIC:8, ?NOOP:8, KeyLen:16,
                                         ExtraLen:8, 0:8, _VBucket:16,
                                         BodyLen:32,
                                         Opaque:32,
                                         _CAS:64>>) ->
    {_Extra, _Key, _Body} = read_message(Socket, KeyLen, ExtraLen, BodyLen),
    ok = gen_fsm:sync_send_event(StorageServer, {?NOOP, Opaque}, infinity);
process_message(Socket, StorageServer, <<?REQ_MAGIC:8, ?SET_VBUCKET_STATE:8,
                                         KeyLen:16, ExtraLen:8, 0:8, VBucket:16,
                                         BodyLen:32,
                                         Opaque:32,
                                         CAS:64>>) ->
    {Extra, Key, Body} = read_message(Socket, KeyLen, ExtraLen, BodyLen),
    respond(Socket, ?SET_VBUCKET_STATE, Opaque,
            gen_fsm:sync_send_all_state_event(StorageServer,
                                              {?SET_VBUCKET_STATE, VBucket, Extra, Key, Body, CAS},
                                            infinity));
process_message(Socket, StorageServer, <<?REQ_MAGIC:8, ?SNAPSHOT_VB_STATES:8,
                                         KeyLen:16, ExtraLen:8, 0:8, _VBucket:16,
                                         BodyLen:32,
                                         Opaque:32,
                                         _CAS:64>>) ->
    {_Extra, _Key, Body} = read_message(Socket, KeyLen, ExtraLen, BodyLen),
    respond(Socket, ?SNAPSHOT_VB_STATES, Opaque,
            gen_fsm:sync_send_all_state_event(StorageServer,
                                              {?SNAPSHOT_VB_STATES, Body, BodyLen},
                                              infinity));
process_message(Socket, StorageServer, <<?REQ_MAGIC:8, OpCode:8, KeyLen:16,
                                         ExtraLen:8, 0:8, VBucket:16,
                                         BodyLen:32,
                                         Opaque:32,
                                         CAS:64>>) ->

    {Extra, Key, Body} = read_message(Socket, KeyLen, ExtraLen, BodyLen),

    % Hand the request off to the server.
    case gen_fsm:sync_send_event(StorageServer, {OpCode, VBucket, Extra, Key, Body, CAS}, infinity) of
        quiet -> ok;
        Res -> respond(Socket, OpCode, Opaque, Res)
    end.

init(Socket) ->
    {ok, Handler} = mc_daemon:start_link(Socket),
    loop(Socket, Handler).

loop(Socket, Handler) ->
    case gen_tcp:recv(Socket, ?HEADER_LEN) of
        {ok, Data} ->
            process_message(Socket, Handler, Data),
            loop(Socket, Handler);
        {error, closed} ->
            ok;
        {error, Error} ->
            ?LOG_ERROR("Error receiving from socket: ~p", [Error])
    end.
