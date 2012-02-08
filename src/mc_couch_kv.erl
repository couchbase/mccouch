-module(mc_couch_kv).

-include("couch_db.hrl").

-export([get/2, grok_doc/1, set/6, delete/2, mk_doc/5, mk_doc/6]).

%% ok, Flags, Expiration, Cas, Data
-spec get(_, binary()) -> {ok, integer(), integer(), binary()} | not_found.
get(Db, Key) ->
    case couch_db:open_doc_int(Db, Key, [json_bin_body]) of
        {ok, Doc} -> grok_doc(Doc);
        _ -> not_found
    end.


%% ok, Flags, Expiration, Cas, Data
-spec grok_doc(#doc{}) -> {ok, integer(), integer(), term()}.
grok_doc(Doc) ->
    #doc{body=Binary, rev = {_RevNo, RevId}} = Doc,
    case RevId of
        <<_Cas:64, Expiration:32, Flags:32>> ->
            {ok, Flags, Expiration, Binary};
        _ ->
            {ok, 0, 0, Binary}
    end.


mk_doc(Key, Flags, Expiration, Value, WantJson) ->
    Doc = couch_doc:from_binary(Key, Value, WantJson),
    Doc#doc{rev = {1, <<0:64, Expiration:32, Flags:32>>}}.

mk_doc(Key, Flags, Expiration, Value, MetaData, WantJson) ->
    Doc = couch_doc:from_binary(Key, Value, WantJson),
    <<_T:8, _ML:8, Seqno:32, Cas:64, _VLen:32, _F:32>> = MetaData,
    Doc#doc{rev = {Seqno, <<Cas:64, Expiration:32, Flags:32>>}}.

-spec set(_, binary(), integer(), integer(), binary(), boolean()) -> integer().
set(Db, Key, Flags, Expiration, Value, JsonMode) ->
    Doc = mk_doc(Key, Flags, Expiration, Value, JsonMode),
    ok = couch_db:update_doc(Db, Doc, [clobber]),
    0.

-spec delete(_, binary()) -> ok.
delete(Db, Key) ->
    Doc = #doc{id = Key, deleted = true},
    ok = couch_db:update_doc(Db, Doc, [clobber]),
    ok.
