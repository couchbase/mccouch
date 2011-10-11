-module(mc_couch_kv).

-include("couch_db.hrl").

-export([get/2, grok_doc/1, set/6, delete/2, mk_doc/5, mk_doc/6]).

dig_out_attachment(Doc, FileName) ->
    case [A || A <- Doc#doc.atts, A#att.name == FileName] of
        [] ->
            not_found;
        [Att] ->
            Segs = couch_doc:att_foldl(Att, fun(Seg, Acc) -> [Seg|Acc] end, []),
            {ok, iolist_to_binary(lists:reverse(Segs))}
    end.

cleanup([], _Remove, Acc) -> lists:reverse(Acc);
cleanup(EJson, [], Acc) -> lists:reverse(Acc, EJson);
cleanup([{K,_V}|EJson], [K|Tl], Acc) -> cleanup(EJson, Tl, Acc);
cleanup([KV|EJson], [_K|Tl], Acc) -> cleanup(EJson, Tl, [KV|Acc]).
cleanup(EJson) ->
    cleanup(EJson, [<<"_id">>, <<"_rev">>, <<"$flags">>, <<"$expiration">>], []).

%% ok, Flags, Expiration, Cas, Data
-spec get(_, binary()) -> {ok, integer(), integer(), integer(), binary()} | not_found.
get(Db, Key) ->
    case couch_db:open_doc_int(Db, Key, []) of
        {ok, Doc} -> grok_doc(Doc);
        _ -> not_found
    end.

%% ok, Flags, Expiration, Cas, Data
-spec grok_doc(#doc{}) -> {ok, integer(), integer(), integer(), binary()}.
grok_doc(Doc) ->
    {EJson} = couch_doc:to_json_obj(Doc, []),
    Flags = proplists:get_value(<<"$flags">>, EJson, 0),
    Expiration = proplists:get_value(<<"$expiration">>, EJson, 0),

    case dig_out_attachment(Doc, <<"value">>) of
        {ok, AttData} ->
            {ok, Flags, Expiration, 0, AttData};
        _ ->
            Encoded = iolist_to_binary(?JSON_ENCODE(
                                         {cleanup(EJson)})),
            {ok, Flags, Expiration, 0, Encoded}
    end.

mk_att_doc(Key, Flags, Expiration, Value, MetaData, Reason) ->
    Doc = #doc{id=Key,
               body = {[
                   {<<"$flags">>, Flags},
                   {<<"$expiration">>, Expiration},
                   {<<"$att_reason">>, Reason}
               ]},
               atts = [#att{
                   name= <<"value">>,
                   type= <<"application/content-stream">>,
                   data= Value}
               ]},
    case MetaData of
        <<_T:8, _ML:8, Seqno:32, Cas:64, _VLen:32, _F:32>> ->
            Doc#doc{
                revs = {Seqno, [<<Cas:64, Flags:32, Expiration:32>>]} % Make revid 128 bits
            };
        <<>> ->
            Doc
    end.

%% Reject docs that have keys starting with _ or $
validate([]) -> ok;
validate([{<<$_:8,_/binary>>,_Val}|_Tl]) -> throw(invalid_key);
validate([{<<$$:8,_/binary>>, _Val}|_Tl]) -> throw(invalid_key);
validate([_|Tl]) -> validate(Tl).

jsonish(<<"{", _/binary>>) -> true;
jsonish(<<" ", _/binary>>) -> true;
jsonish(<<"\r", _/binary>>) -> true;
jsonish(<<"\n", _/binary>>) -> true;
jsonish(_) -> false.

parse_json(Value) ->
    case jsonish(Value) of
        true ->
            case ?JSON_DECODE(Value) of
                {J} ->
                    validate(J),
                    J;
                _ -> throw({invalid_json, Value})
            end;
        _ ->
            throw({invalid_json, Value})
    end.

mk_json_doc(Key, Flags, Expiration, Value, MetaData) ->
    case (catch parse_json(Value)) of
        {invalid_json, _} ->
            mk_att_doc(Key, Flags, Expiration, Value, MetaData, <<"invalid_json">>);
        invalid_key ->
            mk_att_doc(Key, Flags, Expiration, Value, MetaData, <<"invalid_key">>);
        EJson ->
            Doc = #doc{id=Key,
                       body={[
                           {<<"$flags">>, Flags},
                           {<<"$expiration">>, Expiration}
                           | cleanup(EJson)]}
                      },
            case MetaData of
                <<_T:8, _ML:8, Seqno:32, Cas:64, _VLen:32, _F:32>> ->
                    Doc#doc{
                        revs = {Seqno, [<<Cas:64, Flags:32, Expiration:32>>]} % Make revid 128 bits
                    };
                <<>> ->
                    Doc
            end
    end.

mk_doc(Key, Flags, Expiration, Value, WantJson) ->
    mk_doc(Key, Flags, Expiration, Value, <<>>, WantJson).

mk_doc(Key, Flags, Expiration, Value, MetaData, WantJson) ->
    case WantJson of
        true ->
            mk_json_doc(Key, Flags, Expiration, Value, MetaData);
        _ ->
            mk_att_doc(Key, Flags, Expiration, Value, MetaData, <<"non-JSON mode">>)
    end.

-spec set(_, binary(), integer(), integer(), binary(), boolean()) -> integer().
set(Db, Key, Flags, Expiration, Value, JsonMode) ->
    Doc = mk_doc(Key, Flags, Expiration, Value, JsonMode),
    {ok, _NewRev} = couch_db:update_doc(Db, Doc, [clobber]),
    0.

-spec delete(_, binary()) -> ok|not_found.
delete(Db, Key) ->
    Doc = #doc{id = Key, deleted = true, body = {[]}},
    {ok, _NewRev} = couch_db:update_doc(Db, Doc, [clobber]),
    ok.
