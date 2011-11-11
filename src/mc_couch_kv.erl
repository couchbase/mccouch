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

%% ok, Flags, Expiration, Cas, Data
-spec get(_, binary()) -> {ok, integer(), integer(), integer(), binary()} | not_found.
get(Db, Key) ->
    case couch_db:open_doc_int(Db, Key, [json_bin_body]) of
        {ok, Doc} -> grok_doc(Doc);
        _ -> not_found
    end.


find_number_value_before_comma(<<$,, Json/binary>>, Acc) ->
    % found the comma, return acc as integer and remaining json
    {list_to_integer(lists:reverse(Acc)), Json};
find_number_value_before_comma(<<$}>>, Acc) ->
    % got to end, return acc as integer, remaining json is end curly
    {list_to_integer(lists:reverse(Acc)), <<"}">>};
find_number_value_before_comma(<<Char, Json/binary>>, Acc) ->
    find_number_value_before_comma(Json, [Char| Acc]).


fast_parse_leading_key_number(Item, StrippedJson, Default) ->
    Size = size(Item),
    case StrippedJson of
    <<$", Item:Size/binary, $", $:, Rest/binary>> ->
        % got it. find comma or trailing }
        find_number_value_before_comma(Rest, []);
    _ ->
        {Default, StrippedJson}
    end.


%% ok, Flags, Expiration, Cas, Data
-spec grok_doc(#doc{}) -> {ok, integer(), integer(), integer(), binary()}.
grok_doc(Doc) ->
    #doc{body= <<${, StrippedJsonBinary/binary>>} = Doc,
    {Flags, StrippedJsonBinary2} = fast_parse_leading_key_number(
            <<"$flags">>, StrippedJsonBinary, 0),
    {Expiration, StrippedJsonBinary3} = fast_parse_leading_key_number(
            <<"$expiration">>, StrippedJsonBinary2, 0),
    case dig_out_attachment(Doc, <<"value">>) of
        {ok, AttData} ->
            {ok, Flags, Expiration, 0, AttData};
        _ ->
            Json = iolist_to_binary([${, StrippedJsonBinary3]),
            {ok, Flags, Expiration, 0, Json}
    end.

mk_att_doc(Key, Flags, Expiration, Value, MetaData, Reason) ->
    Doc = #doc{id=Key,
               body = iolist_to_binary(
                   ["{\"$flags\":", integer_to_list(Flags),
                   ",\"$expiration\":", integer_to_list(Expiration),
                   ",\"$att_reason\":\"", Reason, "\"}"]),
               atts = [#att{
                   name= <<"value">>,
                   type= <<"application/content-stream">>,
                   data= Value}
               ]},
    case MetaData of
        <<_T:8, _ML:8, Seqno:32, Cas:64, VLen:32, F:32>> ->
            Doc#doc{
                revs = {Seqno, [<<Cas:64, VLen:32, F:32>>]}
            };
        <<>> ->
            Doc
    end.


mk_json_doc(Key, Flags, Expiration, Value, MetaData) ->
    case ejson:validate(Value, <<"_$">>) of
        {error, invalid_json} ->
            mk_att_doc(Key, Flags, Expiration, Value, MetaData, <<"invalid_json">>);
        {error, private_field_set} ->
            mk_att_doc(Key, Flags, Expiration, Value, MetaData, <<"invalid_key">>);
        {error, garbage_after_value} ->
            mk_att_doc(Key, Flags, Expiration, Value, MetaData, <<"invalid_json">>);
        {ok, <<${, Json/binary>>} -> % remove leading curly
            Doc = #doc{id=Key, % now add in new meta
                       body=iolist_to_binary(
                            ["{\"$flags\":", integer_to_list(Flags),
                            ",\"$expiration\":", integer_to_list(Expiration),
                            ",", Json])
                      },
            case MetaData of
                <<_T:8, _ML:8, Seqno:32, Cas:64, VLen:32, F:32>> ->
                    Doc#doc{
                        revs = {Seqno, [<<Cas:64, VLen:32, F:32>>]}
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
