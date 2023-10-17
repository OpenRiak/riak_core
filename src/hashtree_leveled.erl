%% -------------------------------------------------------------------
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% 

-module(hashtree_leveled).

-export([
    new/1,
    close/2,
    close_group/1,
    destroy/1,
    get/2,
    mput/2,
    put/3,
    delete/2,
    clear_buckets/2,
    multi_select_segment/4,
    snapshot/2,
    encode_key/1
    ]).

-export([fake_close/1]).

-type db_key() :: {binary(), binary()}|{{binary(), binary()}, binary()}.
-type select_fun(T) :: fun((orddict:orddict()) -> T).
-type active_store() :: pid().
-type active_snap() :: pid().
-type store_ref() :: active_store()|undefined.
-type snapshot_ref() :: active_snap()|undefined.

-define(HEAD_TAG, h).
-define(STORE_FORCED_LOGS, []).
-define(STORE_LOG_LEVEL, warn).
-define(SNAP_FORCED_LOGS, []).
-define(SNAP_LOG_LEVEL, warn).
-define(PAUSE_ON_BUSY, 40).

-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%%===================================================================
%%% API
%%%===================================================================

-spec new(proplists:proplist()) -> {active_store(), string()}.
new(Options) ->
    DataDir = hashtree:get_path(Options),
    LeveledOpts =
        [
        {root_path, DataDir},
        {max_journalobjectcount, 20000},
            %% one tenth of standard size - as head_only
        {log_level, ?STORE_LOG_LEVEL},
        {forced_logs, ?STORE_FORCED_LOGS},
        {database_id, 65534}, % 65535 used for tictac aae stores
        {head_only, with_lookup},
        {cache_size, 2000},
        {sync_strategy, none},
        {compression_method, native},
        {compression_point, on_receipt},
        {snapshot_timeout_short, 3600},
        {snapshot_timeout_long, 172800}
            %% Use the 2 days used in kv_index_tictactree
            %% question over whether in an idle cluster there may be a need for a
            %% non-finite timeout 
        ],
    ok = filelib:ensure_dir(DataDir),
    {ok, DB} = leveled_bookie:book_start(LeveledOpts),
    {DB, DataDir}.

-spec close(store_ref(), snapshot_ref()) -> ok.
close(undefined, undefined) ->
    ok;
close(DB, undefined) ->
    close_db(DB);
close(DB, Snapshot) ->
    close_db(Snapshot),
    close(DB, undefined).

%% @doc
%% Close all the snapshots in the group of hashtrees, before closing the actual
%% stores.  Otherwise if each {Snapshot, DB} pair is closed in turn, and the
%% store is a shared (linked) store it might be it will be closed prior to a
%% snapshot in another pair on the same store.  In this case the store will
%% wait for 20s to close to give a chance for the snapshot to clear.
-spec close_group(list({store_ref(), snapshot_ref()})) -> ok.
close_group(DBList) ->
    {DBs, Snapshots} = lists:unzip(DBList),
    lists:foreach(
        fun(DB) -> close_db(DB) end,
        lists:usort(Snapshots) ++ lists:usort(DBs)).

-spec destroy(string()) -> ok.
destroy(Path) ->
    hashtree:destroy(Path).

-spec encode_key(
    {segment, hashtree:tree_id_bin(), integer(), binary()}|
    {bucket, hashtree:tree_id_bin(), integer(), integer()}|
    {meta, binary()}) -> db_key().
encode_key({segment, TreeId, Segment, Key}) ->
    {{<<$t, TreeId:22/binary>>, <<Segment:64/integer>>}, <<Key/binary>>};
encode_key({bucket, TreeId, Level, Bucket}) ->
    {<<$b, TreeId:22/binary>>, <<Level:64/integer, Bucket:64/integer>>};
encode_key({meta, Key}) ->
    {<<$m>>, <<Key/binary>>}.

-spec snapshot(term(), term()) -> {ok, term()}.
snapshot(DB, undefined) ->
    {ok, Snapshot} = leveled_bookie:book_start([{snapshot_bookie, DB}]),
    ok = leveled_bookie:book_loglevel(Snapshot, ?SNAP_LOG_LEVEL),
    ok = leveled_bookie:book_addlogs(Snapshot, ?SNAP_FORCED_LOGS),
    {ok, Snapshot};
snapshot(DB, Snapshot) ->
    close_db(Snapshot),
    snapshot(DB, undefined).

-spec get(
    active_store()|active_snap(), db_key())
        -> {ok, binary()}| not_found | {error, any()}.
get(DB, {Bucket, Key}) ->
    leveled_bookie:book_headonly(DB, Bucket, Key, null).

-spec put(store_ref(), db_key(), binary()) -> ok.
put(DB, {Bucket, Key}, Value) ->
    put_object_specs(DB, [{add, v1, Bucket, Key, null, undefined, Value}]).

-spec mput(
    active_store(), list({put, db_key(), binary()}|{delete, db_key()}))
        -> ok.
mput(DB, Updates) ->
    %% Buffer has been built backwards and reversed
    %% ... so most recent updates are now at the tail of the list
    %% e.g. [FirstUpdate, SecondUpdate ..., NthUpdate]
    %% Need to de-duplicate this, so only the most recent change is added for
    %% each key - so reverse before ukeysort. Order expected for leveled is:
    %% [NthUpdate, ..., SecondUpdate, FirstUpdate] - so don't re-reverse
    ObjectSpecs =
        lists:map(
            fun(Action) ->
                case Action of
                    {put, {Bucket, Key}, Value} ->
                        {add, v1, Bucket, Key, null, undefined, Value};
                    {delete, {Bucket, Key}} ->
                        {remove, v1, Bucket, Key, null, undefined, null}
                end
            end,
            lists:ukeysort(2, lists:reverse(Updates))
        ),
    put_object_specs(DB, ObjectSpecs).

-spec delete(active_store(), db_key()) -> ok.
delete(DB, {Bucket, Key}) ->
    put_object_specs(DB, [{remove, v1, Bucket, Key, null, undefined, null}]).

-spec clear_buckets(hashtree:tree_id_bin(), active_store()) -> ok.
clear_buckets(Id, DB) ->
    FoldFun =
        fun(Bucket, {Key, null}, Acc) ->
            [{remove, v1, Bucket, Key, null, undefined, null}|Acc]
        end,
    {async, BucketFolder} =
        leveled_bookie:book_keylist(
            DB,
            ?HEAD_TAG,
            element(1, encode_key({bucket, Id, 0, 0})),
            {FoldFun, []}
        ),
    BucketKeyList = BucketFolder(),
    put_object_specs(DB, BucketKeyList),
    ?LOG_DEBUG("Tree ~p cleared ~p segments.\n", [Id, length(BucketKeyList)]),
    ok.

-spec multi_select_segment(
    hashtree:tree_id_bin(), active_snap(), list('*'|integer()), select_fun(T))
        -> [{integer(), T}].
multi_select_segment(Id, Itr, Segments, F) ->
    DBType =
        element(1, element(1, encode_key({segment, Id, 0, <<>>}))),
    FoldFun =
        fun(Bucket, {Key, null}, Value, Acc) ->
            case Bucket of
                {DBType, <<Seg:64/integer>>} ->
                    NewEntry = {hashtree:external_encode(Id, Seg, Key), Value},
                    case Acc of
                        [] -> [{Seg, [NewEntry]}];
                        [{Seg, KVL}|T] -> [{Seg, [NewEntry|KVL]}|T];
                        Acc -> [{Seg, [NewEntry]}|Acc]
                    end;
                _ ->
                    Acc
            end
        end,
    {async, Folder} =
        case Segments of
            ['*', '*'] ->
                leveled_bookie:book_headfold(
                    Itr, ?HEAD_TAG, {FoldFun, []}, false, false, false);
            Segments ->
                BList =
                    lists:map(
                        fun(S) ->
                            element(1, encode_key({segment, Id, S, <<>>}))
                        end,
                        Segments
                    ),
                leveled_bookie:book_headfold(
                    Itr, ?HEAD_TAG, {bucket_list, BList}, {FoldFun, []},
                    false, false, false
                )
        end,
    SegKeyValues = Folder(),
    Result =
        lists:map(
            fun({S, KVL}) -> {S, F(lists:reverse(KVL))} end, SegKeyValues),
    lists:reverse(Result).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec close_db(store_ref()) -> ok.
close_db(undefined) ->
    ok;
close_db(Pid) when is_pid(Pid) ->
    case is_process_alive(Pid) of
        true -> leveled_bookie:book_close(Pid);
        false -> ok
    end.

-spec put_object_specs(
    pid(),
    [{add|remove, v1,
        {binary(), binary()}|binary(), binary(), null,
        undefined,
        binary()|null}]) -> ok.
put_object_specs(DB, ObjectSpecs) ->
    case leveled_bookie:book_mput(DB, ObjectSpecs) of
        ok -> ok;
        pause -> timer:sleep(?PAUSE_ON_BUSY)
    end.

%%%===================================================================
%%% EUnit
%%%===================================================================

fake_close(DB) ->
    catch leveled_bookie:book_close(DB).