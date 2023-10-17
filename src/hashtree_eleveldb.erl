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

-module(hashtree_eleveldb).

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

-record(itr_state, {
    itr                :: term(),
    id                 :: tree_id_bin(),
    current_segment    :: '*' | integer(),
    remaining_segments :: ['*' | integer()],
    acc_fun            :: fun(([{binary(),binary()}]) -> any()),
    segment_acc        :: [{binary(), binary()}],
    final_acc          :: [{integer(), any()}],
    prefetch=false     :: boolean()
   }).

-type tree_id_bin() :: <<_:176>>.
-type bucket_bin()  :: <<_:320>>.
-type db_key() :: binary().
-type select_fun(T) :: fun((orddict:orddict()) -> T).

-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%%===================================================================
%%% API
%%%===================================================================

-spec new(proplists:proplist()) -> {term(), string()}.
new(Options) ->
    DataDir = hashtree:get_path(Options),

    DefaultWriteBufferMin = 4 * 1024 * 1024,
    DefaultWriteBufferMax = 14 * 1024 * 1024,
    ConfigVars =
        hashtree:get_env(
            anti_entropy_leveldb_opts,
            [{write_buffer_size_min, DefaultWriteBufferMin},
                {write_buffer_size_max, DefaultWriteBufferMax}]),
    Config = orddict:from_list(ConfigVars),

    %% Use a variable write buffer size to prevent against all buffers being
    %% flushed to disk at once when under a heavy uniform load.
    WriteBufferMin =
        proplists:get_value(
            write_buffer_size_min, Config, DefaultWriteBufferMin),
    WriteBufferMax =
        proplists:get_value(
            write_buffer_size_max, Config, DefaultWriteBufferMax),
    Offset = rand:uniform(1 + WriteBufferMax - WriteBufferMin),
    WriteBufferSize = WriteBufferMin + Offset,
    Config2 = orddict:store(write_buffer_size, WriteBufferSize, Config),
    Config3 = orddict:erase(write_buffer_size_min, Config2),
    Config4 = orddict:erase(write_buffer_size_max, Config3),
    Config5 = orddict:store(is_internal_db, true, Config4),
    Config6 = orddict:store(use_bloomfilter, true, Config5),
    StartupOptions = orddict:store(create_if_missing, true, Config6),

    ok = filelib:ensure_dir(DataDir),
    {ok, Ref} = eleveldb:open(DataDir, StartupOptions),
    {Ref, DataDir}.

-spec close(term(), term()) -> ok.
close(DB, Snapshot) ->
    close_iterator(Snapshot),
    catch eleveldb:close(DB),
    ok.

-spec close_group(list({term(), term()})) -> ok.
close_group(DBList) ->
    lists:foreach(fun({DB, Snapshot}) -> close(DB, Snapshot) end, DBList).

-spec destroy(string()) -> ok.
destroy(Path) ->
    eleveldb:destroy(Path, []).

-spec encode_key(
    {segment, tree_id_bin(), integer(), binary()}|
    {bucket, tree_id_bin(), integer(), integer()}|
    {meta, binary()}) -> db_key().
encode_key({segment, TreeId, Segment, Key}) ->
    hashtree:external_encode(TreeId, Segment, Key);
encode_key({bucket, TreeId, Level, Bucket}) ->
    <<$b,TreeId:22/binary,$b,Level:64/integer,Bucket:64/integer>>;
encode_key({meta, Key}) ->
    <<$m,Key/binary>>.


-spec snapshot(term(), term()) -> {ok, term()}.
snapshot(DB, Snapshot) ->
    %% Abuse eleveldb iterators as snapshots
    catch eleveldb:iterator_close(Snapshot),
    eleveldb:iterator(DB, []).

-spec get(term(), db_key()) -> {ok, binary()}| not_found | {error, any()}.
get(DB, HKey) ->
    eleveldb:get(DB, HKey, []).

-spec put(term(), db_key(), binary()) -> ok.
put(DB, HKey, Bin) ->
    eleveldb:put(DB, HKey, Bin, []).

-spec mput(term(), list({put, db_key(), binary()}|{delete, db_key()})) -> ok.
mput(DB, Updates) ->
    eleveldb:write(DB, Updates, []).

-spec delete(term(), db_key()) -> ok.
delete(DB, HKey) ->
    eleveldb:delete(DB, HKey, []).

-spec clear_buckets(tree_id_bin(), term()) -> ok.
clear_buckets(Id, DB) ->
    Fun = fun({K,_V},Acc) ->
        try
            case decode_bucket(K) of
                {Id, _, _} ->
                    ok = eleveldb:delete(DB, K, []),
                    Acc + 1;
                _ ->
                    throw({break, Acc})
            end
        catch
            _:_ -> % not a decodable bucket
                throw({break, Acc})
        end
    end,
    Opts = [{first_key, encode_key({bucket, Id, 0, 0})}],
    Removed = 
        try
            eleveldb:fold(DB, Fun, 0, Opts)
        catch
            {break, AccFinal} ->
                AccFinal
        end,
    ?LOG_DEBUG("Tree ~p cleared ~p segments.\n", [Id, Removed]),
    ok.


-spec multi_select_segment(
    term(), term(), list('*'|integer()), select_fun(T)) -> [{integer(), T}].
multi_select_segment(Id, Itr, Segments, F) ->
    [First | Rest] = Segments,
    IS1 = #itr_state{itr=Itr,
                     id=Id,
                     current_segment=First,
                     remaining_segments=Rest,
                     acc_fun=F,
                     segment_acc=[],
                     final_acc=[]},
    Seek = case First of
               '*' ->
                   encode_key({segment, Id, 0, <<>>});
               _ ->
                   encode_key({segment, Id, First, <<>>})
           end,
    IS2 = try
              iterate(iterator_move(Itr, Seek), IS1)
          after
              %% Always call prefetch stop to ensure the iterator
              %% is safe to use in the compare.  Requires
              %% eleveldb > 2.0.16 or this may segv/hang.
              _ = iterator_move(Itr, prefetch_stop)
          end,
    #itr_state{remaining_segments = LeftOver,
               current_segment=LastSegment,
               segment_acc=LastAcc,
               final_acc=FA} = IS2,

    %% iterate completes without processing the last entries in the state.  Compute
    %% the final visited segment, and add calls to the F([]) for all of the segments
    %% that do not exist at the end of the file (due to deleting the last entry in the
    %% segment).
    Result = [{LeftSeg, F([])} || LeftSeg <- lists:reverse(LeftOver),
                  LeftSeg =/= '*'] ++
    [{LastSegment, F(LastAcc)} | FA],
    case Result of
        [{'*', _}] ->
            %% Handle wildcard select when all segments are empty
            [];
        _ ->
            Result
    end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

iterator_move(undefined, _Seek) ->
    {error, invalid_iterator};
iterator_move(Itr, Seek) ->
    try
        eleveldb:iterator_move(Itr, Seek)
    catch
        _:badarg ->
            {error, invalid_iterator}
    end.

-spec iterate({'error','invalid_iterator'} | {'ok',binary(),binary()},
              #itr_state{}) -> #itr_state{}.

%% Ended up at an invalid_iterator likely due to encountering a missing dirty
%% segment - e.g. segment dirty, but removed last entries for it
iterate({error, invalid_iterator}, IS=#itr_state{current_segment='*'}) ->
    IS;
iterate({error, invalid_iterator}, IS=#itr_state{itr=Itr,
                                                 id=Id,
                                                 current_segment=CurSeg,
                                                 remaining_segments=Segments,
                                                 acc_fun=F,
                                                 segment_acc=Acc,
                                                 final_acc=FinalAcc}) ->
    case Segments of
        [] ->
            IS;
        ['*'] ->
            IS;
        [NextSeg | Remaining] ->
            Seek = encode_key({segment, Id, NextSeg, <<>>}),
            IS2 = IS#itr_state{current_segment=NextSeg,
                               remaining_segments=Remaining,
                               segment_acc=[],
                               final_acc=[{CurSeg, F(Acc)} | FinalAcc]},
            iterate(iterator_move(Itr, Seek), IS2)
    end;
iterate({ok, K, V}, IS=#itr_state{itr=Itr,
                                  id=Id,
                                  current_segment=CurSeg,
                                  remaining_segments=Segments,
                                  acc_fun=F,
                                  segment_acc=Acc,
                                  final_acc=FinalAcc}) ->
    {SegId, Seg, _} = safe_decode(K),
    Segment = case CurSeg of
                  '*' ->
                      Seg;
                  _ ->
                      CurSeg
              end,
    case {SegId, Seg, Segments, IS#itr_state.prefetch} of
        {bad, -1, _, _} ->
            %% Non-segment encountered, end traversal
            IS;
        {Id, Segment, _, _} ->
            %% Still reading existing segment
            IS2 = IS#itr_state{current_segment=Segment,
                               segment_acc=[{K,V} | Acc],
                               prefetch=true},
            iterate(iterator_move(Itr, prefetch), IS2);
        {Id, _, [Seg|Remaining], _} ->
            %% Pointing at next segment we are interested in
            IS2 = IS#itr_state{current_segment=Seg,
                               remaining_segments=Remaining,
                               segment_acc=[{K,V}],
                               final_acc=[{Segment, F(Acc)} | FinalAcc],
                               prefetch=true},
            iterate(iterator_move(Itr, prefetch), IS2);
        {Id, _, ['*'], _} ->
            %% Pointing at next segment we are interested in
            IS2 = IS#itr_state{current_segment=Seg,
                               remaining_segments=['*'],
                               segment_acc=[{K,V}],
                               final_acc=[{Segment, F(Acc)} | FinalAcc],
                               prefetch=true},
            iterate(iterator_move(Itr, prefetch), IS2);
        {Id, _, [NextSeg | Remaining], true} ->
            %% Pointing at uninteresting segment, but need to halt the
            %% prefetch to ensure the iterator can be reused
            IS2 = IS#itr_state{current_segment=NextSeg,
                               segment_acc=[],
                               remaining_segments=Remaining,
                               final_acc=[{Segment, F(Acc)} | FinalAcc],
                               prefetch=true}, % will be after second move
            _ = iterator_move(Itr, prefetch_stop), % ignore the pre-fetch,
            Seek = encode_key({segment, Id, NextSeg, <<>>}),      % and risk wasting a reseek
            iterate(iterator_move(Itr, Seek), IS2);% to get to the next segment
        {Id, _, [NextSeg | Remaining], false} ->
            %% Pointing at uninteresting segment, seek to next interesting one
            Seek = encode_key({segment, Id, NextSeg, <<>>}),
            IS2 = IS#itr_state{current_segment=NextSeg,
                               remaining_segments=Remaining,
                               segment_acc=[],
                               final_acc=[{Segment, F(Acc)} | FinalAcc]},
            iterate(iterator_move(Itr, Seek), IS2);
        {_, _, _, true} ->
            %% Done with traversal, but need to stop the prefetch to
            %% ensure the iterator can be reused. The next operation
            %% with this iterator is a seek so no need to be concerned
            %% with the data returned here.
            _ = iterator_move(Itr, prefetch_stop),
            IS#itr_state{prefetch=false};
        {_, _, _, false} ->
            %% Done with traversal
            IS
    end.

close_iterator(Itr) ->
    try
        eleveldb:iterator_close(Itr)
    catch
        _:_ ->
            ok
    end.

-spec decode_bucket(bucket_bin()) -> {tree_id_bin(), integer(), integer()}.
decode_bucket(Bin) ->
    <<$b,TreeId:22/binary,$b,Level:64/integer,Bucket:64/integer>> = Bin,
    {TreeId, Level, Bucket}.

-spec safe_decode(binary()) -> {tree_id_bin() | bad, integer(), binary()}.
safe_decode(Bin) ->
    case Bin of
        <<$t,TreeId:22/binary,$s,Segment:64/integer,Key/binary>> ->
            {TreeId, Segment, Key};
        _ ->
            {bad, -1, <<>>}
    end.
%%%===================================================================
%%% EUnit
%%%===================================================================


fake_close(DB) ->
    catch eleveldb:close(DB).