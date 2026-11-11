-module(riak_core_ring_manager_deadlock_tests).

-include_lib("eunit/include/eunit.hrl").

refresh_during_clear_test_() ->
    [{"clear completes when no refresh overlaps",
      {timeout, 10, fun() -> refresh_during_clear(control) end}},
     {"local refresh must allow an in-flight clear to finish",
      {timeout, 10, fun() -> refresh_during_clear(call) end}},
     {"remote refresh must allow an in-flight clear to finish",
      {timeout, 10, fun() -> refresh_during_clear(cast) end}},
     {"overlapping refresh calls share completion and both receive replies",
      {timeout, 10, fun() -> refresh_during_clear(duplicate) end}}].

refresh_during_clear(Entry) ->
    undefined = whereis(riak_core_ring_manager),
    undefined = whereis(riak_core_claimant),
    Dir = filename:join("/tmp", "refresh-deadlock-" ++ os:getpid() ++ "-" ++
                        integer_to_list(erlang:unique_integer([positive]))),
    Settings = [{claimant_tick, 600000}, {cluster_name, "refresh_deadlock"},
                {ring_state_dir, Dir}, {ring_creation_size, 16}, {target_n_val, 4},
                {wants_claim_fun,
                 {riak_core_membership_claim, default_wants_claim}},
                {choose_claim_fun,
                 {riak_core_membership_claim, default_choose_claim}}],
    Saved = [{K, application:get_env(riak_core, K)} || {K, _} <- Settings],
    Globals = [{K, riak_core_mochiglobal:get(K)}
               || K <- [riak_ring, riak_ring_id_epoch]],
    [ok = application:set_env(riak_core, K, V) || {K, V} <- Settings],
    ok = meck:new(riak_core, [passthrough]),
    ok = meck:expect(riak_core, bucket_fixups, fun() -> [] end),
    ok = meck:expect(riak_core, stop, fun(_) -> ok end),
    ok = meck:new(riak_core_ring_events, [passthrough]),
    ok = meck:expect(riak_core_ring_events, ring_update, fun(_) -> ok end),
    try
        {ok, Manager} = gen_server:start({local, riak_core_ring_manager},
                                       riak_core_ring_manager, [test], []),
        {ok, Claimant} = gen_server:start({local, riak_core_claimant},
                                        riak_core_claimant, [], []),
        ok = sys:suspend(Manager),
        Requests = start_refresh(Entry),
        await_messages(Manager, length(Requests) + cast_count(Entry)),
        Clear = gen_server:send_request(Claimant, clear),
        assert_queued(Entry, Manager, Claimant),
        ok = sys:resume(Manager),
        ?assertEqual({reply, ok}, gen_server:receive_response(Clear, 1500)),
        [?assertEqual({reply, ok}, gen_server:receive_response(R, 1500))
         || R <- Requests],
        case Entry of
            control -> ok;
            _ ->
                ok = meck:wait(riak_core, stop, 1, 1500),
                _ = sys:get_state(Manager),
                ?assertEqual(1, meck:num_calls(riak_core, stop, 1)),
                {ok, Current} = riak_core_ring_manager:get_my_ring(),
                {ok, File} = riak_core_ring_manager:find_latest_ringfile(),
                Persisted = riak_core_ring_manager:read_ringfile(File),
                ?assert(riak_core_ring:check_lastgasp(Current)),
                ?assertNot(riak_core_ring:check_lastgasp(Persisted))
        end,
        ?assert(is_process_alive(Manager)),
        ?assert(is_process_alive(Claimant))
    after
        kill_and_wait(whereis(riak_core_claimant)),
        kill_and_wait(whereis(riak_core_ring_manager)),
        meck:unload(riak_core_ring_events),
        meck:unload(riak_core),
        [restore_env(K, V) || {K, V} <- Saved],
        [restore_global(K, V) || {K, V} <- Globals],
        file:del_dir_r(Dir)
    end.

start_refresh(control) -> [];
start_refresh(duplicate) -> [refresh_request(), refresh_request()];
start_refresh(call) -> [refresh_request()];
start_refresh(cast) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring_manager:refresh_ring(
        node(), riak_core_ring:cluster_name(Ring)),
    [].

refresh_request() ->
    gen_server:send_request(riak_core_ring_manager, refresh_my_ring).

cast_count(cast) -> 1;
cast_count(_) -> 0.

assert_queued(control, Manager, Claimant) ->
    await_messages(Manager, 1),
    {messages, [RingTrans | _]} = process_info(Manager, messages),
    ?assertMatch({'$gen_call', {Claimant, _}, {ring_trans, _, ok}}, RingTrans);
assert_queued(duplicate, Manager, Claimant) ->
    await_messages(Manager, 3),
    {messages, [First, Second, RingTrans | _]} = process_info(Manager, messages),
    assert_refresh(call, First),
    assert_refresh(call, Second),
    ?assertMatch({'$gen_call', {Claimant, _}, {ring_trans, _, ok}}, RingTrans);
assert_queued(Entry, Manager, Claimant) ->
    await_messages(Manager, 2),
    {messages, [Refresh, RingTrans | _]} = process_info(Manager, messages),
    assert_refresh(Entry, Refresh),
    ?assertMatch({'$gen_call', {Claimant, _}, {ring_trans, _, ok}}, RingTrans).

assert_refresh(call, Message) ->
    ?assertMatch({'$gen_call', _, refresh_my_ring}, Message);
assert_refresh(cast, Message) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ?assertEqual({'$gen_cast',
                  {refresh_my_ring, riak_core_ring:cluster_name(Ring)}}, Message).

await_messages(Pid, Count) ->
    Deadline = erlang:monotonic_time(millisecond) + 2000,
    await_messages(Pid, Count, Deadline).

await_messages(Pid, Count, Deadline) ->
    case process_info(Pid, message_queue_len) of
        {message_queue_len, N} when N >= Count -> ok;
        Other ->
            case erlang:monotonic_time(millisecond) < Deadline of
                true ->
                    receive after 5 -> ok end,
                    await_messages(Pid, Count, Deadline);
                false -> error({requests_not_queued, Count, Other})
            end
    end.

kill_and_wait(undefined) -> ok;
kill_and_wait(Pid) ->
    Ref = monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, process, Pid, _} -> ok end.

restore_env(Key, undefined) -> application:unset_env(riak_core, Key);
restore_env(Key, {ok, Value}) -> application:set_env(riak_core, Key, Value).

restore_global(Key, undefined) -> riak_core_mochiglobal:delete(Key);
restore_global(Key, Value) -> riak_core_mochiglobal:put(Key, Value).
