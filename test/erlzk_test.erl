-module(erlzk_test).

-include_lib("eunit/include/eunit.hrl").
-include("erlzk.hrl").
%% Note: spawn_watch/1 is a macro because this way EUnit can display
%% the value of ActualEvent in case of a failure
-define(spawn_watch(ExpectedEvent),
        element(1,spawn_monitor(fun () ->
                                        receive
                                            ActualEvent ->
                                                ?assertMatch(ExpectedEvent, ActualEvent)
                                        end
                                end))).

-define(assertCompleted(Watch),
        (fun () ->
                 receive
                     {'DOWN', _, process, Watch, Reason} ->
                         ?assertEqual(normal, Reason)
                 after
                     100 ->
                         ?assertEqual(triggered, {not_triggered, Watch})
                 end
         end)()).

-define(assertPending(Watch),
        ?assertEqual(true, is_process_alive(Watch))).

-define(assertListContain(Elem, List2), begin
                                             case Elem of
                                                 Elem when is_list(Elem) ->
                                                     lists:all(fun(E) -> lists:member(E, List2) end, Elem);
                                                 _ ->
                                                     lists:member(Elem, List2)
                                             end
                                         end).

erlzk_test_() ->
    {setup,
     fun setup_docker/0,
     fun cleanup_docker/1,
     {foreach,
      fun setup/0,
      [{with, [T]} || T <- [fun create/1,
                            fun create_api/1,
                            fun create_mode/1,
                            fun delete/1,
                            fun exists/1,
                            fun get_data/1,
                            fun set_data/1,
                            fun get_acl/1,
                            fun set_acl/1,
                            fun get_children/1,
                            fun multi/1,
                            fun auth_data/1,
                            fun chroot/1,
                            fun watch/1,
                            fun reconnect_to_same/1,
                            fun reconnect_to_different/1,
                            fun reconnect_with_stale_watches/1]]}}.

setup_docker() ->
    Stacks = os:cmd("docker stack ls"),
    case string:str(Stacks, ?MODULE_STRING) of
        0 ->
            %% Start the Docker stack
            os:cmd("docker stack deploy -c ../test/erlzk.yaml " ?MODULE_STRING),
            true;
        _Exists ->
            %% The Docker stack is already running, leave it intact
            false
    end.

cleanup_docker(false) ->
    ok;
cleanup_docker(true) ->
    os:cmd("docker stack rm " ?MODULE_STRING).

setup() ->
    erlzk:start(),
    {ok, [ServerList, Timeout, Chroot, AuthData]} = file:consult("../test/erlzk.conf"),
    {ServerList, Timeout, Chroot, AuthData}.

create({ServerList, Timeout, _Chroot, _AuthData}) ->
    {ok, Pid} = connect_and_wait(ServerList, Timeout),

    ?assertMatch({error, bad_arguments}, erlzk:create(Pid, ".")),
    ?assertMatch({error, bad_arguments}, erlzk:create(Pid, "/.")),
    ?assertMatch({error, bad_arguments}, erlzk:create(Pid, "/..")),
    ?assertMatch({error, bad_arguments}, erlzk:create(Pid, "a")),

    ?assertMatch({error, no_node}, erlzk:create(Pid, "/a/")),
    ?assertMatch({error, no_node}, erlzk:create(Pid, "/a/./b")),
    ?assertMatch({error, no_node}, erlzk:create(Pid, "/a/../b")),
    ?assertMatch({error, no_node}, erlzk:create(Pid, "/a/b")),

    ?assertMatch({error, node_exists}, erlzk:create(Pid, "/zookeeper")), % /zookeeper is reserved
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a")),
    ?assertMatch({error, node_exists}, erlzk:create(Pid, "/a")),
    ?assertMatch(ok, erlzk:delete(Pid, "/a")),

    ?assertMatch({error, invalid_acl}, erlzk:create(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL)),

    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", ephemeral)),
    ?assertMatch({error, no_children_for_ephemerals}, erlzk:create(Pid, "/a/b")),
    ?assertMatch(ok, erlzk:delete(Pid, "/a")),

    ?assertMatch(ok, erlzk:add_auth(Pid, "foo", "bar")),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL)),
    erlzk:close(Pid),
    {ok, P} = connect_and_wait(ServerList, Timeout),
    ?assertMatch({error, no_auth}, erlzk:create(P, "/a/b")),
    ?assertMatch(ok, erlzk:add_auth(P, "foo", "bar")),
    ?assertMatch({ok, "/a/b"}, erlzk:create(P, "/a/b")),
    ?assertMatch(ok, erlzk:delete(P, "/a/b")),
    ?assertMatch(ok, erlzk:delete(P, "/a")),
    erlzk:close(P),
    ok.

create_api({ServerList, Timeout, _Chroot, _AuthData}) ->
    {ok, Pid} = connect_and_wait(ServerList, Timeout),

    ?assertMatch(ok, erlzk:add_auth(Pid, "foo", "bar")),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a")),
    ?assertMatch({ok, "/b"}, erlzk:create(Pid, "/b", ?ZK_ACL_CREATOR_ALL_ACL)),
    ?assertMatch({ok, "/c"}, erlzk:create(Pid, "/c", [?ZK_ACL_CREATOR_ALL_ACL])),
    ?assertMatch({ok, "/d"}, erlzk:create(Pid, "/d", persistent)),
    ?assertMatch({ok, "/e"}, erlzk:create(Pid, "/e", <<"e">>, ?ZK_ACL_CREATOR_ALL_ACL)),
    ?assertMatch({ok, "/f"}, erlzk:create(Pid, "/f", <<"f">>, [?ZK_ACL_CREATOR_ALL_ACL])),
    ?assertMatch({ok, "/g"}, erlzk:create(Pid, "/g", <<"g">>, persistent)),
    ?assertMatch({ok, "/h"}, erlzk:create(Pid, "/h", ?ZK_ACL_CREATOR_ALL_ACL, persistent)),
    ?assertMatch({ok, "/i"}, erlzk:create(Pid, "/i", [?ZK_ACL_CREATOR_ALL_ACL], persistent)),
    ?assertMatch({ok, "/j"}, erlzk:create(Pid, "/j", <<"j">>, ?ZK_ACL_CREATOR_ALL_ACL, persistent)),
    ?assertMatch({ok, "/k"}, erlzk:create(Pid, "/k", <<"k">>, [?ZK_ACL_CREATOR_ALL_ACL], persistent)),

    ?assertMatch(ok, erlzk:delete(Pid, "/a")),
    ?assertMatch(ok, erlzk:delete(Pid, "/b")),
    ?assertMatch(ok, erlzk:delete(Pid, "/c")),
    ?assertMatch(ok, erlzk:delete(Pid, "/d")),
    ?assertMatch(ok, erlzk:delete(Pid, "/e")),
    ?assertMatch(ok, erlzk:delete(Pid, "/f")),
    ?assertMatch(ok, erlzk:delete(Pid, "/g")),
    ?assertMatch(ok, erlzk:delete(Pid, "/h")),
    ?assertMatch(ok, erlzk:delete(Pid, "/i")),
    ?assertMatch(ok, erlzk:delete(Pid, "/j")),
    ?assertMatch(ok, erlzk:delete(Pid, "/k")),
    erlzk:close(Pid),
    ok.

create_mode({ServerList, Timeout, _Chroot, _AuthData}) ->
    {ok, Pid} = connect_and_wait(ServerList, Timeout),

    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", ephemeral)),
    erlzk:close(Pid),
    {ok, P} = connect_and_wait(ServerList, Timeout),
    ?assertMatch({error, no_node}, erlzk:exists(P, "/a")),

    ?assertMatch({ok, "/seq"}, erlzk:create(P, "/seq")),
    {ok, "/seq/a0000000000"} = erlzk:create(P, "/seq/a", persistent_sequential),
    {ok, "/seq/a0000000001"} = erlzk:create(P, "/seq/a", persistent_sequential),
    {ok, "/seq/a0000000002"} = erlzk:create(P, "/seq/a", persistent_sequential),
    {ok, Children} = erlzk:get_children(P, "/seq"),
    ["a0000000000","a0000000001","a0000000002"] = lists:sort(Children),
    erlzk:close(P),
    {ok, P0} = connect_and_wait(ServerList, Timeout),
    ?assertMatch(ok, erlzk:delete(P0, "/seq/a0000000000")),
    ?assertMatch(ok, erlzk:delete(P0, "/seq/a0000000001")),
    ?assertMatch(ok, erlzk:delete(P0, "/seq/a0000000002")),
    ?assertMatch(ok, erlzk:delete(P0, "/seq")),

    ?assertMatch({ok, "/seq"}, erlzk:create(P0, "/seq")),
    {ok, "/seq/a0000000000"} = erlzk:create(P0, "/seq/a", ephemeral_sequential),
    {ok, "/seq/a0000000001"} = erlzk:create(P0, "/seq/a", ephemeral_sequential),
    {ok, "/seq/a0000000002"} = erlzk:create(P0, "/seq/a", ephemeral_sequential),
    {ok, Children0} = erlzk:get_children(P0, "/seq"),
    ["a0000000000","a0000000001","a0000000002"] = lists:sort(Children0),
    erlzk:close(P0),
    {ok, P1} = connect_and_wait(ServerList, Timeout),
    ?assertMatch({ok, []}, erlzk:get_children(P1, "/seq")),
    ?assertMatch(ok, erlzk:delete(P1, "/seq")),
    erlzk:close(P1),
    ok.

delete({ServerList, Timeout, _Chroot, _AuthData}) ->
    {ok, Pid} = connect_and_wait(ServerList, Timeout),

    ?assertMatch({error, no_node}, erlzk:delete(Pid, "/a")),

    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a")),
    ?assertMatch({ok, "/a/b"}, erlzk:create(Pid, "/a/b")),
    ?assertMatch({error, not_empty}, erlzk:delete(Pid, "/a")),
    ?assertMatch(ok, erlzk:delete(Pid, "/a/b")),
    ?assertMatch(ok, erlzk:delete(Pid, "/a")),

    % {ok, {"/a", Stat}} = erlzk:create2(Pid, "/a"),
    % ?assertMatch({error, bad_version}, erlzk:delete(Pid, "/a", Stat#stat.version + 1)),
    % ?assertMatch(ok, erlzk:delete(Pid, "/a", Stat#stat.version)),

    ?assertMatch(ok, erlzk:add_auth(Pid, "foo", "bar")),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL)),
    ?assertMatch({ok, "/a/b"}, erlzk:create(Pid, "/a/b")),
    erlzk:close(Pid),
    {ok, P} = connect_and_wait(ServerList, Timeout),
    ?assertMatch({error, no_auth}, erlzk:delete(P, "/a/b")),
    ?assertMatch(ok, erlzk:add_auth(P, "foo", "bar")),
    ?assertMatch(ok, erlzk:delete(P, "/a/b")),
    ?assertMatch(ok, erlzk:delete(P, "/a")),
    erlzk:close(P),
    ok.

exists({ServerList, Timeout, _Chroot, _AuthData}) ->
    {ok, Pid} = connect_and_wait(ServerList, Timeout),

    ?assertMatch({error, no_node}, erlzk:exists(Pid, "/a")),

    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a")),
    ?assertMatch({ok, _Stat}, erlzk:exists(Pid, "/a")),
    ?assertMatch(ok, erlzk:delete(Pid, "/a")),
    erlzk:close(Pid),
    ok.

get_data({ServerList, Timeout, _Chroot, _AuthData}) ->
    {ok, Pid} = connect_and_wait(ServerList, Timeout),

    ?assertMatch({error, no_node}, erlzk:get_data(Pid, "/a")),

    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", <<"a">>)),
    ?assertMatch({ok, {<<"a">>, _Stat}}, erlzk:get_data(Pid, "/a")),
    ?assertMatch(ok, erlzk:delete(Pid, "/a")),

    ?assertMatch(ok, erlzk:add_auth(Pid, "foo", "bar")),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", <<"a">>, ?ZK_ACL_CREATOR_ALL_ACL)),
    erlzk:close(Pid),
    {ok, P} = connect_and_wait(ServerList, Timeout),
    ?assertMatch({error, no_auth}, erlzk:get_data(P, "/a")),
    ?assertMatch(ok, erlzk:add_auth(P, "foo", "bar")),
    ?assertMatch({ok, {<<"a">>, _Stat}}, erlzk:get_data(P, "/a")),
    ?assertMatch(ok, erlzk:delete(P, "/a")),
    erlzk:close(P),
    ok.

set_data({ServerList, Timeout, _Chroot, _AuthData}) ->
    {ok, Pid} = connect_and_wait(ServerList, Timeout),

    ?assertMatch({error, no_node}, erlzk:set_data(Pid, "/a", <<"a">>)),

    % {ok, {"/a", Stat}} = erlzk:create2(Pid, "/a", <<"a">>),
    % ?assertMatch({error, bad_version}, erlzk:set_data(Pid, "/a", <<"b">>, Stat#stat.version + 1)),
    % ?assertMatch({ok, _Stat}, erlzk:set_data(Pid, "/a", <<"b">>, Stat#stat.version)),
    % ?assertMatch({ok, {<<"b">>, _Stat}}, erlzk:get_data(Pid, "/a")),
    % ?assertMatch(ok, erlzk:delete(Pid, "/a")),

    ?assertMatch(ok, erlzk:add_auth(Pid, "foo", "bar")),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", <<"a">>, ?ZK_ACL_CREATOR_ALL_ACL)),
    erlzk:close(Pid),
    {ok, P} = connect_and_wait(ServerList, Timeout),
    ?assertMatch({error, no_auth}, erlzk:set_data(P, "/a", <<"b">>)),
    ?assertMatch(ok, erlzk:add_auth(P, "foo", "bar")),
    ?assertMatch({ok, _Stat}, erlzk:set_data(P, "/a", <<"b">>)),
    ?assertMatch({ok, {<<"b">>, _Stat}}, erlzk:get_data(P, "/a")),
    ?assertMatch(ok, erlzk:delete(P, "/a")),
    erlzk:close(P),
    ok.

get_acl({ServerList, Timeout, _Chroot, _AuthData}) ->
    {ok, Pid} = connect_and_wait(ServerList, Timeout),

    ?assertMatch({error, no_node}, erlzk:get_acl(Pid, "/a")),

    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a")),
    ?assertMatch({ok, {[{rwcdr,"world","anyone"}], _Stat}}, erlzk:get_acl(Pid, "/a")),
    ?assertMatch(ok, erlzk:delete(Pid, "/a")),
    erlzk:close(Pid),
    ok.

set_acl({ServerList, Timeout, _Chroot, _AuthData}) ->
    {ok, Pid} = connect_and_wait(ServerList, Timeout),

    ?assertMatch({error, no_node}, erlzk:set_acl(Pid, "/a", ?ZK_ACL_OPEN_ACL_UNSAFE)),
    ?assertMatch({error, invalid_acl}, erlzk:set_acl(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL)),

    % {ok, {"/a", Stat}} = erlzk:create2(Pid, "/a"),
    % ?assertMatch({error, bad_version}, erlzk:set_acl(Pid, "/a", ?ZK_ACL_READ_ACL_UNSAFE, Stat#stat.version + 1)),
    % ?assertMatch({ok, _Stat}, erlzk:set_acl(Pid, "/a", ?ZK_ACL_READ_ACL_UNSAFE, Stat#stat.version)),
    % ?assertMatch({ok, {[{r,"world","anyone"}], _Stat}}, erlzk:get_acl(Pid, "/a")),
    % ?assertMatch(ok, erlzk:delete(Pid, "/a")),

    ?assertMatch(ok, erlzk:add_auth(Pid, "foo", "bar")),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL)),
    Digest = erlzk:generate_digest("foo", "bar"),
    ?assertMatch({ok, {[{rwcdr,"digest",Digest}], _Stat}}, erlzk:get_acl(Pid, "/a")),
    erlzk:close(Pid),
    {ok, P} = connect_and_wait(ServerList, Timeout),
    ?assertMatch({error, no_auth}, erlzk:set_acl(P, "/a", ?ZK_ACL_READ_ACL_UNSAFE)),
    ?assertMatch(ok, erlzk:add_auth(P, "foo", "bar")),
    ?assertMatch({ok, _Stat}, erlzk:set_acl(P, "/a", ?ZK_ACL_READ_ACL_UNSAFE)),
    ?assertMatch({ok, {[{r,"world","anyone"}], _Stat}}, erlzk:get_acl(P, "/a")),
    ?assertMatch(ok, erlzk:delete(P, "/a")),
    erlzk:close(P),
    ok.

get_children({ServerList, Timeout, _Chroot, _AuthData}) ->
    {ok, Pid} = connect_and_wait(ServerList, Timeout),

    ?assertMatch({error, no_node}, erlzk:get_children(Pid, "/a")),

    ?assertMatch(ok, erlzk:add_auth(Pid, "foo", "bar")),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL)),
    ?assertMatch({ok, "/b"}, erlzk:create(Pid, "/b", ?ZK_ACL_CREATOR_ALL_ACL)),
    ?assertMatch({ok, "/b/a"}, erlzk:create(Pid, "/b/a")),
    ?assertMatch({ok, "/b/b"}, erlzk:create(Pid, "/b/b")),
    ?assertMatch({ok, "/b/c"}, erlzk:create(Pid, "/b/c")),
    erlzk:close(Pid),
    {ok, P} = connect_and_wait(ServerList, Timeout),
    ?assertMatch({error, no_auth}, erlzk:get_children(P, "/a")),
    ?assertMatch(ok, erlzk:add_auth(P, "foo", "bar")),
    ?assertMatch({ok, []}, erlzk:get_children(P, "/a")),
    {ok, Children} = erlzk:get_children(P, "/b"),
    ?assertMatch(["a","b","c"], lists:sort(Children)),
    ?assertMatch(ok, erlzk:delete(P, "/a")),
    ?assertMatch(ok, erlzk:delete(P, "/b/a")),
    ?assertMatch(ok, erlzk:delete(P, "/b/b")),
    ?assertMatch(ok, erlzk:delete(P, "/b/c")),
    ?assertMatch(ok, erlzk:delete(P, "/b")),
    erlzk:close(P),
    ok.

multi({ServerList, Timeout, _Chroot, _AuthData}) ->
    {ok, Pid} = connect_and_wait(ServerList, Timeout),

    ?assertMatch({error, {bad_version,[{error,ok},{error,bad_version},{error,runtime_inconsistency}]}},
                 erlzk:multi(Pid, [erlzk:op({create, "/a"}),erlzk:op({check, "/a", 1}),erlzk:op({delete, "/a", 0})])),
    ?assertMatch({error, no_node}, erlzk:exists(Pid, "/a")),

    ?assertMatch({error, {bad_version,[{error,ok},{error,ok},{error,bad_version}]}},
                 erlzk:multi(Pid, [erlzk:op({create, "/a"}),erlzk:op({check, "/a", 0}),erlzk:op({delete, "/a", 1})])),
    ?assertMatch({error, no_node}, erlzk:exists(Pid, "/a")),

    ?assertMatch({ok, [{create,"/a"},{check},{delete}]},
                 erlzk:multi(Pid, [erlzk:op({create, "/a"}),erlzk:op({check, "/a", 0}),erlzk:op({delete, "/a", 0})])),
    ?assertMatch({error, no_node}, erlzk:exists(Pid, "/a")),

    ?assertMatch({error, {bad_version,[{error,ok},{error,ok},{error,bad_version},{error,runtime_inconsistency}]}},
                 erlzk:multi(Pid, [erlzk:op({create, "/a"}),erlzk:op({check, "/a", 0}),erlzk:op({set_data, "/a", <<"a">>, 1}),erlzk:op({delete, "/a", 1})])),
    ?assertMatch({error, no_node}, erlzk:exists(Pid, "/a")),

    ?assertMatch({ok, [{create,"/a"},{check},{set_data,_Stat},{delete}]},
                 erlzk:multi(Pid, [erlzk:op({create, "/a"}),erlzk:op({check, "/a", 0}),erlzk:op({set_data, "/a", <<"a">>, 0}),erlzk:op({delete, "/a", 1})])),
    ?assertMatch({error, no_node}, erlzk:exists(Pid, "/a")),

    ?assertMatch({error, {not_empty,[{error,ok},{error,ok},{error,not_empty}]}},
                 erlzk:multi(Pid, [erlzk:op({create, "/a"}),erlzk:op({create, "/a/b"}),erlzk:op({delete, "/a"})])),
    ?assertMatch({error, no_node}, erlzk:exists(Pid, "/a")),

    ?assertMatch({ok, [{create,"/a"},{create,"/a/b"},{delete},{delete}]},
                 erlzk:multi(Pid, [erlzk:op({create, "/a"}),erlzk:op({create, "/a/b"}),erlzk:op({delete, "/a/b"}),erlzk:op({delete, "/a"})])),
    ?assertMatch({error, no_node}, erlzk:exists(Pid, "/a")),

    erlzk:close(Pid),
    ok.

auth_data({ServerList, Timeout, _Chroot, AuthData}) ->

    {ok, Pid} = connect_and_wait(ServerList, Timeout, [{auth_data, AuthData}]),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL)),
    ?assertMatch(ok, erlzk:delete(Pid, "/a")),
    ok.

chroot({ServerList, Timeout, Chroot, _AuthData}) ->
    {ok, Pid} = connect_and_wait(ServerList, Timeout),
    ?assertMatch({ok, Chroot}, erlzk:create(Pid, Chroot)),
    erlzk:close(Pid),
    {ok, P} = connect_and_wait(ServerList, Timeout, [{chroot, Chroot}]),

    ?assertMatch({ok, "/a"}, erlzk:create(P, "/a", <<"a">>)),
    ?assertMatch({error, no_node}, erlzk:exists(P, Chroot ++ "/a")),
    ?assertMatch({ok, _Stat}, erlzk:exists(P, "/a")),
    ?assertMatch({ok, _Stat}, erlzk:exists(P, "/")),
    ?assertMatch({ok, {<<"a">>, _Stat}}, erlzk:get_data(P, "/a")),
    ?assertMatch({ok, _Stat}, erlzk:set_data(P, "/a", <<"b">>)),
    ?assertMatch({ok, {<<"b">>, _Stat}}, erlzk:get_data(P, "/a")),
    ?assertMatch({ok, {[{rwcdr,"world","anyone"}], _Stat}}, erlzk:get_acl(P, "/a")),
    ?assertMatch({ok, _Stat}, erlzk:set_acl(P, "/a", {rwcd,"world","anyone"})),
    ?assertMatch({ok, {[{rwcd,"world","anyone"}], _Stat}}, erlzk:get_acl(P, "/a")),
    ?assertMatch({ok, "/a/a"}, erlzk:create(P, "/a/a")),
    ?assertMatch({ok, "/a/b"}, erlzk:create(P, "/a/b")),
    ?assertMatch({ok, "/a/c"}, erlzk:create(P, "/a/c")),
    {ok, Children} = erlzk:get_children(P, "/a"),
    ?assertMatch(["a","b","c"], lists:sort(Children)),
    ?assertMatch(ok, erlzk:delete(P, "/a/a")),
    ?assertMatch(ok, erlzk:delete(P, "/a/b")),
    ?assertMatch(ok, erlzk:delete(P, "/a/c")),
    ?assertMatch(ok, erlzk:delete(P, "/a")),

    erlzk:close(P),
    {ok, P0} = connect_and_wait(ServerList, Timeout),
    ?assertMatch(ok, erlzk:delete(P0, Chroot)),
    erlzk:close(P0),
    ok.

watch({ServerList, Timeout, _Chroot, _AuthData}) ->
    {ok, Pid} = connect_and_wait(ServerList, Timeout),

    ExistCreateWatch = ?spawn_watch({node_created, <<"/a">>}),
    ?assertMatch({error, no_node}, erlzk:exists(Pid, "/a", ExistCreateWatch)),
    GetDataCreateWatch = ?spawn_watch(should_not_receive_event),
    ?assertMatch({error, no_node}, erlzk:get_data(Pid, "/a", GetDataCreateWatch)),
    GetChildCreateWatch = ?spawn_watch({node_children_changed, <<"/">>}),
    {ok, Children1} = erlzk:get_children(Pid, "/", GetChildCreateWatch),
    ?assertListContain(["zookeeper"], Children1),
    ?assertPending(ExistCreateWatch),
    ?assertPending(GetDataCreateWatch),
    ?assertPending(GetChildCreateWatch),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a")),
    ?assertCompleted(ExistCreateWatch),
    ?assertPending(GetDataCreateWatch),
    ?assertCompleted(GetChildCreateWatch),

    ExistAndGetDataChangedWatchReceiver = ?spawn_watch(node_data_changed),
    {ExistAndGetDataChangedWatch, _} = spawn_monitor(fun() ->
        receive
            WatchedEvent ->
                ?assertMatch({node_data_changed, <<"/a">>}, WatchedEvent),
                ExistAndGetDataChangedWatchReceiver ! node_data_changed,
                receive
                    %% should not receive the same event again
                    WatchedEvent ->
                        ?assert(false)
                end
        end
    end),
    ?assertMatch({ok, _Stat}, erlzk:exists(Pid, "/a", ExistAndGetDataChangedWatch)),
    ?assertMatch({ok, {<<>>, _Stat}}, erlzk:get_data(Pid, "/a", ExistAndGetDataChangedWatch)),
    ExistChangedWatch = ?spawn_watch({node_data_changed, <<"/a">>}),
    ?assertMatch({ok, _Stat}, erlzk:exists(Pid, "/a", ExistChangedWatch)),
    GetDataChangedWatch = ?spawn_watch({node_data_changed, <<"/a">>}),
    ?assertMatch({ok, {<<>>, _Stat}}, erlzk:get_data(Pid, "/a", GetDataChangedWatch)),
    GetChildDeleteWatch = ?spawn_watch({node_children_changed, <<"/">>}),
    {ok, Children2} = erlzk:get_children(Pid, "/", GetChildDeleteWatch),
    ?assertListContain(["a", "zookeeper"], Children2),
    GetChildDeleteWatch0 = ?spawn_watch({node_deleted, <<"/a">>}),
    ?assertMatch({ok, []}, erlzk:get_children(Pid, "/a", GetChildDeleteWatch0)),
    ?assertPending(ExistChangedWatch),
    ?assertPending(GetDataChangedWatch),
    ?assertPending(ExistAndGetDataChangedWatch),
    ?assertPending(ExistAndGetDataChangedWatchReceiver),
    ?assertPending(GetChildDeleteWatch),
    ?assertPending(GetChildDeleteWatch0),
    ?assertMatch({ok, _Stat}, erlzk:set_data(Pid, "/a", <<"a">>)),
    ?assertCompleted(ExistChangedWatch),
    ?assertCompleted(GetDataChangedWatch),
    ?assertPending(ExistAndGetDataChangedWatch),
    ?assertPending(GetChildDeleteWatch),
    ?assertPending(GetChildDeleteWatch0),
    ?assertCompleted(ExistAndGetDataChangedWatchReceiver),

    Receiver1 = ?spawn_watch(node_deleted),
    Receiver2 = ?spawn_watch(node_children_changed),
    {AllWatch, _} = spawn_monitor(fun() ->
        loop_watch_all([
                        {{node_deleted, <<"/a">>}, {Receiver1, node_deleted}},
                        {{node_children_changed, <<"/">>}, {Receiver2, node_children_changed}}])
    end),
    ExistDeleteWatch = ?spawn_watch({node_deleted, <<"/a">>}),
    ?assertMatch({ok, _Stat}, erlzk:exists(Pid, "/a", ExistDeleteWatch)),
    ?assertMatch({ok, _Stat}, erlzk:exists(Pid, "/a", AllWatch)),
    GetDataDeleteWatch = ?spawn_watch({node_deleted, <<"/a">>}),
    ?assertMatch({ok, {<<"a">>, _Stat}}, erlzk:get_data(Pid, "/a", GetDataDeleteWatch)),
    ?assertMatch({ok, {<<"a">>, _Stat}}, erlzk:get_data(Pid, "/a", AllWatch)),
    {ok, Children3} = erlzk:get_children(Pid, "/", AllWatch),
    ?assertListContain(["a","zookeeper"], Children3),
    ?assertPending(ExistDeleteWatch),
    ?assertPending(GetDataDeleteWatch),
    ?assertPending(GetChildDeleteWatch),
    ?assertPending(GetChildDeleteWatch0),
    ?assertPending(Receiver1),
    ?assertPending(Receiver2),
    ?assertMatch(ok, erlzk:delete(Pid, "/a")),
    ?assertCompleted(ExistDeleteWatch),
    ?assertCompleted(GetDataDeleteWatch),
    ?assertCompleted(GetChildDeleteWatch),
    ?assertCompleted(GetChildDeleteWatch0),
    ?assertPending(AllWatch),
    ?assertCompleted(Receiver1),
    ?assertCompleted(Receiver2),

    erlzk:close(Pid),
    ok.

reconnect_to_same({ServerList, Timeout, _Chroot, _AuthData}) ->
    %% Ensure we will reconnect to the same ZK server by passing only
    %% one server to the connection
    {Host, Port} = hd(ServerList),
    {ok, Addr} = inet:getaddr(Host, inet),
    {ok, Pid} = connect_and_wait([{Host, Port}], Timeout),

    ExistCreateWatch = ?spawn_watch({node_created, <<"/a">>}),
    ?assertMatch({error, no_node}, erlzk:exists(Pid, "/a", ExistCreateWatch)),
    erlzk:kill_connection(Pid),

    receive
        DisconnectedMsg -> ?assertEqual({disconnected, Addr, Port}, DisconnectedMsg)
    after
        Timeout -> ?assert(false)
    end,
    receive
        ReconnectedMsg -> ?assertEqual({connected, Addr, Port}, ReconnectedMsg)
    after
        Timeout -> ?assert(false)
    end,

    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a")),
    ?assertCompleted(ExistCreateWatch),
    ?assertMatch(ok, erlzk:delete(Pid, "/a")),

    erlzk:close(Pid),
    ok.

reconnect_to_different({ServerList, Timeout, _Chroot, _AuthData}) ->
    %% Connect to ZK and make note of the ZK host selected by the
    %% connection process
    {ok, Pid} = erlzk:connect(ServerList, Timeout, [{monitor, self()}]),
    {Host, Port} = receive
                       {connected, H, P} -> {H, P}
                   after
                       Timeout ->  ?assert(false)
                   end,

    ExistCreateWatch = ?spawn_watch({node_created, <<"/a">>}),
    ?assertMatch({error, no_node}, erlzk:exists(Pid, "/a", ExistCreateWatch)),

    %% To ensure the connection will be established towards a
    %% different ZK host, remove the current server from the state of
    %% `Pid'
    sys:replace_state(Pid,
                      fun (State) ->
                              OldServers = element(2, State),
                              NewServers = lists:delete({Host, Port}, OldServers),
                              setelement(2, State, NewServers)
                      end,
                      infinity),
    erlzk:kill_connection(Pid),

    receive
        DisconnectedMsg -> ?assertEqual({disconnected, Host, Port}, DisconnectedMsg)
    after
        Timeout -> ?assert(false)
    end,
    receive
        ReconnectedMsg -> ?assertMatch({connected, NewHost, NewPort} when NewHost =/= Host orelse NewPort =/= Port,
                                       ReconnectedMsg)
    after
        Timeout -> ?assert(false)
    end,

    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a")),
    ?assertCompleted(ExistCreateWatch),
    ?assertMatch(ok, erlzk:delete(Pid, "/a")),

    erlzk:close(Pid),
    ok.

reconnect_with_stale_watches({ServerList, Timeout, _Chroot, _AuthData}) ->
    {ok, Pid} = connect_and_wait(ServerList, Timeout),

    ?assertEqual({ok, "/a"}, erlzk:create(Pid, "/a")),
    StaleDataWatch = ?spawn_watch({node_deleted, <<"/a">>}),
    ?assertMatch({ok, {<<>>, _}}, erlzk:get_data(Pid, "/a", StaleDataWatch)),
    StaleChildWatch = ?spawn_watch({node_deleted, <<"/a">>}),
    ?assertMatch({ok, []}, erlzk:get_children(Pid, "/a", StaleChildWatch)),
    StaleExistsWatch = ?spawn_watch({node_created, <<"/b">>}),
    ?assertEqual({error, no_node}, erlzk:exists(Pid, "/b", StaleExistsWatch)),

    ?assertEqual({ok, "/c"}, erlzk:create(Pid, "/c")),
    ValidDataWatch = ?spawn_watch({node_data_changed, <<"/c">>}),
    ?assertMatch({ok, {<<>>, _}}, erlzk:get_data(Pid, "/c", ValidDataWatch)),

    {ok, P} = connect_and_wait(ServerList, Timeout),
    erlzk:block_incoming_data(Pid),
    ?assertEqual(ok, erlzk:delete(P, "/a")),
    ?assertEqual({ok, "/b"}, erlzk:create(P, "/b")),
    ?assertEqual(ok, erlzk:delete(P, "/b")),
    ?assertMatch({ok, _}, erlzk:set_data(P, "/c", <<"changed">>)),

    erlzk:kill_connection(Pid),
    receive
        {connected, _Host, _Port} -> ok
    after
        Timeout -> ?assert(false)
    end,

    ?assertCompleted(ValidDataWatch),
    ?assertCompleted(StaleDataWatch),
    ?assertCompleted(StaleChildWatch),

    % This seems to be a ZooKeeper bug: the exists watch is not
    % triggered, but it should. So let's trigger it with a new event
    % until the bug gets fixed!
    erlzk:create(Pid, "/b"),
    ?assertCompleted(StaleExistsWatch),

    erlzk:close(P),
    erlzk:delete(Pid, "/a"),
    erlzk:delete(Pid, "/b"),
    erlzk:delete(Pid, "/c"),
    erlzk:close(Pid),
    ok.

loop_watch_all([]) ->
    receive
        _ ->
            %% should not receive events any more
            ?assert(false)
    end;
loop_watch_all(EventsList) ->
    Events = dict:from_list(EventsList),
    receive
        WatchedEvent ->
            ?assert(dict:is_key(WatchedEvent, Events)),
            {Receiver, Msg} = dict:fetch(WatchedEvent, Events),
            Receiver ! Msg,
            loop_watch_all(dict:to_list(dict:erase(WatchedEvent, Events)))
    end.

connect_and_wait(ServerList, Timeout) ->
    connect_and_wait(ServerList, Timeout, []).

connect_and_wait(ServerList, Timeout, Options) ->
    {ok, Pid} = erlzk:connect(ServerList, Timeout, [{monitor, self()} | Options]),
    receive
        {connected, _Host, _Port} -> {ok, Pid}
    after
        Timeout -> ?assert(false)
    end.
