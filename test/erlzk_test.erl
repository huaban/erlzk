-module(erlzk_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("../include/erlzk.hrl").

erlzk_test_() ->
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
                              fun auth_data/1,
                              fun chroot/1,
                              fun watch/1]]}.

setup() ->
    {ok, [ServerList, Timeout, Chroot, AuthData]} = file:consult("../test/erlzk.conf"),
    {ServerList, Timeout, Chroot, AuthData}.

create({ServerList, Timeout, _Chroot, _AuthData}) ->
    erlzk:start(),
    {ok, Pid} = erlzk:connect(ServerList, Timeout),

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
    ?assertMatch({ok}, erlzk:delete(Pid, "/a")),

    ?assertMatch({error, invalid_acl}, erlzk:create(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL)),

    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", ephemeral)),
    ?assertMatch({error, no_children_for_ephemerals}, erlzk:create(Pid, "/a/b")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/a")),

    ?assertMatch({ok}, erlzk:add_auth(Pid, "foo", "bar")),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL)),
    erlzk:close(Pid),
    {ok, P} = erlzk:connect([{"localhost",2181}], 30000),
    ?assertMatch({error, no_auth}, erlzk:create(P, "/a/b")),
    ?assertMatch({ok}, erlzk:add_auth(P, "foo", "bar")),
    ?assertMatch({ok, "/a/b"}, erlzk:create(P, "/a/b")),
    ?assertMatch({ok}, erlzk:delete(P, "/a/b")),
    ?assertMatch({ok}, erlzk:delete(P, "/a")),
    erlzk:close(P),
    ok.

create_api({ServerList, Timeout, _Chroot, _AuthData}) ->
    erlzk:start(),
    {ok, Pid} = erlzk:connect(ServerList, Timeout),

    ?assertMatch({ok}, erlzk:add_auth(Pid, "foo", "bar")),
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

    ?assertMatch({ok}, erlzk:delete(Pid, "/a")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/b")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/c")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/d")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/e")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/f")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/g")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/h")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/i")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/j")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/k")),
    erlzk:close(Pid),
    ok.

create_mode({ServerList, Timeout, _Chroot, _AuthData}) ->
    erlzk:start(),
    {ok, Pid} = erlzk:connect(ServerList, Timeout),

    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", ephemeral)),
    erlzk:close(Pid),
    {ok, P} = erlzk:connect(ServerList, Timeout),
    ?assertMatch({error, no_node}, erlzk:exists(P, "/a")),

    ?assertMatch({ok, "/seq"}, erlzk:create(P, "/seq")),
    {ok, "/seq/a0000000000"} = erlzk:create(P, "/seq/a", persistent_sequential),
    {ok, "/seq/a0000000001"} = erlzk:create(P, "/seq/a", persistent_sequential),
    {ok, "/seq/a0000000002"} = erlzk:create(P, "/seq/a", persistent_sequential),
    {ok, Children} = erlzk:get_children(P, "/seq"),
    ["a0000000000","a0000000001","a0000000002"] = lists:sort(Children),
    erlzk:close(P),
    {ok, P0} = erlzk:connect(ServerList, Timeout),
    ?assertMatch({ok}, erlzk:delete(P0, "/seq/a0000000000")),
    ?assertMatch({ok}, erlzk:delete(P0, "/seq/a0000000001")),
    ?assertMatch({ok}, erlzk:delete(P0, "/seq/a0000000002")),
    ?assertMatch({ok}, erlzk:delete(P0, "/seq")),

    ?assertMatch({ok, "/seq"}, erlzk:create(P0, "/seq")),
    {ok, "/seq/a0000000000"} = erlzk:create(P0, "/seq/a", ephemeral_sequential),
    {ok, "/seq/a0000000001"} = erlzk:create(P0, "/seq/a", ephemeral_sequential),
    {ok, "/seq/a0000000002"} = erlzk:create(P0, "/seq/a", ephemeral_sequential),
    {ok, Children0} = erlzk:get_children(P0, "/seq"),
    ["a0000000000","a0000000001","a0000000002"] = lists:sort(Children0),
    erlzk:close(P0),
    {ok, P1} = erlzk:connect(ServerList, Timeout),
    ?assertMatch({ok, []}, erlzk:get_children(P1, "/seq")),
    ?assertMatch({ok}, erlzk:delete(P1, "/seq")),
    erlzk:close(P1),
    ok.

delete({ServerList, Timeout, _Chroot, _AuthData}) ->
    erlzk:start(),
    {ok, Pid} = erlzk:connect(ServerList, Timeout),

    ?assertMatch({error, no_node}, erlzk:delete(Pid, "/a")),

    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a")),
    ?assertMatch({ok, "/a/b"}, erlzk:create(Pid, "/a/b")),
    ?assertMatch({error, not_empty}, erlzk:delete(Pid, "/a")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/a/b")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/a")),

    {ok, {"/a", Stat}} = erlzk:create2(Pid, "/a"),
    ?assertMatch({error, bad_version}, erlzk:delete(Pid, "/a", Stat#stat.version + 1)),
    ?assertMatch({ok}, erlzk:delete(Pid, "/a", Stat#stat.version)),

    ?assertMatch({ok}, erlzk:add_auth(Pid, "foo", "bar")),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL)),
    ?assertMatch({ok, "/a/b"}, erlzk:create(Pid, "/a/b")),
    erlzk:close(Pid),
    {ok, P} = erlzk:connect(ServerList, Timeout),
    ?assertMatch({error, no_auth}, erlzk:delete(P, "/a/b")),
    ?assertMatch({ok}, erlzk:add_auth(P, "foo", "bar")),
    ?assertMatch({ok}, erlzk:delete(P, "/a/b")),
    ?assertMatch({ok}, erlzk:delete(P, "/a")),
    erlzk:close(P),
    ok.

exists({ServerList, Timeout, _Chroot, _AuthData}) ->
    erlzk:start(),
    {ok, Pid} = erlzk:connect(ServerList, Timeout),

    ?assertMatch({error, no_node}, erlzk:exists(Pid, "/a")),

    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a")),
    ?assertMatch({ok, _Stat}, erlzk:exists(Pid, "/a")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/a")),
    erlzk:close(Pid),
    ok.

get_data({ServerList, Timeout, _Chroot, _AuthData}) ->
    erlzk:start(),
    {ok, Pid} = erlzk:connect(ServerList, Timeout),

    ?assertMatch({error, no_node}, erlzk:get_data(Pid, "/a")),

    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", <<"a">>)),
    ?assertMatch({ok, {<<"a">>, _Stat}}, erlzk:get_data(Pid, "/a")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/a")),

    ?assertMatch({ok}, erlzk:add_auth(Pid, "foo", "bar")),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", <<"a">>, ?ZK_ACL_CREATOR_ALL_ACL)),
    erlzk:close(Pid),
    {ok, P} = erlzk:connect(ServerList, Timeout),
    ?assertMatch({error, no_auth}, erlzk:get_data(P, "/a")),
    ?assertMatch({ok}, erlzk:add_auth(P, "foo", "bar")),
    ?assertMatch({ok, {<<"a">>, _Stat}}, erlzk:get_data(P, "/a")),
    ?assertMatch({ok}, erlzk:delete(P, "/a")),
    erlzk:close(P),
    ok.

set_data({ServerList, Timeout, _Chroot, _AuthData}) ->
    erlzk:start(),
    {ok, Pid} = erlzk:connect(ServerList, Timeout),

    ?assertMatch({error, no_node}, erlzk:set_data(Pid, "/a", <<"a">>)),

    {ok, {"/a", Stat}} = erlzk:create2(Pid, "/a", <<"a">>),
    ?assertMatch({error, bad_version}, erlzk:set_data(Pid, "/a", <<"b">>, Stat#stat.version + 1)),
    ?assertMatch({ok, _Stat}, erlzk:set_data(Pid, "/a", <<"b">>, Stat#stat.version)),
    ?assertMatch({ok, {<<"b">>, _Stat}}, erlzk:get_data(Pid, "/a")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/a")),

    ?assertMatch({ok}, erlzk:add_auth(Pid, "foo", "bar")),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", <<"a">>, ?ZK_ACL_CREATOR_ALL_ACL)),
    erlzk:close(Pid),
    {ok, P} = erlzk:connect(ServerList, Timeout),
    ?assertMatch({error, no_auth}, erlzk:set_data(P, "/a", <<"b">>)),
    ?assertMatch({ok}, erlzk:add_auth(P, "foo", "bar")),
    ?assertMatch({ok, _Stat}, erlzk:set_data(P, "/a", <<"b">>)),
    ?assertMatch({ok, {<<"b">>, _Stat}}, erlzk:get_data(P, "/a")),
    ?assertMatch({ok}, erlzk:delete(P, "/a")),
    erlzk:close(P),
    ok.

get_acl({ServerList, Timeout, _Chroot, _AuthData}) ->
    erlzk:start(),
    {ok, Pid} = erlzk:connect(ServerList, Timeout),

    ?assertMatch({error, no_node}, erlzk:get_acl(Pid, "/a")),

    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a")),
    ?assertMatch({ok, {[{rwcdr,"world","anyone"}], _Stat}}, erlzk:get_acl(Pid, "/a")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/a")),
    erlzk:close(Pid),
    ok.

set_acl({ServerList, Timeout, _Chroot, _AuthData}) ->
    erlzk:start(),
    {ok, Pid} = erlzk:connect(ServerList, Timeout),

    ?assertMatch({error, no_node}, erlzk:set_acl(Pid, "/a", ?ZK_ACL_OPEN_ACL_UNSAFE)),
    ?assertMatch({error, invalid_acl}, erlzk:set_acl(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL)),

    {ok, {"/a", Stat}} = erlzk:create2(Pid, "/a"),
    ?assertMatch({error, bad_version}, erlzk:set_acl(Pid, "/a", ?ZK_ACL_READ_ACL_UNSAFE, Stat#stat.version + 1)),
    ?assertMatch({ok, _Stat}, erlzk:set_acl(Pid, "/a", ?ZK_ACL_READ_ACL_UNSAFE, Stat#stat.version)),
    ?assertMatch({ok, {[{r,"world","anyone"}], _Stat}}, erlzk:get_acl(Pid, "/a")),
    ?assertMatch({ok}, erlzk:delete(Pid, "/a")),

    ?assertMatch({ok}, erlzk:add_auth(Pid, "foo", "bar")),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL)),
    Digest = erlzk:generate_digest("foo", "bar"),
    ?assertMatch({ok, {[{rwcdr,"digest",Digest}], _Stat}}, erlzk:get_acl(Pid, "/a")),
    erlzk:close(Pid),
    {ok, P} = erlzk:connect(ServerList, Timeout),
    ?assertMatch({error, no_auth}, erlzk:set_acl(P, "/a", ?ZK_ACL_READ_ACL_UNSAFE)),
    ?assertMatch({ok}, erlzk:add_auth(P, "foo", "bar")),
    ?assertMatch({ok, _Stat}, erlzk:set_acl(P, "/a", ?ZK_ACL_READ_ACL_UNSAFE)),
    ?assertMatch({ok, {[{r,"world","anyone"}], _Stat}}, erlzk:get_acl(P, "/a")),
    ?assertMatch({ok}, erlzk:delete(P, "/a")),
    erlzk:close(P),
    ok.

get_children({ServerList, Timeout, _Chroot, _AuthData}) ->
    erlzk:start(),
    {ok, Pid} = erlzk:connect(ServerList, Timeout),

    ?assertMatch({error, no_node}, erlzk:get_children(Pid, "/a")),

    ?assertMatch({ok}, erlzk:add_auth(Pid, "foo", "bar")),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL)),
    ?assertMatch({ok, "/b"}, erlzk:create(Pid, "/b", ?ZK_ACL_CREATOR_ALL_ACL)),
    ?assertMatch({ok, "/b/a"}, erlzk:create(Pid, "/b/a")),
    ?assertMatch({ok, "/b/b"}, erlzk:create(Pid, "/b/b")),
    ?assertMatch({ok, "/b/c"}, erlzk:create(Pid, "/b/c")),
    erlzk:close(Pid),
    {ok, P} = erlzk:connect(ServerList, Timeout),
    ?assertMatch({error, no_auth}, erlzk:get_children(P, "/a")),
    ?assertMatch({ok}, erlzk:add_auth(P, "foo", "bar")),
    ?assertMatch({ok, []}, erlzk:get_children(P, "/a")),
    {ok, Children} = erlzk:get_children(P, "/b"),
    ?assertMatch(["a","b","c"], lists:sort(Children)),
    ?assertMatch({ok}, erlzk:delete(P, "/a")),
    ?assertMatch({ok}, erlzk:delete(P, "/b/a")),
    ?assertMatch({ok}, erlzk:delete(P, "/b/b")),
    ?assertMatch({ok}, erlzk:delete(P, "/b/c")),
    ?assertMatch({ok}, erlzk:delete(P, "/b")),
    erlzk:close(P),
    ok.

auth_data({ServerList, Timeout, _Chroot, AuthData}) ->
    erlzk:start(),

    {ok, Pid} = erlzk:connect(ServerList, Timeout, [{auth_data, AuthData}]),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL)),
    ?assertMatch({ok}, erlzk:delete(Pid, "/a")),
    ok.

chroot({ServerList, Timeout, Chroot, _AuthData}) ->
    erlzk:start(),
    {ok, Pid} = erlzk:connect(ServerList, Timeout),
    ?assertMatch({ok, Chroot}, erlzk:create(Pid, Chroot)),
    erlzk:close(Pid),
    {ok, P} = erlzk:connect(ServerList, Timeout, [{chroot, Chroot}]),

    ?assertMatch({ok, "/a"}, erlzk:create(P, "/a", <<"a">>)),
    ?assertMatch({error, no_node}, erlzk:exists(P, Chroot ++ "/a")),
    ?assertMatch({ok, _Stat}, erlzk:exists(P, "/a")),
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
    ?assertMatch({ok}, erlzk:delete(P, "/a/a")),
    ?assertMatch({ok}, erlzk:delete(P, "/a/b")),
    ?assertMatch({ok}, erlzk:delete(P, "/a/c")),
    ?assertMatch({ok}, erlzk:delete(P, "/a")),
 
    erlzk:close(P),
    {ok, P0} = erlzk:connect(ServerList, Timeout),
    ?assertMatch({ok}, erlzk:delete(P0, Chroot)),
    erlzk:close(P0),
    ok.

watch({ServerList, Timeout, _Chroot, _AuthData}) ->
    erlzk:start(),
    {ok, Pid} = erlzk:connect(ServerList, Timeout),

    ExistCreateWatch = spawn(fun() ->
        receive
            WatchedEvent ->
                ?assertMatch({exists,"/a",node_created}, WatchedEvent)
        end
    end),
    ?assertMatch({error, no_node}, erlzk:exists(Pid, "/a", ExistCreateWatch)),
    GetDataCreateWatch = spawn(fun() ->
        receive
            WatchedEvent ->
                ?assertMatch({get_data,"/a",node_created}, WatchedEvent)
        end
    end),
    ?assertMatch({error, no_node}, erlzk:get_data(Pid, "/a", GetDataCreateWatch)),
    GetChildCreateWatch = spawn(fun() ->
        receive
            WatchedEvent ->
                ?assertMatch({get_children,"/",node_children_changed}, WatchedEvent)
        end
    end),
    ?assertMatch({ok, ["zookeeper"]}, erlzk:get_children(Pid, "/", GetChildCreateWatch)),
    ?assertEqual(true, erlang:is_process_alive(ExistCreateWatch)),
    ?assertEqual(true, erlang:is_process_alive(GetDataCreateWatch)),
    ?assertEqual(true, erlang:is_process_alive(GetChildCreateWatch)),
    ?assertMatch({ok, "/a"}, erlzk:create(Pid, "/a")),
    ?assertEqual(false, erlang:is_process_alive(ExistCreateWatch)),
    ?assertEqual(false, erlang:is_process_alive(GetDataCreateWatch)),
    ?assertEqual(false, erlang:is_process_alive(GetChildCreateWatch)),

    ExistChangedWatch = spawn(fun() ->
        receive
            WatchedEvent ->
                ?assertMatch({exists,"/a",node_data_changed}, WatchedEvent)
        end
    end),
    ?assertMatch({ok, _Stat}, erlzk:exists(Pid, "/a", ExistChangedWatch)),
    GetDataChangedWatch = spawn(fun() ->
        receive
            WatchedEvent ->
                ?assertMatch({get_data,"/a",node_data_changed}, WatchedEvent)
        end
    end),
    ?assertMatch({ok, {<<>>, _Stat}}, erlzk:get_data(Pid, "/a", GetDataChangedWatch)),
    GetChildDeleteWatch = spawn(fun() ->
        receive
            WatchedEvent ->
                ?assertMatch({get_children,"/",node_children_changed}, WatchedEvent)
        end
    end),
    {ok, Children} = erlzk:get_children(Pid, "/", GetChildDeleteWatch),
    ?assertMatch(["a","zookeeper"], lists:sort(Children)),
    GetChildDeleteWatch0 = spawn(fun() ->
        receive
            WatchedEvent ->
                ?assertMatch({get_children,"/a",node_deleted}, WatchedEvent)
        end
    end),
    ?assertMatch({ok, []}, erlzk:get_children(Pid, "/a", GetChildDeleteWatch0)),
    ?assertEqual(true, erlang:is_process_alive(ExistChangedWatch)),
    ?assertEqual(true, erlang:is_process_alive(GetDataChangedWatch)),
    ?assertEqual(true, erlang:is_process_alive(GetChildDeleteWatch)),
    ?assertEqual(true, erlang:is_process_alive(GetChildDeleteWatch0)),
    ?assertMatch({ok, _Stat}, erlzk:set_data(Pid, "/a", <<"a">>)),
    ?assertEqual(false, erlang:is_process_alive(ExistChangedWatch)),
    ?assertEqual(false, erlang:is_process_alive(GetDataChangedWatch)),
    ?assertEqual(true, erlang:is_process_alive(GetChildDeleteWatch)),
    ?assertEqual(true, erlang:is_process_alive(GetChildDeleteWatch0)),

    ExistDeleteWatch = spawn(fun() ->
        receive
            WatchedEvent ->
                ?assertMatch({exists,"/a",node_deleted}, WatchedEvent)
        end
    end),
    ?assertMatch({ok, _Stat}, erlzk:exists(Pid, "/a", ExistDeleteWatch)),
    GetDataDeleteWatch = spawn(fun() ->
        receive
            WatchedEvent ->
                ?assertMatch({get_data,"/a",node_deleted}, WatchedEvent)
        end
    end),
    ?assertMatch({ok, {<<"a">>, _Stat}}, erlzk:get_data(Pid, "/a", GetDataDeleteWatch)),
    ?assertEqual(true, erlang:is_process_alive(ExistDeleteWatch)),
    ?assertEqual(true, erlang:is_process_alive(GetDataDeleteWatch)),
    ?assertEqual(true, erlang:is_process_alive(GetChildDeleteWatch)),
    ?assertEqual(true, erlang:is_process_alive(GetChildDeleteWatch0)),
    ?assertMatch({ok}, erlzk:delete(Pid, "/a")),
    ?assertEqual(false, erlang:is_process_alive(ExistDeleteWatch)),
    ?assertEqual(false, erlang:is_process_alive(GetDataDeleteWatch)),
    ?assertEqual(false, erlang:is_process_alive(GetChildDeleteWatch)),
    ?assertEqual(false, erlang:is_process_alive(GetChildDeleteWatch0)),

    erlzk:close(Pid),
    ok.
