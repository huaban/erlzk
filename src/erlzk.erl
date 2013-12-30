-module(erlzk).

-include("erlzk.hrl").

-export([start/0, stop/0, connect/2, connect/3, connect/4, close/1]).
-export([create/2, create/3, create/4, create/5, delete/2, delete/3, exists/2, exists/3,
         get_data/2, get_data/3, set_data/3, set_data/4, get_acl/2, set_acl/3, set_acl/4,
         get_children/2, get_children/3, sync/2, get_children2/2, get_children2/3,
         create2/2, create2/3, create2/4, create2/5, add_auth/3]).
-export([generate_digest/2]).

%% ===================================================================
%% Initiate Functions
%% ===================================================================
start() ->
    application:load(erlzk),
    erlzk_app:ensure_deps_started(),
    application:start(erlzk).

stop() ->
    application:stop(erlzk).

connect(ServerList, Timeout) ->
    connect(ServerList, Timeout, []).

connect(ServerList, Timeout, Options) when is_list(Options) ->
    erlzk_conn_sup:start_conn([ServerList, Timeout, Options]);

connect(ServerName, ServerList, Timeout) when is_integer(Timeout) ->
    connect(ServerName, ServerList, Timeout, []).

connect(ServerName, ServerList, Timeout, Options) ->
    erlzk_conn_sup:start_conn([ServerName, ServerList, Timeout, Options]).

close(Pid) ->
    erlzk_conn:stop(Pid).

%% ===================================================================
%% ZooKeeper API
%% ===================================================================
create(Pid, Path) ->
    create(Pid, Path, <<>>).

create(Pid, Path, Data) when is_binary(Data) ->
    create(Pid, Path, Data, [?ZK_ACL_OPEN_ACL_UNSAFE], persistent);
create(Pid, Path, Acl) when is_tuple(Acl) orelse is_list(Acl) ->
    create(Pid, Path, <<>>, Acl, persistent);
create(Pid, Path, CreateMode) when is_atom(CreateMode) ->
    create(Pid, Path, <<>>, [?ZK_ACL_OPEN_ACL_UNSAFE], CreateMode).

create(Pid, Path, Data, Acl) when is_binary(Data) andalso (is_tuple(Acl) orelse is_list(Acl)) ->
    create(Pid, Path, Data, Acl, persistent);
create(Pid, Path, Data, CreateMode) when is_binary(Data) andalso is_atom(CreateMode) ->
    create(Pid, Path, Data, [?ZK_ACL_OPEN_ACL_UNSAFE], CreateMode);
create(Pid, Path, Acl, CreateMode) when (is_tuple(Acl) orelse is_list(Acl)) andalso is_atom(CreateMode) ->
    create(Pid, Path, <<>>, Acl, CreateMode).

create(Pid, Path, Data, Acl, CreateMode) when is_binary(Data) andalso is_tuple(Acl) andalso is_atom(CreateMode) ->
    create(Pid, Path, Data, [Acl], CreateMode);
create(Pid, Path, Data, Acl, CreateMode) when is_binary(Data) andalso is_list(Acl) andalso is_atom(CreateMode) ->
    erlzk_conn:create(Pid, Path, Data, Acl, CreateMode).

delete(Pid, Path) ->
    delete(Pid, Path, -1).

delete(Pid, Path, Version) ->
    erlzk_conn:delete(Pid, Path, Version).

exists(Pid, Path) ->
    erlzk_conn:exists(Pid, Path, false).

exists(Pid, Path, Watcher) ->
    erlzk_conn:exists(Pid, Path, true, Watcher).

get_data(Pid, Path) ->
    erlzk_conn:get_data(Pid, Path, false).

get_data(Pid, Path, Watcher) ->
    erlzk_conn:get_data(Pid, Path, true, Watcher).

set_data(Pid, Path, Data) ->
    set_data(Pid, Path, Data, -1).

set_data(Pid, Path, Data, Version) when is_list(Data) ->
    set_data(Pid, Path, list_to_binary(Data), Version);
set_data(Pid, Path, Data, Version) when is_binary(Data) ->
    erlzk_conn:set_data(Pid, Path, Data, Version).

get_acl(Pid, Path) ->
    erlzk_conn:get_acl(Pid, Path).

set_acl(Pid, Path, Acl) ->
    set_acl(Pid, Path, Acl, -1).

set_acl(Pid, Path, Acl, Version) when is_tuple(Acl) ->
    set_acl(Pid, Path, [Acl], Version);
set_acl(Pid, Path, Acl, Version) when is_list(Acl) ->
    erlzk_conn:set_acl(Pid, Path, Acl, Version).

get_children(Pid, Path) ->
    erlzk_conn:get_children(Pid, Path, false).

get_children(Pid, Path, Watcher) ->
    erlzk_conn:get_children(Pid, Path, true, Watcher).

sync(Pid, Path) ->
    erlzk_conn:sync(Pid, Path).

get_children2(Pid, Path) ->
    erlzk_conn:get_children2(Pid, Path, false).

get_children2(Pid, Path, Watcher) ->
    erlzk_conn:get_children2(Pid, Path, true, Watcher).

create2(Pid, Path) ->
    create2(Pid, Path, <<>>).

create2(Pid, Path, Data) when is_binary(Data) ->
    create2(Pid, Path, Data, [?ZK_ACL_OPEN_ACL_UNSAFE], persistent);
create2(Pid, Path, Acl) when is_tuple(Acl) orelse is_list(Acl) ->
    create2(Pid, Path, <<>>, Acl, persistent);
create2(Pid, Path, CreateMode) when is_atom(CreateMode) ->
    create2(Pid, Path, <<>>, [?ZK_ACL_OPEN_ACL_UNSAFE], CreateMode).

create2(Pid, Path, Data, Acl) when is_binary(Data) andalso (is_tuple(Acl) orelse is_list(Acl)) ->
    create2(Pid, Path, Data, Acl, persistent);
create2(Pid, Path, Data, CreateMode) when is_binary(Data) andalso is_atom(CreateMode) ->
    create2(Pid, Path, Data, [?ZK_ACL_OPEN_ACL_UNSAFE], CreateMode);
create2(Pid, Path, Acl, CreateMode) when (is_tuple(Acl) orelse is_list(Acl)) andalso is_atom(CreateMode) ->
    create2(Pid, Path, <<>>, Acl, CreateMode).

create2(Pid, Path, Data, Acl, CreateMode) when is_binary(Data) andalso is_tuple(Acl) andalso is_atom(CreateMode) ->
    create2(Pid, Path, Data, [Acl], CreateMode);
create2(Pid, Path, Data, Acl, CreateMode) when is_binary(Data) andalso is_list(Acl) andalso is_atom(CreateMode) ->
    erlzk_conn:create2(Pid, Path, Data, Acl, CreateMode).

add_auth(Pid, Username, Password) when is_list(Password) ->
    Auth = list_to_binary(Username ++ ":" ++ Password),
    add_auth(Pid, "digest", Auth);
add_auth(Pid, Scheme, Auth) when is_binary(Auth) ->
    erlzk_conn:add_auth(Pid, Scheme, Auth).

%% ===================================================================
%% Helper Functions
%% ===================================================================
generate_digest(Username, Password) ->
    Auth = list_to_binary(Username ++ ":" ++ Password),
    Sha1 = crypto:hash(sha, Auth),
    Base64 = base64:encode(Sha1),
    Username ++ ":" ++ binary_to_list(Base64).
