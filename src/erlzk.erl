-module(erlzk).

-include_lib("../include/erlzk.hrl").

-export([start_link/2, start_link/3, stop/1]).
-export([create/2, create/3, create/4, create/5, delete/2, delete/3, exists/2, exists/3,
         get_data/2, get_data/3, set_data/3, set_data/4, get_acl/2, set_acl/3, set_acl/4,
         get_children/2, get_children/3, sync/2, get_children2/2, get_children2/3]).

start_link(ServerList, Timeout) ->
    erlzk_conn_sup:start_conn([ServerList, Timeout]).

start_link(ServerName, ServerList, Timeout) ->
    erlzk_conn_sup:start_conn([ServerName, ServerList, Timeout]).

stop(Pid) ->
    erlzk_conn:stop(Pid).

create(Pid, Path) ->
    create(Pid, Path, <<>>).

create(Pid, Path, Data) when is_list(Data) orelse is_binary(Data) ->
    create(Pid, Path, Data, [?ZK_ACL_OPEN_ACL_UNSAFE], persistent);
create(Pid, Path, Acl) when is_tuple(Acl) orelse is_list(Acl) ->
    create(Pid, Path, <<>>, Acl, persistent);
create(Pid, Path, CreateMode) when is_atom(CreateMode) ->
    create(Pid, Path, <<>>, [?ZK_ACL_OPEN_ACL_UNSAFE], CreateMode).

create(Pid, Path, Data, Acl) when (is_list(Data) orelse is_binary(Data)) andalso (is_tuple(Acl) orelse is_list(Acl)) ->
    create(Pid, Path, Data, Acl, persistent);
create(Pid, Path, Data, CreateMode) when (is_list(Data) orelse is_binary(Data)) andalso is_atom(CreateMode) ->
    create(Pid, Path, Data, [?ZK_ACL_OPEN_ACL_UNSAFE], CreateMode);
create(Pid, Path, Acl, CreateMode) when (is_tuple(Acl) orelse is_list(Acl)) andalso is_atom(CreateMode) ->
    create(Pid, Path, <<>>, Acl, CreateMode).

create(Pid, Path, Data, Acl, CreateMode) when is_list(Data) andalso is_tuple(Acl) andalso is_atom(CreateMode) ->
    create(Pid, Path, list_to_binary(Data), [Acl], CreateMode);
create(Pid, Path, Data, Acl, CreateMode) when is_binary(Data) andalso is_tuple(Acl) andalso is_atom(CreateMode) ->
    create(Pid, Path, Data, [Acl], CreateMode);
create(Pid, Path, Data, Acl, CreateMode) when is_list(Data) andalso is_list(Acl) andalso is_atom(CreateMode) ->
    create(Pid, Path, list_to_binary(Data), Acl, CreateMode);
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
