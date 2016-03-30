%% 2013-2014 (c) Mega Yu <yuhg2310@gmail.com>
%% 2013-2014 (c) huaban.com <www.huaban.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%    http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%% @doc Convenience API to connect / disconnect to and to communicate with ZooKeeper.
-module(erlzk).

-include("erlzk.hrl").

-export([start/0, stop/0, connect/2, connect/3, connect/4, kill_session/1, close/1]).
-export([create/2, create/3, create/4, create/5, delete/2, delete/3, exists/2, exists/3,
         get_data/2, get_data/3, set_data/3, set_data/4, get_acl/2, set_acl/3, set_acl/4,
         get_children/2, get_children/3, sync/2, get_children2/2, get_children2/3,
         multi/2, create2/2, create2/3, create2/4, create2/5, add_auth/3]).
-export([generate_digest/2, op/1]).

-type server_list() :: [{Host::nonempty_string(), Port::pos_integer()}].
-type options()     :: [{chroot, nonempty_string()} |
                        {disable_watch_auto_reset, boolean()} |
                        {disable_expire_reconnect, boolean()} |
                        {auth_data, [{Scheme::nonempty_string(), Id::binary()}]} |
                        {monitor, pid()}].
-type acl()         :: {[perms()], scheme(), id()}.
-type perms()       :: r | w | c | d | a.
-type scheme()      :: nonempty_string().
-type id()          :: string().
-type create_mode() :: persistent | p | ephemeral | e | persistent_sequential | ps | ephemeral_sequential | es.
-type op()          :: {create, nonempty_string(), binary(), [acl()], create_mode()} |
                       {delete, nonempty_string(), integer()} |
                       {set_data, nonempty_string(), binary(), integer()} |
                       {check, nonempty_string(), integer()}.
-type op_result()   :: {create, nonempty_string()} | {delete} | {set_data, #stat{}} | {check} | {error, atom()}.

%% ===================================================================
%% Initiate Functions
%% ===================================================================
%% @doc Start the application.
-spec start() -> ok | {error, term()}.
start() ->
    application:load(erlzk),
    erlzk_app:ensure_deps_started(),
    application:start(erlzk).

%% @doc Stop the application.
-spec stop() -> ok | {error, term()}.
stop() ->
    application:stop(erlzk).

%% @doc Connect to ZooKeeper.
%% @see connect/4
-spec connect(server_list(), pos_integer()) -> {ok, pid()} | {error, atom()}.
connect(ServerList, Timeout) ->
    connect(ServerList, Timeout, []).

%% @doc Connect to ZooKeeper.
%% @see connect/4
-spec connect(server_list(), pos_integer(), options()) -> {ok, pid()} | {error, atom()};
             ({local, Name::atom()} | {global, GlobalName::term()} | {via, Module::atom(), ViaName::term()},
              server_list(), pos_integer()) -> {ok, pid()} | {error, atom()}.
connect(ServerList, Timeout, Options) when is_list(Options) ->
    erlzk_conn_sup:start_conn([ServerList, Timeout, Options]);
connect(ServerName, ServerList, Timeout) when is_integer(Timeout) ->
    connect(ServerName, ServerList, Timeout, []).

%% @doc Connect to ZooKeeper.
%%
%% Timeout in milliseconds, Options contains:
%%
%% <em>chroot</em>: specify a node under which erlzk will operate on
%%
%% <em>disable_watch_auto_reset</em>: whether to disable resetting of watches after reconnection, default is false
%%
%% <em>disable_expire_reconnect</em>: whether to disable reconnection after a session has expired, default is false
%%
%% <em>auth_data</em>: the auths need to be added after connected
%%
%% <em>monitor</em>: a process receiving the message of connection state changing
-spec connect({local, Name::atom()} | {global, GlobalName::term()} | {via, Module::atom(), ViaName::term()},
              server_list(), pos_integer(), options()) -> {ok, pid()} | {error, atom()}.
connect(ServerName, ServerList, Timeout, Options) ->
    erlzk_conn_sup:start_conn([ServerName, ServerList, Timeout, Options]).

%% @doc Cause the session to terminate while retaining connection.
%%  (useful when testing session restart behaviour).
-spec kill_session(pid()) -> ok.
kill_session(Pid) ->
    erlzk_conn:kill_session(Pid).

%% @doc Disconnect to ZooKeeper.
-spec close(pid()) -> ok.
close(Pid) ->
    erlzk_conn:stop(Pid).

%% ===================================================================
%% ZooKeeper API
%% ===================================================================
%% @doc Create a node with the given path, return the actual path of the node.
%% @see create/5
-spec create(pid(), iodata()) -> {ok, Path::nonempty_string()} | {error, atom()}.
create(Pid, Path) ->
    create(Pid, Path, <<>>, [?ZK_ACL_OPEN_ACL_UNSAFE], persistent).

%% @doc Create a node with the given path, return the actual path of the node.
%% @see create/5
-spec create(pid(), iodata(), binary())      -> {ok, Path::nonempty_string()} | {error, atom()};
            (pid(), iodata(), acl())         -> {ok, Path::nonempty_string()} | {error, atom()};
            (pid(), iodata(), [acl()])       -> {ok, Path::nonempty_string()} | {error, atom()};
            (pid(), iodata(), create_mode()) -> {ok, Path::nonempty_string()} | {error, atom()}.
create(Pid, Path, Data) when is_binary(Data) ->
    create(Pid, Path, Data, [?ZK_ACL_OPEN_ACL_UNSAFE], persistent);
create(Pid, Path, Acl) when is_tuple(Acl) orelse is_list(Acl) ->
    create(Pid, Path, <<>>, Acl, persistent);
create(Pid, Path, CreateMode) when is_atom(CreateMode) ->
    create(Pid, Path, <<>>, [?ZK_ACL_OPEN_ACL_UNSAFE], CreateMode).

%% @doc Create a node with the given path, return the actual path of the node.
%% @see create/5
-spec create(pid(), iodata(), binary(), acl())         -> {ok, Path::nonempty_string()} | {error, atom()};
            (pid(), iodata(), binary(), [acl()])       -> {ok, Path::nonempty_string()} | {error, atom()};
            (pid(), iodata(), binary(), create_mode()) -> {ok, Path::nonempty_string()} | {error, atom()};
            (pid(), iodata(), acl(), create_mode())    -> {ok, Path::nonempty_string()} | {error, atom()};
            (pid(), iodata(), [acl()], create_mode())  -> {ok, Path::nonempty_string()} | {error, atom()}.
create(Pid, Path, Data, Acl) when is_binary(Data) andalso (is_tuple(Acl) orelse is_list(Acl)) ->
    create(Pid, Path, Data, Acl, persistent);
create(Pid, Path, Data, CreateMode) when is_binary(Data) andalso is_atom(CreateMode) ->
    create(Pid, Path, Data, [?ZK_ACL_OPEN_ACL_UNSAFE], CreateMode);
create(Pid, Path, Acl, CreateMode) when (is_tuple(Acl) orelse is_list(Acl)) andalso is_atom(CreateMode) ->
    create(Pid, Path, <<>>, Acl, CreateMode).

%% @doc Create a node with the given path, return the actual path of the node.
%%
%% Create a node with the given path in ZooKeeper, node data will be the given Data,
%% node acl will be the given acl, and create mode affect the creation of node.
%% An ephemeral node will be removed by the ZooKeeper automatically when the
%% connection associated with the creation of the node expires.
%%
%% The create mode can also specify to create a sequential node.
%% The actual path name of a sequential node will be the given path plus a
%% suffix "i" where i is the current sequential number of the node.
%% The sequence number is always fixed length of 10 digits, 0 padded.
%% Once such a node is created, the sequential number will be incremented by one.
%%
%% If the path is not start with "/" or end with "/" or contains illegal characters,
%% will return {error, bad_arguments}.
%%
%% {error, node_exists} will be returned if a node with the same actual path already exists.
%%
%% {error, no_node} will be returned if the parent node does not exist.
%%
%% {error, no_auth} will be returned if no authority.
%%
%% {error, invalid_acl} will be returned if the acl is empty or invalid.
%%
%% {error, no_children_for_ephemerals} will be returned if the parent node of the given
%% path is ephemeral, because an ephemeral node can't have children.
%%
%% {error, closed} will be returned during reconnecting.
%%
%% If the call is successful, will trigger all the watches left on the
%% node of the given path by {@link exists/3} and {@link get_data/3},
%% and the watches left on the parent node by {@link get_children/3}.
-spec create(pid(), iodata(), binary(), acl(), create_mode())   -> {ok, Path::nonempty_string()} | {error, atom()};
            (pid(), iodata(), binary(), [acl()], create_mode()) -> {ok, Path::nonempty_string()} | {error, atom()}.
create(Pid, Path, Data, Acl, CreateMode) when is_binary(Data) andalso is_tuple(Acl) andalso is_atom(CreateMode) ->
    create(Pid, Path, Data, [Acl], CreateMode);
create(Pid, Path, Data, Acl, CreateMode) when is_binary(Data) andalso is_list(Acl) andalso is_atom(CreateMode) ->
    erlzk_conn:create(Pid, Path, Data, Acl, CreateMode).

%% @doc Delete the node with the given path.
%% @see delete/3
-spec delete(pid(), iodata()) -> ok | {error, atom()}.
delete(Pid, Path) ->
    delete(Pid, Path, -1).

%% @doc Delete the node with the given path.
%%
%% The call will succeed if such a node exists, and the given version matches the node's version
%% (if the given version is -1, it matches any node's versions).
%%
%% {error, no_node} will be returned if the node does not exist.
%%
%% {error, bad_version} will be returned if the given version does not match the node's version.
%%
%% {error, no_auth} will be returned if no authority.
%%
%% {error, not_empty} will be returned if the node has children.
%%
%% {error, closed} will be returned during reconnecting.
%%
%% If the call is successful, will trigger all the watches left on the node of
%% the given path by {@link exists/3} and {@link get_data/3} and {@link get_children/3},
%% and the watches left on the parent node by {@link get_children/3}.
-spec delete(pid(), iodata(), integer()) -> ok | {error, atom()}.
delete(Pid, Path, Version) ->
    erlzk_conn:delete(Pid, Path, Version).

%% @doc Checks the existence of the node of the given path, return the stat of the node.
%% @see exists/3
-spec exists(pid(), iodata()) -> {ok, Stat::#stat{}} | {error, atom()}.
exists(Pid, Path) ->
    erlzk_conn:exists(Pid, Path, false).

%% @doc Checks the existence of the node of the given path, return the stat of the node.
%%
%% {error, no_node} will be returned if the node does not exist.
%%
%% {error, closed} will be returned during reconnecting.
%%
%% If the call is successful, a watch will be left on the node with the given path.
%% The watch will be triggered by a successful operation that {@link set_data/4} on the node,
%% or {@link create/5} / {@link delete/3} the node.
-spec exists(pid(), iodata(), pid()) -> {ok, Stat::#stat{}} | {error, atom()}.
exists(Pid, Path, Watcher) ->
    erlzk_conn:exists(Pid, Path, true, Watcher).

%% @doc Return the data and the stat of the node of the given path.
%% @see get_data/3
-spec get_data(pid(), iodata()) -> {ok, {Data::binary(), Stat::#stat{}}} | {error, atom()}.
get_data(Pid, Path) ->
    erlzk_conn:get_data(Pid, Path, false).

%% @doc Return the data and the stat of the node of the given path.
%%
%% {error, no_node} will be returned if the node does not exist.
%%
%% {error, no_auth} will be returned if no authority.
%%
%% {error, closed} will be returned during reconnecting.
%%
%% If the call is successful, a watch will be left on the node with the given path.
%% The watch will be triggered by a successful operation that {@link set_data/4} on the node,
%% or {@link create/5} / {@link delete/3} the node.
-spec get_data(pid(), iodata(), pid()) -> {ok, {Data::binary(), Stat::#stat{}}} | {error, atom()}.
get_data(Pid, Path, Watcher) ->
    erlzk_conn:get_data(Pid, Path, true, Watcher).

%% @doc Set the data for the node of the given path, return the stat of the node.
%% @see set_data/4
-spec set_data(pid(), iodata(), binary()) -> {ok, Stat::#stat{}} | {error, atom()}.
set_data(Pid, Path, Data) ->
    set_data(Pid, Path, Data, -1).

%% @doc Set the data for the node of the given path, return the stat of the node.
%%
%% The call will succeed if such a node exists, and the given version matches the node's version
%% (if the given version is -1, it matches any node's versions).
%%
%% {error, no_node} will be returned if the node does not exist.
%%
%% {error, bad_version} will be returned if the given version does not match the node's version.
%%
%% {error, no_auth} will be returned if no authority.
%%
%% {error, closed} will be returned during reconnecting.
%%
%% If the call is successful, will trigger all the watches on the node of the given path
%% left by {@link exists/3} and {@link get_data/3} calls.
-spec set_data(pid(), iodata(), binary(), integer()) -> {ok, Stat::#stat{}} | {error, atom()}.
set_data(Pid, Path, Data, Version) when is_binary(Data) ->
    erlzk_conn:set_data(Pid, Path, Data, Version).

%% @doc Get the ACL of the node of the given path, return the ACL and the stat of the node.
%%
%% {error, no_node} will be returned if the node does not exist.
%%
%% {error, closed} will be returned during reconnecting.
-spec get_acl(pid(), iodata()) -> {ok, {Acl::[acl()], Stat::#stat{}}} | {error, atom()}.
get_acl(Pid, Path) ->
    erlzk_conn:get_acl(Pid, Path).

%% @doc Set the ACL of the node of the given path, return the ACL and the stat of the node.
%% @see set_acl/4
-spec set_acl(pid(), iodata(), acl())   -> {ok, Stat::#stat{}} | {error, atom()};
             (pid(), iodata(), [acl()]) -> {ok, Stat::#stat{}} | {error, atom()}.
set_acl(Pid, Path, Acl) ->
    set_acl(Pid, Path, Acl, -1).

%% @doc Set the ACL of the node of the given path, return the ACL and the stat of the node.
%%
%% {error, no_node} will be returned if the node does not exist.
%%
%% {error, bad_version} will be returned if the given version does not match the node's version.
%%
%% {error, no_auth} will be returned if no authority.
%%
%% {error, closed} will be returned during reconnecting.
%%
%% If the acl is empty or invalid, will return {error, invalid_acl}.
-spec set_acl(pid(), iodata(), acl(), integer())   -> {ok, Stat::#stat{}} | {error, atom()};
             (pid(), iodata(), [acl()], integer()) -> {ok, Stat::#stat{}} | {error, atom()}.
set_acl(Pid, Path, Acl, Version) when is_tuple(Acl) ->
    set_acl(Pid, Path, [Acl], Version);
set_acl(Pid, Path, Acl, Version) when is_list(Acl) ->
    erlzk_conn:set_acl(Pid, Path, Acl, Version).

%% @doc Return the list of the children of the node of the given path.
%% @see get_children/3
-spec get_children(pid(), iodata()) -> {ok, Children::[nonempty_string()]} | {error, atom()}.
get_children(Pid, Path) ->
    erlzk_conn:get_children(Pid, Path, false).

%% @doc Return the list of the children of the node of the given path.
%%
%% {error, no_node} will be returned if the node does not exist.
%%
%% {error, no_auth} will be returned if no authority.
%%
%% {error, closed} will be returned during reconnecting.
%%
%% If the call is successful, a watch will be left on the node with the given path.
%% The watch will be triggered by a successful operation that {@link delete/3} the node of the given path
%% or {@link create/5} / {@link delete/3} a child under the node.
%%
%% The list of children returned is not sorted and no guarantee is provided
%% as to its natural or lexical order.
-spec get_children(pid(), iodata(), pid()) -> {ok, Children::[nonempty_string()]} | {error, atom()}.
get_children(Pid, Path, Watcher) ->
    erlzk_conn:get_children(Pid, Path, true, Watcher).

%% @doc Flushes channel between process and leader.
%%
%% {error, closed} will be returned during reconnecting.
%%
%% If it is important that multiple client read the same value, call this function before read.
-spec sync(pid(), iodata()) -> {ok, Path::nonempty_string()} | {error, atom()}.
sync(Pid, Path) ->
    erlzk_conn:sync(Pid, Path).

%% @doc Return the list of the children and the stat of the node of the given path.
%% @see get_children/3
-spec get_children2(pid(), iodata()) -> {ok, {Children::[nonempty_string()], Stat::#stat{}}} | {error, atom()}.
get_children2(Pid, Path) ->
    erlzk_conn:get_children2(Pid, Path, false).

%% @doc Return the list of the children and the stat of the node of the given path.
%% @see get_children/3
-spec get_children2(pid(), iodata(), pid()) -> {ok, {Children::[nonempty_string()], Stat::#stat{}}} | {error, atom()}.
get_children2(Pid, Path, Watcher) ->
    erlzk_conn:get_children2(Pid, Path, true, Watcher).

%% @doc Executes multiple ZooKeeper operations or none of them.
%%
%% Use {@link op/1} to constructe operations.
%%
%% On success, a list of results is returned.
%%
%% On failure, a tuple contains first error code and a list of results is returned, or
%% {error, closed} will be returned during reconnecting.
-spec multi(pid(), [op()]) -> {ok, [op_result()]} | {error, {atom(), [op_result()]}}.
multi(Pid, Ops) ->
    erlzk_conn:multi(Pid, Ops).

%% @doc Create a node with the given path, return the actual path and the stat of the node.
%%
%% ATTENTION: ZooKeeper latest stable v3.4.6 removed create2 function, but it works fine in
%% v3.4.5 or current developing v3.5.0 (ZooKeeper added it again).
%%
%% @see create/5
-spec create2(pid(), iodata()) -> {ok, {Path::nonempty_string(), Stat::#stat{}}} | {error, atom()}.
create2(Pid, Path) ->
    create2(Pid, Path, <<>>, [?ZK_ACL_OPEN_ACL_UNSAFE], persistent).

%% @doc Create a node with the given path, return the actual path and the stat of the node.
%%
%% ATTENTION: ZooKeeper latest stable v3.4.6 removed create2 function, but it works fine in
%% v3.4.5 or current developing v3.5.0 (ZooKeeper added it again).
%%
%% @see create/5
-spec create2(pid(), iodata(), binary())      -> {ok, {Path::nonempty_string(), Stat::#stat{}}} | {error, atom()};
             (pid(), iodata(), acl())         -> {ok, {Path::nonempty_string(), Stat::#stat{}}} | {error, atom()};
             (pid(), iodata(), [acl()])       -> {ok, {Path::nonempty_string(), Stat::#stat{}}} | {error, atom()};
             (pid(), iodata(), create_mode()) -> {ok, {Path::nonempty_string(), Stat::#stat{}}} | {error, atom()}.
create2(Pid, Path, Data) when is_binary(Data) ->
    create2(Pid, Path, Data, [?ZK_ACL_OPEN_ACL_UNSAFE], persistent);
create2(Pid, Path, Acl) when is_tuple(Acl) orelse is_list(Acl) ->
    create2(Pid, Path, <<>>, Acl, persistent);
create2(Pid, Path, CreateMode) when is_atom(CreateMode) ->
    create2(Pid, Path, <<>>, [?ZK_ACL_OPEN_ACL_UNSAFE], CreateMode).

%% @doc Create a node with the given path, return the actual path and the stat of the node.
%%
%% ATTENTION: ZooKeeper latest stable v3.4.6 removed create2 function, but it works fine in
%% v3.4.5 or current developing v3.5.0 (ZooKeeper added it again).
%%
%% @see create/5
-spec create2(pid(), iodata(), binary(), acl())         -> {ok, {Path::nonempty_string(), Stat::#stat{}}} | {error, atom()};
             (pid(), iodata(), binary(), [acl()])       -> {ok, {Path::nonempty_string(), Stat::#stat{}}} | {error, atom()};
             (pid(), iodata(), binary(), create_mode()) -> {ok, {Path::nonempty_string(), Stat::#stat{}}} | {error, atom()};
             (pid(), iodata(), acl(), create_mode())    -> {ok, {Path::nonempty_string(), Stat::#stat{}}} | {error, atom()};
             (pid(), iodata(), [acl()], create_mode())  -> {ok, {Path::nonempty_string(), Stat::#stat{}}} | {error, atom()}.
create2(Pid, Path, Data, Acl) when is_binary(Data) andalso (is_tuple(Acl) orelse is_list(Acl)) ->
    create2(Pid, Path, Data, Acl, persistent);
create2(Pid, Path, Data, CreateMode) when is_binary(Data) andalso is_atom(CreateMode) ->
    create2(Pid, Path, Data, [?ZK_ACL_OPEN_ACL_UNSAFE], CreateMode);
create2(Pid, Path, Acl, CreateMode) when (is_tuple(Acl) orelse is_list(Acl)) andalso is_atom(CreateMode) ->
    create2(Pid, Path, <<>>, Acl, CreateMode).

%% @doc Create a node with the given path, return the actual path and the stat of the node.
%%
%% ATTENTION: ZooKeeper latest stable v3.4.6 removed create2 function, but it works fine in
%% v3.4.5 or current developing v3.5.0 (ZooKeeper added it again).
%%
%% @see create/5
-spec create2(pid(), iodata(), binary(), acl(), create_mode())   -> {ok, {Path::nonempty_string(), Stat::#stat{}}} | {error, atom()};
             (pid(), iodata(), binary(), [acl()], create_mode()) -> {ok, {Path::nonempty_string(), Stat::#stat{}}} | {error, atom()}.
create2(Pid, Path, Data, Acl, CreateMode) when is_binary(Data) andalso is_tuple(Acl) andalso is_atom(CreateMode) ->
    create2(Pid, Path, Data, [Acl], CreateMode);
create2(Pid, Path, Data, Acl, CreateMode) when is_binary(Data) andalso is_list(Acl) andalso is_atom(CreateMode) ->
    erlzk_conn:create2(Pid, Path, Data, Acl, CreateMode).

%% @doc Add the specified scheme and auth information to this connection.
%%
%% If using <em>digest</em> scheme, the auth information is the username:password in binary,
%% or username and password in clear text directly.
%%
%% If using <em>ip</em> scheme, the auth information is the form addr/bits where the most significant bits of addr are
%% matched against the most significant bits of the client host IP.
-spec add_auth(pid(), nonempty_string(), nonempty_string()) -> ok | {error, atom()};
              (pid(), nonempty_string(), binary())          -> ok | {error, atom()}.
add_auth(Pid, Username, Password) when is_list(Password) ->
    Auth = list_to_binary(Username ++ ":" ++ Password),
    add_auth(Pid, "digest", Auth);
add_auth(Pid, Scheme, Auth) when is_binary(Auth) ->
    erlzk_conn:add_auth(Pid, Scheme, Auth).

%% ===================================================================
%% Helper Functions
%% ===================================================================
%% @doc Generate Username:base64 encoded SHA1 Password digest string
-spec generate_digest(nonempty_string(), nonempty_string()) -> EncodedString::nonempty_string().
generate_digest(Username, Password) ->
    Auth = list_to_binary(Username ++ ":" ++ Password),
    Sha1 = crypto:hash(sha, Auth),
    Base64 = base64:encode(Sha1),
    Username ++ ":" ++ binary_to_list(Base64).

%% @doc Construtes a single operation in a multi-operation transaction.
%%
%% Each operation can be a <em>create</em>, <em>set_data</em> or <em>delete</em> or can just be a version <em>check</em>.
%%
%% Accept a tuple as parameters, including a operation atom and args, the format of the args is the
%% same as the function of the corresponding operation, except no pid.
%%
%% <em>check</em> accept the Path of a node (nonempty_string()) and the Version of the node (non_neg_integer())
%%
%% @see create/2
%% @see create/3
%% @see create/4
%% @see create/5
%% @see delete/2
%% @see delete/3
%% @see set_data/3
%% @see set_data/4
op({create, Path}) ->
    {create, Path, <<>>, [?ZK_ACL_OPEN_ACL_UNSAFE], persistent};
op({create, Path, Data}) when is_binary(Data) ->
    {create, Path, Data, [?ZK_ACL_OPEN_ACL_UNSAFE], persistent};
op({create, Path, Acl}) when is_tuple(Acl) orelse is_list(Acl) ->
    {create, Path, <<>>, Acl, persistent};
op({create, Path, CreateMode}) when is_atom(CreateMode) ->
    {create, Path, <<>>, [?ZK_ACL_OPEN_ACL_UNSAFE], CreateMode};
op({create, Path, Data, Acl}) when is_binary(Data) andalso (is_tuple(Acl) orelse is_list(Acl)) ->
    {create, Path, Data, Acl, persistent};
op({create, Path, Data, CreateMode}) when is_binary(Data) andalso is_atom(CreateMode) ->
    {create, Path, Data, [?ZK_ACL_OPEN_ACL_UNSAFE], CreateMode};
op({create, Path, Acl, CreateMode}) when (is_tuple(Acl) orelse is_list(Acl)) andalso is_atom(CreateMode) ->
    {create, Path, <<>>, Acl, CreateMode};
op({create, Path, Data, Acl, CreateMode}) when is_binary(Data) andalso is_tuple(Acl) andalso is_atom(CreateMode) ->
    {create, Path, Data, [Acl], CreateMode};
op({create, Path, Data, Acl, CreateMode}) when is_binary(Data) andalso is_list(Acl) andalso is_atom(CreateMode) ->
    {create, Path, Data, Acl, CreateMode};
op({delete, Path}) ->
    {delete, Path, -1};
op({delete, Path, Version}) ->
    {delete, Path, Version};
op({set_data, Path, Data}) ->
    {set_data, Path, Data, -1};
op({set_data, Path, Data, Version}) when is_list(Data) ->
    {set_data, Path, list_to_binary(Data), Version};
op({set_data, Path, Data, Version}) when is_binary(Data) ->
    {set_data, Path, Data, Version};
op({check, Path, Version}) ->
    {check, Path, Version}.
