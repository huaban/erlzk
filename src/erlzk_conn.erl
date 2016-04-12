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
-module(erlzk_conn).
-behaviour(gen_server).

-include("erlzk.hrl").
-include_lib("kernel/include/inet.hrl").

-export([start/3, start/4, start_link/3, start_link/4, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([create/5, delete/3, exists/3, exists/4, get_data/3, get_data/4, set_data/4, get_acl/2, set_acl/4,
         get_children/3, get_children/4, sync/2, get_children2/3, get_children2/4,
         multi/2, create2/5, add_auth/3, no_heartbeat/1, kill_session/1]).

-define(ZK_SOCKET_OPTS, [binary, {active, true}, {packet, 4}, {reuseaddr, true}, {linger, {true, 5}}]).
-ifdef(zk_connect_timeout).
-define(ZK_CONNECT_TIMEOUT, ?zk_connect_timeout).
-else.
-define(ZK_CONNECT_TIMEOUT, 10000).
-endif.
-ifdef(zk_reconnect_interval).
-define(ZK_RECONNECT_INTERVAL, ?zk_reconnect_interval).
-else.
-define(ZK_RECONNECT_INTERVAL, 1000).
-endif.
-ifdef(pre18).
-define(RANDOM_UNIFORM, random:uniform()).
-else.
-define(RANDOM_UNIFORM, rand:uniform()).
-endif.

-record(state, {
    servers = [],
    auth_data = [],
    chroot = "/",
    socket,
    host,
    port,
    proto_ver,
    timeout,
    session_id,
    password,
    ping_interval,
    xid = 1,
    zxid = 0,
    reset_watch = true,
    reconnect_expired = true,
    monitor,
    heartbeat_watcher,
    reqs = dict:new(),
    auths = queue:new(),
    watchers = {dict:new(), dict:new(), dict:new()}
}).

%% ===================================================================
%% Public API
%% ===================================================================
start(ServerList, Timeout, Options) ->
    gen_server:start(?MODULE, [ServerList, Timeout, Options], []).

start(ServerName, ServerList, Timeout, Options) ->
    gen_server:start(ServerName, ?MODULE, [ServerList, Timeout, Options], []).

start_link(ServerList, Timeout, Options) ->
    gen_server:start_link(?MODULE, [ServerList, Timeout, Options], []).

start_link(ServerName, ServerList, Timeout, Options) ->
    gen_server:start_link(ServerName, ?MODULE, [ServerList, Timeout, Options], []).

stop(Pid) ->
    gen_server:call(Pid, stop).

create(Pid, Path, Data, Acl, CreateMode) ->
    gen_server:call(Pid, {create, {Path, Data, Acl, CreateMode}}).

delete(Pid, Path, Version) ->
    gen_server:call(Pid, {delete, {Path, Version}}).

exists(Pid, Path, Watch) ->
    gen_server:call(Pid, {exists, {Path, Watch}}).

exists(Pid, Path, Watch, Watcher) ->
    gen_server:call(Pid, {exists, {Path, Watch}, Watcher}).

get_data(Pid, Path, Watch) ->
    gen_server:call(Pid, {get_data, {Path, Watch}}).

get_data(Pid, Path, Watch, Watcher) ->
    gen_server:call(Pid, {get_data, {Path, Watch}, Watcher}).

set_data(Pid, Path, Data, Version) ->
    gen_server:call(Pid, {set_data, {Path, Data, Version}}).

get_acl(Pid, Path) ->
    gen_server:call(Pid, {get_acl, {Path}}).

set_acl(Pid, Path, Acl, Version) ->
    gen_server:call(Pid, {set_acl, {Path, Acl, Version}}).

get_children(Pid, Path, Watch) ->
    gen_server:call(Pid, {get_children, {Path, Watch}}).

get_children(Pid, Path, Watch, Watcher) ->
    gen_server:call(Pid, {get_children, {Path, Watch}, Watcher}).

sync(Pid, Path) ->
    gen_server:call(Pid, {sync, {Path}}).

get_children2(Pid, Path, Watch) ->
    gen_server:call(Pid, {get_children2, {Path, Watch}}).

get_children2(Pid, Path, Watch, Watcher) ->
    gen_server:call(Pid, {get_children2, {Path, Watch}, Watcher}).

multi(Pid, Ops) ->
    gen_server:call(Pid, {multi, Ops}).

create2(Pid, Path, Data, Acl, CreateMode) ->
    gen_server:call(Pid, {create2, {Path, Data, Acl, CreateMode}}).

add_auth(Pid, Scheme, Auth) ->
    gen_server:call(Pid, {add_auth, {Scheme, Auth}}).

no_heartbeat(Pid) ->
    gen_server:cast(Pid, no_heartbeat).

kill_session(Pid) ->
    gen_server:call(Pid, kill_session).

%% ===================================================================
%% gen_server Callbacks
%% ===================================================================
init([ServerList, Timeout, Options]) ->
    Monitor = proplists:get_value(monitor, Options),
    ResetWatch = case proplists:get_value(disable_watch_auto_reset, Options) of
        true  -> false;
        _     -> true
    end,
    ReconnectExpired = case proplists:get_value(disable_expire_reconnect, Options) of
        true  -> false;
        _     -> true
    end,
    AuthData = case proplists:get_value(auth_data, Options) of
        undefined -> [];
        AdValue   -> AdValue
    end,
    Chroot = case proplists:get_value(chroot, Options) of
        undefined -> "/";
        BinValue when is_binary(BinValue) -> get_chroot_path(binary_to_list(BinValue));
        CrValue   -> get_chroot_path(CrValue)
    end,
    process_flag(trap_exit, true),
    ResolvedServerList = resolve_servers(ServerList),
    DedupedServerList = lists:usort(ResolvedServerList),
    ShuffledServerList = shuffle(DedupedServerList),
    ProtocolVersion = 0,
    Zxid = 0,
    SessionId = 0,
    Password = <<0:128>>,
    case connect(ShuffledServerList, ProtocolVersion, Zxid, Timeout, SessionId, Password) of
        {ok, State=#state{host=Host, port=Port, ping_interval=PingIntv,
                          heartbeat_watcher=HeartbeatWatcher}} ->
            NewState = State#state{auth_data=AuthData, chroot=Chroot,
                                   reset_watch=ResetWatch, reconnect_expired=ReconnectExpired,
                                   monitor=Monitor, heartbeat_watcher=HeartbeatWatcher},
            add_init_auths(AuthData, NewState),
            notify_monitor_server_state(Monitor, connected, Host, Port),
            {ok, NewState, PingIntv};
        {error, Reason} ->
            error_logger:error_msg("Connect fail: ~p, will be try again after ~ps~n", [Reason, ?ZK_RECONNECT_INTERVAL]),
            erlang:send_after(?ZK_RECONNECT_INTERVAL, self(), reconnect),
            State = #state{servers=ShuffledServerList, auth_data=AuthData, chroot=Chroot,
                           proto_ver=ProtocolVersion, timeout=Timeout, session_id=SessionId, password=Password,
                           reset_watch=ResetWatch, reconnect_expired=ReconnectExpired, monitor=Monitor},
            {ok, State}
    end.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_, _From, State=#state{socket=undefined, ping_interval=PingIntv}) ->
    {reply, {error, closed}, State, PingIntv};
handle_call({add_auth, Args}, From, State=#state{socket=Socket, ping_interval=PingIntv, auths=Auths}) ->
    case gen_tcp:send(Socket, erlzk_codec:pack(add_auth, Args, -4)) of
        ok ->
            NewAuths = queue:in(From, Auths),
            {noreply, State#state{auths=NewAuths}, PingIntv};
        {error, Reason} ->
            {reply, {error, Reason}, State, PingIntv}
    end;
handle_call({set_watches, Args}, _From, State=#state{socket=Socket, ping_interval=PingIntv}) ->
    case gen_tcp:send(Socket, erlzk_codec:pack(set_watches, Args, -8)) of
        ok ->
            {noreply, State, PingIntv};
        {error, Reason} ->
            {reply, {error, Reason}, State, PingIntv}
    end;
handle_call({Op, Args}, From, State=#state{chroot=Chroot, socket=Socket, xid=Xid, ping_interval=PingIntv, reqs=Reqs}) ->
    case gen_tcp:send(Socket, erlzk_codec:pack(Op, Args, Xid, Chroot)) of
        ok ->
            NewReqs = dict:store(Xid, {Op, From}, Reqs),
            {noreply, State#state{xid=Xid+1, reqs=NewReqs}, PingIntv};
        {error, Reason} ->
            {reply, {error, Reason}, State#state{xid=Xid+1}, PingIntv}
    end;
handle_call({Op, Args, Watcher}, From, State=#state{chroot=Chroot, socket=Socket, xid=Xid, ping_interval=PingIntv, reqs=Reqs}) ->
    case gen_tcp:send(Socket, erlzk_codec:pack(Op, Args, Xid, Chroot)) of
        ok ->
            Path = element(1, Args),
            NewReqs = dict:store(Xid, {Op, From, Path, Watcher}, Reqs),
            {noreply, State#state{xid=Xid+1, reqs=NewReqs}, PingIntv};
        {error, Reason} ->
            {reply, {error, Reason}, State#state{xid=Xid+1}, PingIntv}
    end;
handle_call(kill_session, _From, State=#state{servers=ServerList, proto_ver=ProtoVer, zxid=Zxid,
                                              timeout=Timeout, session_id=SessionId, password=Passwd,
                                              ping_interval=PingIntv}) ->
    % create a second connection for this zk session then close it - this is the approved
    % way to make a zk session timeout.
    case connect(ServerList, ProtoVer, Zxid, Timeout, SessionId, Passwd) of
        {ok, #state{socket=Socket, heartbeat_watcher=HeartbeatWatcher}} ->
            stop_heartbeat(HeartbeatWatcher),
            inet:setopts(Socket, [{active, false}]),
            close_connection(Socket),
            {reply, ok, State, PingIntv};
        {error, Reason} ->
            {reply, {error, Reason}, State, PingIntv}
    end;
handle_call(_Request, _From, State=#state{ping_interval=PingIntv}) ->
    {noreply, State, PingIntv}.

handle_cast(no_heartbeat, State=#state{host=Host, port=Port, monitor=Monitor, socket=Socket}) ->
    error_logger:error_msg("Connection to ~p:~p is not responding, will be closed and reconnect~n", [Host, Port]),
    close_connection(Socket, false),
    notify_monitor_server_state(Monitor, disconnected, Host, Port),
    State1 = notify_callers_closed(State),
    reconnect(State1);
handle_cast(_Request, State=#state{ping_interval=PingIntv}) ->
    {noreply, State, PingIntv}.

handle_info(timeout, State=#state{socket=undefined, ping_interval=PingIntv}) ->
    {noreply, State, PingIntv};
handle_info(timeout, State=#state{socket=Socket, ping_interval=PingIntv}) ->
    gen_tcp:send(Socket, <<-2:32, 11:32>>),
    {noreply, State, PingIntv};
handle_info({tcp, Socket, Packet}, State=#state{chroot=Chroot, socket=Socket, ping_interval=PingIntv,
                                               auths=Auths, reqs=Reqs, watchers=Watchers,
                                               heartbeat_watcher={HeartbeatWatcher, _HeartbeatRef}}) ->
    {Xid, Zxid, Code, Body} = erlzk_codec:unpack(Packet),
    erlzk_heartbeat:beat(HeartbeatWatcher),
    case Xid of
        -1 -> % watched event
            {EventType, _KeeperState, PathList} = erlzk_codec:unpack(watched_event, Body, Chroot),
            Path = list_to_binary(PathList),
            {Receivers, NewWatchers} = find_and_erase_watchers(EventType, Path, Watchers),
            send_watched_event(Receivers, Path, EventType),
            {noreply, State#state{zxid=Zxid, watchers=NewWatchers}, PingIntv};
        -2 -> % ping
            {noreply, State#state{zxid=Zxid}, PingIntv};
        -8 -> % set watches
            case Code of
                ok -> {noreply, State#state{zxid=Zxid}, PingIntv};
                _  -> {noreply, State#state{zxid=Zxid, watchers={dict:new(), dict:new(), dict:new()}}, PingIntv}
            end;
        -4 -> % auth
            case queue:out(Auths) of
                {{value, From}, NewAuths} ->
                    Reply = case Code of
                        ok -> ok;
                        _  -> {error, Code}
                    end,
                    if From =/= self() -> % init auth data reply don't need to notify
                        gen_server:reply(From, Reply)
                    end,
                    {noreply, State#state{zxid=Zxid, auths=NewAuths}, PingIntv};
                {empty, _} ->
                    {noreply, State#state{zxid=Zxid}, PingIntv}
            end;
        _  -> % normal reply
            case dict:find(Xid, Reqs) of
                {ok, Req} ->
                    {Op, From} = case Req of
                        {X, Y}       -> {X, Y};
                        {X, Y, _, _} -> {X, Y}
                    end,
                    NewReqs = dict:erase(Xid, Reqs),
                    Reply = get_reply_from_body(Code, Op, Body, Chroot),
                    NewWatchers = maybe_store_watcher(Code, Req, Watchers),
                    gen_server:reply(From, Reply),
                    {noreply, State#state{zxid=Zxid, reqs=NewReqs, watchers=NewWatchers}, PingIntv};
                error ->
                    {noreply, State#state{zxid=Zxid}, PingIntv}
            end
    end;
handle_info({tcp_closed, Socket}, State=#state{socket=Socket, host=Host, port=Port, monitor=Monitor, heartbeat_watcher=HeartbeatWatcher}) ->
    error_logger:error_msg("Connection to ~p:~p is broken, reconnect now~n", [Host, Port]),
    stop_heartbeat(HeartbeatWatcher),
    notify_monitor_server_state(Monitor, disconnected, Host, Port),
    State1 = notify_callers_closed(State),
    reconnect(State1#state{socket=undefined, heartbeat_watcher=undefined});
handle_info({tcp_error, Socket, Reason}, State=#state{socket=Socket, host=Host, port=Port, monitor=Monitor, heartbeat_watcher=HeartbeatWatcher}) ->
    error_logger:error_msg("Connection to ~p:~p meet an error, will be closed and reconnect: ~p~n", [Host, Port, Reason]),
    close_connection(Socket, false),
    stop_heartbeat(HeartbeatWatcher),
    notify_monitor_server_state(Monitor, disconnected, Host, Port),
    State1 = notify_callers_closed(State),
    reconnect(State1#state{socket=undefined, heartbeat_watcher=undefined});
handle_info(reconnect, State) ->
    reconnect(State);
handle_info({'DOWN', Ref, process, Pid, Reason}, State=#state{heartbeat_watcher={Pid, Ref}}) ->
    maybe_restart_heartbeat(Reason, State);
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State=#state{ping_interval=PingIntv}) ->
    {noreply, State, PingIntv};
handle_info({'EXIT', _Ref, Reason}, State) ->
    {stop, Reason, State};
handle_info(_Info, State=#state{ping_interval=PingIntv}) ->
    {noreply, State, PingIntv}.

terminate(normal, #state{socket=Socket, heartbeat_watcher=HeartbeatWatcher}) ->
    stop_heartbeat(HeartbeatWatcher),
    close_connection(Socket),
    error_logger:warning_msg("Server is closed~n"),
    ok;
terminate(shutdown, #state{socket=Socket, heartbeat_watcher=HeartbeatWatcher}) ->
    stop_heartbeat(HeartbeatWatcher),
    close_connection(Socket),
    error_logger:warning_msg("Server is shutdown~n"),
    ok;
terminate(Reason, #state{socket=Socket, heartbeat_watcher=HeartbeatWatcher}) ->
    error_logger:error_msg("Connection terminating with reason: ~p~n", [Reason]),
    stop_heartbeat(HeartbeatWatcher),
    close_connection(Socket),
    error_logger:error_msg("Server is terminated: ~p~n", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Internal Functions
%% ===================================================================
resolve_servers(ServerList) ->
    resolve_servers(ServerList, []).

resolve_servers([], ResolvedServerAcc) ->
    lists:flatten(ResolvedServerAcc);
resolve_servers([Server|Left], ResolvedServerAcc) ->
    resolve_servers(Left, [resolve_server(Server) | ResolvedServerAcc]).

resolve_server({Host, Port}) ->
    case inet:gethostbyname(Host) of
        {ok, #hostent{h_addr_list=Addresses}} ->
            [{Address, Port} || Address <- Addresses];
        {error, Reason} ->
            error_logger:error_msg("Resolving ~p:~p meet an error: ~p~n", [Host, Port, Reason]),
            []
    end.
    
shuffle(L) ->
    % Uses rand module rather than random when available, so initial seed is not constant and list is shuffled differently on first call
    [X||{_,X} <- lists:sort([{?RANDOM_UNIFORM, N} || N <- L])].

connect(ServerList, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword) ->
    connect(ServerList, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword, []).

connect([], _ProtocolVersion, _LastZxidSeen, _Timeout, _LastSessionId, _LastPassword, _FailedServerList) ->
    {error, no_available_server};
connect([Server={Host,Port}|Left], ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword, FailedServerList) ->
    error_logger:info_msg("Connecting to ~p:~p~n", [Host, Port]),
    case gen_tcp:connect(Host, Port, ?ZK_SOCKET_OPTS, ?ZK_CONNECT_TIMEOUT) of
        {ok, Socket} ->
            error_logger:info_msg("Connected ~p:~p, sending connect command~n", [Host, Port]),
            case gen_tcp:send(Socket, erlzk_codec:pack(connect, {ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword})) of
                ok ->
                    receive
                        {tcp, Socket, Packet} ->
                            {ProtoVer, RealTimeOut, SessionId, Password} = erlzk_codec:unpack(connect, Packet),
                            case SessionId of
                                0 ->
                                    error_logger:warning_msg("Session expired, connection to ~p:~p will be closed~n", [Host, Port]),
                                    gen_tcp:close(Socket),
                                    {error, {session_expired, Host, Port}};
                                _ ->
                                    error_logger:info_msg("Connection to ~p:~p is established~n", [Host, Port]),
                                    {ok, HeartbeatWatcher} = erlzk_heartbeat:start(self(), RealTimeOut * 2 div 3),
                                    ServerList = Left ++ lists:reverse([Server|FailedServerList]),
                                    {ok, #state{servers=ServerList, socket=Socket, host=Host, port=Port,
                                                proto_ver=ProtoVer, timeout=RealTimeOut, session_id=SessionId, password=Password,
                                                ping_interval=(RealTimeOut div 3), heartbeat_watcher={HeartbeatWatcher, erlang:monitor(process, HeartbeatWatcher)}}}
                            end;
                        {tcp_closed, Socket} ->
                            error_logger:error_msg("Connection to ~p:~p is closed~n", [Host, Port]),
                            connect(Left, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword, [Server|FailedServerList]);
                        {tcp_error, Socket, Reason} ->
                            error_logger:error_msg("Connection to ~p:~p meet an error: ~p~n", [Host, Port, Reason]),
                            gen_tcp:close(Socket),
                            connect(Left, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword, [Server|FailedServerList])
                    after ?ZK_CONNECT_TIMEOUT ->
                        error_logger:error_msg("Connection to ~p:~p timeout while waiting for connect reply: ~p~n", [Host, Port]),
                        gen_tcp:close(Socket),
                        connect(Left, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword, [Server|FailedServerList])
                    end;
                {error, Reason} ->
                    error_logger:error_msg("Sending connect command to ~p:~p meet an error: ~p~n", [Host, Port, Reason]),
                    gen_tcp:close(Socket),
                    connect(Left, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword, [Server|FailedServerList])
            end;
        {error, Reason} ->
            error_logger:error_msg("Connecting to ~p:~p meet an error: ~p~n", [Host, Port, Reason]),
            connect(Left, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword, [Server|FailedServerList])
    end.

reconnect(State=#state{servers=ServerList, auth_data=AuthData, chroot=Chroot, host=OldHost, port=OldPort,
                       proto_ver=ProtoVer, zxid=Zxid, timeout=Timeout, session_id=SessionId, password=Passwd,
                       xid=Xid, reset_watch=ResetWatch, reconnect_expired=ReconnectExpired,
                       monitor=Monitor, watchers=Watchers}) ->
    case connect(ServerList, ProtoVer, Zxid, Timeout, SessionId, Passwd) of
        {ok, NewState=#state{host=Host, port=Port, ping_interval=PingIntv, heartbeat_watcher=HeartbeatWatcher}} ->
            error_logger:warning_msg("Reconnect to ~p:~p successful~n", [Host, Port]),
            RenewState = NewState#state{auth_data=AuthData, chroot=Chroot, xid=Xid, zxid=Zxid,
                                        reset_watch=ResetWatch, reconnect_expired=ReconnectExpired,
                                        monitor=Monitor, heartbeat_watcher=HeartbeatWatcher, watchers=Watchers},
            RenewState2 = case {Host, Port} of
                {OldHost, OldPort} -> RenewState;
                _ -> reset_watch_return_new_state(RenewState, Watchers)
            end,
            notify_monitor_server_state(Monitor, connected, Host, Port),
            {noreply, RenewState2, PingIntv};
        {error, {session_expired, Host, Port}} ->
            notify_monitor_server_state(Monitor, expired, Host, Port),
            case ReconnectExpired of
                true ->
                    error_logger:warning_msg("Session expired, creating new connection now"),
                    reconnect_after_session_expired(State);
                false ->
                    error_logger:warning_msg("Session expired, will not reconnect"),
                    {noreply, State}
            end;
        {error, Reason} ->
            error_logger:error_msg("Connect fail: ~p, will be try again after ~ps~n", [Reason, ?ZK_RECONNECT_INTERVAL]),
            erlang:send_after(?ZK_RECONNECT_INTERVAL, self(), reconnect),
            {noreply, State}
    end.

reconnect_after_session_expired(State=#state{servers=ServerList, auth_data=AuthData, chroot=Chroot,
                                             timeout=Timeout, reset_watch=ResetWatch, monitor=Monitor, watchers=Watchers}) ->
    case connect(ServerList, 0, 0, Timeout, 0, <<0:128>>) of
        {ok, NewState=#state{host=Host, port=Port, ping_interval=PingIntv, heartbeat_watcher=HeartbeatWatcher}} ->
            error_logger:warning_msg("Create a new connection to ~p:~p successful~n", [Host, Port]),
            RenewState = reset_watch_return_new_state(NewState#state{auth_data=AuthData, chroot=Chroot,
                                                                     reset_watch=ResetWatch, monitor=Monitor,
                                                                     heartbeat_watcher=HeartbeatWatcher}, Watchers),
            add_init_auths(AuthData, RenewState),
            notify_monitor_server_state(Monitor, connected, Host, Port),
            {noreply, RenewState, PingIntv};
        {error, Reason} ->
            error_logger:error_msg("Connect fail: ~p, will be try again after ~ps~n", [Reason, ?ZK_RECONNECT_INTERVAL]),
            erlang:send_after(?ZK_RECONNECT_INTERVAL, self(), reconnect),
            {noreply, State}
    end.

add_init_auths([], _State) ->
    ok;
add_init_auths([AuthData|Left], State) ->
    handle_call({add_auth, AuthData}, self(), State),
    add_init_auths(Left, State).

reset_watch_return_new_state(State=#state{zxid=Zxid, reset_watch=ResetWatch}, Watchers={DataWatchers, ExistWatchers, ChildWatchers}) ->
    case ResetWatch of
        true  ->
            Args = {Zxid, dict:fetch_keys(DataWatchers), dict:fetch_keys(ExistWatchers), dict:fetch_keys(ChildWatchers)},
            handle_call({set_watches, Args}, self(), State),
            State#state{watchers=Watchers};
        false ->
            State#state{watchers={dict:new(), dict:new(), dict:new()}}
    end.

notify_monitor_server_state(Monitor, State, Host, Port) ->
    case Monitor of
        undefined ->
            ok;
        _ ->
            Monitor ! {State, Host, Port}
    end.

should_add_watcher(no_node, exists) -> true;
should_add_watcher(ok, _Op)         -> true;
should_add_watcher(_Code, _Op)      -> false.

maybe_store_watcher(_Code, {_Op, _From}, Watchers) ->
    Watchers;
maybe_store_watcher(Code, {Op, _From, Path, Watcher}, Watchers) ->
    case should_add_watcher(Code, Op) of
        false -> Watchers;
        true -> store_watcher(Op, Path, Watcher, Watchers)
    end.

notify_callers_closed(State=#state{reqs=Reqs}) ->
    notify_callers_closed(dict:to_list(Reqs)),
    State#state{reqs=dict:new()};
notify_callers_closed([]) ->
    ok;
notify_callers_closed([{_Xid, {_Op, From}}|Left]) ->
    gen_server:reply(From, {error, closed}),
    notify_callers_closed(Left).

store_watcher(Op, Path, Watcher, Watchers) when not is_binary(Path)->
    store_watcher(Op, iolist_to_binary(Path), Watcher, Watchers);
store_watcher(Op, Path, Watcher, Watchers) ->
    {Index, DestWatcher} = get_watchers_by_op(Op, Watchers),
    NewWatchers = dict:append(Path, Watcher, DestWatcher),
    setelement(Index, Watchers, NewWatchers).

get_watchers_by_op(Op, {DataWatchers, ExistWatchers, ChildWatchers}) ->
    case Op of
        get_data      -> {1, DataWatchers};
        exists        -> {2, ExistWatchers};
        get_children  -> {3, ChildWatchers};
        get_children2 -> {3, ChildWatchers};
        _             -> {1, DataWatchers} % just in case, shouldn't be here
    end.

find_and_erase_watchers(node_created, Path, Watchers) ->
    find_and_erase_watchers([exists], Path, Watchers);
find_and_erase_watchers(node_deleted, Path, Watchers) ->
    find_and_erase_watchers([get_data, exists, get_children], Path, Watchers);
find_and_erase_watchers(node_data_changed, Path, Watchers) ->
    find_and_erase_watchers([get_data, exists], Path, Watchers);
find_and_erase_watchers(node_children_changed, Path, Watchers) ->
    find_and_erase_watchers([get_children], Path, Watchers);
find_and_erase_watchers(Ops, Path, Watchers) ->
    find_and_erase_watchers(Ops, Path, Watchers, sets:new()).

find_and_erase_watchers([], _Path, Watchers, Receivers) ->
    {sets:to_list(Receivers), Watchers};
find_and_erase_watchers([Op|Left], Path, Watchers, Receivers) ->
    {Index, DestWatcher} = get_watchers_by_op(Op, Watchers),
    R = case dict:find(Path, DestWatcher) of
        {ok, X} -> sets:union(sets:from_list(X), Receivers);
        error   -> Receivers
    end,
    NewWatchers = dict:erase(Path, DestWatcher),
    W = setelement(Index, Watchers, NewWatchers),
    find_and_erase_watchers(Left, Path, W, R).

send_watched_event([], _Path, _EventType) ->
    ok;
send_watched_event([Watcher|Left], Path, EventType) ->
    Watcher ! {EventType, Path},
    send_watched_event(Left, Path, EventType).

get_chroot_path(P) -> get_chroot_path0(lists:reverse(P)).
get_chroot_path0("/" ++ P) -> get_chroot_path0(P);
get_chroot_path0(P) -> lists:reverse(P).

close_connection(Socket) ->
    close_connection(Socket, true).

close_connection(undefined, _) ->
    ok;
close_connection(Socket, true) ->
    gen_tcp:send(Socket, <<1:32, -11:32>>),
    gen_tcp:close(Socket);
close_connection(Socket, false) ->
    gen_tcp:close(Socket).

stop_heartbeat(undefined) -> ok;
stop_heartbeat({HeartbeatWatcher, HeartbeatRef}) ->
    erlang:demonitor(HeartbeatRef),
    erlzk_heartbeat:stop(HeartbeatWatcher).

get_reply_from_body(ok, _Op, <<>>, _Chroot) -> ok;
get_reply_from_body(ok, Op, Body, Chroot) ->
    Result = erlzk_codec:unpack(Op, Body, Chroot),
    multi_result(Op, Result);
get_reply_from_body(no_node, _Op, _Body, _Chroot) -> {error, no_node};
get_reply_from_body(Code, _, _, _) -> {error, Code}.

multi_result(multi, {ok, _}=Result) -> Result;
multi_result(multi, Result)         -> {error, Result};
multi_result(_, Result)             -> {ok, Result}.

maybe_restart_heartbeat(normal, State=#state{ping_interval=PingIntv}) ->
    {noreply, State#state{heartbeat_watcher=undefined}, PingIntv};
maybe_restart_heartbeat(shutdown, State=#state{ping_interval=PingIntv}) ->
    {noreply, State#state{heartbeat_watcher=undefined}, PingIntv};
maybe_restart_heartbeat(Reason, State=#state{timeout=TimeOut, ping_interval=PingIntv}) ->
    error_logger:warning_msg("Heartbeat process exit: ~p~n", [Reason]),
    {ok, NewHeartbeatWatcher} = erlzk_heartbeat:start(self(), TimeOut * 2 div 3),
    {noreply, State#state{heartbeat_watcher={NewHeartbeatWatcher, erlang:monitor(process, NewHeartbeatWatcher)}}, PingIntv}.
