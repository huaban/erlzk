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

-export([start/3, start/4, start_link/3, start_link/4, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([create/5, delete/3, exists/3, exists/4, get_data/3, get_data/4, set_data/4, get_acl/2, set_acl/4,
         get_children/3, get_children/4, sync/2, get_children2/3, get_children2/4,
         create2/5, add_auth/3]).

-define(ZK_SOCKET_OPTS, [binary, {active, true}, {packet, 4}, {reuseaddr, true}]).
-define(ZK_CONNECT_TIMEOUT, 10000).
-define(ZK_RECONNECT_INTERVAL, 10000).

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
    monitor = undefined,
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

create2(Pid, Path, Data, Acl, CreateMode) ->
    gen_server:call(Pid, {create2, {Path, Data, Acl, CreateMode}}).

add_auth(Pid, Scheme, Auth) ->
    gen_server:call(Pid, {add_auth, {Scheme, Auth}}).

%% ===================================================================
%% gen_server Callbacks
%% ===================================================================
init([ServerList, Timeout, Options]) ->
    Monitor = proplists:get_value(monitor, Options),
    ResetWatch = case proplists:get_value(disable_watch_auto_reset, Options) of
        true  -> false;
        _     -> true
    end,
    AuthData = case proplists:get_value(auth_data, Options) of
        undefined -> [];
        AdValue   -> AdValue
    end,
    Chroot = case proplists:get_value(chroot, Options) of
        undefined -> "/";
        CrValue   -> get_chroot_path(CrValue)
    end,
    process_flag(trap_exit, true),
    case connect(shuffle(ServerList), 0, 0, Timeout, 0, <<0:128>>) of
        {ok, State=#state{ping_interval=PingIntv}} ->
            NewState = State#state{servers=ServerList, auth_data=AuthData, chroot=Chroot, reset_watch=ResetWatch, monitor=Monitor},
            add_init_auths(AuthData, NewState),
            {ok, NewState, PingIntv};
        {error, Reason} ->
            error_logger:error_msg("Connect fail: ~p~n", [Reason]),
            {stop, Reason}
    end.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
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
handle_call({Op, Args, Watcher}, From, State=#state{chroot=Chroot, socket=Socket, xid=Xid, ping_interval=PingIntv, reqs=Reqs, watchers=Watchers}) ->
    case gen_tcp:send(Socket, erlzk_codec:pack(Op, Args, Xid, Chroot)) of
        ok ->
            NewReqs = dict:store(Xid, {Op, From}, Reqs),
            Path = element(1, Args),
            NewWatchers = store_watcher(Op, Path, Watcher, Watchers),
            {noreply, State#state{xid=Xid+1, reqs=NewReqs, watchers=NewWatchers}, PingIntv};
        {error, Reason} ->
            {reply, {error, Reason}, State#state{xid=Xid+1}, PingIntv}
    end;
handle_call(_Request, _From, State=#state{ping_interval=PingIntv}) ->
    {noreply, State, PingIntv}.

handle_cast(_Request, State=#state{ping_interval=PingIntv}) ->
    {noreply, State, PingIntv}.

handle_info(timeout, State=#state{socket=Socket, ping_interval=PingIntv}) ->
    gen_tcp:send(Socket, <<-2:32, 11:32>>),
    {noreply, State, PingIntv};
handle_info({tcp, _Port, Packet}, State=#state{chroot=Chroot, ping_interval=PingIntv, auths=Auths, reqs=Reqs, watchers=Watchers}) ->
    {Xid, Zxid, Code, Body} = erlzk_codec:unpack(Packet),
    case Xid of
        -1 -> % watched event
            {EventType, _KeeperState, Path} = erlzk_codec:unpack(watched_event, Body, Chroot),
            {Receivers, NewWatchers} = find_and_erase_watchers(EventType, Path, Watchers),
            send_watched_event(Receivers, EventType),
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
                        ok -> {ok};
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
                {ok, {Op, From}} ->
                    NewReqs = dict:erase(Xid, Reqs),
                    Reply = case Code of
                        ok ->
                            if size(Body) =:= 0 ->
                                {ok};
                               true ->
                                {ok, erlzk_codec:unpack(Op, Body, Chroot)}
                            end;
                        _  ->
                            {error, Code}
                    end,
                    gen_server:reply(From, Reply),
                    {noreply, State#state{zxid=Zxid, reqs=NewReqs}, PingIntv};
                error ->
                    {noreply, State#state{zxid=Zxid}, PingIntv}
            end
    end;
handle_info({tcp_closed, _Port}, State=#state{host=Host, port=Port, monitor=Monitor}) ->
    error_logger:error_msg("Connection to ~p:~p is broken, reconnect now~n", [Host, Port]),
    notify_monitor_server_state(Monitor, disconnected, Host, Port),
    reconnect(State#state{socket=undefined, host=undefined, port=undefined});
handle_info({tcp_error, _Port, Reason}, State=#state{socket=Socket, host=Host, port=Port, monitor=Monitor}) ->
    error_logger:error_msg("Connection to ~p:~p meet an error, will be closed and reconnect: ~p~n", [Host, Port, Reason]),
    gen_tcp:close(Socket),
    notify_monitor_server_state(Monitor, disconnected, Host, Port),
    reconnect(State#state{socket=undefined, host=undefined, port=undefined});
handle_info(reconnect, State) ->
    reconnect(State);
handle_info(_Info, State=#state{ping_interval=PingIntv}) ->
    {noreply, State, PingIntv}.

terminate(normal, #state{socket=Socket}) ->
    gen_tcp:send(Socket, <<1:32, -11:32>>),
    gen_tcp:close(Socket),
    error_logger:warning_msg("Server is closed~n"),
    ok;
terminate(shutdown, #state{socket=Socket}) ->
    gen_tcp:send(Socket, <<1:32, -11:32>>),
    gen_tcp:close(Socket),
    error_logger:warning_msg("Server is shutdown~n"),
    ok;
terminate(Reason, #state{socket=Socket}) ->
    gen_tcp:send(Socket, <<1:32, -11:32>>),
    gen_tcp:close(Socket),
    error_logger:error_msg("Server is terminated: ~p~n", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}. 

%% ===================================================================
%% Internal Functions
%% ===================================================================
shuffle(L) ->
    [X||{_,X} <- lists:sort([{random:uniform(), N} || N <- L])].

connect([], _ProtocolVersion, _LastZxidSeen, _Timeout, _LastSessionId, _LastPassword) ->
    {error, no_available_server};
connect([{Host,Port}|Left], ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword) ->
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
                                    {error, session_expired};
                                _ ->
                                    error_logger:info_msg("Connection to ~p:~p is established~n", [Host, Port]),
                                    {ok, #state{socket=Socket, host=Host, port=Port,
                                                proto_ver=ProtoVer, timeout=RealTimeOut, session_id=SessionId, password=Password,
                                                ping_interval=(RealTimeOut div 3)}}
                            end;
                        {tcp_closed, Socket} ->
                            error_logger:error_msg("Connection to ~p:~p is closed~n", [Host, Port]),
                            connect(Left, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword);
                        {tcp_error, Socket, Reason} ->
                            error_logger:error_msg("Connection to ~p:~p meet an error: ~p~n", [Host, Port, Reason]),
                            gen_tcp:close(Socket),
                            connect(Left, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword)
                    end;
                {error, Reason} ->
                    error_logger:error_msg("Sending connect command to ~p:~p meet an error: ~p~n", [Host, Port, Reason]),
                    gen_tcp:close(Socket),
                    connect(Left, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword)
            end;
        {error, Reason} ->
            error_logger:error_msg("Connecting to ~p:~p meet an error: ~p~n", [Host, Port, Reason]),
            connect(Left, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword)
    end.

reconnect(State=#state{servers=ServerList, auth_data=AuthData, chroot=Chroot,
                       proto_ver=ProtoVer, zxid=Zxid, timeout=Timeout, session_id=SessionId, password=Passwd,
                       xid=Xid, reset_watch=ResetWatch, monitor=Monitor, watchers=Watchers}) ->
    case connect(shuffle(ServerList), ProtoVer, Zxid, Timeout, SessionId, Passwd) of
        {ok, NewState=#state{host=Host, port=Port, ping_interval=PingIntv}} ->
            error_logger:warning_msg("Reconnect to ~p:~p successful~n", [Host, Port]),
            RenewState = NewState#state{servers=ServerList, auth_data=AuthData, chroot=Chroot,
                                        xid=Xid, zxid=Zxid, reset_watch=ResetWatch, monitor=Monitor, watchers=Watchers},
            notify_monitor_server_state(Monitor, connected, Host, Port),
            {noreply, RenewState, PingIntv};
        {error, session_expired} ->
            error_logger:warning_msg("Session expired, creating new connection now"),
            reconnect_after_session_expired(State);
        {error, Reason} ->
            error_logger:error_msg("Connect fail: ~p, will be try again after ~ps~n", [Reason, ?ZK_RECONNECT_INTERVAL]),
            erlang:send_after(?ZK_RECONNECT_INTERVAL, self(), reconnect),
            {noreply, State}
    end.

reconnect_after_session_expired(State=#state{servers=ServerList, auth_data=AuthData, chroot=Chroot,
                                             timeout=Timeout, reset_watch=ResetWatch, monitor=Monitor, watchers=Watchers}) ->
    case connect(shuffle(ServerList), 0, 0, Timeout, 0, <<0:128>>) of
        {ok, NewState=#state{host=Host, port=Port, ping_interval=PingIntv}} ->
            error_logger:warning_msg("Create a new connection to ~p:~p successful~n", [Host, Port]),
            RenewState = reset_watch_return_new_state(NewState#state{servers=ServerList, auth_data=AuthData, chroot=Chroot,
                                                                     reset_watch=ResetWatch, monitor=Monitor}, Watchers),
            add_init_auths(AuthData, RenewState),
            notify_monitor_server_state(Monitor, expired, Host, Port),
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

store_watcher(Op, Path, Watcher, Watchers) ->
    {Index, DestWatcher} = get_watchers_by_op(Op, Watchers),
    NewWatchers = dict:store(Path, {erlang:now(), Op, Path, Watcher}, DestWatcher),
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
    find_and_erase_watchers([get_data, exists], Path, Watchers);
find_and_erase_watchers(node_deleted, Path, Watchers) ->
    find_and_erase_watchers([get_data, exists, get_children], Path, Watchers);
find_and_erase_watchers(node_data_changed, Path, Watchers) ->
    find_and_erase_watchers([get_data, exists], Path, Watchers);
find_and_erase_watchers(node_children_changed, Path, Watchers) ->
    find_and_erase_watchers([get_children], Path, Watchers);
find_and_erase_watchers(Ops, Path, Watchers) ->
    find_and_erase_watchers(Ops, Path, Watchers, []).

find_and_erase_watchers([], _Path, Watchers, Receivers) ->
    {lists:sort(Receivers), Watchers};
find_and_erase_watchers([Op|Left], Path, Watchers, Receivers) ->
    {Index, DestWatcher} = get_watchers_by_op(Op, Watchers),
    R = case dict:find(Path, DestWatcher) of
        {ok, X} -> [X|Receivers];
        error   -> Receivers
    end,
    NewWatchers = dict:erase(Path, DestWatcher),
    W = setelement(Index, Watchers, NewWatchers),
    find_and_erase_watchers(Left, Path, W, R).

send_watched_event([], _EventType) ->
    ok;
send_watched_event([{_Time, Op, Path, Watcher}|Left], EventType) ->
    Watcher ! {Op, Path, EventType},
    send_watched_event(Left, EventType).

get_chroot_path(P) -> get_chroot_path0(lists:reverse(P)).
get_chroot_path0("/" ++ P) -> get_chroot_path0(P);
get_chroot_path0(P) -> lists:reverse(P).
