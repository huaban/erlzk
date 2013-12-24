-module(erlzk_conn).
-behaviour(gen_server).

-include_lib("../include/erlzk.hrl").

-export([start/2, start/3, start_link/2, start_link/3, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([create/5, delete/3, exists/3, exists/4, get_data/3, get_data/4, set_data/4, get_acl/2, set_acl/4,
         get_children/3, get_children/4, sync/2, get_children2/3, get_children2/4,
         create2/5, add_auth/3]).

-define(ZK_SOCKET_OPTS, [binary, {active, true}, {packet, 4}, {reuseaddr, true}]).
-define(ZK_CONNECT_TIMEOUT, 10000).

-record(state, {
    servers = [],
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
    reqs = dict:new(),
    auths = queue:new(),
    watchers = {dict:new(), dict:new(), dict:new()}
}).

%% ===================================================================
%% Public API
%% ===================================================================
start(ServerList, Timeout) ->
    gen_server:start(?MODULE, [ServerList, Timeout], []).

start(ServerName, ServerList, Timeout) ->
    gen_server:start(ServerName, ?MODULE, [ServerList, Timeout], []).

start_link(ServerList, Timeout) ->
    gen_server:start_link(?MODULE, [ServerList, Timeout], []).

start_link(ServerName, ServerList, Timeout) ->
    gen_server:start_link(ServerName, ?MODULE, [ServerList, Timeout], []).

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
init([ServerList, Timeout]) ->
    process_flag(trap_exit, true),
    case connect(shuffle(ServerList), 0, 0, Timeout, 0, <<0:128>>) of
        {ok, State=#state{ping_interval=PingIntv}} ->
            {ok, State#state{servers=ServerList}, PingIntv};
        {error, Reason} ->
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
handle_call({Op, Args}, From, State=#state{socket=Socket, xid=Xid, ping_interval=PingIntv, reqs=Reqs}) ->
    case gen_tcp:send(Socket, erlzk_codec:pack(Op, Args, Xid)) of
        ok ->
            NewReqs = dict:store(Xid, {Op, From}, Reqs),
            {noreply, State#state{xid=Xid+1, reqs=NewReqs}, PingIntv};
        {error, Reason} ->
            {reply, {error, Reason}, State#state{xid=Xid+1}, PingIntv}
    end;
handle_call({Op, Args, Watcher}, From, State=#state{socket=Socket, xid=Xid, ping_interval=PingIntv, reqs=Reqs, watchers=Watchers}) ->
    case gen_tcp:send(Socket, erlzk_codec:pack(Op, Args, Xid)) of
        ok ->
            NewReqs = dict:store(Xid, {Op, From}, Reqs),
            Path = element(1, Args),
            NewWatchers = append_watcher(Op, Path, Watcher, Watchers),
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
handle_info({tcp, _Port, Packet}, State=#state{ping_interval=PingIntv, auths=Auths, reqs=Reqs, watchers=Watchers}) ->
    {Xid, Zxid, Code, Body} = erlzk_codec:unpack(Packet),
    case Xid of
        -1 -> % watched event
            {EventType, KeeperState, Path} = erlzk_codec:unpack(watched_event, Body),
            {Receivers, NewWatchers} = find_and_erase_watchers(EventType, Path, Watchers),
            send_watched_event(Receivers, {EventType, KeeperState, Path}),
            {noreply, State#state{zxid=Zxid, watchers=NewWatchers}, PingIntv};
        -2 -> % ping
            {noreply, State#state{zxid=Zxid}, PingIntv};
        -8 -> % set watches
            {noreply, State#state{zxid=Zxid}, PingIntv};
        -4 -> % auth
            case queue:out(Auths) of
                {{value, From}, NewAuths} ->
                    Reply = case Code of
                        ok -> {ok};
                        _  -> {error, Code}
                    end,
                    gen_server:reply(From, Reply),
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
                                {ok, erlzk_codec:unpack(Op, Body)}
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
handle_info({tcp_closed, _Port}, State) ->
    {stop, tcp_closed, State};
handle_info({tcp_error, _Port, Reason}, State) ->
    {stop, Reason, State};
handle_info(_Info, State=#state{ping_interval=PingIntv}) ->
    {noreply, State, PingIntv}.

terminate(normal, #state{socket=Socket}) ->
    gen_tcp:send(Socket, <<1:32, -11:32>>),
    gen_tcp:close(Socket),
    ok;
terminate(shutdown, #state{socket=Socket}) ->
    gen_tcp:send(Socket, <<1:32, -11:32>>),
    gen_tcp:close(Socket),
    ok;
terminate(_Reason, #state{socket=Socket}) ->
    gen_tcp:send(Socket, <<1:32, -11:32>>),
    gen_tcp:close(Socket),
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
    case gen_tcp:connect(Host, Port, ?ZK_SOCKET_OPTS, ?ZK_CONNECT_TIMEOUT) of
        {ok, Socket} ->
            case gen_tcp:send(Socket, erlzk_codec:pack(connect, {ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword})) of
                ok ->
                    receive
                        {tcp, Socket, Packet} ->
                            {ProtoVer, RealTimeOut, SessionId, Password} = erlzk_codec:unpack(connect, Packet),
                            {ok, #state{socket=Socket, host=Host, port=Port,
                                        proto_ver=ProtoVer, timeout=RealTimeOut, session_id=SessionId, password=Password,
                                        ping_interval=(RealTimeOut div 3)}};
                        {tcp_closed, Socket} ->
                            connect(Left, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword);
                        {tcp_error, Socket, _Reason} ->
                            connect(Left, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword)
                    end;
                {error, _Reason} ->
                    connect(Left, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword)
            end;
        {error, _Reason} ->
            connect(Left, ProtocolVersion, LastZxidSeen, Timeout, LastSessionId, LastPassword)
    end.

append_watcher(Op, Path, Watcher, Watchers) ->
    {Index, AppendWatchers} = get_watchers_by_op(Op, Watchers),
    NewWatchers = dict:append(Path, {Op, Path, Watcher}, AppendWatchers),
    setelement(Index, Watchers, NewWatchers).

get_watchers_by_op(Op, {DataWatchers, ExistWatchers, ChildWatchers}) ->
    case Op of
        get_data      -> {1, DataWatchers};
        exists        -> {2, ExistWatchers};
        get_children  -> {3, ChildWatchers};
        get_children2 -> {3, ChildWatchers};
        _             -> {1, DataWatchers} % just in case, shouldn't be here
    end.

find_and_erase_watchers(none, Path, Watchers) ->
    find_and_erase_watchers([get_data, exists, get_children], Path, Watchers);
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
    {Receivers, Watchers};
find_and_erase_watchers([Op|Left], Path, Watchers, Receivers) ->
    {Index, OpWatchers} = get_watchers_by_op(Op, Watchers),
    R = case dict:find(Path, OpWatchers) of
        {ok, X} -> Receivers ++ X;
        error   -> Receivers
    end,
    NewWatchers = dict:erase(Path, OpWatchers),
    W = setelement(Index, Watchers, NewWatchers),
    find_and_erase_watchers(Left, Path, W, R).

send_watched_event([], _WatchedEvent) ->
    ok;
send_watched_event([{Op, Path, Watcher}|Left], WatchedEvent) ->
    Watcher ! {Op, Path, WatchedEvent},
    send_watched_event(Left, WatchedEvent).
