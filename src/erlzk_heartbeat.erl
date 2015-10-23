%% monitor whether the ZooKeeper server is responsive
%%
%% close session then reconnect a new one
%% if there is no package received from server in 2 / 3 * timeout.
%%
%% thanks @kevinbombadil
-module(erlzk_heartbeat).

-behaviour(gen_server).

-export([start/2, stop/1, beat/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {pid, interval}).

%% ===================================================================
%% Public API
%% ===================================================================
start(ConnPid, Interval) ->
    gen_server:start(?MODULE, [ConnPid, Interval], []).

stop(Pid) ->
    gen_server:call(Pid, stop).

beat(Pid) ->
    gen_server:cast(Pid, beat).

%% ===================================================================
%% gen_server Callbacks
%% ===================================================================
init([ConnPid, Interval]) ->
    {ok, #state{pid=ConnPid, interval=Interval}, Interval}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State=#state{interval=Interval}) ->
    {reply, ok, State, Interval}.

handle_cast(_Request, State=#state{interval=Interval}) ->
    {noreply, State, Interval}.

handle_info(timeout, State=#state{pid=Pid}) ->
    erlzk_conn:no_heartbeat(Pid),
    {stop, shutdown, State};
handle_info(_Info, State=#state{interval=Interval}) ->
    {noreply, State, Interval}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
