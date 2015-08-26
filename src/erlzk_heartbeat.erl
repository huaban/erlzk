%%%-------------------------------------------------------------------
%%% @author kevinbombadil
%%% @doc
%%%   receive heartbeats from the erlzk_conn, send a 'no_heartbest'
%%%   back to it if we've had to wait too long.
%%% @end
%%%-------------------------------------------------------------------
-module(erlzk_heartbeat).

-behaviour(gen_fsm).

%% API
-export([start_link/2, beat/1, no_beat/1]).

%% gen_fsm callbacks
-export([init/1, beating/2, handle_event/3,
	 handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {connection, interval, timer}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Connection, PartInterval) ->
    gen_fsm:start_link(?MODULE, [Connection, PartInterval], []).

beat(Pid) ->
    gen_fsm:send_event(Pid, {beat}).

no_beat(Pid) ->
    gen_fsm:send_event(Pid, {no_beat}).


%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([Connection, PartInterval]) ->
    Interval = PartInterval * 3,
    Timer=gen_fsm:start_timer(Interval,no_beat),
    {ok, beating, #state{connection=Connection, 
			 timer=Timer,
			 interval=Interval}}.
%%--------------------------------------------------------------------

beating({beat},State=#state{interval=Interval, timer=Timer}) ->
    gen_fsm:cancel_timer(Timer),
    Timer1=gen_fsm:start_timer(Interval,no_beat),
    {next_state, beating, State#{timer=Timer1}};

beating({timeout,_Timer,no_beat},State=#state{Connection=connection}) ->
    erlzk_conn:no_heartbeat(Connection),
    {stop,shutdown,State}.

%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

