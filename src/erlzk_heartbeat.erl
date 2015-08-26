%%%-------------------------------------------------------------------
%%% @author kevinbombadil
%%% @doc
%%%   Given a callback and an interval start a timer.
%%%   When you receive a beat, reset the timer.
%%%   When the timer expires run the callback.
%%%   Note:
%%%      - We ignore a noproc exit from the callback
%%%      - This module terminates on timeout
%%% @end
%%%-------------------------------------------------------------------
-module(erlzk_heartbeat).

-behaviour(gen_fsm).

%% API
-export([start_link/2, beat/1]).

%% gen_fsm callbacks
-export([init/1, beating/2, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).
-define(NOBEAT, no_beat).

-record(state, {callback, interval, timer}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Callback, Interval) ->
  gen_fsm:start_link(?MODULE, [Callback, Interval], []).

beat(Pid) ->
  gen_fsm:send_event(Pid, {beat}).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([Callback, Interval]) ->
  Timer=gen_fsm:start_timer(Interval,?NOBEAT),
  process_flag(trap_exit, true),

  {ok, beating, #state{callback=Callback,
                       timer=Timer,
                       interval=Interval}}.
%%--------------------------------------------------------------------

beating({beat},State=#state{interval=Interval,timer=Timer}) ->
%  error_logger:info_msg("Heartbeat: beat~n"),
  gen_fsm:cancel_timer(Timer),
  Timer1 = gen_fsm:start_timer(Interval,{?NOBEAT}),
  {next_state, beating, State#state{timer=Timer1}};

beating({timeout,_Timer,{?NOBEAT}},State=#state{callback=Callback}) ->
  error_logger:info_msg("Heartbeat: timeout~n"),
  try
    {M,F,A}=Callback,
    M:F(A)
  catch exit:{noproc,_} ->
    ok
  end,
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

%%--------------------------------------------------------------------
