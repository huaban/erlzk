-module(erlzk_conn_sup).
-behaviour(supervisor).

-export([start_conn/1]).
-export([start_link/0]).
-export([init/1]).

-define(SUPERVISOR, ?MODULE).

start_conn(Args) ->
    supervisor:start_child(?SUPERVISOR, Args).

start_link() ->
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

init([]) ->
    {ok, {{simple_one_for_one, 5, 5}, [{erlzk_conn,
                                        {erlzk_conn, start_link, []},
                                        temporary,
                                        5000,
                                        worker,
                                        [erlzk_conn]}]}}.
