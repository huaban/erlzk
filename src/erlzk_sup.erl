-module(erlzk_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SUPERVISOR, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 5, 60}, [{erlzk_conn_sup,
                                    {erlzk_conn_sup, start_link, []},
                                    permanent,
                                    5000,
                                    supervisor,
                                    [erlzk_conn_sup]}]}}.
