%%%-------------------------------------------------------------------
%%% @author Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%% @copyright (C) 2010, Erlang Solutions Ltd.
%%% @doc ComputErl application callback module
%%% @end
%%% Created :  1 Oct 2010 by Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%%-------------------------------------------------------------------
-module(computerl_app).

-behaviour(application).

-include("computerl_int.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    db_init(),
    computerl_sup:start_link().

stop(_State) ->
    ok.

-spec(db_init/0 :: () -> any()).
db_init() ->
    case mnesia:system_info(extra_db_nodes) of
        [] ->
            application:stop(mnesia),
            mnesia:create_schema([node()]),
            application:start(mnesia, permanent),
            create_tables();
        _ ->
            ok
    end.

-spec(create_tables/0 :: () -> any()).
create_tables() ->
    mnesia:create_table(ce_task, [{ram_copies, [node() | nodes()]},
                                  {attributes, record_info(fields, ce_task)}]),
    mnesia:add_table_copy(ce_task, node(), ram_copies).
