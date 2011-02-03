%%%-------------------------------------------------------------------
%%% @author Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%% @copyright (C) 2011, Erlang Solutions Ltd.
%%% @doc Implementation of simple slave process for master slave paradigm.
%%%
%%% Slave processes are spawned by their master and are responsible
%%% for performing the simpliest parts of computations.
%%%
%%% Slaves are started in pools: when slave finishes its part of
%%% job, it asks its master for the next chunk.
%%%
%%% @end
%%% Created :  3 Feb 2011 by Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%%-------------------------------------------------------------------
-module(ce_ms_slave).

-export([start_link/1]).
-export([init/2]).

-record(slave_state, {script_path :: string(),
                      port :: port(),
                      parent :: pid()}).

-spec(start_link/1 :: (string()) -> {ok, pid()} | {error, term()}).
start_link(ScriptPath) ->
    proc_lib:start_link(?MODULE, init, [ScriptPath, self()]).

-spec(init/2 :: (string(), pid()) -> no_return()).
init(ScriptPath, Parent) ->
    case file:is_file(ScriptPath) of
        true ->
            proc_lib:init_ack(Parent, {ok, self()}),
            loop(#slave_state{script_path = ScriptPath,
                              parent = Parent});
        false ->
            proc_lib:init_ack(Parent, {error, {enoent, ScriptPath}})
    end.

-spec(loop/1 :: (#slave_state{}) -> no_return()).
loop(State) ->
    ok.
