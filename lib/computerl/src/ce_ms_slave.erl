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

-export([start_link/1, input_data/2, stop/1]).
-export([init/2]).

-record(slave_state, {script_path :: string(),
                      port :: port(),
                      parent :: pid()}).

-spec(start_link/1 :: (string()) -> {ok, pid()} | {error, term()}).
start_link(ScriptPath) ->
    proc_lib:start_link(?MODULE, init, [ScriptPath, self()]).

-spec(stop/1 :: (pid()) -> any()).
stop(Slave) ->
    unlink(Slave),
    Slave ! stop.

-spec(input_data/2 :: (pid(), binary()) -> any()).
input_data(Slave, Data) ->
    Slave ! {input_data, Data}.

-spec(init/2 :: (string(), pid()) -> no_return()).
init(ScriptPath, Parent) ->
    case filelib:is_file(ScriptPath) of
        true ->
            proc_lib:init_ack(Parent, {ok, self()}),
            loop(#slave_state{script_path = ScriptPath,
                              parent = Parent});
        false ->
            proc_lib:init_ack(Parent, {error, {enoent, ScriptPath}})
    end.

-spec(loop/1 :: (#slave_state{}) -> no_return()).
loop(#slave_state{port = Port} = State) ->
    NewState = receive
                   {input_data, Data} ->
                       start_computations(State, Data);
                   {Port, Result} ->
                       submit_results(State, Result);
                   stop ->
                       exit(normal)
               end,
    loop(NewState).

-spec(start_computations/2 :: (#slave_state{}, binary()) -> #slave_state{}).
start_computations(#slave_state{script_path = Script} = State, Data) ->
    Port = open_port({spawn, Script}, [stream, {line, 10240}, binary,
                                       use_stdio, stderr_to_stdout]),
    port_command(Port, Data),
    State#slave_state{port = Port}.

%% TODO: check if there can be other value submitted by port (than we
%% pattern match on here)
-spec(submit_results/2 :: (#slave_state{}, binary()) -> #slave_state{}).
submit_results(State, {data, {_, Result}}) ->
    catch port_close(State#slave_state.port),
    State#slave_state.parent ! {self(), Result},
    State.
