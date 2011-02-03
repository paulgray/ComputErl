%%%-------------------------------------------------------------------
%%% @author Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%% @copyright (C) 2010, Erlang Solutions Ltd.
%%% @doc Implementation of master-slave computation paradigm.
%%%      Basically, there are two types of processes involved in
%%%      this kind of computations: masters, which only role is
%%%      to partition and distribute the data among their workers;
%%%      and workers, who are performing the actual computations.
%%%
%%%      In order to provide a satisiable efficiency, in the case
%%%      when a number of tasks to perform is much greater than
%%%      the number of workers, it is possible to spawn an additional
%%%      level of masters (divide & conquer strategy). This concept
%%%      is often refered in the literature as 'fat tree'.
%%%
%%%      TODO: describe possible options
%%% @end
%%% Created : 24 Oct 2010 by Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%%-------------------------------------------------------------------
-module(ce_master_slave).

-behaviour(ce_task_type).

-export([init/2, start_computations/1,
         incoming_data/3, exit_notification/3]).

-define(DEFAULT_WORKERS_NO, 128).

-record(state, {output_fd :: pid(),
                output_path :: string(),
                input_fd :: pid(),
                script :: string(),
                workers :: gb_tree()}).

%% TODO: implement 'fat tree'
-spec(init/2 :: (list(), string()) -> {ok, #state{}}).
init(Config, InputPath) ->
    process_flag(trap_exit, true),

    OutputPath = proplists:get_value(output_file, Config),
    Script = proplists:get_value(script, Config),

    {ok, IFd} = file:open(InputPath, [read, binary, raw, read_ahead]),
    {ok, OFd} = file:open(OutputPath, [write, binary, raw, delayed_write]),

    MaxTasksPerWorker = proplists:get_value(max_tasks_per_worker,
                                            Config, infinity),
    MastersPerLevel = proplists:get_value(masters_per_level, Config, 2),
    NWorkers = proplists:get_value(number_of_workers, Config,
                                   ?DEFAULT_WORKERS_NO),

    TaskNo = number_of_tasks(InputPath, IFd),
    Workers = if
                  %% if we know that we are going to overload workers
                  TaskNo > NWorkers*MaxTasksPerWorker ->
                      start_masters(MastersPerLevel, Config, gb_trees:empty());
                  true ->
                      start_workers(NWorkers, Script, gb_trees:empty())
              end,

    {ok, #state{input_fd = IFd,
                output_path = OutputPath,
                output_fd = OFd,
                script = Script,
                workers = Workers}}.

-spec(start_computations/1 :: (#state{}) -> ce_task:task_return()).
start_computations(State) ->
    send_input_lines(State, gb_trees:iterator(State#state.workers)).

-spec(incoming_data/3 :: (pid(), binary(), #state{}) -> ce_task:task_return()).
incoming_data(WorkerPid, Result, State) ->
    file:write(State#state.output_fd, [Result, $\n]),

    feed_worker(WorkerPid, State).

-spec(exit_notification/3 :: (pid(), term(), #state{}) -> ce_task:task_return()).
exit_notification(WorkerPid, _Reason, State) ->
    {ok, Pid} = ce_ms_slave:start_link(State#state.script),
    Input = gb_trees:get(WorkerPid, State#state.workers),
    ce_ms_slave:input_data(Pid, Input),

    {ok, State#state{workers = gb_trees:enter(
                                 Pid, Input,
                                 gb_trees:delete(WorkerPid, State#state.workers))}}.

-spec(start_workers/3 :: (integer(), string(), gb_tree()) -> gb_tree()).
start_workers(0, _, Workers) ->
    Workers;
start_workers(N, Script, Workers) when N > 0 ->
    {ok, Pid} = ce_ms_slave:start_link(Script),
    start_workers(N-1, Script, gb_trees:insert(Pid, undefined, Workers)).

%% TODO: figure out how to divide the file into chunks and pass them
%% to the newly created masters
%% TODO: think about reusing ce_master_slave code, we do not really
%% have anything like ce_ms_master now, and I think we do not need any
-spec(start_masters/3 :: (integer(), list(), gb_tree()) -> gb_tree()).
start_masters(0, _, Masters) ->
    Masters;
start_masters(N, Config, Masters) ->
    {ok, Pid} = ce_ms_master:start_link(Config),
    start_masters(N-1, Config, gb_trees:insert(Pid, undefined, Masters)).

-spec(send_input_lines/2 :: (#state{}, term()) -> {ok, #state{}}).
send_input_lines(State, Iter) ->
    case gb_trees:next(Iter) of
        none ->
            {ok, State};
        {Pid, _, NewIter} ->
            case feed_worker(Pid, State) of
                {ok, NewState} ->
                    send_input_lines(NewState, NewIter);
                {stop, NewState} ->
                    {ok, NewState};
                Error ->
                    Error
            end
    end.

-spec(feed_worker/2 :: (pid(), #state{}) -> {ok | stop, #state{}} | {error, term()}).
feed_worker(Pid, #state{input_fd = IFd} = State) ->
    case {gb_trees:size(State#state.workers), IFd} of
        {1, eof} ->
            file:close(State#state.output_fd),
            {stop, State#state.output_path};
        {_, eof} ->
            ce_ms_slave:stop(Pid),
            {ok, State#state{workers = gb_trees:delete(Pid, State#state.workers)}};
        _ ->
            case file:read_line(IFd) of
                {ok, Data} ->
                    ce_ms_slave:input_data(Pid, Data),
                    {ok, State#state{workers = gb_trees:enter(Pid, Data,
                                                              State#state.workers)}};
                eof ->
                    file:close(IFd),
                    ce_ms_slave:stop(Pid),
                    {ok, State#state{input_fd = eof,
                                     workers = gb_trees:delete(Pid, State#state.workers)}};
                Error ->
                    Error
            end
    end.

-spec(number_of_tasks/2 :: (string(), port()) -> integer()).
number_of_tasks(InputPath, Fd) ->
    %% TODO: check what is the most efficient way of estimation
    %% or accurate computing the total number of lines in the file
    %% A helpful hint: maybe it might be good to have something
    %% in the configuration saying what is the expected line length
    %% and basing on that + actual file size we will be able to
    %% guess the number of inputs.
    %% The problematic part are pipes which obviously do not have
    %% size. Nonetheless, when reading from pipe we will never be
    %% able to estimate anything, thus maybe one more option will
    %% be good for us? (like {pipe, true}). Then user who is issuing
    %% the computations will be forced to provide exact configuration
    %% for ms (depth and span of the tree).
    0.
