%%%-------------------------------------------------------------------
%%% @author Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%% @copyright (C) 2010, Erlang Solutions Ltd.
%%% @doc Helper module for starting and completing the tasks.
%%% @end
%%% Created :  1 Oct 2010 by Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%%-------------------------------------------------------------------
-module(ce_task).

-export([start_task/4]).
-export([finish_task/2]).

-include("computerl_int.hrl").

-spec(start_task/4 :: (reference(), string(), string(), pid()) -> skip | {ok, pid()}).
start_task(Ref, JobSpecPath, InputPath, Caller) ->
    case file:consult(JobSpecPath) of
        {ok, Config} ->
            start_task_internal(Ref, Config, InputPath, Caller);
        {error, Error} ->
            error_logger:warning_msg("~p:~p can not read job (~p) configuration file: "
                                     "~p, reason: ~p~n",
                                     [?MODULE, ?LINE, Ref, JobSpecPath, Error]),
            skip
    end.

-spec(start_task_internal/4 :: (reference(), list(), string(), pid()) -> skip | {ok, pid()}).
start_task_internal(Ref, Config, InputPath, Caller) ->
    case ce_task_type:start_task(Ref, Config, InputPath) of
        {ok, Pid} ->
            mnesia:dirty_write(ce_task, #ce_task{ref = Ref,
                                                 started_at = now(),
                                                 input_path = InputPath,
                                                 config = Config,
                                                 caller = Caller}),
            {ok, Pid};
        {error, Error} ->
            error_logger:warning_msg("~p:~p starting job (~p) failed - reason: ~p~n",
                                     [?MODULE, ?LINE, Ref, Error]),
            skip
    end.

-spec(finish_task/2 :: (reference(), string()) -> any()).
finish_task(Ref, OutputPath) ->
    case mnesia:dirty_read(ce_task, Ref) of
        [Task] ->
            Task#ce_task.caller ! {job_finished, Ref, OutputPath},
            mnesia:dirty_delete(ce_task, Ref);
        _ ->
            ok
    end.

