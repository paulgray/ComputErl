%%%-------------------------------------------------------------------
%%% @author Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%% @copyright (C) 2010, Erlang Solutions Ltd.
%%% @doc Helper module for starting and completing the tasks.
%%% @end
%%% Created :  1 Oct 2010 by Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%%-------------------------------------------------------------------
-module(ce_task).

-export([start_task/3]).

-spec(start_task/3 :: (reference(), string(), string()) -> skip | {ok, pid()}).
start_task(Ref, JobSpecPath, InputPath) ->
    case file:consult(JobSpecPath) of
        {ok, Config} ->
            start_task_internal(Ref, Config, InputPath);
        {error, Error} ->
            error_logger:warning_msg("~p:~p can not read job (~p) configuration file: "
                                     "~p, reason: ~p~n",
                                     [?MODULE, ?LINE, Ref, JobSpecPath, Error]),
            skip
    end.

-spec(start_task_internal/3 :: (reference(), list(), string()) -> skip | {ok, pid()}).
start_task_internal(Ref, Config, InputPath) ->
    case lists:keysearch(computation_type, 1, Config) of
        false ->
            error_logger:warning_msg("~p:~p computation_type not specified, skipping"
                                     " job (~p)~n",
                                     [?MODULE, ?LINE, Ref]),
            skip;
        {value, {computation_type, Type, TypeParams}} ->
            start_task_type(Ref, Config, InputPath, Type, TypeParams)
    end.

-spec(start_task_type/5 :: (reference(), list(), string(), atom(), list()) -> 
                                {ok, pid()} | skip).
start_task_type(Ref, Config, InputPath, Type, TypeParams) ->
    case ce_task_type:start_task(Ref, Config, InputPath, Type, TypeParams) of
        {ok, Pid} ->
            {ok, Pid};
        {error, Error} ->
            error_logger:warning_msg("~p:~p starting job (~p) failed - reason: ~p~n",
                                     [?MODULE, ?LINE, Ref, Error]),
            skip
    end.
