%%%-------------------------------------------------------------------
%%% @author Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%% @copyright (C) 2010, Erlang Solutions Ltd.
%%% @doc Behaviour definition module for task types. 
%%%      Every new task type must implement the following callback functions:
%%%      * init/2
%%%      * start_computations/3
%%% @end
%%% Created :  1 Oct 2010 by Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%%-------------------------------------------------------------------
-module(ce_task_type).

-export([behaviour_info/1]).
-export([start_task/3]).

-spec(behaviour_info/1 :: (atom()) -> term()).
behaviour_info(callbacks) ->
    [{init, 2, 
      start_computations, 3}];
behaviour_info(_) ->
    undefined.

-spec(start_task/3 :: (reference(), list(), string()) -> 
                           {ok, pid()} | {error, term()}).
start_task(Ref, Config, InputPath) ->
    ga_scheduler:call(ce_task_sup, start_task, 
                      [Ref, Config, InputPath]).
