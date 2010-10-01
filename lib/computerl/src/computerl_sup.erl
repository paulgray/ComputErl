%%%-------------------------------------------------------------------
%%% @author Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%% @copyright (C) 2010, Erlang Solutions Ltd.
%%% @doc ComputErl main supervisor callback module
%%% @end
%%% Created :  1 Oct 2010 by Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%%-------------------------------------------------------------------
-module(computerl_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init([]) ->
    Input = {input, {ce_input, start_link, []},
             permanent, 2000, worker, [ce_input]},
    Scheduler = {scheduler, {ce_scheduler, start_link, []},
                 permanent, 2000, worker, [ce_scheduler]},

    {ok, { {one_for_one, 3, 1000}, [Scheduler, Input]} }.
