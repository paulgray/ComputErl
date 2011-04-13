%%%-------------------------------------------------------------------
%%% @author Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%% @copyright (C) 2011, Erlang Solutions Ltd.
%%% @doc Implementation of the master process for master slave paradigm.
%%%
%%% Slave processes are spawned by their master and are responsible
%%% for performing the simpliest parts of computations.
%%%
%%% Slaves are started in pools: when slave finishes its part of
%%% job, it asks its master for the next chunk.
%%%
%%% @end
%%% @end
%%% Created : 13 Apr 2011 by Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%%-------------------------------------------------------------------
-module(ce_ms_master).

