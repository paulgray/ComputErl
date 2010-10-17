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
-export([start_task/3, start_link/3]).
-export([init/4]).

-record(state, {callback_state :: term(),
                mod :: atom(), %% callback module
                ref :: reference(), %% task reference
                config, 
                input_path}).

-spec(behaviour_info/1 :: (atom()) -> term()).
behaviour_info(callbacks) ->
    [{init, 1}, 
     {start_computations, 3},
     {incoming_data, 3},
     {port_exit, 3}];
behaviour_info(_) ->
    undefined.

-spec(start_task/3 :: (reference(), list(), string()) -> 
                           {ok, pid()} | {error, term()}).
start_task(Ref, Config, InputPath) ->
    ga_scheduler:call(ce_task_sup, start_task, 
                      [Ref, Config, InputPath]).

-spec(start_link/3 :: (reference(), list(), string()) ->
                           {ok, pid()} | {error, term()}).
start_link(Ref, Config, InputPath) ->
    proc_lib:start_link(?MODULE, init, [self(), Ref, Config, InputPath]).

-spec(init/4 :: (pid(), reference(), list(), string()) ->
                     no_return()).
init(Parent, Ref, Config, InputPath) ->
    {value, {computation_type, Type, TypeParams}} = 
        lists:keysearch(computation_type, 1, Config),
    case catch Type:init(TypeParams) of
        {ok, CallbackState0} ->
            proc_lib:init_ack(Parent, {ok, self()}),
            CallbackState = Type:start_computations(CallbackState0),

            %% TODO - fault tolerance aspect - register 
            %% what is computed where
            
            %% TODO - consider calling term_to_binary/compressed 
            %% on 'config' to reduce a memory consumption
            loop(#state{callback_state = CallbackState,
                        mod = Type, 
                        ref = Ref,
                        config = Config,
                        input_path = InputPath});
        Else ->
            proc_lib:init_ack(Parent, {error, Else})
    end.

-spec(loop/1 :: (#state{}) -> no_return()).
loop(State) ->
    receive
        {Port, Data} when is_port(Port) ->
            CallbackState = (State#state.mod):incoming_data(
                              Port, Data, State#state.callback_state),
            loop(State#state{callback_state = CallbackState});

        {'EXIT', Port, Reason} when is_port(Port) ->
            CallbackState = (State#state.mod):port_exit(
                              Port, Reason, State#state.callback_state),
            loop(State#state{callback_state = CallbackState});

        _ ->
            loop(State)
    end.
