%%%-------------------------------------------------------------------
%%% @author Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%% @copyright (C) 2010, Erlang Solutions Ltd.
%%% @doc Behaviour definition module for task types. 
%%%      Every new task type must implement the following callback functions:
%%%      * init/2
%%%      * start_computations/3
%%%      TODO: update callback functions list, describe what should they do
%%%      TODO: define spec return types
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
    [{init, 2}, 
     {start_computations, 1},
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
    {value, {computation_type, Type0, TypeParams}} = 
        lists:keysearch(computation_type, 1, Config),
    Type = list_to_atom("ce_" ++ Type0),
    case catch Type:init(TypeParams, InputPath) of
        {ok, CallbackState} ->
            proc_lib:init_ack(Parent, {ok, self()}),
            
            %% TODO - fault tolerance aspect - register 
            %% what is computed where
            
            %% TODO - consider calling term_to_binary/compressed 
            %% on 'config' to reduce a memory consumption
            next_action(#state{mod = Type, 
                               ref = Ref,
                               config = Config,
                               input_path = InputPath}, 
                        Type:start_computations(CallbackState));
        Else ->
            proc_lib:init_ack(Parent, {error, Else})
    end.

%% FIXME: change return value from CallbackState to {ok, NewCallbackState}
%%        or {stop, Result}
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

-spec(next_action/2 :: (#state{}, {ok, term()} | {stop, term()} | {error, term()}) ->
                            no_return()).
next_action(State, {ok, CallbackState}) ->
    loop(State#state{callback_state = CallbackState});
next_action(State, {stop, Result}) ->
    %% TODO: implement complete action
    io:format("Result: ~p~n", [Result]);
next_action(State, {error, Reason}) ->
    %% TODO: implement error handling branch
    io:format("Error: ~p~n", [Reason]).
