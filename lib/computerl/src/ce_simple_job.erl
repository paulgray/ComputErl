%%%-------------------------------------------------------------------
%%% @author Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%% @copyright (C) 2010, Erlang Solutions Ltd.
%%% @doc Simple, one-off sequential job which will be executed
%%%      on one of the available machines within the ComputErl
%%%      cluster.
%%%
%%%      TODO: describe the job options here
%%% @end
%%% Created : 17 Oct 2010 by Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%%-------------------------------------------------------------------
-module(ce_simple_job).

-behaviour(ce_task_type).

-export([init/2, start_computations/1,
         incoming_data/3, exit_notification/3]).

-record(state, {input_fd :: pid(),
                output_fd :: pid(),
                port :: port(),
                output_file_path :: string()}).

-spec(init/2 :: (list(), string()) -> {ok, #state{}}).
init(Config, InputPath) ->
    Script = proplists:get_value(script, Config),
    OutputPath = proplists:get_value(output_file, Config),

    {ok, IFd} = file:open(InputPath, [read, binary, raw, read_ahead]),
    {ok, OFd} = file:open(OutputPath, [write, binary, raw, delayed_write]),

    Port = open_port({spawn, Script}, [stream, {line, 10240}, binary,
                                       use_stdio, stderr_to_stdout]),
    {ok, #state{input_fd = IFd,
                output_fd = OFd,
                port = Port,
                output_file_path = OutputPath}}.

-spec(start_computations/1 :: (#state{}) -> ce_task:task_return()).
start_computations(State) ->
    send_next_input_line(State).

-spec(incoming_data/3 :: (port(), term(), #state{}) -> ce_task:task_return()).
incoming_data(_Port, {data, {eol, Data}}, State) ->
    file:write(State#state.output_fd, Data),
    file:write(State#state.output_fd, "\n"),

    send_next_input_line(State);
incoming_data(_Port, {data, {line, Data}}, State) ->
    file:write(State#state.output_fd, Data),

    send_next_input_line(State).

-spec(exit_notification/3 :: (port(), term(), #state{}) ->
                                  {error, {port_exit, term()}}).
exit_notification(_Port, Reason, State) ->
    terminate(State),
    {error, {port_exit, Reason}}.

-spec(send_next_input_line/1 :: (#state{}) -> ok).
send_next_input_line(State) ->
    case file:read_line(State#state.input_fd) of
        {ok, Data} ->
            port_command(State#state.port, Data),
            {ok, State};

        eof ->
            ResultFilePath = terminate(State),
            {stop, ResultFilePath};

        Error ->
            Error
    end.

-spec(terminate/1 :: (#state{}) -> string()).
terminate(State) ->
    file:close(State#state.input_fd),
    file:close(State#state.output_fd),

    catch port_close(State#state.port),

    State#state.output_file_path.
