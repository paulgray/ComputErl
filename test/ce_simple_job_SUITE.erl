%%%-------------------------------------------------------------------
%%% @author Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%% @copyright (C) 2011, Erlang Solutions Ltd.
%%% @doc Test verifying that simple job works
%%%
%%% @end
%%% Created : 13 Apr 2011 by Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%%-------------------------------------------------------------------
-module(ce_simple_job_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    [{group, success}].

groups() ->
    [{success, [sequence], [adder,
                            word_counter]}].

init_per_group(_GroupName, _Config) ->
    application:start(mnesia),
    application:start(sasl),
    application:start(computerl).

end_per_group(_GroupName, _Config) ->
    application:stop(computerl),
    application:stop(sasl),
    application:stop(mnesia).

adder(Config0) ->
    Config = ct:get_config(simple_job, Config0),

    JobCfg = filename:join([root_dir(), proplists:get_value(config, Config)]),
    Input = filename:join([root_dir(), proplists:get_value(input, Config)]),

    write_n_words(Input, "\n", 100),

    mnesia:info(),
    {ok, Terms} = file:consult(JobCfg),
    {value, {_, simple_job, Opts}} = lists:keysearch(computation_type, 1, Terms),
    Output = proplists:get_value(output_file, Opts),
    file:delete(Output),

    file:set_cwd(root_dir()),

    {ok, Ref} = ce_input:compute(JobCfg, Input),
    {ok, OutputPath} = wait_for_job_to_finish(
                         Ref, proplists:get_value(job_timeout, Config, 10000)),

    true = filelib:is_file(OutputPath),
    verify_output(OutputPath).

word_counter(Config0) ->
    Config = ct:get_config(simple_job, Config0),

    JobCfg = filename:join([root_dir(), proplists:get_value(config, Config)]),
    Input = filename:join([root_dir(), proplists:get_value(input, Config)]),

    write_n_words(Input, " ", 100),

    mnesia:info(),
    {ok, Terms} = file:consult(JobCfg),
    {value, {_, simple_job, Opts}} = lists:keysearch(computation_type, 1, Terms),
    Output = proplists:get_value(output_file, Opts),
    file:delete(Output),

    file:set_cwd(root_dir()),

    {ok, Ref} = ce_input:compute(JobCfg, Input),
    {ok, OutputPath} = wait_for_job_to_finish(
                         Ref, proplists:get_value(job_timeout, Config, 10000)),

    true = filelib:is_file(OutputPath),
    {ok, <<"100\n">>} = file:read_file(OutputPath).

wait_for_job_to_finish(Ref, Timeout) ->
    receive
        {job_finished, Ref, OutputPath} ->
            {ok, OutputPath}
    after Timeout ->
            {error, timeout}
    end.

root_dir() ->
    filename:join([filename:dirname(code:which(computerl_app)), "..", "..", ".."]).

write_n_words(Input, Delimiter, N) ->
    {ok, Fd} = file:open(filename:join([root_dir(), Input]), [write]),
    write_n_words0(Fd, Delimiter, N).

write_n_words0(Fd, _, 0) ->
    io:format(Fd, "~n", []),
    ok = file:close(Fd);
write_n_words0(Fd, Delimiter, N) ->
    Str = lists:duplicate(N, $a),
    io:format(Fd, "~s~s", [Str, Delimiter]),
    write_n_words0(Fd, Delimiter, N-1).

verify_output(Path) ->
    {ok, Fd} = file:open(Path, [read, binary]),
    verify_output(Fd, 1).

verify_output(Fd, 101) ->
    {ok, <<"0\n">>} = file:read_line(Fd),
    eof = file:read_line(Fd),
    file:close(Fd);
verify_output(Fd, N) ->
    {ok, Bin} = file:read_line(Fd),
    1 = list_to_integer(string:strip(binary_to_list(Bin), right, $\n)),
    verify_output(Fd, N+1).
