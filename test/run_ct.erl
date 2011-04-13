-module(run_ct).
-export([ct/0]).

-define(CT_DIR, filename:join([".", "test"])).
-define(CT_REPORT, filename:join([".", "test", "ct_report"])).
-define(CT_CONFIG, filename:join([".", "test", "ct_computerl.cfg"])).

ct() ->
    ct:run_test([{config, [?CT_CONFIG]},
                 {dir, ?CT_DIR},
                 {logdir, ?CT_REPORT}]),
    init:stop(0).
