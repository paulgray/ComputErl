-record(node, {name :: atom(),
               priority = 0 :: integer()}).

-record(ce_task, {ref :: reference(),
                  started_at :: tuple(),
                  config :: string(),
                  input_path :: string(),
                  caller :: pid()}).
