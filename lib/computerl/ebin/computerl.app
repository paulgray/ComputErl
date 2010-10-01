{application, computerl,
 [
  {description, "Coordination and execution framework for large scale fine-grained applications"},
  {vsn, "2.0"},
  {registered, [computerl_sup, ce_input, ce_scheduler]},
  {modules, [computerl_app, computerl_sup, ce_input, ce_scheduler]},
  {applications, [
                  kernel,
                  stdlib,
                  mnesia,
                  sasl
                 ]},
  {mod, { computerl_app, []}},
  {env, []}
 ]}.
