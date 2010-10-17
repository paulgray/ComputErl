%%%-------------------------------------------------------------------
%%% @author Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%% @copyright (C) 2010, Erlang Solutions Ltd.
%%% @doc Process monitoring the current cluster state. 
%%%      When new node joins the cluster this process will be notified
%%%      about it and then informs everyone interested. 
%%%      The opposite situation (node disconnect) is also handled.
%%% @end
%%% Created :  1 Oct 2010 by Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%%-------------------------------------------------------------------
-module(ce_node_monitor).

-behaviour(gen_server).

-include("computerl_int.hrl").

%% API
-export([start_link/0]).
-export([get_all_nodes/0, operational/0]).
-export([subscribe/0, unsubscribe/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {subscribers = []}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% When new node wakes up it calls every other connected node in the cluster
%% in order to check if the given node can handle the computations.
-spec(operational/0 :: () -> true).
operational() ->
    true.

-spec(get_all_nodes/0 :: () -> list(atom())).
get_all_nodes() ->
    ets:tab2list(ce_nodes).

-spec(subscribe/0 :: () -> ok).
subscribe() ->                          
    gen_server:cast(?MODULE, {subscribe, self()}).

-spec(unsubscribe/0 :: () -> ok).
unsubscribe() ->
    gen_server:cast(?MODULE, {unsubscribe, self()}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) -> 
    init_mnesia(),
    
    net_kernel:monitor_nodes(true),
    lists:foreach(fun register_node/1, collect_nodes()),
    
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({subscribe, Pid}, #state{subscribers = Subs} = State) ->
    {noreply, State#state{subscribers = [Pid | Subs]}};
handle_cast({unsubscribe, Pid}, #state{subscribers = Subs} = State) ->
    {noreply, State#state{subscribers = lists:delete(Pid, Subs)}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({nodeup, Node} = Info, State) ->
    register_node(Node),
    inform_subscribers(Info, State#state.subscribers),
    {noreply, State};
handle_info({nodedown, Node} = Info, State) ->
    unregister_node(Node),
    inform_subscribers(Info, State#state.subscribers),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec(collect_nodes/0 :: () -> list(atom())).
collect_nodes() ->
    lists:filter(fun is_node_operational/1, [node() | nodes()]).

-spec(is_node_operational/1 :: (atom()) -> boolean()).
is_node_operational(Node) ->
    true =:= rpc:call(Node, ?MODULE, operational, []).

-spec(inform_subscribers/2 :: (term(), list(pid())) -> any()).
inform_subscribers(Info, Subscribers) ->
    [Pid ! Info || Pid <- Subscribers].

-spec(register_node/1 :: (atom()) -> (any())).
register_node(Node) ->
    F = fun() ->
                mnesia:write(ce_nodes, #node{name = Node}, write)
        end,
    mnesia:activity(sync_dirty, F).

-spec(unregister_node/1 :: (atom()) -> (any())).
unregister_node(Node) ->
    F = fun() ->
                mnesia:delete(ce_nodes, #node{name = Node})
        end,
    mnesia:activity(sync_dirty, F).

-spec(init_mnesia/0 :: () -> any()).
init_mnesia() ->
    mnesia:create_table(ce_nodes, 
                        [{ram_copies, [node()]},
                         {record_name, node},
                         {attributes, record_info(fields, node)}]),
    mnesia:add_table_copy(ce_nodes, node(), ram_copies).
