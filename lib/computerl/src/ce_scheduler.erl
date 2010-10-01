%%%-------------------------------------------------------------------
%%% @author Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%% @copyright (C) 2010, Erlang Solutions Ltd.
%%% @doc Node scheduler distributing the processing requests across 
%%%      the cluster of computing nodes.
%%% @end
%%% Created :  1 Oct 2010 by Michal Ptaszek <michal.ptaszek@erlang-solutions.com>
%%%-------------------------------------------------------------------
-module(ce_scheduler).

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([call/3, cast/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {nodes :: queue()}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec(call/3 :: (atom(), atom(), list()) -> term()).
call(Module, Func, Args) ->
    Node = gen_server:call(?MODULE, get_node),
    rpc:call(Node, Module, Func, Args).

-spec(cast/3 :: (atom(), atom(), list()) -> term()).
cast(Module, Func, Args) ->
    Node = gen_server:call(?MODULE, get_node),
    rpc:cast(Node, Module, Func, Args).

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
    ce_node_monitor:subscribe(),
    Nodes = load_nodes(),

    {ok, #state{nodes = Nodes}}.

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
%% Round-robin node rotation
handle_call(get_node, _From, State) ->
    {{value, Node}, NewQ} = queue:out(State#state.nodes),
    {reply, Node, State#state{nodes = queue:in(Node, NewQ)}}.

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
handle_cast(_Msg, State) ->
    {noreply, State}.

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
%% Let's do not pretend we will always be on the same page as ce_node_monitor
%% and underlying mnesia. Refresh the entire state after each cluster 
%% reconfiguration.
handle_info({nodeup, _}, State) ->
    {noreply, State#state{nodes = load_nodes()}};
handle_info({nodedown, _}, State) ->
    {noreply, State#state{nodes = load_nodes()}}.

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
-spec(load_nodes/0 :: () -> queue()).
load_nodes() ->
    queue:from_list(ce_node_monitor:get_all_nodes()).
