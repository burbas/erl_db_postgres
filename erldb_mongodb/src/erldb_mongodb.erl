-module(erldb_mongodb).
-behaviour(gen_server).
-behaviour(poolboy_worker).

%% Api
-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% Only for testing
-export([build_insert_query/1, build_update_query/1, build_table/1]).

-record(state, {
          database,
          read_mode,
          write_mode,
          read_connection,
          write_connection,
          username :: binary(),
          password :: binary()
         }).


%%%%%% API %%%%%%%%
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%%%%% Internals %%%%%%%%

init(Args) ->
    Username = proplists:get_value(username, Args),
    Password = proplists:get_value(password, Args),
    Database = proplists:get_value(database, Args, "test"),
    WriteMode = proplists:get_value(write_mode, Options, safe),
    ReadMode = proplists:get_value(read_mode, Options, master),

    %% Make the read connection
    ReadConnection =
        case  proplists:get_value(replica_set, Args) of
            undefined ->
                Host = proplists:get_value(hostname, Options, localhost),
                Port = proplists:get_value(port, Options, 27017),
                {ok, Conn} = mongo:connect({Host, Port}),
                Conn;
            ReplicaSet ->
                RSConn = mongo:rs_connect(ReplSet),
                {ok, RSConn1} = case ReadMode of
                                    master -> mongo_replset:primary(RSConn);
                                    slave_ok -> mongo_replset:secondary_ok(RSConn)
                                end,
                RSConn1
        end,

    WriteConnection = case proplists:get_value(write_host, Options) of
        undefined ->
            ReadConnection;
        WHost ->
            WPort = proplists:get_value(write_host_port, Options, 27017),
            {ok, WConn} = mongo:connect({WHost, WPort}),
            WConn
    end,

    State = #state{
      database = list_to_atom(Database),
      write_mode = WriteMode,
      read_mode = ReadMode,
      read_connection = ReadConnection,
      write_connection = WriteConnection,
      username = Username,
      password = Password
     }.


terminate(_Reason, #state{read_connection = RC, write_connection = WC}) ->
    close_connection =
        fun(X) ->
                case element(1, X) of
                    connection -> mongo:disconnect(X);
                    rs_connection -> mongo:rs_disconnect(X)
                end
        end,
    close_connection(RC),
    close_connection(WC),
    ok.

%% Execute without auth
execute(read, Fun, State = #state{username = undefined}) ->
    mongo:do(State#state.write_mode, State#state.read_mode, State#state.read_connection, State#state.database, Fun);
execute(write, Fun, State = #state{username = undefined}) ->
    mongo:do(State#state.write_mode, State#state.read_mode, State#state.write_connection, State#state.database, Fun);
%% Execute with auth
execute(read, Fun, State) ->
    mongo:do(WriteMode, ReadMode, RC, DB, fun() -> true=mongo:auth(State#state.username, State#state.password), Fun(), end);
execute(write, Fun, State) ->
    mongo:do(WriteMode, ReadMode, WC, DB, fun() -> true=mongo:auth(State#state.username, State#state.password), Fun(), end).


handle_call({insert, Object}, _From, State) when is_tuple(Object) ->
    Model = element(1, Object),
    Attributes = Model:module_info(attributes),
    Fields = proplists:get_all_values(field, Attributes), %% Returns a list with {Fieldname ::atom(), pos :: integer(), type :: atom(), [ArgList]}
    [{PKName, PKPos, PKType, PKArgs}|_] = [ X || X = {_, _, ArgList} <- Fields,
                            lists:member(primary_key, ArgList) ],

    Doc = lists:foldl(
            fun({PKName, PKPos, _, _}, Acc) ->
                    case element(PKPos, Object) of
                        id ->
                            Acc;
                        DefinedId ->
                            PackedVal = pack_value(element(PKPos, Object)),
                            [PKName, PackedVal|Acc]
                    end;
               ({FldName, FldPos, FldType}) ->
                    PackedVal = pack_value(element(FldPos, Object)),
                    [FldName, PackedVal|Acc]
            end, [], Fields),

    execute(write, fun() ->
                           mongo:insert(atom_to_list(element(1, Object)), Doc)
                   end, State);
