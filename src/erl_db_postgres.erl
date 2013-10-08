%%%-------------------------------------------------------------------
%%% @author Niclas Axelsson <niclas@burbasconsulting.com>
%%% @doc
%%% ErlDB handler for postgres
%%% @end
%%%-------------------------------------------------------------------
-module(erl_db_postgres).
-author('Niclas Axelsson <niclas@burbasconsulting.com>').

-behaviour(gen_server).
-behaviour(poolboy_worker).

%% Api
-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% Only for testing
%%-export([build_insert_query/1, build_update_query/1, build_table/1, build_select_query/2]).

-record(state, {
          name :: atom(),
          type :: atom(),
          conn :: pid()
         }).

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
start_link(Args) ->
    WorkerArgs = proplists:get_value(worker_args, Args),
    gen_server:start_link(?MODULE, WorkerArgs, []).



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
init(Args) ->
    DBName = proplists:get_value(db_name, Args),
    DBType = proplists:get_value(db_type, Args),
    Hostname = proplists:get_value(hostname, Args),
    Database = proplists:get_value(database, Args),
    Username = proplists:get_value(username, Args),
    Password = proplists:get_value(password, Args),

    erl_db_log:msg(debug, "~p started with args: ~p.", [?MODULE, [Args]]),

    {ok, Conn} = pgsql:connect(Hostname, Username, Password, [
        {database, Database}
    ]),

    {ok, #state{
       name = DBName,
       type = DBType,
       conn = Conn}}.


handle_call({refresh, Model}, _From, #state{conn=Conn}=State) ->
    Table = erlang:element(1, Model),
    ModelAttributes = Table:module_info(attributes),
    Fields = proplists:get_value(fields, ModelAttributes),
    [PrimaryKeyField] = [ X || {X, Y, Z} <- Fields, Y == primary_key ],

    Where = build_select_query(Table, [{PrimaryKeyField, 'equal', Model:PrimaryKeyField()}]),
    {reply, ok, State};

handle_call({delete, Model}, _From, #state{conn=Conn}=State) ->
    {reply, ok, State};

handle_call({save, Model}, _From, #state{conn=Conn, name = DBName}=State) ->
    Modelname = element(1, Model),
    Table = get_table(Modelname, DBName),

    %% Create the table if it does not exist
    [ build_table(Modelname, Table, Conn) || not table_exist(Table, Conn) ],

    Fields = erl_db_utils:get_fields(Modelname),

    Query =
        case erl_db_utils:is_update_query(Model) of
            false ->
                build_insert_query(Model);
            _ ->
                build_update_query(Model)
        end,

    Result = pgsql:equery(Conn, Query, []),

    Reply =
        case Result of
            {ok, _, _, [{Id}|_Tl]} ->
                %% We should set this to something useful
                {ok, erl_db_utils:set_primary_key(Model, Id)};
            {ok, _Id} ->
                {ok, Model};
            {error, Reason} ->
                {error, Reason}
        end,
    {reply, Reply, State};

handle_call({find, Tablename, Conditions, _Options}, _From, #state{conn=Conn}=State) ->
    %% This is a bit tricky since we need to know which table this belongs to
    Query = build_select_query(Tablename, Conditions),
    io:format("Query: ~p~n", [Query]),
    {ok, Columns, Results} = pgsql:squery(Conn, lists:concat(Query)),
    %% We're getting correct results, but we need to model this after our model.
    Models = merge_result(Tablename, Columns, Results),
    {reply, {ok, Models}, State};


handle_call({supported_conditions}, _From, State) ->
    SupportedConditions = [
                           'equals',
                           'not_equals',
                           'greater_than',
                           'less_than',
                           'greater_equal_than',
                           'less_equal_than',
                           'like',
                           'in'
                          ],
    {reply, {ok, SupportedConditions}, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    ok = pgsql:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



get_table(Modelname, DBName) ->
    Backends = proplists:get_value(backend, Modelname:module_info(attributes), []),
    case proplists:get_value(DBName, Backends) of
        undefined ->
            throw({error, model_error});
        BackendArgs ->
            case proplists:get_value(table, BackendArgs) of
                undefined ->
                    Modelname;
                Table ->
                    Table
            end
    end.

table_exist(Table, Conn) when is_atom(Table) ->
    Query = "SELECT * FROM information_schema.tables WHERE table_name=$1",
    case pgsql:equery(Conn, Query, [Table]) of
        {ok, _Columns, []} ->
            false;
        {ok, _Columns, _Field} ->
            true;
        _ ->
            false
    end.

build_table(Modelname, Table, Conn) when is_atom(Modelname) ->
    ModelAttributes = Modelname:module_info(attributes),
    Fields = proplists:get_value(fields, ModelAttributes),

    Attributes =
        lists:foldl(
          fun({Field, Type, Args, _Index}, Acc) ->
                  case Type of
                      string ->
                          Val =
                              case proplists:get_value(max_length, Args) of
                                  undefined ->
                                      [atom_to_list(Field) ++ " TEXT "];
                                  Length ->
                                      [atom_to_list(Field) ++ " VARCHAR(" ++ integer_to_list(Length) ++ ") "]
                              end,
                          case proplists:get_value(not_null, Args) of
                              true ->
                                  [Val ++ " NOT NULL "|Acc];
                              _ ->
                                  Val ++ Acc
                          end;
                      binary ->
                          case proplists:get_value(not_null, Args) of
                              true ->
                                  [atom_to_list(Field) ++ " BYTEA NOT NULL"|Acc];
                              _ ->
                                  [atom_to_list(Field) ++ " BYTEA"|Acc]
                          end;
                      integer ->
                          case proplists:get_value(not_null, Args) of
                              true ->
                                  [atom_to_list(Field) ++ " INTEGER NOT NULL"|Acc];
                              _ ->
                                  [atom_to_list(Field) ++ " INTEGER"|Acc]
                          end;
                      float ->
                          case proplists:get_value(not_null, Args) of
                              true ->
                                  [atom_to_list(Field) ++ " REAL NOT NULL"|Acc];
                              _ ->
                                  [atom_to_list(Field) ++ " REAL"|Acc]
                          end;
                      boolean ->
                          case proplists:get_value(not_null, Args) of
                              true ->
                                  [atom_to_list(Field) ++ " BOOLEAN NOT NULL"|Acc];
                              _ ->
                                  [atom_to_list(Field) ++ " BOOLEAN"]
                          end;
                      datetime ->
                          [atom_to_list(Field) ++ " TIMESTAMP"|Acc];
                      primary_key ->
                          case proplists:get_value(auto_increment, Args) of
                              true ->
                                  [atom_to_list(Field) ++ " SERIAL PRIMARY KEY"|Acc];
                              _ ->
                                  [atom_to_list(Field) ++ " INTEGER PRIMARY KEY"|Acc]
                          end
                  end
          end, [], lists:reverse(Fields)),

    Query =
        ["CREATE TABLE ", atom_to_list(Table), " (",
         string:join(Attributes, ","), ")"],
    pgsql:equery(Conn, Query, []).

build_select_query(Tablename, []) ->
    ["SELECT * FROM ", Tablename];
build_select_query(Tablename, Fields) ->
    ["SELECT * FROM ", Tablename, " WHERE ", build_conditions(Fields)].

build_update_query(Model) ->
    Modelname = element(1, Model),
    Tablename = atom_to_list(Modelname),
    ModelAttributes = Modelname:module_info(attributes),
    Fields = proplists:get_value(fields, ModelAttributes),
    {Wheres, Values} =
        lists:foldl(
          fun({Field, primary_key, _Args, Index}, {_, Vals}) ->
                  {[atom_to_list(Field), "=", pack_value(element(Index, Model))], Vals};
             ({Field, _Type, _Args, Index}, {Where, Vals}) ->
                  {Where, [lists:concat([atom_to_list(Field) ++ "=" ++ pack_value(element(Index, Model))])|Vals]}
          end,
          {[], []}, Fields),
    ["UPDATE ", Tablename, " SET ",
     string:join(Values, ", "),
     " WHERE "
     |Wheres].

build_insert_query(Model) ->
    Modelname = element(1, Model),
    Tablename = atom_to_list(Modelname),
    ModelAttributes = Modelname:module_info(attributes),
    Fields = proplists:get_value(fields, ModelAttributes),
    {Attributes, Values} =
        lists:foldl(
          fun({Field, primary_key, Args, Index}, {Attrs, Vals}=Acc) ->
                  case proplists:get_value(auto_increment, Args) of
                      true ->
                          Acc;
                      false ->
                          {[atom_to_list(Field)|Attrs], [pack_value(element(Index, Model))|Vals]}
                  end;
             ({Field, _Type, _Args, Index}, {Attrs, Vals}) ->
                  {[atom_to_list(Field)|Attrs], [pack_value(element(Index, Model))|Vals]}
          end,
          {[], []}, Fields),
    ["INSERT INTO ", Tablename, " (",
     string:join(Attributes, ", "),
     ") VALUES (",
     string:join(Values, ", "),
     ")",
     " RETURNING id"].

merge_result(Modelname, Columns, Results) ->
    Merge = [ merge_result(Columns, tuple_to_list(X)) || X <- Results ],
    Model = Modelname:dummy(),
    Models = [ build_model(Model, Data) || Data <- Merge ].


build_model(Model, []) -> Model;
build_model(Model, [{Fieldname, Data}|Tl]) ->
    build_model(erl_db_utils:set_field(Model, Fieldname, Data), Tl).

merge_result([], []) -> [];
merge_result([{column, Fieldname, Type, _, _, _}|ColTl], [Res|Results]) ->
    [{binary_to_atom(Fieldname, utf8), unpack_value(Res, Type)}|merge_result(ColTl, Results)].


condition(Key, 'in', Val) when is_list(Val) ->
    CommaVal = string:join(Val, ","),
    lists:concat([Key, "in", "(", CommaVal, ")"]);
condition(Key, Op, Val) ->
    lists:concat([Key, Op, pack_value(Val)]).

build_conditions(Conditions) ->
    ConditionLst = build_conditions1(Conditions),
    string:join(ConditionLst, " AND ").

build_conditions1([]) ->
    [];
build_conditions1([{Key, 'equals', Value}|Tl]) ->
    [condition(Key, " = ", Value)|build_conditions1(Tl)];
build_conditions1([{Key, 'not_equals', Value}|Tl]) ->
    [condition(Key, " != ", Value)|build_conditions1(Tl)];
build_conditions1([{Key, 'greater_than', Value}|Tl]) ->
    [condition(Key, " > ", Value)|build_conditions1(Tl)];
build_conditions1([{Key, 'less_than', Value}|Tl]) ->
    [condition(Key, " < ", Value)|build_conditions1(Tl)];
build_conditions1([{Key, 'greater_equal_than', Value}|Tl]) ->
    [condition(Key, " >= ", Value)|build_conditions1(Tl)];
build_conditions1([{Key, 'less_equal_than', Value}|Tl]) ->
    [condition(Key, " <= ", Value)|build_conditions1(Tl)];
build_conditions1([{Key, 'like', Value}|Tl]) ->
    [condition(Key, " LIKE ", Value)|build_conditions1(Tl)];
build_conditions1([{Key, 'in', Value}|Tl]) ->
    [condition(Key, " IN ", Value)|Tl].


escape_sql(Value) ->
    escape_sql1(Value, []).

escape_sql1([], Acc) ->
    lists:reverse(Acc);
escape_sql1([$'|Rest], Acc) ->
    escape_sql1(Rest, [$', $'|Acc]);
escape_sql1([C|Rest], Acc) ->
    escape_sql1(Rest, [C|Acc]).


pack_datetime({Date, {Y, M, S}}) when is_float(S) ->
    pack_datetime({Date, {Y, M, erlang:round(S)}});
pack_datetime(DateTime) ->
    "TIMESTAMP '" ++ erlydtl_filters:date(DateTime, "c") ++ "'".

pack_now(Now) -> pack_datetime(calendar:now_to_datetime(Now)).

pack_value(undefined) ->
    "null";
pack_value(V) when is_binary(V) ->
    pack_value(binary_to_list(V));
pack_value(V) when is_list(V) ->
    "'" ++ escape_sql(V) ++ "'";
pack_value({MegaSec, Sec, MicroSec}) when is_integer(MegaSec) andalso is_integer(Sec) andalso is_integer(MicroSec) ->
    pack_now({MegaSec, Sec, MicroSec});
pack_value({{_, _, _}, {_, _, _}} = Val) ->
    pack_datetime(Val);
pack_value(Val) when is_integer(Val) ->
    integer_to_list(Val);
pack_value(Val) when is_float(Val) ->
    float_to_list(Val);
pack_value(true) ->
    "TRUE";
pack_value(false) ->
    "FALSE".


unpack_value(Value, int4) when is_binary(Value) ->
    binary_to_integer(Value);
unpack_value(Value, varchar) when is_binary(Value) ->
    binary_to_list(Value).

