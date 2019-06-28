%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(emqx_rule_sqlparser).

-include("rule_engine.hrl").
-include("rule_events.hrl").

-export([parse_select/1]).

-export([ select_fields/1
        , select_from/1
        , select_where/1
        ]).

-export([ hook/1
        , unquote/1
        ]).

-record(select, {fields, from, where}).

-opaque(select() :: #select{}).

-type(const() :: {const, number()|binary()}).

-type(variable() :: binary() | list(binary())).

-type(alias() :: binary() | list(binary())).

-type(field() :: const() | variable()
               | {as, field(), alias()}
               | {'fun', atom(), list(field())}).

-export_type([select/0]).

-define(SELECT(Fields, From, Where),
        {select, [{fields, Fields}, {from, From}, {where, Where}|_]}).

%% Parse one select statement.
-spec(parse_select(string() | binary())
      -> {ok, select()} | {parse_error, term()} | {lex_error, term()}).
parse_select(Sql) ->
    try case sqlparse:parsetree(Sql) of
            {ok, [{?SELECT(Fields, From, Where), _Extra}]} ->
                {ok, preprocess(#select{fields = Fields, from = From, where = Where})};
            Error -> Error
        end
    catch
        _Error:Reason ->
            {parse_error, Reason}
    end.

-spec(select_fields(select()) -> list(field())).
select_fields(#select{fields = Fields}) ->
    Fields.

-spec(select_from(select()) -> list(binary())).
select_from(#select{from = From}) ->
    From.

-spec(select_where(select()) -> tuple()).
select_where(#select{where = Where}) ->
    Where.

preprocess(#select{fields = Fields, from = Hooks, where = Conditions}) ->
    Selected = [preprocess_field(Field) || Field <- Fields],
    Froms = [hook(unquote(H)) || H <- Hooks],
    #select{fields = Selected,
            from   = Froms,
            where  = preprocess_condition(Conditions, maps:merge(fixed_columns(Froms), as_columns(Selected)))}.

preprocess_field(<<"*">>) ->
    '*';
preprocess_field({'as', Field, Alias}) when is_binary(Alias) ->
    {'as', transform_non_const_field(Field), transform_alias(Alias)};
preprocess_field(Field) ->
    transform_non_const_field(Field).

preprocess_condition({'=', L, R}, Columns) ->
    Lh = preprocess_condition(L, Columns),
    Rh = preprocess_condition(R, Columns),
    case {is_topic_var(Lh), is_topic_var(Rh)} of
        {true, _} -> {match, Lh, Rh};
        {false, true} -> {match, Rh, Lh};
        {false, false} -> {'=', Lh, Rh}
    end;
preprocess_condition({Op, L, R}, Columns) when ?is_arith(Op);
                                               ?is_logical(Op);
                                               ?is_comp(Op) ->
    {Op, preprocess_condition(L, Columns), preprocess_condition(R, Columns)};
preprocess_condition({in, Field, {list, Vals}}, Columns) ->
    {in, transform_field(Field, Columns), {list, [transform_field(Val, Columns) || Val <- Vals]}};
preprocess_condition({'not', X}, Columns) ->
    {'not', preprocess_condition(X, Columns)};
preprocess_condition({}, _Columns) ->
    {};
preprocess_condition(Field, Columns) ->
    transform_field(Field, Columns).

transform_field({const, Val}, _Columns) ->
    {const, Val};
transform_field(<<Q, Val/binary>>, _Columns) when Q =:= $'; Q =:= $" ->
    {const, binary:part(Val, {0, byte_size(Val)-1})};
transform_field(Val, Columns) ->
    case is_number_str(Val) of
        true -> {const, Val};
        false ->
            do_transform_field(Val, Columns)
    end.

do_transform_field(<<"payload.", Attr/binary>>, Columns) ->
    validate_var(<<"payload">>, Columns),
    {payload, parse_nested(Attr)};
do_transform_field(Var, Columns) when is_binary(Var) ->
    case is_topic(Var, Columns) of
        false ->
            {var, validate_var(parse_nested(Var), Columns)};
        true ->
            {topic, validate_var(parse_nested(Var), Columns)}
    end;
do_transform_field({'fun', Name, Args}, Columns) when is_binary(Name) ->
    Fun = list_to_existing_atom(binary_to_list(Name)),
    {'fun', Fun, [transform_field(Arg, Columns) || Arg <- Args]}.

transform_non_const_field(<<"payload.", Attr/binary>>) ->
    {payload, parse_nested(Attr)};
transform_non_const_field({Op, Arg1, Arg2}) when ?is_arith(Op) ->
    {Op, transform_non_const_field(Arg1), transform_non_const_field(Arg2)};
transform_non_const_field(Var) when is_binary(Var) ->
    {var, parse_nested(Var)};
transform_non_const_field({'fun', Name, Args}) when is_binary(Name) ->
    Fun = list_to_existing_atom(binary_to_list(Name)),
    {'fun', Fun, [transform_non_const_field(Arg) || Arg <- Args]}.

validate_var(Var, SupportedColumns) ->
    case {Var, maps:is_key(Var, SupportedColumns)} of
        {_, true} -> Var;
        {[TopVar | _], false} ->
            maps:is_key(TopVar, SupportedColumns) orelse error({unknown_column, Var}),
            Var;
        {_, false} ->
            error({unknown_column, Var})
    end.

is_number_str(Str) when is_binary(Str) ->
    try _ = binary_to_integer(Str), true
    catch _:_ ->
        try _ = binary_to_float(Str), true
        catch _:_ -> false
        end
    end;
is_number_str(_NonStr) ->
    false.

as_columns(Selected) ->
    as_columns(Selected, #{}).
as_columns([], Acc) -> Acc;
as_columns([{as, {_, OrgVal}, Val} | Selected], Acc) ->
    as_columns(Selected, Acc#{Val => OrgVal});
as_columns([_ | Selected], Acc) ->
    as_columns(Selected, Acc).

fixed_columns(Events) when is_list(Events) ->
    lists:foldl(
        fun(Event, Acc) ->
            fixed_columns(Event, Acc)
        end, #{}, Events).
fixed_columns(Event, Acc) ->
    lists:foldl(
        fun(Column, Acc0) ->
            Acc0#{Column => Column}
        end, Acc, ?COLUMNS(Event)).

transform_alias(Alias) ->
    parse_nested(unquote(Alias)).

parse_nested(Attr) ->
    case string:split(Attr, <<".">>, all) of
        [Attr] -> Attr;
        Nested -> Nested
    end.

is_topic(<<"topic">>, _) -> true;
is_topic(Var, Aliases) ->
    case maps:find(Var, Aliases) of
        {ok, <<"topic">>} -> true;
        _ -> false
    end.

is_topic_var({topic, _}) -> true;
is_topic_var(_) -> false.

unquote(Topic) ->
    string:trim(Topic, both, "\"'").

hook(<<"client.connected">>) ->
    'client.connected';
hook(<<"client.disconnected">>) ->
    'client.disconnected';
hook(<<"client.subscribe">>) ->
    'client.subscribe';
hook(<<"client.unsubscribe">>) ->
    'client.unsubscribe';
hook(<<"message.publish">>) ->
    'message.publish';
hook(<<"message.deliver">>) ->
    'message.deliver';
hook(<<"message.acked">>) ->
    'message.acked';
hook(<<"message.dropped">>) ->
    'message.dropped';
hook(_) ->
    error(unknown_event_type).
