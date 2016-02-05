defmodule RethinkDB.Ecto.NormalizedQuery do
  alias Ecto.Query
  alias Ecto.Query.{QueryExpr, SelectExpr, JoinExpr}
  alias Ecto.Association

  import RethinkDB.Lambda

  alias RethinkDB.Query, as: ReQL

  def all(query, params) do
    from(query)
    |> join(query, params)
    |> where(query, params)
    |> order_by(query, params)
    |> offset(query)
    |> limit(query)
    |> select(query, params)
  end

  def insert(model, fields) do
    from(model)
    |> ReQL.insert(Enum.into(fields, %{}))
  end

  def update(model, fields, filters) do
    from(model)
    |> ReQL.get(filters[:id])
    |> ReQL.update(Enum.into(fields, %{}))
  end

  def delete(model, filters) do
    from(model)
    |> ReQL.get(filters[:id])
    |> ReQL.delete()
  end

  defp from(%{source: {_prefix, table}}), do: ReQL.table(table)

  defp from(%Query{from: {table, _model}}), do: ReQL.table(table)

  defp join(reql, %Query{joins: joins, from: {right_table, right_model}}, params) do
    Enum.reduce(joins, reql, fn (%JoinExpr{source: {left_table, left_model}, on: on}, reql) ->
      {field, related_key} = resolve_assoc(left_model, right_model)
      ReQL.table(left_table)
      |> ReQL.eq_join(related_key, ReQL.table(right_table))
      |> ReQL.map(lambda &ReQL.merge(&1["right"], %{field => &1["left"]}))
    end)
  end

  defp where(reql, %Query{wheres: wheres}, params) do
    Enum.reduce(wheres, reql, fn (%QueryExpr{expr: expr}, reql) ->
      ReQL.filter(reql, &evaluate(expr, params, [&1]))
    end)
  end

  defp order_by(reql, %Query{order_bys: order_bys}, params) do
    Enum.reduce(order_bys, reql, fn (%QueryExpr{expr: expr}, reql) ->
      Enum.reduce(expr, reql, fn ({order, arg}, reql) ->
        ReQL.order_by(reql, apply(ReQL, order, [extract_arg(arg, params)]))
      end)
    end)
  end

  defp limit(reql, %Query{limit: nil}), do: reql

  defp limit(reql, %Query{limit: limit}) do
    ReQL.limit(reql, limit.expr)
  end

  defp offset(reql, %Query{offset: nil}), do: reql

  defp offset(reql, %Query{offset: offset}) do
    ReQL.skip(reql, offset.expr)
  end

  defp select(reql, %Query{select: %SelectExpr{expr: expr}}, params) when is_list(expr) do
    fields = Enum.map(expr, &extract_arg(&1, params))
    ReQL.map(reql, fn record ->
      Enum.map(fields, &ReQL.bracket(record, &1))
    end)
  # ReQL.with_fields(reql, Enum.map(expr, &extract_arg(&1, params)))
  end

  defp select(reql, %Query{select: %SelectExpr{expr: {op, _, _} = expr}}, params) when not is_atom(op) do
    ReQL.get_field(reql, extract_arg(expr, params))
  end

  defp select(reql, %Query{select: %SelectExpr{expr: {:&, _, _}}}, _params), do: reql

  defp select(reql, %Query{select: %SelectExpr{expr: {op, _, args}}}, params) do
    [field|args] = Enum.map(args, &extract_arg(&1, params))
    aggregate(ReQL.bracket(reql, field), op, List.first(args))
  end

  defp aggregate(reql, :count, :distinct) do
    ReQL.distinct(reql) |> ReQL.count()
  end

  defp aggregate(reql, op, nil) do
    apply(ReQL, op, [reql])
  end

  defp evaluate({op, _, args}, params, records) do
    args = Enum.map(args, &extract_arg(&1, params, records))
    case op do
      :==  -> apply(ReQL, :eq, args)
      :!=  -> apply(ReQL, :ne, args)
      :<   -> apply(ReQL, :lt, args)
      :<=  -> apply(ReQL, :le, args)
      :>   -> apply(ReQL, :gt, args)
      :>=  -> apply(ReQL, :ge, args)
      :in  -> apply(ReQL, :contains, Enum.reverse(args))
      :and -> apply(ReQL, :and_r, args)
      :or  -> apply(ReQL, :or_r, args)
      :not -> apply(ReQL, :not_r, args)
    end
  end

  defp resolve_assoc(left_model, right_model) do
    Enum.find_value(right_model.__schema__(:associations), fn assoc ->
      %Association.Has{field: field, related: related, related_key: related_key} = right_model.__schema__(:association, assoc)
      if related == left_model, do: {field, related_key}
    end)
  end

  defp extract_arg(expr, params, records \\ [])
  defp extract_arg({:^, _, [index]}, params, _records), do: Enum.at(params, index)
  defp extract_arg({{:., _, [{:&, _, [0]}, field]}, _, _}, _params, []), do: field
  defp extract_arg({{:., _, [{:&, _, [index]}, field]}, _, _}, _params, records), do: ReQL.bracket(Enum.at(records, index), field)
  defp extract_arg({_op, _, _args} = expr, params, records), do: evaluate(expr, params, records)
  defp extract_arg(expr, _params, _records), do: expr
end
