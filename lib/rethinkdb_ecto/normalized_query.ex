defmodule RethinkDB.Ecto.NormalizedQuery do
  alias Ecto.Query
  alias Ecto.Query.{QueryExpr, SelectExpr}

  alias RethinkDB.Query, as: ReQL

  def all(query, params) do
    from(query)
    |> where(query, params)
    |> order_by(query, params)
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

  defp where(reql, %Query{wheres: wheres}, params) do
    Enum.reduce(wheres, reql, fn (%QueryExpr{expr: expr}, reql) ->
      ReQL.filter(reql, &filter(&1, expr, params))
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

  defp select(reql, %Query{select: %SelectExpr{expr: expr}}, params) when is_list(expr) do
    ReQL.with_fields(reql, Enum.map(expr, &extract_arg(&1, params)))
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

  defp filter(record, {op, _, args}, params) do
    args = Enum.map(args, &extract_arg(&1, params, record))
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

  defp extract_arg(expr, params, record \\ nil)
  defp extract_arg({:^, _, [index]}, params, _), do: Enum.at(params, index)
  defp extract_arg({{:., _, [{:&, _, [0]}, field]}, _, _}, _, record), do: if record, do: ReQL.bracket(record, field), else: field
  defp extract_arg(expr, params, record) when is_tuple(expr), do: filter(record, expr, params)
  defp extract_arg(expr, _, _), do: expr
end
