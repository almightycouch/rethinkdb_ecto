defmodule RethinkDB.Ecto.NormalizedQuery do
  alias Ecto.Query
  alias Ecto.Query.QueryExpr

  alias RethinkDB.Query, as: ReQL

  def all(query, params) do
    from(query)
    |> where(query, params)
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

  defp filter(_, {:^, _, [index]}, params), do: Enum.at(params, index)
  defp filter(record, {{:., _, [{:&, _, [0]}, field]}, _, _}, _), do: ReQL.bracket(record, field)
  defp filter(record, {op, _, args}, params) do
    args = Enum.map(args, &filter(record, &1, params))
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
  defp filter(_, expr, _), do: expr
end
