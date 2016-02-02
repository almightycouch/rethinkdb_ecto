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
      case expr do
        {:==, _, [{{_, _, [_, index]}, _, _}, _]} ->
          ReQL.filter(reql, %{index => List.first(params)})
      end
    end)
  end
end
