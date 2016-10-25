defmodule RethinkDB.Ecto.NormalizedQuery do
  @moduledoc false

  alias Ecto.Query
  alias Ecto.Query.{QueryExpr, SelectExpr, JoinExpr}

  alias RethinkDB.Query, as: ReQL

  def all(query, params) do
    normalize_query(query, params)
  end

  def insert(model, fields) do
    from(model)
    |> ReQL.insert(Enum.into(fields, %{}))
  end

  def insert_all(model, fields) do
    from(model)
    |> ReQL.insert(Enum.map(fields, &Enum.into(&1, %{})))
  end

  def update(model, fields, filters) do
    from(model)
    |> ReQL.get(filters[:id])
    |> ReQL.update(Enum.into(fields, %{}))
  end

  def update_all(query, params) do
    Enum.reduce(query.updates, normalize_query(query, params), fn %QueryExpr{expr: expr}, reql ->
      Enum.reduce(expr, reql, fn expr, reql ->
        ReQL.update(reql, &evaluate(expr, params, [&1]))
      end)
    end)
  end

  def delete(model, filters) do
    from(model)
    |> ReQL.get(filters[:id])
    |> ReQL.delete()
  end

  def delete_all(query, params) do
    normalize_query(query, params)
    |> ReQL.delete()
  end

  #
  # FROM
  #

  defp from(%{source: {_prefix, table}}), do: ReQL.table(table)
  defp from(%Query{from: {table, _model}}), do: ReQL.table(table)

  #
  # JOIN
  #

  defp join(reql, %Query{joins: joins}, params) do
    Enum.reduce(joins, reql, fn %JoinExpr{on: on, source: {table, _schema}}, reql ->
      ReQL.inner_join(reql, ReQL.table(table), &evaluate(on.expr, params, [&1, &2]))
      |> ReQL.map(&[ReQL.bracket(&1, "left"), ReQL.bracket(&1, "right")])
    end)
  end

  #
  # WHERE
  #

  defp where(reql, %Query{wheres: wheres, sources: sources}, params) do
    Enum.reduce(wheres, reql, fn %QueryExpr{expr: expr}, reql ->
      ReQL.filter(reql, &evaluate(expr, params, resolve(&1, sources)))
    end)
  end

  defp resolve(reql, sources) do
    if tuple_size(sources) == 1 do
      [reql]
    else
      Enum.map_reduce(Tuple.to_list(sources), 0, fn _source, index ->
        {ReQL.bracket(reql, index), index + 1}
      end) |> elem(0)
    end
  end

  #
  # GROUP BY
  #

  defp group_by(reql, %Query{group_bys: groups}, params) do
    Enum.reduce(groups, reql, fn %QueryExpr{expr: expr}, reql ->
      reql
      |> ReQL.group(Enum.map(expr, &evaluate_arg(&1, params)))
      |> ReQL.ungroup()
    end)
  end

  #
  # HAVING
  #

  defp having(reql, %Query{havings: havings}, params) do
    Enum.reduce(havings, reql, fn %QueryExpr{expr: expr}, reql ->
      ReQL.filter(reql, &evaluate(expr, params, [ReQL.bracket(&1, "reduction")]))
    end)
  end

  #
  # ORDER BY
  #

  defp order_by(reql, %Query{order_bys: order_bys}, params) do
    Enum.reduce(order_bys, reql, fn %QueryExpr{expr: expr}, reql ->
      Enum.reduce(expr, reql, fn {order, arg}, reql ->
        ReQL.order_by(reql, apply(ReQL, order, [evaluate_arg(arg, params)]))
      end)
    end)
  end

  #
  # LIMIT
  #

  defp limit(reql, %Query{limit: nil}, _), do: reql
  defp limit(reql, %Query{limit: limit}, params) do
    ReQL.limit(reql, evaluate_arg(limit.expr, params))
  end

  #
  # OFFSET
  #

  defp offset(reql, %Query{offset: nil}, _), do: reql
  defp offset(reql, %Query{offset: offset}, params) do
    ReQL.skip(reql, evaluate_arg(offset.expr, params))
  end

  #
  # SELECT
  #

  defp select(reql, %Query{select: nil}, _params), do: reql

  # support for list
  defp select(reql, %Query{select: %SelectExpr{expr: args}, group_bys: groups}, params) when is_list(args) do
    selectize(reql, groups, args, params)
  end

  # support for tuple
  defp select(reql, %Query{select: %SelectExpr{expr: {:{}, _, args}}, group_bys: groups}, params) do
    selectize(reql, groups, args, params)
  end

  # support for map
  defp select(reql, %Query{select: %SelectExpr{expr: {:%{}, _, args}}, group_bys: groups}, params) do
    ReQL.map(reql, fn record ->
      Enum.into(args, [], fn {_key, expr} ->
        selectize(reql, record, groups, evaluate_arg(expr, params), params)
      end)
    end)
  end

  # support for entire model
  defp select(reql, %Query{select: %SelectExpr{expr: {:&, _, _}}, group_bys: []}, _params), do: reql

  # support for entire model (group_by)
  defp select(reql, %Query{select: %SelectExpr{expr: {:&, _, _}}, group_bys: _groups}, _params) do
    ReQL.concat_map(reql, &ReQL.bracket(&1, "reduction"))
  end

  # support for single field
  defp select(reql, %Query{select: %SelectExpr{expr: {op, _, _} = expr}, group_bys: []}, params) when not is_atom(op) do
    ReQL.get_field(reql, evaluate_arg(expr, params))
  end

  # support for single field (group_by)
  defp select(reql, %Query{select: %SelectExpr{expr: {op, _, _} = expr}, group_bys: groups}, params) when not is_atom(op) do
    ReQL.map(reql, &selectize(reql, &1, groups, evaluate_arg(expr, params), params))
  end

  # support for aggregator
  defp select(reql, %Query{select: %SelectExpr{expr: {op, _, args}}, group_bys: []}, params) do
    aggregate(reql, op, args, params)
  end

  # support for aggregator (group_by)
  defp select(reql, %Query{select: %SelectExpr{expr: {_, _, _} = expr}, group_bys: groups}, params) do
    ReQL.map(reql, &selectize(reql, &1, groups, evaluate_arg(expr, params), params))
  end

  #
  # DISTINCT
  #

  defp distinct(reql, %Query{distinct: %QueryExpr{expr: false}}, _params), do: reql
  defp distinct(reql, %Query{distinct: nil}, _params), do: reql

  defp distinct(reql, %Query{distinct: %QueryExpr{expr: true}}, _params), do: ReQL.distinct(reql)
  defp distinct(_reql, %Query{distinct: %QueryExpr{expr: expr}}, params) do
    fields = evaluate_arg(expr, params)
    raise "Can not perform :dictinct on #{inspect fields}, use :group_by instead."
  end

  #
  # Helpers
  #

  defp normalize_query(query, params) do
    from(query)
    |> join(query, params)
    |> where(query, params)
    |> group_by(query, params)
    |> having(query, params)
    |> order_by(query, params)
    |> offset(query, params)
    |> limit(query, params)
    |> select(query, params)
    |> distinct(query, params)
  end

  defp selectize(reql, groups, args, params) do
    modref = {:&, [0]}
    fields = Enum.map(args, &evaluate_arg(&1, params))
    fields =
      if i = Enum.find_index(fields, & &1 == modref) do
        [modref|List.delete_at(fields, i)]
      else
        fields
      end
    ReQL.map(reql, fn record ->
      Enum.map(fields, &selectize(reql, record, groups, &1, params))
    end)
  end

  defp selectize(reql, record, [], expr, params) do
    case expr do
      {:&, _args} ->
        record
      {op, args} ->
        aggregate(reql, op, args, params)
      field when is_atom(field) ->
        ReQL.bracket(record, field)
      value ->
        value
    end
  end

  defp selectize(_reql, record, groups, expr, params) do
    groups = Enum.flat_map(groups, fn %QueryExpr{expr: expr} -> Enum.map(expr, &evaluate_arg(&1, params)) end)
    cond do
      expr in groups ->
        ReQL.bracket(record, "group")
      expr ->
        reduction = ReQL.bracket(record, "reduction")
        selectize(reduction, reduction, [], expr, params)
    end
  end

  defp aggregate(reql, op, args, params) do
    [field|args] = Enum.map(args, &evaluate_arg(&1, params))
    aggregate(ReQL.bracket(reql, field), op, List.first(args))
  end

  defp aggregate(reql, :count, :distinct) do
    ReQL.distinct(reql) |> ReQL.count()
  end

  defp aggregate(reql, op, nil) do
    apply(ReQL, op, [reql])
  end

  defp like([field, match], caseless \\ false) do
    regex = Regex.escape(match)
    regex = if String.first(regex) != "%", do: "^" <> regex, else: String.replace_prefix(regex, "%", "")
    regex = if String.last(regex) != "%", do: regex <> "$", else: String.replace_suffix(regex, "%", "")
    regex = if caseless, do: "(?i)" <> regex, else: regex
    apply(ReQL, :match, [field, regex])
  end

  defp evaluate({op, _, args}, params, records) do
    args = Enum.map(args, &evaluate_arg(&1, params, records))
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
      :is_nil -> apply(ReQL, :ne, args ++ [nil])
      :field  -> apply(ReQL, :bracket, args)
      :like   -> like(args)
      :ilike  -> like(args, true)
      _ -> {op, args}
    end
  end

  defp evaluate({:set, args}, params, records) do
    Enum.into(args, %{}, fn {key, arg} ->
        {key, evaluate_arg(arg, params, records)}
    end)
  end

  defp evaluate({:inc, args}, params, records) do
    Enum.into(args, %{}, fn {key, arg} ->
      {key, ReQL.add(ReQL.bracket(List.first(records), key), evaluate_arg(arg, params, records))}
    end)
  end

  defp evaluate({:push, args}, params, records) do
    Enum.into(args, %{}, fn {key, arg} ->
      {key, ReQL.append(ReQL.bracket(List.first(records), key), evaluate_arg(arg, params, records))}
    end)
  end

  defp evaluate({:pull, args}, params, records) do
    Enum.into(args, %{}, fn {key, arg} ->
      {key, ReQL.without(ReQL.bracket(List.first(records), key), evaluate_arg(arg, params, records))}
    end)
  end

  defp evaluate_arg(expr, params, records \\ [])
  defp evaluate_arg(%Ecto.Query.Tagged{value: arg}, params, records), do: evaluate_arg(arg, params, records)
  defp evaluate_arg(args, params, records) when is_list(args), do: Enum.map(args, &evaluate_arg(&1, params, records))
  defp evaluate_arg({:^, _, [index]}, params, _records), do: Enum.at(params, index)
  defp evaluate_arg({:^, _, [index, count]}, params, _records), do: Enum.slice(params, index, count)
  defp evaluate_arg({:^, _, args}, _params, _records), do: raise "Unsupported pin arguments: #{inspect args}"
  defp evaluate_arg({{:., _, [{:&, _, [0]}, field]}, _, _}, _params, []), do: field
  defp evaluate_arg({{:., _, [{:&, _, [index]}, field]}, _, _}, _params, records), do: ReQL.bracket(Enum.at(records, index), field)
  defp evaluate_arg({_op, _, _args} = expr, params, records), do: evaluate(expr, params, records)
  defp evaluate_arg(expr, _params, _records), do: expr
end
