defmodule RethinkDB.Ecto.NormalizedQuery do
  alias Ecto.Query
  alias Ecto.Query.{QueryExpr, SelectExpr, JoinExpr}
  alias Ecto.Association

  import RethinkDB.Lambda

  alias RethinkDB.Query, as: ReQL

  def all(query, params) do
    normalize_query(query, params)
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

  defp normalize_query(query, params) do
    from(query)
    |> where(query, params)
    |> group_by(query, params)
    |> having(query, params)
    |> order_by(query, params)
    |> offset(query)
    |> limit(query)
    |> select(query, params)
    |> distinct(query, params)
  end

  #
  # from()
  #

  defp from(%{source: {_prefix, table}}), do: ReQL.table(table)
  defp from(%Query{from: {table, _model}}), do: ReQL.table(table)

  #
  # where()
  #

  defp where(reql, %Query{wheres: wheres}, params) do
    Enum.reduce(wheres, reql, fn %QueryExpr{expr: expr}, reql ->
      ReQL.filter(reql, &evaluate(expr, params, [&1]))
    end)
  end

  #
  # group_by()
  #

  defp group_by(reql, %Query{group_bys: groups}, params) do
    Enum.reduce(groups, reql, fn %QueryExpr{expr: expr}, reql ->
      reql
      |> ReQL.group(Enum.map(expr, &evaluate_arg(&1, params)))
      |> ReQL.ungroup()
    end)
  end

  #
  # having()
  #

  defp having(reql, %Query{havings: havings}, params) do
    Enum.reduce(havings, reql, fn %QueryExpr{expr: expr}, reql ->
      ReQL.filter(reql, &evaluate(expr, params, [&1]))
    end)
  end

  #
  # order_by()
  #

  defp order_by(reql, %Query{order_bys: order_bys}, params) do
    Enum.reduce(order_bys, reql, fn %QueryExpr{expr: expr}, reql ->
      Enum.reduce(expr, reql, fn {order, arg}, reql ->
        ReQL.order_by(reql, apply(ReQL, order, [evaluate_arg(arg, params)]))
      end)
    end)
  end

  #
  # limit()
  #

  defp limit(reql, %Query{limit: nil}), do: reql
  defp limit(reql, %Query{limit: limit}), do: ReQL.limit(reql, limit.expr)

  #
  # offset()
  #

  defp offset(reql, %Query{offset: nil}), do: reql
  defp offset(reql, %Query{offset: offset}), do: ReQL.skip(reql, offset.expr)

  #
  # select()
  #

  defp select(reql, %Query{select: nil}, params), do: reql

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
      Enum.into(args, [], fn {key, expr} ->
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
  defp select(reql, %Query{select: %SelectExpr{expr: {op, _, args} = expr}, group_bys: groups}, params) do
    ReQL.map(reql, &selectize(reql, &1, groups, evaluate_arg(expr, params), params))
  end

  #
  # distinct()
  #

  defp distinct(reql, %Query{distinct: nil}, params), do: reql
  defp distinct(reql, %Query{distinct: %QueryExpr{expr: true}}, params), do: ReQL.distinct(reql)


  defp selectize(reql, groups, args, params) do
    fields = Enum.map(args, &evaluate_arg(&1, params))
    ReQL.map(reql, fn record ->
      Enum.map(fields, &selectize(reql, record, groups, &1, params))
    end)
  end

  defp selectize(reql, record, [], expr, params) do
    case expr do
      {:&, args} ->
        record
      {op, args} ->
        aggregate(reql, op, args, params)
      field when is_atom(field) ->
        ReQL.bracket(record, field)
      value ->
        value
    end
  end

  defp selectize(reql, record, groups, expr, params) do
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
    if String.first(regex) != "%", do: regex = "^" <> regex
    if String.last(regex) != "%", do: regex = regex <> "%"
    regex =
      String.strip(regex, ?%)
      |> Regex.compile!(if caseless, do: "i", else: "")
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
  defp evaluate_arg({:^, _, [index]}, params, _records), do: Enum.at(params, index)
  defp evaluate_arg({:^, _, [index, count]}, params, _records), do: Enum.slice(params, index, count)
  defp evaluate_arg({:^, _, args}, params, _records), do: raise "Unsupported pin arguments: #{inspect args}"
  defp evaluate_arg({{:., _, [{:&, _, [0]}, field]}, _, _}, _params, []), do: field
  defp evaluate_arg({{:., _, [{:&, _, [index]}, field]}, _, _}, _params, records), do: ReQL.bracket(Enum.at(records, index), field)
  defp evaluate_arg({_op, _, _args} = expr, params, records), do: evaluate(expr, params, records)
  defp evaluate_arg(expr, _params, _records), do: expr
end
