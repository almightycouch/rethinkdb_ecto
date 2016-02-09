defmodule RethinkDB.Ecto.NormalizedQuery do
  alias Ecto.Query
  alias Ecto.Query.{QueryExpr, SelectExpr, JoinExpr}
  alias Ecto.Association

  import RethinkDB.Lambda

  alias RethinkDB.Query, as: ReQL

  def all(query, params) do
    do_query(query, params)
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
    Enum.reduce(query.updates, do_query(query, params), fn %QueryExpr{expr: expr}, reql ->
      Enum.reduce(expr, reql, fn expr, reql ->
        ReQL.update(reql, &evaluate(expr, params, [&1]))
      end)
    end)
  end

  def delete_all(query, params) do
    do_query(query, params)
    |> ReQL.delete()
  end

  def delete(model, filters) do
    from(model)
    |> ReQL.get(filters[:id])
    |> ReQL.delete()
  end

  defp do_query(query, params) do
    from(query)
    |> join(query, params)
    |> where(query, params)
    |> group_by(query, params)
    |> having(query, params)
    |> order_by(query, params)
    |> offset(query)
    |> limit(query)
    |> select(query, params)
    |> distinct(query, params)
  end

  defp from(%{source: {_prefix, table}}), do: ReQL.table(table)

  defp from(%Query{from: {table, _model}}), do: ReQL.table(table)

  defp join(reql, %Query{joins: joins, from: {right_table, right_model}}, params) do
    Enum.reduce(joins, reql, fn %JoinExpr{source: {left_table, left_model}, on: on}, reql ->
      {field, related_key} = resolve_assoc(left_model, right_model)
      ReQL.table(left_table)
      |> ReQL.eq_join(related_key, ReQL.table(right_table))
      |> ReQL.map(lambda &ReQL.merge(&1["right"], %{field => &1["left"]}))
    end)
  end

  defp where(reql, %Query{wheres: wheres}, params) do
    Enum.reduce(wheres, reql, fn %QueryExpr{expr: expr}, reql ->
      ReQL.filter(reql, &evaluate(expr, params, [&1]))
    end)
  end

  defp group_by(reql, %Query{group_bys: groups}, params) do
    Enum.reduce(groups, reql, fn %QueryExpr{expr: expr}, reql ->
      reql
      |> ReQL.group(Enum.map(expr, &extract_arg(&1, params)))
      |> ReQL.ungroup()
    end)
  end

  defp having(reql, %Query{havings: havings}, params) do
    Enum.reduce(havings, reql, fn %QueryExpr{expr: expr}, reql ->
      ReQL.filter(reql, &evaluate(expr, params, [&1]))
    end)
  end

  defp order_by(reql, %Query{order_bys: order_bys}, params) do
    Enum.reduce(order_bys, reql, fn %QueryExpr{expr: expr}, reql ->
      Enum.reduce(expr, reql, fn {order, arg}, reql ->
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

  defp select(reql, %Query{select: nil}, params), do: reql

  # Support for list
  defp select(reql, %Query{select: %SelectExpr{expr: args}, group_bys: groups}, params) when is_list(args) do
    do_select(reql, groups, args, params)
  end

  # Support for tuple
  defp select(reql, %Query{select: %SelectExpr{expr: {:{}, _, args}}, group_bys: groups}, params) do
    do_select(reql, groups, args, params)
  end

  # Support for map
  defp select(reql, %Query{select: %SelectExpr{expr: {:%{}, _, args}}, group_bys: groups}, params) do
    ReQL.map(reql, fn record ->
      Enum.into(args, [], fn {key, expr} ->
        do_select(reql, record, groups, extract_arg(expr, params), params)
      end)
    end)
  end

  # Support for entire model
  defp select(reql, %Query{select: %SelectExpr{expr: {:&, _, _}}, group_bys: []}, _params), do: reql

  # Support for entire model (group_by)
  defp select(reql, %Query{select: %SelectExpr{expr: {:&, _, _}}, group_bys: _groups}, _params) do
    ReQL.concat_map(reql, &ReQL.bracket(&1, "reduction"))
  end

  # Support for single field
  defp select(reql, %Query{select: %SelectExpr{expr: {op, _, _} = expr}, group_bys: []}, params) when not is_atom(op) do
    ReQL.get_field(reql, extract_arg(expr, params))
  end

  # Support for single field (group_by)
  defp select(reql, %Query{select: %SelectExpr{expr: {op, _, _} = expr}, group_bys: groups}, params) when not is_atom(op) do
    ReQL.map(reql, &do_select(reql, &1, groups, extract_arg(expr, params), params))
  end

  # Support for aggregator
  defp select(reql, %Query{select: %SelectExpr{expr: {op, _, args}}, group_bys: []}, params) do
    aggregate(reql, op, args, params)
  end

  # Support for aggregator (group_by)
  defp select(reql, %Query{select: %SelectExpr{expr: {op, _, args} = expr}, group_bys: groups}, params) do
    ReQL.map(reql, &do_select(reql, &1, groups, extract_arg(expr, params), params))
  end

  defp distinct(reql, %Query{distinct: nil}, params), do: reql

  defp distinct(reql, %Query{distinct: %QueryExpr{expr: true}}, params) do
    ReQL.distinct(reql)
  end

  defp do_select(reql, groups, args, params) do
    fields = Enum.map(args, &extract_arg(&1, params))
    ReQL.map(reql, fn record ->
      Enum.map(fields, &do_select(reql, record, groups, &1, params))
    end)
  end

  defp do_select(reql, record, [], expr, params) do
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

  defp do_select(reql, record, groups, expr, params) do
    groups = Enum.flat_map(groups, fn %QueryExpr{expr: expr} -> Enum.map(expr, &extract_arg(&1, params)) end)
    cond do
      expr in groups ->
        ReQL.bracket(record, "group")
      expr ->
        reduction = ReQL.bracket(record, "reduction")
        do_select(reduction, reduction, [], expr, params)
    end
  end

  defp aggregate(reql, op, args, params) do
    [field|args] = Enum.map(args, &extract_arg(&1, params))
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

  defp resolve_assoc(left_model, right_model) do
    Enum.find_value(right_model.__schema__(:associations), fn assoc ->
      %Association.Has{field: field, related: related, related_key: related_key} = right_model.__schema__(:association, assoc)
      if related == left_model, do: {field, related_key}
    end)
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
      :is_nil -> apply(ReQL, :ne, args ++ [nil])
      :field  -> apply(ReQL, :bracket, args)
      :like   -> like(args)
      :ilike  -> like(args, true)
      _ -> {op, args}
    end
  end

  defp evaluate({:set, args}, params, records) do
    Enum.into(args, %{}, fn {key, arg} ->
        {key, extract_arg(arg, params, records)}
    end)
  end

  defp evaluate({:inc, args}, params, records) do
    Enum.into(args, %{}, fn {key, arg} ->
      {key, ReQL.add(ReQL.bracket(List.first(records), key), extract_arg(arg, params, records))}
    end)
  end

  defp evaluate({:push, args}, params, records) do
    Enum.into(args, %{}, fn {key, arg} ->
      {key, ReQL.append(ReQL.bracket(List.first(records), key), extract_arg(arg, params, records))}
    end)
  end

  defp evaluate({:pull, args}, params, records) do
    Enum.into(args, %{}, fn {key, arg} ->
      {key, ReQL.without(ReQL.bracket(List.first(records), key), extract_arg(arg, params, records))}
    end)
  end

  defp extract_arg(expr, params, records \\ [])
  defp extract_arg({:^, _, [index]}, params, _records), do: Enum.at(params, index)
  defp extract_arg({{:., _, [{:&, _, [0]}, field]}, _, _}, _params, []), do: field
  defp extract_arg({{:., _, [{:&, _, [index]}, field]}, _, _}, _params, records), do: ReQL.bracket(Enum.at(records, index), field)
  defp extract_arg({_op, _, _args} = expr, params, records), do: evaluate(expr, params, records)
  defp extract_arg(expr, _params, _records), do: expr
end
