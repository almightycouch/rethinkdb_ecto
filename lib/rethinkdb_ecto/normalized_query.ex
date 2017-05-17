defmodule RethinkDB.Ecto.NormalizedQuery do
  @moduledoc """
  This module normalizes Ecto SQL-like queries into ReQL queries.

  Most Ecto.Query functions are supported by the RethinkDB.Ecto adapter,
  including queries with aggregations, joins and complex filters and selections.

  For specific implementation details, you should check the source code.
  """

  alias Ecto.Query
  alias Ecto.Query.{BooleanExpr, QueryExpr, SelectExpr, JoinExpr}

  alias RethinkDB.Query, as: ReQL

  @aggregators [:avg, :count, :max, :min, :sum]

  @doc """
  Fetches all entries from the data store matching the given query.
  """
  def all(query, params) do
    normalize_query(query, params)
  end

  @doc """
  Inserts a struct or a changeset.
  """
  def insert(model, fields) do
    from(model)
    |> ReQL.insert(Enum.into(fields, %{}))
  end

  @doc """
  Inserts all entries into the repository.
  """
  def insert_all(model, fields) do
    from(model)
    |> ReQL.insert(Enum.map(fields, &Enum.into(&1, %{})))
  end

  @doc """
  Updates a changeset using its primary key.
  """
  def update(model, fields, filters) do
    from(model)
    |> ReQL.get(filters[:id])
    |> ReQL.update(Enum.into(fields, %{}))
  end

  @doc """
  Updates all entries matching the given query with the given values.
  """
  def update_all(query, params) do
    Enum.reduce(query.updates, normalize_query(query, params), fn %QueryExpr{expr: expr}, reql ->
      Enum.reduce(expr, reql, fn expr, reql ->
        ReQL.update(reql, &evaluate(expr, params, [&1]))
      end)
    end)
  end

  @doc """
  Deletes a struct using its primary key.
  """
  def delete(model, filters) do
    from(model)
    |> ReQL.get(filters[:id])
    |> ReQL.delete()
  end

  @doc """
  Deletes all entries matching the given query.
  """
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

  defp join(reql, %Query{joins: []}, _params), do: reql
  defp join(reql, %Query{joins: joins, from: {_from_table, left_schema}}, params) do
    init_table = elem(List.first(joins).source, 0)
    Enum.reduce(joins, reql, fn %JoinExpr{on: on, source: {table, right_schema}}, reql ->
      init = init_table == table
      case on.expr do
        true ->
          # I assume that true means matching by association,
          # if it is not the case, we should use inner_join
          # with the filter function always returning true instead.
          field =
            Enum.reduce_while(left_schema.__schema__(:associations), nil, fn assoc, key ->
              case left_schema.__schema__(:association, assoc) do
                %Ecto.Association.BelongsTo{related: ^right_schema, owner_key: key} ->
                  {:halt, key}
                _else ->
                  {:cont, key}
              end
            end)

          reql
          |> ReQL.eq_join(field, ReQL.table(table))
          |> merge_join(init)
        expr ->
          reql
          |> ReQL.inner_join(ReQL.table(table), &evaluate_join(&1, &2, expr, params, init))
          |> merge_join(init)
      end
    end)
  end

  #
  # WHERE
  #

  defp where(reql, %Query{wheres: wheres, sources: sources}, params) do
    Enum.reduce(wheres, reql, fn %BooleanExpr{expr: expr}, reql ->
      ReQL.filter(reql, &evaluate(expr, params, resolve(&1, sources)))
    end)
  end

  #
  # GROUP BY
  #

  defp group_by(reql, %Query{group_bys: groups}, params) do
    Enum.reduce(groups, reql, fn %QueryExpr{expr: expr}, reql ->
      ReQL.group(reql, Enum.map(expr, &normalize_arg(&1, params)))
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
        ReQL.order_by(reql, apply(ReQL, order, [normalize_arg(arg, params)]))
      end)
    end)
  end

  #
  # LIMIT
  #

  defp limit(reql, %Query{limit: nil}, _), do: reql
  defp limit(reql, %Query{limit: limit}, params) do
    ReQL.limit(reql, normalize_arg(limit.expr, params))
  end

  #
  # OFFSET
  #

  defp offset(reql, %Query{offset: nil}, _), do: reql
  defp offset(reql, %Query{offset: offset}, params) do
    ReQL.skip(reql, normalize_arg(offset.expr, params))
  end

  #
  # SELECT
  #

  defp select(reql, %Query{select: nil}, _params), do: reql
  defp select(reql, %Query{select: %SelectExpr{expr: expr}, assocs: assocs} = q, _params) do
    assocs    =  Enum.map(assocs, &elem(elem(&1, 1), 0))
    group_by? = !Enum.empty?(q.group_bys)
    reducers? = !group_by? && reducers_only?(q.select)

    # In case of a query containing group_by or aggregation clauses,
    # we transform the query accordingly.
    reql =
      cond do
        group_by? ->
          reql
          |> ReQL.ungroup()
          |> ReQL.map(&ReQL.bracket(&1, "reduction"))
        reducers? ->
          ReQL.coerce_to(reql, "ARRAY")
        :else ->
          reql
      end

    # If select contains only aggregators, eg. {count(x.y), sum(x.z)},
    # we use ReQL.do/2, elsewhise, we use ReQL.map/2.
    {select, aggregator} =
      cond do
      reducers? ->
        reql = ReQL.do_r(reql, &selectize(&1, expr, q, assocs))
        reql = ReQL.do_r(reql, &[&1])
        {reql, nil}
       fields = take_fields(q.select) ->
         reql = ReQL.with_fields(reql, fields)
         {reql, nil}
      :else ->
        {expr, aggregator} = pop_aggregator(expr)
        reql = ReQL.map(reql, &selectize(&1, expr, q, assocs))
        {reql, aggregator}
      end

    # We apply final aggregator only if the query
    # does not contain group_by or multiple aggregator clauses.
    if aggregator && !group_by? && !reducers?,
      do: aggregator.(select),
    else: select
  end

  #
  # DISTINCT
  #

  defp distinct(reql, %Query{distinct: %QueryExpr{expr: false}}, _params), do: reql
  defp distinct(reql, %Query{distinct: nil}, _params), do: reql

  defp distinct(reql, %Query{distinct: %QueryExpr{expr: true}}, _params), do: ReQL.distinct(reql)
  defp distinct(_reql, %Query{distinct: %QueryExpr{expr: expr}}, params) do
    fields = normalize_arg(expr, params)
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

  defp resolve(reql, sources) do
    if tuple_size(sources) == 1 do
      [reql]
    else
      Enum.map_reduce(Tuple.to_list(sources), 0, fn _source, index ->
        {ReQL.bracket(reql, index), index + 1}
      end) |> elem(0)
    end
  end

  defp evaluate_join(first, second, expr, params, true) do
    evaluate(expr, params, [first, second])
  end

  defp evaluate_join(first, second, expr, params, false) do
    ReQL.do_r(ReQL.append(first, second), &evaluate(expr, params, &1))
  end

  defp merge_join(reql, true) do
    ReQL.map(reql, &[ReQL.bracket(&1, "left"), ReQL.bracket(&1, "right")])
  end

  defp merge_join(reql, false) do
    ReQL.map(reql, &ReQL.append(ReQL.bracket(&1, "left"), ReQL.bracket(&1, "right")))
  end

  defp take_fields(%SelectExpr{take: take}) when map_size(take) > 0 do
    # This function is called only when a select is constructed
    # with struct(u, [:id, :name]) or it [:id, :name] shortcut.
    {:any, fields} = take[0]
    fields
  end

  defp take_fields(_select_expr), do: nil

  defp selectize(record, expr, q, assocs) do
    # In order to preload associations we have to
    # return joins usings their source index.
    assocs = Enum.map(assocs, &ReQL.bracket(record, &1))

    # Inserts assocs right after the source.
    # This is a undocumented Ecto requirement.
    case selectize(record, expr, q) do
     [source]       ->  source
     [source|tail]  -> [source|assocs ++ tail]
      source        -> [source|assocs]
    end
  end

  defp selectize(record, expr, q) when is_list(expr) do
    # When selecting {x.foo, x} or any other variant
    # involving returning x with other values,
    # we must ensure that x is the first value of the list.
    this = {:&, [], [0]}
    expr = if i = Enum.find_index(expr, & &1 == this),
      do: [this|List.delete_at(expr, i)],
    else: expr
    Enum.map(expr, &selectize(record, &1, q))
  end

  defp selectize(record, {{:., _, expr}, _, _}, q), do: Enum.reduce(expr, record, &selectize(&2, &1, q))

  defp selectize(record, {:{}, _, expr}, q), do: selectize(record, expr, q)
  defp selectize(record, {:%{}, _, expr}, q), do: selectize(record, Keyword.values(expr), q)

  defp selectize(record, {:&, _, [0]}, %Query{sources: {_}}), do: record
  defp selectize(record, {:&, _, [0, fields, count]}, _) when length(fields) == count, do: ReQL.with_fields(record, fields)
  defp selectize(record, {:&, _, [index]}, _), do: ReQL.bracket(record, index)
  defp selectize(record, {op, _, [expr]}, q) when op in @aggregators do
    record = selectize(record, expr, q)
    apply(ReQL, op, [record])
  end

  defp selectize(record, field, q) when is_atom(field) do
    groups =
      q.group_bys
      |> Enum.map(& &1.expr)
      |> Enum.map(&List.first/1)
      |> Enum.map(&elem(&1, 0))
      |> Enum.map(&elem(&1, 2))
      |> Enum.map(&List.last/1)

    # If the field is in groups,
    # we get the first element.
    if field in groups do
      record
      |> ReQL.bracket(field)
      |> ReQL.bracket(0)
    else
      ReQL.bracket(record, field)
    end
  end

  defp evaluate({op, _, args}, params, records) do
    args = Enum.map(args, &normalize_arg(&1, params, records))
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
      :is_nil -> apply(ReQL, :eq, args ++ [nil])
      :field  -> apply(ReQL, :bracket, args)
      :like   -> like(args)
      :ilike  -> like(args, true)
      _else when op in @aggregators ->
        {op, args}
    end
  end

  defp evaluate({:set, args}, params, records) do
    Enum.into(args, %{}, fn {key, arg} ->
        {key, normalize_arg(arg, params, records)}
    end)
  end

  defp evaluate({:inc, args}, params, records) do
    Enum.into(args, %{}, fn {key, arg} ->
      {key, ReQL.add(ReQL.bracket(List.first(records), key), normalize_arg(arg, params, records))}
    end)
  end

  defp evaluate({:push, args}, params, records) do
    Enum.into(args, %{}, fn {key, arg} ->
      {key, ReQL.append(ReQL.bracket(List.first(records), key), normalize_arg(arg, params, records))}
    end)
  end

  defp evaluate({:pull, args}, params, records) do
    Enum.into(args, %{}, fn {key, arg} ->
      {key, ReQL.without(ReQL.bracket(List.first(records), key), normalize_arg(arg, params, records))}
    end)
  end

  defp like([field, match], caseless \\ false) do
    regex = Regex.escape(match)
    regex = if String.first(regex) != "%", do: "^" <> regex, else: String.replace_prefix(regex, "%", "")
    regex = if String.last(regex) != "%", do: regex <> "$", else: String.replace_suffix(regex, "%", "")
    regex = if caseless, do: "(?i)" <> regex, else: regex
    apply(ReQL, :match, [field, regex])
  end

  defp pop_aggregator(expr) do
    case expr do
      {op, _, expr} when op in @aggregators ->
        if op == :count && List.last(expr) == :distinct do
          expr = List.delete_at(expr, -1)
          aggr = &ReQL.count(ReQL.distinct(&1))
          {expr, aggr}
        else
          aggr = &apply(ReQL, op, [&1])
          {expr, aggr}
        end
      _ ->
        {expr, nil}
    end
  end

  defp reducers_only?(select) do
    (is_list(select.expr) || elem(select.expr, 0) in [:{}, :%{}]) && Enum.all?(select.fields, & elem(&1, 0) in @aggregators)
  end

  defp normalize_arg(expr, params, records \\ [])
  defp normalize_arg(%Ecto.Query.Tagged{value: arg}, params, records), do: normalize_arg(arg, params, records)
  defp normalize_arg(args, params, records) when is_list(args), do: Enum.map(args, &normalize_arg(&1, params, records))
  defp normalize_arg({:^, _, [index]}, params, _records), do: Enum.at(params, index)
  defp normalize_arg({:^, _, [index, count]}, params, _records), do: Enum.slice(params, index, count)
  defp normalize_arg({:^, _, args}, _params, _records), do: raise "Unsupported pin arguments: #{inspect args}"
  defp normalize_arg({{:., _, [{:&, _, [0]}, field]}, _, _}, _params, []), do: field
  defp normalize_arg({{:., _, [{:&, _, [index]}, field]}, _, _}, _params, %RethinkDB.Q{} = records), do: ReQL.bracket(ReQL.bracket(records, index), field)
  defp normalize_arg({{:., _, [{:&, _, [index]}, field]}, _, _}, _params, records) when is_list(records), do: ReQL.bracket(Enum.at(records, index), field)
  defp normalize_arg({_op, _, _args} = expr, params, records), do: evaluate(expr, params, records)
  defp normalize_arg(expr, _params, _records), do: expr
end
