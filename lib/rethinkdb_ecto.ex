defmodule RethinkDB.Ecto do
  alias RethinkDB.Ecto.NormalizedQuery
  alias RethinkDB.Pseudotypes.Time
  import RethinkDB.Query
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage

  defmacro __before_compile__(env) do
    module = env.module
    quote do
      defmodule Connection do
        use RethinkDB.Connection
      end

      defdelegate run(query), to: Connection
      defdelegate run(query, opts), to: Connection

      def __connection__, do: unquote(module).Connection
    end
  end

  def start_link(repo, opts) do
    {:ok, _} = Application.ensure_all_started(:rethinkdb)
    repo.__connection__.start_link(opts)
  end

  def stop(repo, _pid, _timeout) do
    repo.__connection__.stop()
  end

  @epoch :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

  defp encode_timestamp({{year, month, day}, {hour, min, sec, usec}})
      when year <= @timestamp_max_year and hour in 0..23 and min in 0..59 and sec in 0..59 and usec in 0..999_999 do
    datetime = {{year, month, day}, {hour, min, sec}}
    secs = :calendar.datetime_to_gregorian_seconds(datetime) - @epoch
    %RethinkDB.Pseudotypes.Time{epoch_time: secs + usec / 1_000, timezone: "+00:00"}
  end

  defp encode_timestamp(datetime = %Ecto.DateTime{}) do
    {:ok, tuple} = Ecto.DateTime.dump(datetime)
    encode_timestamp(tuple)
  end

  def load(:binary_id, data), do: {:ok, data}

  def load(Ecto.DateTime, %RethinkDB.Pseudotypes.Time{epoch_time: timestamp}) do
    secs = trunc(timestamp)
    usec = trunc((timestamp - secs) * 1_000_000)
    {date, {hour, min, sec}} = :calendar.gregorian_seconds_to_datetime(secs + @epoch)
    Ecto.DateTime.load {date, {hour, min, sec, usec}}
  end

  def load(type, data), do: Ecto.Type.load(type, data, &load/2)

  def dump(:binary_id, data), do: {:ok, data}

  def dump(Ecto.DateTime, nil), do: Ecto.Type.dump(Ecto.DateTime, nil, &dump/2)
  def dump(Ecto.DateTime, data), do: {:ok, encode_timestamp(data)}

  def dump(type, data), do: Ecto.Type.dump(type, data, &dump/2)

  def embed_id(_), do: ""

  def prepare(func, query), do: {:nocache, {func, query}}

  def execute(repo, meta, {func, query}, params, preprocess, _opts) do
    fields =
      case meta.select do
        nil -> []
        select -> select.fields
      end
    apply(NormalizedQuery, func, [query, params])
    |> run(repo, {func, fields}, preprocess)
  end

  def insert(repo, meta, fields, autogenerate_id, _returning, _opts) do
    # filter out nil fields and encode timestamps. Tuple isn't a valid type, so tuple can only be timestamp
    fields = fields 
    |> Enum.reduce([], fn {k, v}, acc -> 
      case v do
        %Ecto.Query.Tagged{value: nil} -> acc
        {{year, month, day}, {hour, minute, sec, usec}} -> 
          [{k, encode_timestamp({{year, month, day}, {hour, minute, sec, usec}})}| acc]
        _ -> [{k, v}| acc]
      end
    end)
    NormalizedQuery.insert(meta, fields)
    |> run(repo, {:insert, fields}, autogenerate_id)
  end

  def update(repo, meta, fields, filters, autogenerate_id, _returning, _opts) do
    fields = fields
    |> Enum.reduce([], fn {k, v}, acc ->
      case v do
        {{year, month, day}, {hour, minute, sec, usec}} -> 
          [{k, encode_timestamp({{year, month, day}, {hour, minute, sec, usec}})}| acc]
        _ -> [{k, v}| acc]
      end
    end)
    NormalizedQuery.update(meta, fields, filters)
    |> run(repo, {:update, fields}, autogenerate_id)
  end

  def delete(repo, meta, filters, autogenerate_id, _opts) do
    NormalizedQuery.delete(meta, filters)
    |> run(repo, {:delete, []}, autogenerate_id)
  end

  def storage_up(opts) do
    repo = opts[:repo]
    name = opts[:database]
    start_link(repo, [])

    case(RethinkDB.Query.db_create(name) |> repo.run) do
      %{data: %{"r" => [error|_]}} ->
        raise error
      %{data: %{"dbs_created" => 1}} ->
        :ok
    end
  end

  def storage_down(opts) do
    repo = opts[:repo]
    name = opts[:database]
    start_link(repo, [])

    case(RethinkDB.Query.db_drop(name) |> repo.run) do
      %{data: %{"r" => [error|_]}} ->
        raise error
      %{data: %{"dbs_dropped" => 1}} ->
        :ok
    end
  end

  def execute_ddl(repo, {:create_if_not_exists, %Ecto.Migration.Table{name: name}, _fields}, _opts) do
    table_create(name) |> repo.run
    :ok
  end

  def execute_ddl(repo, {:create, e = %Ecto.Migration.Table{name: name}, fields}, opts) do
    options = e.options || %{}
    database = e.prefix || repo.config[:database]
    table_create(name, options) |> repo.run
    :ok
  end

  def execute_ddl(repo, {:create, %Ecto.Migration.Index{columns: [column], table: table}}, _opts) do
    table(table) |> index_create(column) |> repo.run
    :ok
  end

  def execute_ddl(repo, {:drop, e = %Ecto.Migration.Table{name: name}}, opts) do
    table_drop(name) |> repo.run
    :ok
  end

  def execute_ddl(repo, {:drop, %Ecto.Migration.Index{columns: [column], table: table}}, _opts) do
    table(table) |> index_drop(column) |> repo.run
    :ok
  end

  def supports_ddl_transaction?, do: false

  defp run(query, repo, {func, fields}, preprocess) when is_function(preprocess) do
    case repo.run(query) do
      %{data: %{"r" => [error|_]}} ->
        raise error
      %{data: data} when is_list(data) ->
        {records, count} = Enum.map_reduce(data, 0, &{process_record(&1, preprocess, fields), &2 + 1})
        {count, records}
      %{data: data} ->
        {1, [[data]]}
    end
  end

  defp run(query, repo, {func, fields}, autogenerate_id) do
    case repo.run(query) do
      %{data: %{"r" => [error|_]}} ->
        {:invalid, [error: error]}
      %{data: %{"first_error" => error}} ->
        {:invalid, [error: error]}
      %{data: %{"generated_keys" => [id|_]} = data} ->
        if autogenerate_id do
          {:ok, Keyword.put(fields, elem(autogenerate_id, 0), id)}
        else
          {:ok, fields}
        end
      %{data: data} ->
        case func do
          :update_all ->
            {data["replaced"], nil}
          :delete_all ->
            {data["deleted"], nil}
          _ ->
            {:ok, fields}
        end
    end
  end

  defp process_record(record, preprocess, args) when is_list(record) do
    Enum.map_reduce(record, args, fn record, [expr|exprs] ->
      {preprocess.(expr, record, nil), exprs}
    end) |> elem(0)
  end

  defp process_record(record, preprocess, expr) do
    Enum.map(expr, &preprocess.(&1, record, nil))
  end
end
