defmodule RethinkDB.Ecto do
  alias RethinkDB.Ecto.NormalizedQuery

  import RethinkDB.Query

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage

  defmacro __before_compile__(env) do
    pool = Module.concat(env.module, Pool)
    quote do
      def __pool__, do: unquote(pool)

      def run(query, opts \\ []) do
        RethinkDB.Connection.run(query, unquote(pool), opts)
      end

      def noreply_wait(timeout \\ 15_000) do
        RethinkDB.Connection.noreply_wait(unquote(pool), timeout)
      end

      def stop do
        RethinkDB.Connection.stop(unquote(pool))
      end

      defoverridable [__pool__: 0]
    end
  end

  def ensure_all_started(repo, type) do
    {_, opts} = repo.__pool__
    with {:ok, pool} <- DBConnection.ensure_all_started(opts, type),
         {:ok, adapter} <- Application.ensure_all_started(:rethinkdb, type),
         # We always return the adapter to force it to be restarted if necessary
         do: {:ok, pool ++ List.delete(adapter, :rethinkdb) ++ [:rethinkdb]}
  end

  def child_spec(repo, opts) do
    opts = Keyword.put_new(opts, :name, repo.__pool__)
    DBConnection.child_spec(RethinkDB.Connection, opts)
  end

  def autogenerate(:id), do: nil

  def autogenerate(:embed_id), do: Ecto.UUID.generate()

  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  def loaders(:uuid, _type), do: [&Ecto.UUID.dump/1]

  def loaders(:datetime, _type) do
    [fn %RethinkDB.Pseudotypes.Time{epoch_time: timestamp, timezone: _timezone} ->
      secs = trunc(timestamp)
      usec = trunc((timestamp - secs) * 1_000_000)
      {date, {hour, min, sec}} = :calendar.gregorian_seconds_to_datetime(secs + epoch)
      {:ok, Ecto.DateTime.load {date, {hour, min, sec, usec}}}
    end]
  end

  def loaders(_primitive, type), do: [type]

  def dumpers(:uuid, type), do: [type, &Ecto.UUID.load/1]

  def dumpers(:datetime, type) do
    [type, fn {{year, month, day}, {hour, min, sec, usec}} ->
      epoch_time = :calendar.datetime_to_gregorian_seconds({{year, month, day}, {hour, min, sec}}) - epoch
      {:ok, %RethinkDB.Pseudotypes.Time{epoch_time: epoch_time + usec / 1_000, timezone: "+00:00"}}
    end]
  end

  def dumpers(_primitive, type), do: [type]

  def prepare(func, query), do: {:nocache, {func, query}}

  def execute(repo, meta, {_cache, {func, query}}, params, preprocess, _opts) do
    apply(NormalizedQuery, func, [query, params])
    |> run(repo, {func, meta.fields}, preprocess)
  end

  def insert(repo, meta, fields, returning, _opts) do
    returning =
      unless meta.schema.__schema__(:autogenerate_id) do
        returning ++ meta.schema.__schema__(:primary_key)
      else
        returning
      end
    NormalizedQuery.insert(meta, fields)
    |> run(repo, {:insert, fields}, returning)
  end

  def insert_all(repo, meta, _header, fields, returning, _opts) do
    NormalizedQuery.insert_all(meta, fields)
    |> run(repo, {:insert_all, fields}, returning)
  end

  def update(repo, meta, fields, filters, returning, _opts) do
    NormalizedQuery.update(meta, fields, filters)
    |> run(repo, {:update, fields}, returning)
  end

  def delete(repo, meta, filters, _opts) do
    NormalizedQuery.delete(meta, filters)
    |> run(repo, {:delete, []}, [])
  end

  def storage_up(opts) do
    repo = opts[:repo]
    name = opts[:database] || "test"

    case repo.run(db_create(name)) do
      %{data: %{"dbs_created" => 1}} ->
        :ok
      %{data: %{"r" => [error|_]}} ->
        {:error, error}
    end
  end

  def storage_down(opts) do
    repo = opts[:repo]
    name = opts[:database] || "test"

    case repo.run(db_drop(name)) do
      %{data: %{"dbs_dropped" => 1}} ->
        :ok
      %{data: %{"r" => [error|_]}} ->
        {:error, error}
    end
  end

 def execute_ddl(repo, {:create_if_not_exists, %Ecto.Migration.Table{name: name}, _fields}, _opts) do
    table_create(name) |> repo.run
    :ok
  end

  def execute_ddl(repo, {:create, e = %Ecto.Migration.Table{name: name}, _fields}, _opts) do
    options = e.options || %{}
    table_create(name, options)
    |> repo.run
    :ok
  end

  def execute_ddl(repo, {:create, %Ecto.Migration.Index{columns: [column], table: table}}, _opts) do
    table(table)
    |> index_create(column)
    |> repo.run
    :ok
  end

  def execute_ddl(repo, {:drop, %Ecto.Migration.Table{name: name}}, _opts) do
    table_drop(name)
    |> repo.run
    :ok
  end

  def execute_ddl(repo, {:drop, %Ecto.Migration.Index{columns: [column], table: table}}, _opts) do
    table(table)
    |> index_drop(column)
    |> repo.run
    :ok
  end

  def supports_ddl_transaction?, do: false

  defp run(query, repo, {func, fields}, process) do
    case RethinkDB.run(query, repo.__pool__) do
      %{data: %{"r" => [error|_]}} ->
        {:invalid, [error: error]}
      %{data: data} when is_list(data) ->
        {records, count} = Enum.map_reduce(data, 0, &{process_record(&1, process, fields), &2 + 1})
        {count, records}
      %{data: data} ->
        case func do
          :insert_all ->
            {data["inserted"], nil}
          :update_all ->
            {data["replaced"], nil}
          :delete_all ->
            {data["deleted"], nil}
          _func ->
            new_fields = for field <- process, id <- data["generated_keys"], do: {field, id}
            new_fields = Keyword.merge(new_fields, fields)
            {:ok, new_fields}
        end
    end
  end

  defp epoch do
    :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
  end

  defp process_record(record, process, ast) do
    Enum.map(ast, fn
      {:&, _, [_, fields, _]} = expr ->
        data =
          fields
          |> Enum.map(&Atom.to_string/1)
          |> Enum.map(&Map.get(record, &1))
        process.(expr, data, nil)
      expr ->
        process.(expr, record, nil)
    end)
  end
end
