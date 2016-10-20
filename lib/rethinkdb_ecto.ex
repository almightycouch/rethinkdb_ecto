defmodule RethinkDB.Ecto do

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Migration

  alias RethinkDB.Ecto.NormalizedQuery
  alias RethinkDB.Query, as: ReQL

  require Logger

  #
  # Adapter callbacks
  #

  defmacro __before_compile__(env) do
    module = env.module
    config = Module.get_attribute(env.module, :config)
    norm_config = normalize_config(config)

    quote do
      defmodule Connection do
        use RethinkDB.Connection
      end

      defdelegate run(query), to: Connection
      defdelegate run(query, options), to: Connection

      def __connection__, do: unquote(module).Connection
      def __config__, do: unquote(Macro.escape(norm_config))
    end
  end

  def ensure_all_started(_repo, type) do
    Application.ensure_all_started(:rethinkdb_ecto, type)
  end


  def child_spec(repo, _options) do
    import Supervisor.Spec
    worker(repo.__connection__, [repo.__config__])
  end

  def autogenerate(:id), do: nil

  def autogenerate(:embed_id), do: Ecto.UUID.generate()

  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  def loaders(:uuid, _type), do: [&Ecto.UUID.dump/1]

  def loaders(:datetime, _type) do
    [fn %RethinkDB.Pseudotypes.Time{epoch_time: timestamp, timezone: _timezone} ->
      secs = trunc(timestamp)
      usec = trunc((timestamp - secs) * 1_000_000)
      base = :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
      {date, {hour, min, sec}} = :calendar.gregorian_seconds_to_datetime(secs + base)
      {:ok, Ecto.DateTime.load({date, {hour, min, sec, usec}})}
    end]
  end

  def loaders(_primitive, type), do: [type]

  def dumpers(:uuid, type), do: [type, &Ecto.UUID.load/1]

  def dumpers(:datetime, type) do
    [type, fn {{year, month, day}, {hour, min, sec, usec}} ->
      base = :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
      epoch_time = :calendar.datetime_to_gregorian_seconds({{year, month, day}, {hour, min, sec}}) - base
      {:ok, %RethinkDB.Pseudotypes.Time{epoch_time: epoch_time + usec / 1_000, timezone: "+00:00"}}
    end]
  end

  def dumpers(_primitive, type), do: [type]

  def prepare(func, query), do: {:nocache, {func, query}}

  def execute(repo, meta, {_cache, {func, query}}, params, preprocess, _options) do
    NormalizedQuery
    |> apply(func, [query, params])
    |> execute_query(repo, {func, meta.fields}, preprocess)
  end

  def insert(repo, meta, fields, returning, _options) do
    returning =
      unless meta.schema.__schema__(:autogenerate_id) do
        returning ++ meta.schema.__schema__(:primary_key)
      else
        returning
      end

    NormalizedQuery.insert(meta, fields)
    |> execute_query(repo, {:insert, fields}, returning)
  end

  def insert_all(repo, meta, _header, fields, returning, _options) do
    NormalizedQuery.insert_all(meta, fields)
    |> execute_query(repo, {:insert_all, fields}, returning)
  end

  def update(repo, meta, fields, filters, returning, _options) do
    NormalizedQuery.update(meta, fields, filters)
    |> execute_query(repo, {:update, fields}, returning)
  end

  def delete(repo, meta, filters, _options) do
    NormalizedQuery.delete(meta, filters)
    |> execute_query(repo, {:delete, []}, [])
  end


  #
  # Storage callbacks
  #

  def storage_down(options) do
    repo = Keyword.fetch!(options, :repo)
    conf = repo.__config__
    name = Keyword.get(options, :database, conf[:db])

    repo.__connection__.start_link(conf)
    case repo.run(ReQL.db_drop(name)) do
      {:ok, %RethinkDB.Record{data: %{"dbs_dropped" => 1}}} ->
        :ok
      {:error, %RethinkDB.Response{data: %{"r" => [error|_]}}} ->
        raise error
    end
  end

  def storage_up(options) do
    repo = Keyword.fetch!(options, :repo)
    conf = repo.__config__
    name = Keyword.get(options, :database, conf[:db])

    repo.__connection__.start_link(conf)
    case repo.run(ReQL.db_create(name)) do
      {:ok, %RethinkDB.Record{data: %{"dbs_created" => 1}}} ->
        :ok
      {:error, %RethinkDB.Response{data: %{"r" => [error|_]}}} ->
        raise error
    end
  end

  #
  # Migration callbacks
  #

  def execute_ddl(repo, {:create_if_not_exists, %Ecto.Migration.Table{name: name}, _fields}, _options) do
    ReQL.table_create(name)
    |> repo.run()
    :ok
  end

  def execute_ddl(repo, {:create, e = %Ecto.Migration.Table{name: name}, _fields}, _options) do
    options = e.options || %{}
    ReQL.table_create(name, options)
    |> repo.run()
    :ok
  end

  def execute_ddl(repo, {:create, %Ecto.Migration.Index{columns: cols, table: table}}, _options) do
    cols
    |> Enum.reduce(ReQL.table(table), fn col, reql -> ReQL.index_create(reql, col) end)
    |> repo.run()
    :ok
  end

  def execute_ddl(repo, {:drop, %Ecto.Migration.Table{name: name}}, _options) do
    ReQL.table_drop(name)
    |> repo.run()
    :ok
  end

  def execute_ddl(repo, {:drop, %Ecto.Migration.Index{columns: cols, table: table}}, _options) do
    cols
    |> Enum.reduce(ReQL.table(table), fn col, reql -> ReQL.index_drop(reql, col) end)
    |> repo.run()
    :ok
  end

  def supports_ddl_transaction?, do: false

  #
  # Transaction callbacks
  #

  def in_transaction?(_repo), do: false

  def rollback(_repo, _value), do:
    raise BadFunctionError, message: "#{inspect __MODULE__} does not support transactions."

  #
  # Helpers
  #

  defp normalize_config(options) do
    [host: String.to_charlist(Keyword.get(options, :hostname, "localhost")),
     port: Keyword.get(options, :port, 28015),
     db: Keyword.get(options, :database, "test")]
  end

  defp execute_query(query, repo, {func, fields}, process) do
    case RethinkDB.run(query, repo.__connection__) do
      {:ok, %RethinkDB.Collection{data: data}} when is_list(data) ->
        {records, count} = Enum.map_reduce(data, 0, &{process_record(&1, process, fields), &2 + 1})
        {count, records}
      {:ok, %RethinkDB.Record{data: data}} ->
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
      {:error, %RethinkDB.Response{data: %{"r" => [error|_]}}} ->
        raise error
    end
 end

 defp process_record(record, process, ast) do
    Enum.map(ast, fn
      {:&, _, [_, fields, _]} = expr when is_list(fields) ->
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
